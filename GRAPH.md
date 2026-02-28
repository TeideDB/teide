# Graph Engine

Graph storage, traversal, and query processing as native libteide primitives. CSR edge indices alongside columnar tables. Graph opcodes alongside OP_JOIN and OP_GROUP. Factorized execution, worst-case optimal joins, sideways information passing — all in the same morsel-driven pipeline.

No query language. No SQL. No Cypher. Pure C17 pipeline primitives that a planner (teide-rs or anything else) composes into DAGs via the existing `td_graph_new()` → `td_op_t*` → `td_execute()` API.

## Status

Fully implemented and audit-hardened (174 tests, 5 rounds of deep review).

---

## 1. CSR Storage

### 1.1 Data structures

```c
/* Compressed Sparse Row edge index.
 *
 * offsets[i]..offsets[i+1] gives the range in targets[] for node i's neighbors.
 * Stored as td_t I64 vectors — same allocator, mmap, COW as everything else.
 *
 * If sorted == true, targets within each adjacency list are sorted ascending.
 * Required for OP_WCO_JOIN (Leapfrog Triejoin).
 */
typedef struct td_csr {
    td_t*    offsets;      /* I64 vec, length = n_nodes + 1                 */
    td_t*    targets;      /* I64 vec, length = n_edges                     */
    td_t*    rowmap;       /* I64 vec, length = n_edges (CSR pos → prop row)*/
    td_t*    props;        /* optional edge property table (td_t TD_TABLE)  */
    int64_t  n_nodes;
    int64_t  n_edges;
    bool     sorted;       /* targets sorted per adjacency list             */
} td_csr_t;

/* Relationship: double-indexed CSR (forward + reverse).
 *
 * from_table/to_table are opaque IDs assigned by the caller (planner).
 * libteide does not manage a table registry — it just stores the IDs
 * so the caller can identify which tables this rel connects.
 */
typedef struct td_rel {
    uint16_t    from_table;
    uint16_t    to_table;
    int64_t     name_sym;     /* relationship name as symbol ID */
    td_csr_t    fwd;          /* src → dst */
    td_csr_t    rev;          /* dst → src */
} td_rel_t;
```

### 1.2 On-disk format

CSR indices are stored as column files — the same `td_col_save` / `td_col_mmap` used for splayed tables:

```
<dir>/
  fwd_offsets.col        td_col_save(rel->fwd.offsets, path)
  fwd_targets.col        td_col_save(rel->fwd.targets, path)
  fwd_rowmap.col         (if props exist)
  rev_offsets.col
  rev_targets.col
  rev_rowmap.col
  props/                 optional edge property table (td_splay_save)
    weight.col
    created_at.col
```

No new storage format. No new serialization. Just I64 column files with specific semantics.

### 1.3 CSR construction

```c
/* Build a relationship (double CSR) from a source table's FK column.
 *
 * from_table:     the source node table (td_t TD_TABLE)
 * fk_col:         name of the FK column in from_table
 * n_target_nodes: number of nodes in the target table (for reverse CSR sizing)
 * sort_targets:   if true, sort targets within each adjacency list (needed for LFTJ)
 *
 * Returns: heap-allocated td_rel_t (caller frees with td_rel_free)
 *
 * Algorithm:
 *   1. Extract FK column from from_table
 *   2. Build (src_offset, target_offset) pairs
 *   3. Sort by src_offset          → forward offsets + targets (radix or existing sort)
 *   4. Sort by target_offset       → reverse offsets + targets
 *   5. If sort_targets: sort targets within each adjacency list
 *   6. Compute rowmap for property lookup
 *
 * Uses: td_alloc for all vectors, td_sort primitives for sorting.
 */
td_rel_t* td_rel_build(td_t* from_table, const char* fk_col,
                        int64_t n_target_nodes, bool sort_targets);

/* Build from explicit edge table with (src, dst, prop...) columns. */
td_rel_t* td_rel_from_edges(td_t* edge_table,
                             const char* src_col, const char* dst_col,
                             int64_t n_src_nodes, int64_t n_dst_nodes,
                             bool sort_targets);

/* Persistence */
td_err_t  td_rel_save(td_rel_t* rel, const char* dir);
td_rel_t* td_rel_load(const char* dir);
td_rel_t* td_rel_mmap(const char* dir);    /* zero-copy via td_col_mmap */
void      td_rel_free(td_rel_t* rel);
```

### 1.4 Inline neighbor access

```c
/* O(1) neighbor range lookup — the core primitive everything else builds on. */
static inline int64_t td_csr_degree(td_csr_t* csr, int64_t node) {
    if (!csr || !csr->offsets || node < 0 || node >= csr->n_nodes) return 0;
    int64_t* o = (int64_t*)td_data(csr->offsets);
    return o[node + 1] - o[node];
}

static inline int64_t* td_csr_neighbors(td_csr_t* csr, int64_t node, int64_t* out_count) {
    if (!csr || !csr->offsets || !csr->targets || node < 0 || node >= csr->n_nodes) {
        if (out_count) *out_count = 0; return NULL;
    }
    int64_t* o = (int64_t*)td_data(csr->offsets);
    int64_t* t = (int64_t*)td_data(csr->targets);
    *out_count = o[node + 1] - o[node];
    return &t[o[node]];
}
```

---

## 2. Graph Opcodes

### 2.1 New opcodes

```c
#define OP_EXPAND        80   /* 1-hop CSR neighbor expansion       */
#define OP_VAR_EXPAND    81   /* variable-length BFS/DFS            */
#define OP_SHORTEST_PATH 82   /* BFS shortest path                  */
#define OP_WCO_JOIN      83   /* worst-case optimal join (LFTJ)     */
```

### 2.2 Extended op node

```c
/* Union member for graph ops — added to td_op_ext_t's union. */
struct {
    td_rel_t* rel;            /* relationship to traverse          */
    uint8_t   direction;      /* 0=fwd, 1=rev, 2=both             */
    uint8_t   min_depth;      /* VAR_EXPAND: min path length       */
    uint8_t   max_depth;      /* VAR_EXPAND: max path length       */
    uint8_t   path_tracking;  /* 1 = emit full path column         */
} graph;

/* For OP_WCO_JOIN: */
struct {
    td_rel_t** rels;          /* array of relationships             */
    uint8_t    n_rels;        /* number of relationships            */
    uint8_t    n_vars;        /* number of pattern variables        */
    /* Variable binding order and rel-to-var mapping stored in trailing bytes */
} wco;
```

These go inside the existing `td_op_ext_t` union, same as `sort`, `join`, `window`.

### 2.3 DAG construction API

Same pattern as `td_join`, `td_filter`, `td_group`, etc. in `graph.c`:

```c
/* 1-hop expansion.
 *
 * src_nodes: upstream op producing I64 vector of source node offsets
 * rel:       relationship to traverse
 * direction: 0=forward (src→dst), 1=reverse (dst→src), 2=both
 *
 * Output type: TD_TABLE with columns (_src I64, _dst I64)
 * Downstream ops can OP_SCAN("_src") or OP_SCAN("_dst") from the result.
 */
td_op_t* td_expand(td_graph_t* g, td_op_t* src_nodes,
                    td_rel_t* rel, uint8_t direction);

/* Variable-length path expansion (BFS).
 *
 * start_nodes: upstream op producing I64 vector of start offsets
 * min/max_depth: hop range [min..max], both inclusive
 *
 * Output: TD_TABLE with (_start I64, _end I64, _depth I64 [, _path LIST])
 */
td_op_t* td_var_expand(td_graph_t* g, td_op_t* start_nodes,
                        td_rel_t* rel, uint8_t direction,
                        uint8_t min_depth, uint8_t max_depth,
                        bool track_path);

/* Shortest path (BFS, unweighted).
 *
 * src/dst: upstream ops producing single-element I64 vectors (or scalars)
 * max_depth: search limit
 *
 * Output: TD_TABLE representing the path (_node I64, _depth I64)
 *         or TD_ERR_PTR(TD_ERR_RANGE) if no path within max_depth
 */
td_op_t* td_shortest_path(td_graph_t* g, td_op_t* src, td_op_t* dst,
                           td_rel_t* rel, uint8_t max_depth);

/* Worst-case optimal join (Leapfrog Triejoin) for cyclic patterns.
 *
 * rels:    array of relationships in the pattern
 * n_rels:  number of relationships
 * n_vars:  number of pattern variables
 *
 * Variable-to-relationship mapping encoded in trailing bytes.
 * Requires sorted CSR (rel->fwd.sorted == true).
 *
 * Output: TD_TABLE with one column per pattern variable (_v0, _v1, ..., _vN)
 */
td_op_t* td_wco_join(td_graph_t* g,
                      td_rel_t** rels, uint8_t n_rels,
                      uint8_t n_vars);
```

### 2.4 Multi-table graph DAG

Current `td_graph_t` binds to a single `td_t* table`. Graph queries span multiple tables (expand from tasks to persons). Two approaches:

**Option A: OP_CONST table injection.** The caller passes each table as `td_const_table(g, persons_table)`, and OP_EXPAND outputs reference the appropriate table. The graph binds to the primary table; secondary tables are injected as constants.

**Option B: Extend td_graph_t with a table registry.** Add `td_t** tables` + `uint16_t n_tables` to `td_graph_t`. OP_SCAN takes a table index. This is cleaner for multi-table graphs but changes the `td_graph_t` struct.

Recommendation: **Option B** — the existing single-table `g->table` is already limiting for multi-table JOINs. Adding a table registry benefits both relational and graph workloads.

```c
/* Extended td_graph_t (backward compatible — single table still works) */
typedef struct td_graph {
    td_op_t*       nodes;
    uint32_t       node_count;
    uint32_t       node_cap;
    td_t*          table;          /* primary table (legacy, still works) */
    td_t**         tables;         /* table registry (indexed by table_id) */
    uint16_t       n_tables;       /* number of registered tables */
    td_op_ext_t**  ext_nodes;
    uint32_t       ext_count;
    uint32_t       ext_cap;
    td_t*          selection;
} td_graph_t;

/* Register a table. Returns table_id (0-based index). */
uint16_t td_graph_add_table(td_graph_t* g, td_t* table);

/* OP_SCAN from a specific table (not just g->table) */
td_op_t* td_scan_table(td_graph_t* g, uint16_t table_id, const char* col_name);
```

---

## 3. Execution

### 3.1 exec_expand (OP_EXPAND)

Same two-pass count-then-fill pattern as `exec_join`:

```
Phase 1: count output pairs
  For each source node in input morsel:
    pairs += offsets[node+1] - offsets[node]

Phase 2: prefix-sum across morsels → global output offsets

Phase 3: fill
  For each source node:
    copy targets[offsets[node]..offsets[node+1]] into output
    write source node ID alongside each target

Output: td_t table with columns _src (I64) and _dst (I64)

Parallel: dispatch source node morsels via td_pool_dispatch.
Each morsel produces an independent chunk. Merge via prefix-sum offsets.
```

When `direction == 2` (both): run forward expansion, then reverse expansion, concatenate results.

### 3.2 exec_var_expand (OP_VAR_EXPAND)

Iterative BFS with depth limit and cycle detection:

```
frontier = input start nodes (I64 vector)
visited  = td_sel_new(n_total_nodes)    ← reuse TD_SEL bitmap

for depth in 1..max_depth:
    next_frontier = empty I64 vector
    for each node in frontier:
        neighbors = td_csr_neighbors(csr, node, &count)
        for each neighbor:
            if not TD_SEL_BIT_TEST(visited, neighbor):
                TD_SEL_BIT_SET(visited, neighbor)
                append to next_frontier
                if depth >= min_depth:
                    emit (start_node, neighbor, depth)
    frontier = next_frontier
    if frontier is empty: break
```

Cycle detection uses `TD_SEL` bitmaps (already exists: `td_sel_new`, `TD_SEL_BIT_TEST`, `TD_SEL_BIT_SET`). Segment flags (`TD_SEL_NONE`/`TD_SEL_ALL`/`TD_SEL_MIX`) enable morsel-level skip of already-visited regions.

Path tracking (when `path_tracking == 1`): maintain a `td_t LIST` per traversal with the sequence of node IDs. Emitted as an additional `_path` column.

### 3.3 exec_shortest_path (OP_SHORTEST_PATH)

BFS from src, terminate on first hit of dst:

```
src_node = resolve from src op (single I64)
dst_node = resolve from dst op (single I64)

BFS with parent tracking:
  parent[node] = -1 for all
  parent[src] = src
  queue = [src]

  while queue not empty and depth <= max_depth:
    for node in queue:
      if node == dst: reconstruct path from parent[], return
      for neighbor in csr_neighbors(node):
        if parent[neighbor] == -1:
          parent[neighbor] = node
          next_queue.append(neighbor)
    queue = next_queue; depth++

Output: td_t table (_node I64, _depth I64) representing the path
```

Parent array allocated via `td_alloc` (buddy allocator), freed after path reconstruction.

### 3.4 exec_wco_join (OP_WCO_JOIN) — Leapfrog Triejoin

For cyclic patterns (triangles, k-cliques). Requires sorted CSR.

#### Iterator

```c
/* Trie iterator over sorted CSR adjacency list */
typedef struct td_lftj_iter {
    int64_t* targets;        /* pointer into CSR targets data */
    int64_t  start;          /* current range start */
    int64_t  end;            /* current range end */
    int64_t  pos;            /* current position in [start, end) */
} td_lftj_iter_t;

/* O(1) */
static inline int64_t  lftj_key(td_lftj_iter_t* it)    { return it->targets[it->pos]; }
static inline bool     lftj_at_end(td_lftj_iter_t* it)  { return !it->targets || it->pos >= it->end; }
static inline void     lftj_next(td_lftj_iter_t* it)    { it->pos++; }

/* O(log degree) — binary search within [pos, end) */
static inline void lftj_seek(td_lftj_iter_t* it, int64_t v) {
    int64_t lo = it->pos, hi = it->end;
    while (lo < hi) {
        int64_t mid = lo + (hi - lo) / 2;
        if (it->targets[mid] < v) lo = mid + 1;
        else hi = mid;
    }
    it->pos = lo;
}

/* Open trie level: set iterator to a node's adjacency list */
static inline void lftj_open(td_lftj_iter_t* it, td_csr_t* csr, int64_t parent) {
    if (!csr || !csr->offsets || !csr->targets
        || parent < 0 || parent >= csr->n_nodes) {
        it->targets = NULL; it->start = 0; it->end = 0; it->pos = 0;
        return;
    }
    int64_t* o = (int64_t*)td_data(csr->offsets);
    it->targets = (int64_t*)td_data(csr->targets);
    it->start = o[parent];
    it->end   = o[parent + 1];
    it->pos   = it->start;
}
```

#### Leapfrog search

```c
/* Intersect k sorted iterators. Returns true + sets *out if intersection found. */
bool leapfrog_search(td_lftj_iter_t** iters, int k, int64_t* out) {
    if (k <= 0) return false;

    /* Check for any exhausted iterator */
    for (int i = 0; i < k; i++)
        if (lftj_at_end(iters[i])) return false;

    /* Find initial max */
    int max_idx = 0;
    for (int i = 1; i < k; i++)
        if (lftj_key(iters[i]) > lftj_key(iters[max_idx])) max_idx = i;

    for (;;) {
        int64_t max_val = lftj_key(iters[max_idx]);
        int next = (max_idx + 1) % k;

        lftj_seek(iters[next], max_val);
        if (lftj_at_end(iters[next])) return false;

        if (lftj_key(iters[next]) == max_val) {
            /* Check all iterators agree */
            bool all_equal = true;
            for (int i = 0; i < k; i++) {
                if (lftj_key(iters[i]) != max_val) { all_equal = false; break; }
            }
            if (all_equal) { *out = max_val; return true; }
        }
        max_idx = next;
    }
}
```

#### Backtracking enumeration

```
For a triangle (a)-[r1]->(b)-[r2]->(c)-[r3]->(a):
  Variables: [a, b, c]
  Variable a: iterators from r1.fwd (a→b) and r3.rev (a←c)
  Variable b: iterators from r1.rev (constrained by bound a) and r2.fwd (b→c)
  Variable c: iterators from r2.rev (constrained by bound b) and r3.fwd (c→a)

  enum(depth=0):
    leapfrog_search over a-iterators → bind a
    for each a:
      open b-iterators at bound a
      enum(depth=1):
        leapfrog_search over b-iterators → bind b
        for each b:
          open c-iterators at bound b
          enum(depth=2):
            leapfrog_search over c-iterators → bind c
            for each c: emit (a, b, c)
```

Output: `td_t` table with columns `_v0`, `_v1`, ..., `_v{n_vars-1}` (I64 vectors).

The planner (teide-rs) is responsible for determining variable ordering and mapping relationships to variable pairs. libteide receives the pre-computed binding plan.

---

## 4. Factorized Vectors

### 4.1 Motivation

When OP_EXPAND produces many-to-many results (one source node → many targets), the standard approach materializes all (src, dst) pairs. Downstream GROUP BY then scans the full flat result.

Factorized execution avoids this: the source node stays "flat" (single value) while targets are "unflat" (full vector). Downstream aggregation multiplies by cardinality instead of iterating.

### 4.2 Data structure

```c
/* Factorization state — pipeline concept, NOT added to td_t.
 *
 * Lives in the pipeline context (td_pipe_t or equivalent).
 * td_t itself remains unchanged.
 */
typedef struct td_fvec {
    td_t*    vec;            /* underlying td_t vector (I64, SYM, etc.) */
    int64_t  cur_idx;        /* >= 0: flat (single value at index)      */
                             /* -1: unflat (full vector is active)      */
    int64_t  cardinality;    /* for flat: how many rows this represents */
} td_fvec_t;

/* Factorized Table — accumulation buffer for ASP-Join */
typedef struct td_ftable {
    td_fvec_t*  columns;     /* array of factorized vectors   */
    uint16_t    n_cols;
    int64_t     n_tuples;    /* factorized tuple count        */
    td_t*       semijoin;    /* TD_SEL bitmap of qualifying keys */
} td_ftable_t;
```

### 4.3 Factorized OP_EXPAND

When the executor detects that results will feed into aggregation, it can emit factorized output:

```
Standard OP_EXPAND:
  input: [node_5, node_8]
  output: [(5,2), (5,7), (5,9), (8,1), (8,4)]   ← 5 flat rows

Factorized OP_EXPAND:
  input: [node_5, node_8]
  output:
    _src: flat (cur_idx=0, value=5), cardinality=3
    _dst: unflat (cur_idx=-1, vec=[2,7,9])
  then:
    _src: flat (cur_idx=0, value=8), cardinality=2
    _dst: unflat (cur_idx=-1, vec=[1,4])
```

Downstream OP_GROUP + OP_COUNT:
- _src is flat → it's the group key
- _dst is unflat → count = cardinality = vec.len

No iteration over the unflat vector for COUNT. For SUM/AVG, the unflat vector must be scanned, but only once per flat group.

### 4.4 When to factorize

Factorization is beneficial when:
- OP_EXPAND is followed by aggregation (GROUP BY on the source side)
- The expansion has high fan-out (many targets per source)
- The downstream pipeline doesn't need the flat cross-product

The optimizer (or executor) decides based on the DAG shape. Simple heuristic: if OP_EXPAND feeds directly into OP_GROUP and the group key is the source column, use factorized mode.

---

## 5. ASP-Join

### 5.1 S-Join (simple case — flat keys)

Enhancement to existing `exec_join`: extract a semijoin filter during hash-build, pass it to the scan side.

```
Current exec_join:
  Phase 1: build hash table on right side
  Phase 2: probe from left side

S-Join addition:
  Phase 1: build hash table + extract qualifying key set → TD_SEL
  Phase 1.5: pass TD_SEL to left-side scan (skip non-matching source rows)
  Phase 2: probe from filtered left side
```

This is a minimal change to `exec_join` — add semijoin filter extraction during the build phase, attach as selection to the probe side's OP_SCAN.

### 5.2 ASP-Join (unflat keys)

When join keys are unflat (from factorized OP_EXPAND output):

```
Pipeline 1 — Accumulate:
  Collect factorized tuples into td_ftable_t
  Extract semijoin filter: hash set of all key values appearing in unflat vectors
  Pass filter to pipeline 2

Pipeline 2 — Semijoin-filtered build:
  Scan right table, skip rows not in semijoin filter (via TD_SEL)
  Build hash table from filtered rows

Pipeline 3 — Probe:
  Re-scan td_ftable_t
  For each factorized tuple:
    if flat key: single probe
    if unflat key: iterate key vector, probe each
  Emit joined factorized results
```

### 5.3 Join selection

The planner (teide-rs) decides which join algorithm to use:

```
Pattern                  Keys      Algorithm       Engine opcode
─────────────────────    ────      ─────────       ─────────────
Standard FK join         flat      S-Join          OP_JOIN (enhanced)
Post-expand join         unflat    ASP-Join        OP_JOIN (factorized mode)
Cyclic pattern           N/A       LFTJ            OP_WCO_JOIN
```

The decision is communicated via flags on the OP_JOIN ext node or via separate opcodes. libteide executes; it does not decide which algorithm to use.

---

## 6. SIP (Sideways Information Passing)

### 6.1 New optimizer pass

Added to `td_optimize` as a new pass, running after predicate pushdown:

```
  1. Type inference
  2. Constant folding
  3. SIP (sideways information passing)
  4. Factorize (OP_EXPAND → OP_GROUP optimization)
  5. Fusion
  6. DCE
```

### 6.2 Algorithm

Bottom-up traversal of the DAG. For each OP_EXPAND node:

```
sip_pass(td_graph_t* g, td_op_t* root):
  walk DAG bottom-up
  for each OP_EXPAND node:
    // 1. Find downstream filters on the target side
    target_filter = find_downstream_filter(expand_node)
    if no target_filter: continue

    // 2. Evaluate filter against target table → TD_SEL
    target_sel = evaluate_filter_to_sel(target_filter, target_table)

    // 3. Reverse-CSR: mark source nodes that have any passing target
    source_sel = td_sel_new(source_table_nrows)
    for each target in target_sel:
      for each source in reverse_csr_neighbors(target):
        TD_SEL_BIT_SET(source_sel, source)
    td_sel_recompute(source_sel)  // rebuild segment flags

    // 4. Attach source_sel to upstream of OP_EXPAND
    g->selection = source_sel  // or attach to specific OP_SCAN
```

### 6.3 Chained SIP

For multi-hop patterns:

```
(a) -[r1]-> (b) -[r2]-> (c) WHERE c.x = 5

SIP pass processes bottom-up:
  1. Filter on c → TD_SEL on c
  2. OP_EXPAND(r2): reverse-CSR with c_sel → TD_SEL on b
  3. OP_EXPAND(r1): reverse-CSR with b_sel → TD_SEL on a

Each OP_EXPAND gets a source-side TD_SEL that prunes before expansion.
```

### 6.4 Morsel-level skip

The generated TD_SEL bitmaps have segment flags:

```
TD_SEL_NONE  — no source nodes in this morsel have qualifying targets
               → skip entire morsel (1024 nodes), zero CSR lookups
TD_SEL_ALL   — all source nodes qualify → expand without per-row check
TD_SEL_MIX   — mixed → check bitmap per row before CSR lookup
```

This is the same segment-skip mechanism already used for OP_FILTER. No new infrastructure.

---

## 7. File Layout

```
src/store/csr.h          Type declarations, inline accessors (td_csr_t, td_rel_t)
src/store/csr.c          CSR build, save, load, mmap, free

src/ops/lftj.h           td_lftj_iter_t, leapfrog_search
src/ops/lftj.c           Leapfrog Triejoin iterator + search + enumeration

src/ops/fvec.h           td_fvec_t / td_ftable_t type declarations
src/ops/fvec.c           Factorized vector operations

include/teide/td.h       Opcodes (OP_EXPAND etc.), types, DAG API, td_graph_t

src/ops/graph.c          DAG construction (td_expand, td_var_expand, td_shortest_path, td_wco_join)
src/ops/exec.c           Executor (exec_expand, exec_var_expand, exec_shortest_path, exec_wco_join,
                         factorized OP_EXPAND, ASP-Join)
src/ops/opt.c            Optimizer passes (type inference, SIP, factorize, fusion, DCE)

test/test_csr.c          Graph engine tests (42 tests)
```

---

## 8. Conventions

All new code follows existing libteide conventions:

- **Prefix**: `td_csr_*`, `td_rel_*`, `td_fvec_*`, `lftj_*` (static internal)
- **Constants**: `TD_UPPER_SNAKE_CASE` (`OP_EXPAND`, `TD_CSR_SORTED`)
- **Types**: `td_csr_t`, `td_rel_t`, `td_fvec_t`, `td_ftable_t`, `td_lftj_iter_t`
- **Memory**: `td_alloc` / `td_free` for all vectors. `td_sys_alloc` / `td_sys_free` for structs. Never `malloc`.
- **Morsel processing**: all vector loops via `td_morsel_t` (1024 elements)
- **Error returns**: `td_t*` functions: `TD_ERR_PTR()` / `TD_IS_ERR()`. Others: `td_err_t`.
- **Parallel**: `td_pool_dispatch` for morsels exceeding `TD_PARALLEL_THRESHOLD`
- **No external deps**: pure C17. No includes beyond `<stdint.h>`, `<stdbool.h>`, `<stddef.h>`, `<string.h>`, `<stdatomic.h>`.

---

## 9. Testing

42 graph-specific tests in `test/test_csr.c`:
- CSR construction: build, degree, neighbors, sorted targets
- Persistence: save/load/mmap, reverse CSR round-trip, offsets consistency
- Graph execution: expand (fwd/rev/both), var_expand, shortest_path, factorized expand
- LFTJ: triangle detection, 4-clique enumeration, empty graph, plan builder
- Factorized vectors: materialize, empty, semijoin cleanup
- Edge cases: self-loops, disconnected graphs, bad depth ranges

Run: `./build/test_teide --suite /csr`

---

## 10. References

- [Kùzu CIDR 2023](https://www.cidrdb.org/cidr2023/papers/p48-jin.pdf) — ASP-Join, factorized processing, SIP
- [Leapfrog Triejoin](https://arxiv.org/abs/1210.0481) — Veldhuizen 2014, WCO join algorithm
- [Kùzu GitHub](https://github.com/kuzudb/kuzu) — reference C++ implementation
- [Kùzu Internals](https://critical27.github.io/%E8%AE%BA%E6%96%87/Kuzu-Graph-Database-Management-System/) — ASP-Join pipeline details
- [Leapfrog Triejoin Details](https://www.emergentmind.com/topics/leapfrog-triejoin) — algorithm walkthrough
