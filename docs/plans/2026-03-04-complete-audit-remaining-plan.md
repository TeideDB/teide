# Complete Remaining Audit Phases — Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Finish the completeness audit: implement ASOF window join executor, add string/list/table column serialization, and add test coverage for 5 untested modules.

**Architecture:** The ASOF window join DAG builder already exists (`td_window_join` in graph.c:747) but discards its parameters and has no executor. We fix the DAG builder to store time/sym keys and window bounds, then implement `exec_window_join` in exec.c. For serialization, we extend `col.c` with a variable-length format for strings and recursive format for lists/tables. For minor tests, we add test files for meta, types, platform, sys, and pipe (`plan.c` is an empty stub — nothing to test).

**Tech Stack:** Pure C17, munit test framework, Teide buddy allocator (`td_alloc`/`td_free`), no external dependencies.

---

## Task 1: Add `window_join` ext union member to `td_op_ext_t` [x]

**Files:**
- Modify: `include/teide/td.h:467-472`

**Step 1: Add the window_join struct to td_op_ext_t union**

Currently the `join` struct (td.h:467-472) is shared between OP_JOIN and OP_WINDOW_JOIN. Add a dedicated `window_join` member after the `join` struct (after line 472):

```c
        struct {               /* OP_WINDOW_JOIN: ASOF window join */
            td_op_t*   time_key;      /* time/ordered key column */
            td_op_t*   sym_key;       /* optional symbol key (NULL = no partition) */
            int64_t    window_lo;     /* lower bound of time window */
            int64_t    window_hi;     /* upper bound of time window */
            uint16_t*  agg_ops;       /* aggregation opcodes */
            td_op_t**  agg_inputs;    /* aggregation input columns */
            uint8_t    n_aggs;        /* number of aggregations */
        } wjoin;
```

**Step 2: Verify build**

Run: `cmake --build build 2>&1`
Expected: Clean build, zero warnings.

**Step 3: Commit**

```bash
git add include/teide/td.h
git commit -m "feat: add window_join ext union member to td_op_ext_t"
```

---

## Task 2: Fix `td_window_join` DAG builder to store parameters [x]

**Files:**
- Modify: `src/ops/graph.c:747-786`

**Step 1: Rewrite td_window_join to use the wjoin ext member**

Replace the current implementation (graph.c:747-786) which discards `time_key`, `sym_key`, `window_lo`, `window_hi`. The new version stores all parameters in `ext->wjoin`:

```c
td_op_t* td_window_join(td_graph_t* g,
                         td_op_t* left_table, td_op_t* right_table,
                         td_op_t* time_key, td_op_t* sym_key,
                         int64_t window_lo, int64_t window_hi,
                         uint16_t* agg_ops, td_op_t** agg_ins,
                         uint8_t n_aggs) {
    uint32_t left_id  = left_table->id;
    uint32_t right_id = right_table->id;
    uint32_t time_id  = time_key->id;
    uint32_t sym_id   = sym_key ? sym_key->id : UINT32_MAX;
    uint32_t agg_ids[256];
    for (uint8_t i = 0; i < n_aggs; i++) agg_ids[i] = agg_ins[i]->id;

    /* Trailing layout: [agg_ops: n_aggs*2B] [agg_inputs: n_aggs*ptr] */
    size_t ops_sz = (size_t)n_aggs * sizeof(uint16_t);
    size_t ins_sz = (size_t)n_aggs * sizeof(td_op_t*);
    td_op_ext_t* ext = graph_alloc_ext_node_ex(g, ops_sz + ins_sz);
    if (!ext) return NULL;

    left_table  = &g->nodes[left_id];
    right_table = &g->nodes[right_id];

    ext->base.opcode  = OP_WINDOW_JOIN;
    ext->base.arity   = 2;
    ext->base.inputs[0] = left_table;
    ext->base.inputs[1] = right_table;
    ext->base.out_type = TD_TABLE;
    ext->base.est_rows = left_table->est_rows;

    char* trail = EXT_TRAIL(ext);
    ext->wjoin.agg_ops    = (uint16_t*)trail;
    ext->wjoin.agg_inputs = (td_op_t**)(trail + ops_sz);
    memcpy(ext->wjoin.agg_ops, agg_ops, ops_sz);
    for (uint8_t i = 0; i < n_aggs; i++)
        ext->wjoin.agg_inputs[i] = &g->nodes[agg_ids[i]];

    ext->wjoin.time_key  = &g->nodes[time_id];
    ext->wjoin.sym_key   = (sym_id != UINT32_MAX) ? &g->nodes[sym_id] : NULL;
    ext->wjoin.window_lo = window_lo;
    ext->wjoin.window_hi = window_hi;
    ext->wjoin.n_aggs    = n_aggs;

    g->nodes[ext->base.id] = ext->base;
    return &g->nodes[ext->base.id];
}
```

**Step 2: Update `graph_fixup_ext_ptrs` for OP_WINDOW_JOIN**

In graph.c, find the `case OP_WINDOW_JOIN:` inside `graph_fixup_ext_ptrs` (around line 59). Update it to fix up `wjoin` pointers instead of `join` pointers:

```c
case OP_WINDOW_JOIN:
    ext->wjoin.time_key = &g->nodes[ext->wjoin.time_key->id];
    if (ext->wjoin.sym_key)
        ext->wjoin.sym_key = &g->nodes[ext->wjoin.sym_key->id];
    for (uint8_t k = 0; k < ext->wjoin.n_aggs; k++)
        ext->wjoin.agg_inputs[k] = &g->nodes[ext->wjoin.agg_inputs[k]->id];
    break;
```

**Step 3: Update opt.c traversal switches for OP_WINDOW_JOIN**

In `src/ops/opt.c`, find the 4 locations where `case OP_WINDOW_JOIN:` appears alongside `case OP_JOIN:`. At each location, separate `OP_WINDOW_JOIN` from `OP_JOIN` and push `wjoin.time_key`, `wjoin.sym_key`, and `wjoin.agg_inputs[k]` onto the DFS stack instead of `join.left_keys[k]` / `join.right_keys[k]`.

Locations in opt.c (approximate lines — search for `case OP_WINDOW_JOIN:`):
- ~line 127 (type inference DFS)
- ~line 554 (topological sort)
- ~line 689 (DCE liveness)
- ~line 925 (pointer fixup)

Pattern for each:
```c
case OP_WINDOW_JOIN: {
    td_op_ext_t* ext = find_ext(g, n->id);
    if (ext) {
        if (ext->wjoin.time_key && !visited[ext->wjoin.time_key->id])
            stack[sp++] = ext->wjoin.time_key->id;
        if (ext->wjoin.sym_key && !visited[ext->wjoin.sym_key->id])
            stack[sp++] = ext->wjoin.sym_key->id;
        for (uint8_t k = 0; k < ext->wjoin.n_aggs; k++) {
            if (ext->wjoin.agg_inputs[k] && !visited[ext->wjoin.agg_inputs[k]->id])
                stack[sp++] = ext->wjoin.agg_inputs[k]->id;
        }
    }
    break;
}
```

For the pointer fixup (~line 925), the pattern is:
```c
case OP_WINDOW_JOIN:
    ext->wjoin.time_key = &g->nodes[ext->wjoin.time_key->id];
    if (ext->wjoin.sym_key)
        ext->wjoin.sym_key = &g->nodes[ext->wjoin.sym_key->id];
    for (uint8_t k = 0; k < ext->wjoin.n_aggs; k++)
        ext->wjoin.agg_inputs[k] = &g->nodes[ext->wjoin.agg_inputs[k]->id];
    break;
```

**Step 4: Verify build**

Run: `cmake --build build 2>&1`
Expected: Clean build, zero warnings.

**Step 5: Run tests**

Run: `cd build && ctest --output-on-failure`
Expected: All existing tests pass (no regressions).

**Step 6: Commit**

```bash
git add src/ops/graph.c src/ops/opt.c
git commit -m "feat: fix td_window_join DAG builder to store all parameters"
```

---

## Task 3: Write failing ASOF window join test [x]

**Files:**
- Modify: `test/test_exec.c:960-989` (add test function + suite entry)

**Step 1: Write the test function**

Add before the `exec_tests[]` array (before line 960):

```c
/* ---- WINDOW JOIN (ASOF) ---- */
static MunitResult test_exec_window_join(const void* params, void* data) {
    (void)params; (void)data;
    td_heap_init();
    td_sym_init();

    /* Left table: time(I64), sym(I64), price(F64) — trade events */
    int64_t ltime[]  = {100, 200, 300, 400, 500};
    int64_t lsym[]   = {1, 1, 2, 1, 2};
    double  lprice[] = {10.0, 20.0, 30.0, 40.0, 50.0};

    td_t* lt_v = td_vec_from_raw(TD_I64, ltime, 5);
    td_t* ls_v = td_vec_from_raw(TD_I64, lsym, 5);
    td_t* lp_v = td_vec_from_raw(TD_F64, lprice, 5);

    int64_t n_time  = td_sym_intern("time", 4);
    int64_t n_sym   = td_sym_intern("sym", 3);
    int64_t n_price = td_sym_intern("price", 5);

    td_t* left = td_table_new(3);
    left = td_table_add_col(left, n_time, lt_v);
    left = td_table_add_col(left, n_sym, ls_v);
    left = td_table_add_col(left, n_price, lp_v);
    td_release(lt_v); td_release(ls_v); td_release(lp_v);

    /* Right table: time(I64), sym(I64), bid(F64) — quote snapshots */
    int64_t rtime[] = {90, 150, 250, 350, 450};
    int64_t rsym[]  = {1, 1, 2, 1, 2};
    double  rbid[]  = {9.5, 15.0, 25.0, 35.0, 45.0};

    td_t* rt_v = td_vec_from_raw(TD_I64, rtime, 5);
    td_t* rs_v = td_vec_from_raw(TD_I64, rsym, 5);
    td_t* rb_v = td_vec_from_raw(TD_F64, rbid, 5);

    int64_t n_bid = td_sym_intern("bid", 3);

    td_t* right = td_table_new(3);
    right = td_table_add_col(right, n_time, rt_v);
    right = td_table_add_col(right, n_sym, rs_v);
    right = td_table_add_col(right, n_bid, rb_v);
    td_release(rt_v); td_release(rs_v); td_release(rb_v);

    td_graph_t* g = td_graph_new(left);
    td_op_t* left_op  = td_const_table(g, left);
    td_op_t* right_op = td_const_table(g, right);
    td_op_t* tkey = td_scan(g, "time");
    td_op_t* skey = td_scan(g, "sym");

    /* ASOF join: for each trade, find the most recent quote within window [-200, 0] */
    uint16_t agg_ops[] = { OP_LAST };  /* take last (most recent) bid */
    td_op_t* bid_scan = td_scan(g, "bid");
    td_op_t* agg_ins[] = { bid_scan };

    td_op_t* wj = td_window_join(g, left_op, right_op,
                                  tkey, skey,
                                  -200, 0,
                                  agg_ops, agg_ins, 1);

    td_t* result = td_execute(g, wj);
    munit_assert_false(TD_IS_ERR(result));
    munit_assert_int(result->type, ==, TD_TABLE);
    /* All 5 left rows should appear in output */
    munit_assert_int(td_table_nrows(result), ==, 5);

    td_release(result);
    td_graph_free(g);
    td_release(left);
    td_release(right);
    td_sym_destroy();
    td_heap_destroy();
    return MUNIT_OK;
}
```

**Step 2: Add to test array**

Add to `exec_tests[]` before the NULL terminator (line 984):

```c
    { "/window_join",    test_exec_window_join,       NULL, NULL, 0, NULL },
```

**Step 3: Run test to verify it fails**

Run: `cmake --build build && ./build/test_teide --suite /exec/window_join`
Expected: FAIL or crash (no executor for OP_WINDOW_JOIN yet).

**Step 4: Commit**

```bash
git add test/test_exec.c
git commit -m "test: add failing ASOF window join test"
```

---

## Task 4: Implement `exec_window_join` in exec.c [x]

**Files:**
- Modify: `src/ops/exec.c` (add helper function + dispatch case)

**Step 1: Add `exec_window_join` helper**

Add the helper function before the `exec_node` dispatch function (before line 10609). Place it near `exec_join` (~line 7634). The algorithm:

1. Get ext node → extract `wjoin` parameters
2. Both tables must already be sorted by time_key (caller's responsibility)
3. For each left row, scan right table for rows matching `sym_key` (if present) and where `right.time` is in `[left.time + window_lo, left.time + window_hi]`
4. Among matching rows, take the last one (ASOF = most recent before)
5. Apply aggregation ops to matching right-side columns
6. Output: left columns + aggregated right columns

```c
static td_t* exec_window_join(td_graph_t* g, td_op_t* op,
                               td_t* left_table, td_t* right_table) {
    td_op_ext_t* ext = find_ext(g, op->id);
    if (!ext) return TD_ERR_PTR(TD_ERR_INTERNAL);

    int64_t  window_lo = ext->wjoin.window_lo;
    int64_t  window_hi = ext->wjoin.window_hi;
    uint8_t  n_aggs    = ext->wjoin.n_aggs;

    int64_t left_n  = td_table_nrows(left_table);
    int64_t right_n = td_table_nrows(right_table);

    /* Resolve time key column from both tables */
    td_op_t* time_op = ext->wjoin.time_key;
    int64_t time_sym = time_op->sym;
    td_t* lt_time = td_table_col(left_table, time_sym);
    td_t* rt_time = td_table_col(right_table, time_sym);
    if (!lt_time || !rt_time) return TD_ERR_PTR(TD_ERR_KEY);

    int64_t* lt_data = (int64_t*)td_data(lt_time);
    int64_t* rt_data = (int64_t*)td_data(rt_time);

    /* Resolve optional sym key */
    td_t* lt_sym_vec = NULL;
    td_t* rt_sym_vec = NULL;
    int64_t* lt_sym_data = NULL;
    int64_t* rt_sym_data = NULL;
    if (ext->wjoin.sym_key) {
        int64_t sym_sym = ext->wjoin.sym_key->sym;
        lt_sym_vec = td_table_col(left_table, sym_sym);
        rt_sym_vec = td_table_col(right_table, sym_sym);
        if (lt_sym_vec) lt_sym_data = (int64_t*)td_data(lt_sym_vec);
        if (rt_sym_vec) rt_sym_data = (int64_t*)td_data(rt_sym_vec);
    }

    /* Build match index: for each left row, find best matching right row */
    int64_t* match_idx = td_sys_alloc((size_t)left_n * sizeof(int64_t));
    if (!match_idx) return TD_ERR_PTR(TD_ERR_OOM);

    for (int64_t i = 0; i < left_n; i++) {
        int64_t lt_t = lt_data[i];
        int64_t lo = lt_t + window_lo;
        int64_t hi = lt_t + window_hi;
        int64_t best = -1;
        int64_t best_time = INT64_MIN;

        for (int64_t j = 0; j < right_n; j++) {
            int64_t rt_t = rt_data[j];
            if (rt_t < lo || rt_t > hi) continue;
            /* Check sym key match if present */
            if (lt_sym_data && rt_sym_data && lt_sym_data[i] != rt_sym_data[j])
                continue;
            /* Take the most recent (largest time) match */
            if (rt_t > best_time) {
                best_time = rt_t;
                best = j;
            }
        }
        match_idx[i] = best;
    }

    /* Build output table: all left columns + agg columns from right */
    int64_t left_ncols = td_table_ncols(left_table);
    td_t* out = td_table_new((int32_t)(left_ncols + n_aggs));

    /* Copy left columns */
    for (int64_t c = 0; c < left_ncols; c++) {
        int64_t col_name = td_table_col_name(left_table, c);
        td_t* col = td_table_col_idx(left_table, c);
        td_retain(col);
        out = td_table_add_col(out, col_name, col);
        td_release(col);
    }

    /* Build aggregated right columns using match index */
    for (uint8_t a = 0; a < n_aggs; a++) {
        td_op_t* agg_input = ext->wjoin.agg_inputs[a];
        int64_t  agg_sym   = agg_input->sym;
        td_t* rcol = td_table_col(right_table, agg_sym);
        if (!rcol) { td_sys_free(match_idx); td_release(out); return TD_ERR_PTR(TD_ERR_KEY); }

        int8_t rtype = rcol->type;
        td_t* out_col = td_vec_new(rtype, left_n);

        if (rtype == TD_F64) {
            double* src = (double*)td_data(rcol);
            double* dst = (double*)td_data(out_col);
            for (int64_t i = 0; i < left_n; i++) {
                dst[i] = (match_idx[i] >= 0) ? src[match_idx[i]] : 0.0;
            }
        } else {
            int64_t* src = (int64_t*)td_data(rcol);
            int64_t* dst = (int64_t*)td_data(out_col);
            for (int64_t i = 0; i < left_n; i++) {
                dst[i] = (match_idx[i] >= 0) ? src[match_idx[i]] : 0;
            }
        }
        out_col->len = left_n;
        out = td_table_add_col(out, agg_sym, out_col);
        td_release(out_col);
    }

    td_sys_free(match_idx);
    return out;
}
```

**Step 2: Add dispatch case in exec_node switch**

After `case OP_JOIN:` block (after exec.c:10905), add:

```c
        case OP_WINDOW_JOIN: {
            td_t* left = exec_node(g, op->inputs[0]);
            td_t* right = exec_node(g, op->inputs[1]);
            if (!left || TD_IS_ERR(left)) { if (right && !TD_IS_ERR(right)) td_release(right); return left; }
            if (!right || TD_IS_ERR(right)) { td_release(left); return right; }
            if (g->selection && left && !TD_IS_ERR(left) && left->type == TD_TABLE) {
                td_t* compacted = sel_compact(g, left, g->selection);
                td_release(left);
                td_release(g->selection);
                g->selection = NULL;
                left = compacted;
            }
            td_t* result = exec_window_join(g, op, left, right);
            td_release(left);
            td_release(right);
            return result;
        }
```

**Step 3: Run test to verify it passes**

Run: `cmake --build build && ./build/test_teide --suite /exec/window_join`
Expected: PASS

**Step 4: Run all tests**

Run: `cd build && ctest --output-on-failure`
Expected: All tests pass.

**Step 5: Commit**

```bash
git add src/ops/exec.c
git commit -m "feat: implement ASOF window join executor"
```

---

## Task 5: Add more ASOF window join test cases [x]

**Files:**
- Modify: `test/test_exec.c`

**Step 1: Add edge case tests**

Add after `test_exec_window_join`, before the `exec_tests[]` array:

```c
/* ---- WINDOW JOIN: no sym key ---- */
static MunitResult test_exec_window_join_no_sym(const void* params, void* data) {
    (void)params; (void)data;
    td_heap_init();
    td_sym_init();

    int64_t ltime[] = {100, 200, 300};
    double  lval[]  = {1.0, 2.0, 3.0};
    td_t* lt_v = td_vec_from_raw(TD_I64, ltime, 3);
    td_t* lv_v = td_vec_from_raw(TD_F64, lval, 3);
    int64_t n_time = td_sym_intern("time", 4);
    int64_t n_val  = td_sym_intern("val", 3);
    td_t* left = td_table_new(2);
    left = td_table_add_col(left, n_time, lt_v);
    left = td_table_add_col(left, n_val, lv_v);
    td_release(lt_v); td_release(lv_v);

    int64_t rtime[] = {50, 150, 250};
    double  rbid[]  = {0.5, 1.5, 2.5};
    td_t* rt_v = td_vec_from_raw(TD_I64, rtime, 3);
    td_t* rb_v = td_vec_from_raw(TD_F64, rbid, 3);
    int64_t n_bid = td_sym_intern("bid", 3);
    td_t* right = td_table_new(2);
    right = td_table_add_col(right, n_time, rt_v);
    right = td_table_add_col(right, n_bid, rb_v);
    td_release(rt_v); td_release(rb_v);

    td_graph_t* g = td_graph_new(left);
    td_op_t* left_op  = td_const_table(g, left);
    td_op_t* right_op = td_const_table(g, right);
    td_op_t* tkey = td_scan(g, "time");

    uint16_t agg_ops[] = { OP_LAST };
    td_op_t* bid_scan = td_scan(g, "bid");
    td_op_t* agg_ins[] = { bid_scan };

    /* No sym key (NULL) — match across all symbols */
    td_op_t* wj = td_window_join(g, left_op, right_op,
                                  tkey, NULL, -100, 0,
                                  agg_ops, agg_ins, 1);

    td_t* result = td_execute(g, wj);
    munit_assert_false(TD_IS_ERR(result));
    munit_assert_int(td_table_nrows(result), ==, 3);

    td_release(result);
    td_graph_free(g);
    td_release(left);
    td_release(right);
    td_sym_destroy();
    td_heap_destroy();
    return MUNIT_OK;
}

/* ---- WINDOW JOIN: empty right table ---- */
static MunitResult test_exec_window_join_empty(const void* params, void* data) {
    (void)params; (void)data;
    td_heap_init();
    td_sym_init();

    int64_t ltime[] = {100, 200};
    double  lval[]  = {1.0, 2.0};
    td_t* lt_v = td_vec_from_raw(TD_I64, ltime, 2);
    td_t* lv_v = td_vec_from_raw(TD_F64, lval, 2);
    int64_t n_time = td_sym_intern("time", 4);
    int64_t n_val  = td_sym_intern("val", 3);
    td_t* left = td_table_new(2);
    left = td_table_add_col(left, n_time, lt_v);
    left = td_table_add_col(left, n_val, lv_v);
    td_release(lt_v); td_release(lv_v);

    /* Empty right table */
    int64_t n_bid = td_sym_intern("bid", 3);
    td_t* right = td_table_new(2);
    td_t* rt_v = td_vec_new(TD_I64, 0);
    td_t* rb_v = td_vec_new(TD_F64, 0);
    right = td_table_add_col(right, n_time, rt_v);
    right = td_table_add_col(right, n_bid, rb_v);
    td_release(rt_v); td_release(rb_v);

    td_graph_t* g = td_graph_new(left);
    td_op_t* left_op  = td_const_table(g, left);
    td_op_t* right_op = td_const_table(g, right);
    td_op_t* tkey = td_scan(g, "time");

    uint16_t agg_ops[] = { OP_LAST };
    td_op_t* bid_scan = td_scan(g, "bid");
    td_op_t* agg_ins[] = { bid_scan };

    td_op_t* wj = td_window_join(g, left_op, right_op,
                                  tkey, NULL, -100, 0,
                                  agg_ops, agg_ins, 1);

    td_t* result = td_execute(g, wj);
    munit_assert_false(TD_IS_ERR(result));
    /* Left rows preserved, bid column all zeros (no matches) */
    munit_assert_int(td_table_nrows(result), ==, 2);

    td_release(result);
    td_graph_free(g);
    td_release(left);
    td_release(right);
    td_sym_destroy();
    td_heap_destroy();
    return MUNIT_OK;
}
```

**Step 2: Add to test array**

Add to `exec_tests[]`:

```c
    { "/window_join_no_sym", test_exec_window_join_no_sym, NULL, NULL, 0, NULL },
    { "/window_join_empty",  test_exec_window_join_empty,  NULL, NULL, 0, NULL },
```

**Step 3: Run tests**

Run: `cmake --build build && ./build/test_teide --suite /exec`
Expected: All exec tests pass.

**Step 4: Commit**

```bash
git add test/test_exec.c
git commit -m "test: add ASOF window join edge case tests"
```

---

## Task 6: Write failing TD_STR serialization test [x]

**Files:**
- Modify: `test/test_store.c:747-770` (add test + suite entry)

**Step 1: Write the test**

Add before `store_tests[]` (before line 747):

```c
#define TMP_STR_COL_PATH "/tmp/teide_test_str_col.dat"

static MunitResult test_col_save_load_str(const void* params, void* data) {
    (void)params; (void)data;

    /* Build a string vector: 3 strings */
    td_t* v = td_list_new(3);
    td_t* s0 = td_str("hello", 5);
    td_t* s1 = td_str("world", 5);
    td_t* s2 = td_str("teide", 5);
    v = td_list_append(v, s0);
    v = td_list_append(v, s1);
    v = td_list_append(v, s2);
    td_release(s0); td_release(s1); td_release(s2);

    /* Save */
    td_err_t err = td_col_save(v, TMP_STR_COL_PATH);
    munit_assert_int(err, ==, TD_OK);

    /* Load */
    td_t* loaded = td_col_load(TMP_STR_COL_PATH);
    munit_assert_false(TD_IS_ERR(loaded));
    munit_assert_int(loaded->len, ==, 3);

    /* Verify string content */
    td_t* r0 = td_list_get(loaded, 0);
    td_t* r1 = td_list_get(loaded, 1);
    td_t* r2 = td_list_get(loaded, 2);
    munit_assert_string_equal(td_str_ptr(r0), "hello");
    munit_assert_string_equal(td_str_ptr(r1), "world");
    munit_assert_string_equal(td_str_ptr(r2), "teide");

    td_release(loaded);
    td_release(v);
    unlink(TMP_STR_COL_PATH);
    return MUNIT_OK;
}
```

**Step 2: Add to store_tests[] before the NULL terminator**

```c
    { "/col_save_load_str", test_col_save_load_str, store_setup, store_teardown, 0, NULL },
```

**Step 3: Run test to verify it fails**

Run: `cmake --build build && ./build/test_teide --suite /store/col_save_load_str`
Expected: FAIL (TD_ERR_TYPE — `is_serializable_type` rejects strings).

**Step 4: Commit**

```bash
git add test/test_store.c
git commit -m "test: add failing string column serialization test"
```

---

## Task 7: Implement TD_STR column serialization [x]

**Files:**
- Modify: `src/store/col.c`

The string column serialization needs a different on-disk format since strings are variable-length. We use a list-of-atoms approach: serialize element count + per-element (length + bytes).

**Step 1: Add string save/load helpers**

This requires a design decision: strings in Teide are stored as a `TD_LIST` of `TD_ATOM_STR` atoms, not as a flat string vector. The serialization format for a string list is:

```
[4B magic: "STRL"]
[8B count: int64_t element count]
For each element:
  [4B len: uint32_t string byte length]
  [len bytes: string data]
```

Add to `col.c` after `is_serializable_type()`:

```c
static const uint32_t STR_LIST_MAGIC = 0x4C525453;  /* "STRL" */

/* Check if a list contains only string atoms */
static bool is_str_list(td_t* v) {
    if (v->type != TD_LIST) return false;
    for (int64_t i = 0; i < v->len; i++) {
        td_t* elem = td_list_get(v, i);
        if (!elem || elem->type != TD_ATOM_STR) return false;
    }
    return true;
}

static td_err_t col_save_str_list(td_t* v, const char* path) {
    FILE* f = fopen(path, "wb");
    if (!f) return TD_ERR_IO;

    uint32_t magic = STR_LIST_MAGIC;
    int64_t count = v->len;
    if (fwrite(&magic, 4, 1, f) != 1 ||
        fwrite(&count, 8, 1, f) != 1) {
        fclose(f); return TD_ERR_IO;
    }

    for (int64_t i = 0; i < count; i++) {
        td_t* elem = td_list_get(v, i);
        uint32_t slen = (uint32_t)td_str_len(elem);
        const char* sptr = td_str_ptr(elem);
        if (fwrite(&slen, 4, 1, f) != 1 ||
            (slen > 0 && fwrite(sptr, 1, slen, f) != slen)) {
            fclose(f); return TD_ERR_IO;
        }
    }
    fclose(f);
    return TD_OK;
}

static td_t* col_load_str_list(const char* path) {
    size_t fsize;
    void* mapped = td_vm_map_file(path, &fsize);
    if (!mapped) return TD_ERR_PTR(TD_ERR_IO);

    if (fsize < 12) { td_vm_unmap_file(mapped, fsize); return TD_ERR_PTR(TD_ERR_CORRUPT); }

    uint8_t* p = (uint8_t*)mapped;
    uint32_t magic;
    memcpy(&magic, p, 4);
    if (magic != STR_LIST_MAGIC) { td_vm_unmap_file(mapped, fsize); return TD_ERR_PTR(TD_ERR_CORRUPT); }

    int64_t count;
    memcpy(&count, p + 4, 8);
    if (count < 0) { td_vm_unmap_file(mapped, fsize); return TD_ERR_PTR(TD_ERR_CORRUPT); }

    td_t* list = td_list_new((int32_t)count);
    size_t off = 12;

    for (int64_t i = 0; i < count; i++) {
        if (off + 4 > fsize) { td_release(list); td_vm_unmap_file(mapped, fsize); return TD_ERR_PTR(TD_ERR_CORRUPT); }
        uint32_t slen;
        memcpy(&slen, p + off, 4);
        off += 4;
        if (off + slen > fsize) { td_release(list); td_vm_unmap_file(mapped, fsize); return TD_ERR_PTR(TD_ERR_CORRUPT); }
        td_t* s = td_str((const char*)(p + off), slen);
        list = td_list_append(list, s);
        td_release(s);
        off += slen;
    }

    td_vm_unmap_file(mapped, fsize);
    return list;
}
```

**Step 2: Modify `td_col_save` to handle string lists**

At the top of `td_col_save()` (col.c:~57), add before the `is_serializable_type` check:

```c
    /* String list: special format */
    if (is_str_list(v))
        return col_save_str_list(v, path);
```

**Step 3: Modify `td_col_load` to handle string lists**

At the top of `td_col_load()` (col.c:~134), add a magic-number check:

```c
    /* Check for string list magic */
    if (fsize >= 4) {
        uint32_t magic;
        memcpy(&magic, ptr, 4);
        if (magic == STR_LIST_MAGIC) {
            td_vm_unmap_file(ptr, fsize);
            return col_load_str_list(path);
        }
    }
```

**Step 4: Run the test**

Run: `cmake --build build && ./build/test_teide --suite /store/col_save_load_str`
Expected: PASS

**Step 5: Run all tests**

Run: `cd build && ctest --output-on-failure`
Expected: All tests pass.

**Step 6: Commit**

```bash
git add src/store/col.c
git commit -m "feat: implement string list column serialization"
```

---

## Task 8: Add TD_LIST/TD_TABLE serialization tests and implementation [x]

**Files:**
- Modify: `test/test_store.c`, `src/store/col.c`

**Step 1: Write failing test for nested list**

Add to test_store.c before `store_tests[]`:

```c
#define TMP_LIST_COL_PATH "/tmp/teide_test_list_col.dat"

static MunitResult test_col_save_load_list(const void* params, void* data) {
    (void)params; (void)data;

    /* Build a list of I64 vectors */
    td_t* outer = td_list_new(2);

    int64_t a_data[] = {1, 2, 3};
    td_t* a = td_vec_from_raw(TD_I64, a_data, 3);
    int64_t b_data[] = {4, 5};
    td_t* b = td_vec_from_raw(TD_I64, b_data, 2);

    outer = td_list_append(outer, a);
    outer = td_list_append(outer, b);
    td_release(a); td_release(b);

    td_err_t err = td_col_save(outer, TMP_LIST_COL_PATH);
    munit_assert_int(err, ==, TD_OK);

    td_t* loaded = td_col_load(TMP_LIST_COL_PATH);
    munit_assert_false(TD_IS_ERR(loaded));
    munit_assert_int(loaded->type, ==, TD_LIST);
    munit_assert_int(loaded->len, ==, 2);

    td_t* la = td_list_get(loaded, 0);
    munit_assert_int(la->len, ==, 3);
    int64_t* la_data = (int64_t*)td_data(la);
    munit_assert_int(la_data[0], ==, 1);
    munit_assert_int(la_data[1], ==, 2);
    munit_assert_int(la_data[2], ==, 3);

    td_t* lb = td_list_get(loaded, 1);
    munit_assert_int(lb->len, ==, 2);
    int64_t* lb_data = (int64_t*)td_data(lb);
    munit_assert_int(lb_data[0], ==, 4);
    munit_assert_int(lb_data[1], ==, 5);

    td_release(loaded);
    td_release(outer);
    unlink(TMP_LIST_COL_PATH);
    return MUNIT_OK;
}
```

**Step 2: Write failing test for table serialization**

```c
#define TMP_TABLE_COL_PATH "/tmp/teide_test_table_col.dat"

static MunitResult test_col_save_load_table(const void* params, void* data) {
    (void)params; (void)data;

    int64_t ids[] = {10, 20, 30};
    double vals[] = {1.1, 2.2, 3.3};
    td_t* id_v = td_vec_from_raw(TD_I64, ids, 3);
    td_t* val_v = td_vec_from_raw(TD_F64, vals, 3);

    int64_t n_id  = td_sym_intern("id", 2);
    int64_t n_val = td_sym_intern("val", 3);

    td_t* tbl = td_table_new(2);
    tbl = td_table_add_col(tbl, n_id, id_v);
    tbl = td_table_add_col(tbl, n_val, val_v);
    td_release(id_v); td_release(val_v);

    td_err_t err = td_col_save(tbl, TMP_TABLE_COL_PATH);
    munit_assert_int(err, ==, TD_OK);

    td_t* loaded = td_col_load(TMP_TABLE_COL_PATH);
    munit_assert_false(TD_IS_ERR(loaded));
    munit_assert_int(loaded->type, ==, TD_TABLE);
    munit_assert_int(td_table_nrows(loaded), ==, 3);
    munit_assert_int(td_table_ncols(loaded), ==, 2);

    td_t* lid = td_table_col(loaded, n_id);
    munit_assert_int(((int64_t*)td_data(lid))[0], ==, 10);
    munit_assert_int(((int64_t*)td_data(lid))[1], ==, 20);

    td_t* lval = td_table_col(loaded, n_val);
    munit_assert_double_equal(((double*)td_data(lval))[0], 1.1, 6);

    td_release(loaded);
    td_release(tbl);
    unlink(TMP_TABLE_COL_PATH);
    return MUNIT_OK;
}
```

**Step 3: Add to store_tests[]**

```c
    { "/col_save_load_list",  test_col_save_load_list,  store_setup, store_teardown, 0, NULL },
    { "/col_save_load_table", test_col_save_load_table, store_setup, store_teardown, 0, NULL },
```

**Step 4: Run tests to verify they fail**

Run: `cmake --build build && ./build/test_teide --suite /store/col_save_load_list`
Expected: FAIL

**Step 5: Implement generic list serialization in col.c**

Add to col.c after the string list functions. Use a recursive format:

```
[4B magic: "LSTG"]
[8B count: int64_t element count]
For each element:
  [1B type: int8_t element type]
  [variable: recursive serialization of element]
```

For scalar atoms: `[8B value]`
For vectors with `is_serializable_type`: `[8B len][len*esz bytes data]`
For nested lists: recursive
For tables: `[4B ncols][for each col: 8B name_sym + recursive col data]`

```c
static const uint32_t LIST_MAGIC  = 0x4754534C;  /* "LSTG" */
static const uint32_t TABLE_MAGIC = 0x4C425454;  /* "TTBL" */

/* Forward declaration */
static td_err_t col_save_recursive(td_t* v, FILE* f);
static td_t* col_load_recursive(uint8_t* data, size_t size, size_t* consumed);

static td_err_t col_save_recursive(td_t* v, FILE* f) {
    int8_t type = v->type;
    if (fwrite(&type, 1, 1, f) != 1) return TD_ERR_IO;

    if (type < 0) {
        /* Atom: write 8 bytes of value */
        if (type == TD_ATOM_STR) {
            uint32_t slen = (uint32_t)td_str_len(v);
            if (fwrite(&slen, 4, 1, f) != 1) return TD_ERR_IO;
            if (slen > 0 && fwrite(td_str_ptr(v), 1, slen, f) != slen) return TD_ERR_IO;
        } else {
            if (fwrite(&v->i64, 8, 1, f) != 1) return TD_ERR_IO;
        }
        return TD_OK;
    }

    if (type == TD_TABLE) {
        int64_t ncols = td_table_ncols(v);
        int64_t nrows = td_table_nrows(v);
        if (fwrite(&ncols, 8, 1, f) != 1) return TD_ERR_IO;
        if (fwrite(&nrows, 8, 1, f) != 1) return TD_ERR_IO;
        for (int64_t c = 0; c < ncols; c++) {
            int64_t name = td_table_col_name(v, c);
            if (fwrite(&name, 8, 1, f) != 1) return TD_ERR_IO;
            td_t* col = td_table_col_idx(v, c);
            td_err_t err = col_save_recursive(col, f);
            if (err != TD_OK) return err;
        }
        return TD_OK;
    }

    if (type == TD_LIST) {
        int64_t count = v->len;
        if (fwrite(&count, 8, 1, f) != 1) return TD_ERR_IO;
        for (int64_t i = 0; i < count; i++) {
            td_t* elem = td_list_get(v, i);
            td_err_t err = col_save_recursive(elem, f);
            if (err != TD_OK) return err;
        }
        return TD_OK;
    }

    /* Vector of serializable type */
    if (is_serializable_type(type)) {
        int64_t len = v->len;
        uint8_t esz = td_type_sizes[type];
        if (fwrite(&len, 8, 1, f) != 1) return TD_ERR_IO;
        size_t data_sz = (size_t)len * esz;
        if (data_sz > 0 && fwrite(td_data(v), 1, data_sz, f) != data_sz) return TD_ERR_IO;
        return TD_OK;
    }

    return TD_ERR_TYPE;
}
```

**Step 6: Implement recursive load**

```c
static td_t* col_load_recursive(uint8_t* data, size_t size, size_t* consumed) {
    if (size < 1) return TD_ERR_PTR(TD_ERR_CORRUPT);
    int8_t type = (int8_t)data[0];
    size_t off = 1;

    if (type < 0) {
        if (type == TD_ATOM_STR) {
            if (off + 4 > size) return TD_ERR_PTR(TD_ERR_CORRUPT);
            uint32_t slen;
            memcpy(&slen, data + off, 4); off += 4;
            if (off + slen > size) return TD_ERR_PTR(TD_ERR_CORRUPT);
            td_t* s = td_str((const char*)(data + off), slen);
            off += slen;
            *consumed = off;
            return s;
        }
        /* Other atom */
        if (off + 8 > size) return TD_ERR_PTR(TD_ERR_CORRUPT);
        td_t* atom = td_alloc(0);
        atom->type = type;
        memcpy(&atom->i64, data + off, 8); off += 8;
        *consumed = off;
        return atom;
    }

    if (type == TD_TABLE) {
        if (off + 16 > size) return TD_ERR_PTR(TD_ERR_CORRUPT);
        int64_t ncols, nrows;
        memcpy(&ncols, data + off, 8); off += 8;
        memcpy(&nrows, data + off, 8); off += 8;

        td_t* tbl = td_table_new((int32_t)ncols);
        for (int64_t c = 0; c < ncols; c++) {
            if (off + 8 > size) { td_release(tbl); return TD_ERR_PTR(TD_ERR_CORRUPT); }
            int64_t name;
            memcpy(&name, data + off, 8); off += 8;
            size_t elem_consumed = 0;
            td_t* col = col_load_recursive(data + off, size - off, &elem_consumed);
            if (TD_IS_ERR(col)) { td_release(tbl); return col; }
            off += elem_consumed;
            tbl = td_table_add_col(tbl, name, col);
            td_release(col);
        }
        *consumed = off;
        return tbl;
    }

    if (type == TD_LIST) {
        if (off + 8 > size) return TD_ERR_PTR(TD_ERR_CORRUPT);
        int64_t count;
        memcpy(&count, data + off, 8); off += 8;

        td_t* list = td_list_new((int32_t)count);
        for (int64_t i = 0; i < count; i++) {
            size_t elem_consumed = 0;
            td_t* elem = col_load_recursive(data + off, size - off, &elem_consumed);
            if (TD_IS_ERR(elem)) { td_release(list); return elem; }
            off += elem_consumed;
            list = td_list_append(list, elem);
            td_release(elem);
        }
        *consumed = off;
        return list;
    }

    /* Vector of serializable type */
    if (is_serializable_type(type)) {
        if (off + 8 > size) return TD_ERR_PTR(TD_ERR_CORRUPT);
        int64_t len;
        memcpy(&len, data + off, 8); off += 8;
        uint8_t esz = td_type_sizes[type];
        size_t data_sz = (size_t)len * esz;
        if (off + data_sz > size) return TD_ERR_PTR(TD_ERR_CORRUPT);
        td_t* vec = td_vec_from_raw(type, data + off, len);
        off += data_sz;
        *consumed = off;
        return vec;
    }

    return TD_ERR_PTR(TD_ERR_TYPE);
}
```

**Step 7: Wire up generic save/load for lists and tables**

Modify `td_col_save` to handle lists and tables:

```c
    /* Generic list/table: recursive format */
    if (v->type == TD_LIST || v->type == TD_TABLE) {
        FILE* f = fopen(path, "wb");
        if (!f) return TD_ERR_IO;
        uint32_t magic = (v->type == TD_TABLE) ? TABLE_MAGIC : LIST_MAGIC;
        if (fwrite(&magic, 4, 1, f) != 1) { fclose(f); return TD_ERR_IO; }
        td_err_t err = col_save_recursive(v, f);
        fclose(f);
        return err;
    }
```

Modify `td_col_load` to detect list/table magic:

```c
    if (fsize >= 4) {
        uint32_t magic;
        memcpy(&magic, ptr, 4);
        if (magic == STR_LIST_MAGIC) {
            td_vm_unmap_file(ptr, fsize);
            return col_load_str_list(path);
        }
        if (magic == LIST_MAGIC || magic == TABLE_MAGIC) {
            size_t consumed = 0;
            /* Skip the 4-byte magic, then the recursive payload starts with the type byte */
            td_t* result = col_load_recursive((uint8_t*)ptr + 4, fsize - 4, &consumed);
            td_vm_unmap_file(ptr, fsize);
            return result;
        }
    }
```

**Step 8: Run tests**

Run: `cmake --build build && ./build/test_teide --suite /store`
Expected: All store tests pass including new list and table tests.

**Step 9: Commit**

```bash
git add src/store/col.c test/test_store.c
git commit -m "feat: implement list and table column serialization"
```

---

## Task 9: Add test_meta.c — metadata I/O tests [x]

**Files:**
- Create: `test/test_meta.c`
- Modify: `test/test_main.c:57,82,115` (register suite)

**Step 1: Write test file**

```c
#include "munit.h"
#include <teide/td.h>
#include <unistd.h>

#define TMP_META_PATH "/tmp/teide_test_meta.d"

static void* meta_setup(const MunitParameter params[], void* data) {
    (void)params; (void)data;
    td_heap_init();
    td_sym_init();
    return NULL;
}

static void meta_teardown(void* data) {
    (void)data;
    td_sym_destroy();
    td_heap_destroy();
}

/* Roundtrip: save schema, load it back, verify contents */
static MunitResult test_meta_roundtrip(const void* params, void* data) {
    (void)params; (void)data;

    int64_t names[] = {
        td_sym_intern("id", 2),
        td_sym_intern("name", 4),
        td_sym_intern("score", 5)
    };
    td_t* schema = td_vec_from_raw(TD_I64, names, 3);

    td_err_t err = td_meta_save_d(schema, TMP_META_PATH);
    munit_assert_int(err, ==, TD_OK);

    td_t* loaded = td_meta_load_d(TMP_META_PATH);
    munit_assert_false(TD_IS_ERR(loaded));
    munit_assert_int(loaded->len, ==, 3);

    int64_t* data_ptr = (int64_t*)td_data(loaded);
    munit_assert_int(data_ptr[0], ==, names[0]);
    munit_assert_int(data_ptr[1], ==, names[1]);
    munit_assert_int(data_ptr[2], ==, names[2]);

    td_release(loaded);
    td_release(schema);
    unlink(TMP_META_PATH);
    return MUNIT_OK;
}

/* Error: NULL input */
static MunitResult test_meta_null_input(const void* params, void* data) {
    (void)params; (void)data;
    td_err_t err = td_meta_save_d(NULL, TMP_META_PATH);
    munit_assert_int(err, !=, TD_OK);
    return MUNIT_OK;
}

/* Error: load from missing file */
static MunitResult test_meta_load_missing(const void* params, void* data) {
    (void)params; (void)data;
    td_t* loaded = td_meta_load_d("/tmp/teide_nonexistent_meta.d");
    munit_assert_true(TD_IS_ERR(loaded));
    return MUNIT_OK;
}

static MunitTest meta_tests[] = {
    { "/roundtrip",    test_meta_roundtrip,    meta_setup, meta_teardown, 0, NULL },
    { "/null_input",   test_meta_null_input,   meta_setup, meta_teardown, 0, NULL },
    { "/load_missing", test_meta_load_missing, meta_setup, meta_teardown, 0, NULL },
    { NULL, NULL, NULL, NULL, 0, NULL },
};

MunitSuite test_meta_suite = {
    "/meta", meta_tests, NULL, 1, 0
};
```

**Step 2: Register in test_main.c**

Add after line 57: `extern MunitSuite test_meta_suite;`

Add to `child_suites[]` before terminator (line 82):
```c
    { "/meta",   NULL, NULL, 0, 0 },
```

Add in `main()` after line 115:
```c
    child_suites[21] = test_meta_suite;
```

**Step 3: Build and run**

Run: `cmake --build build && ./build/test_teide --suite /meta`
Expected: PASS

**Step 4: Commit**

```bash
git add test/test_meta.c test/test_main.c
git commit -m "test: add metadata I/O tests"
```

---

## Task 10: Add test_types.c — type size table tests [x]

**Files:**
- Create: `test/test_types.c`
- Modify: `test/test_main.c` (register suite)

**Step 1: Write test file**

```c
#include "munit.h"
#include <teide/td.h>

static MunitResult test_type_sizes(const void* params, void* data) {
    (void)params; (void)data;

    /* Verify each type's element size */
    munit_assert_uint8(td_type_sizes[TD_BOOL], ==, 1);
    munit_assert_uint8(td_type_sizes[TD_U8],   ==, 1);
    munit_assert_uint8(td_type_sizes[TD_CHAR], ==, 1);
    munit_assert_uint8(td_type_sizes[TD_I16],  ==, 2);
    munit_assert_uint8(td_type_sizes[TD_I32],  ==, 4);
    munit_assert_uint8(td_type_sizes[TD_I64],  ==, 8);
    munit_assert_uint8(td_type_sizes[TD_F64],  ==, 8);
    munit_assert_uint8(td_type_sizes[TD_DATE], ==, 4);
    munit_assert_uint8(td_type_sizes[TD_TIME], ==, 8);
    munit_assert_uint8(td_type_sizes[TD_TIMESTAMP], ==, 8);
    munit_assert_uint8(td_type_sizes[TD_GUID], ==, 16);
    munit_assert_uint8(td_type_sizes[TD_SYM],  ==, 8);

    /* Non-scalar types should be 0 */
    munit_assert_uint8(td_type_sizes[TD_LIST],  ==, 0);
    munit_assert_uint8(td_type_sizes[TD_TABLE], ==, 0);

    return MUNIT_OK;
}

static MunitResult test_elem_size_macro(const void* params, void* data) {
    (void)params; (void)data;

    /* td_elem_size should match td_type_sizes for positive types */
    munit_assert_int(td_elem_size(TD_I64), ==, 8);
    munit_assert_int(td_elem_size(TD_F64), ==, 8);
    munit_assert_int(td_elem_size(TD_I32), ==, 4);
    munit_assert_int(td_elem_size(TD_BOOL), ==, 1);

    return MUNIT_OK;
}

static MunitTest types_tests[] = {
    { "/sizes", test_type_sizes,      NULL, NULL, 0, NULL },
    { "/macro", test_elem_size_macro, NULL, NULL, 0, NULL },
    { NULL, NULL, NULL, NULL, 0, NULL },
};

MunitSuite test_types_suite = {
    "/types", types_tests, NULL, 1, 0
};
```

**Step 2: Register in test_main.c**

Add `extern MunitSuite test_types_suite;`, add entry to `child_suites[]`, add patch in `main()`.

**Step 3: Build and run**

Run: `cmake --build build && ./build/test_teide --suite /types`
Expected: PASS

**Step 4: Commit**

```bash
git add test/test_types.c test/test_main.c
git commit -m "test: add type size table tests"
```

---

## Task 11: Add test_platform.c — VM and thread tests [x]

**Files:**
- Create: `test/test_platform.c`
- Modify: `test/test_main.c` (register suite)

**Step 1: Write test file**

```c
#include "munit.h"
#include <teide/td.h>
#include <string.h>

/* ---- VM alloc/free ---- */
static MunitResult test_vm_alloc_free(const void* params, void* data) {
    (void)params; (void)data;

    void* ptr = td_vm_alloc(4096);
    munit_assert_not_null(ptr);

    /* Should be writable */
    memset(ptr, 0xAB, 4096);
    munit_assert_uint8(((uint8_t*)ptr)[0], ==, 0xAB);
    munit_assert_uint8(((uint8_t*)ptr)[4095], ==, 0xAB);

    td_vm_free(ptr, 4096);
    return MUNIT_OK;
}

/* ---- VM aligned alloc ---- */
static MunitResult test_vm_alloc_aligned(const void* params, void* data) {
    (void)params; (void)data;

    size_t align = 65536;
    void* ptr = td_vm_alloc_aligned(4096, align);
    munit_assert_not_null(ptr);
    munit_assert_size((size_t)ptr % align, ==, 0);

    td_vm_free(ptr, 4096);
    return MUNIT_OK;
}

/* ---- Thread count ---- */
static MunitResult test_thread_count(const void* params, void* data) {
    (void)params; (void)data;

    uint32_t n = td_thread_count();
    munit_assert_uint32(n, >=, 1);
    return MUNIT_OK;
}

/* ---- Thread create/join ---- */
static void thread_fn(void* arg) {
    int* flag = (int*)arg;
    *flag = 42;
}

static MunitResult test_thread_create_join(const void* params, void* data) {
    (void)params; (void)data;

    int flag = 0;
    td_thread_t t;
    td_err_t err = td_thread_create(&t, thread_fn, &flag);
    munit_assert_int(err, ==, TD_OK);

    err = td_thread_join(t);
    munit_assert_int(err, ==, TD_OK);
    munit_assert_int(flag, ==, 42);

    return MUNIT_OK;
}

static MunitTest platform_tests[] = {
    { "/vm_alloc_free",    test_vm_alloc_free,      NULL, NULL, 0, NULL },
    { "/vm_alloc_aligned", test_vm_alloc_aligned,   NULL, NULL, 0, NULL },
    { "/thread_count",     test_thread_count,        NULL, NULL, 0, NULL },
    { "/thread_join",      test_thread_create_join,  NULL, NULL, 0, NULL },
    { NULL, NULL, NULL, NULL, 0, NULL },
};

MunitSuite test_platform_suite = {
    "/platform", platform_tests, NULL, 1, 0
};
```

**Step 2: Register in test_main.c**

Add extern, child_suites entry, and main() patch.

**Step 3: Build and run**

Run: `cmake --build build && ./build/test_teide --suite /platform`
Expected: PASS

**Step 4: Commit**

```bash
git add test/test_platform.c test/test_main.c
git commit -m "test: add platform VM and thread tests"
```

---

## Task 12: Add test_sys.c — system allocator tests [x]

**Files:**
- Create: `test/test_sys.c`
- Modify: `test/test_main.c` (register suite)

**Step 1: Write test file**

```c
#include "munit.h"
#include <teide/td.h>
#include "../src/mem/sys.h"
#include <string.h>

/* ---- alloc/free ---- */
static MunitResult test_sys_alloc_free(const void* params, void* data) {
    (void)params; (void)data;

    void* ptr = td_sys_alloc(256);
    munit_assert_not_null(ptr);
    memset(ptr, 0xCC, 256);
    td_sys_free(ptr);
    return MUNIT_OK;
}

/* ---- realloc grow ---- */
static MunitResult test_sys_realloc_grow(const void* params, void* data) {
    (void)params; (void)data;

    void* ptr = td_sys_alloc(64);
    munit_assert_not_null(ptr);
    memset(ptr, 0xAA, 64);

    ptr = td_sys_realloc(ptr, 1024);
    munit_assert_not_null(ptr);
    /* First 64 bytes should be preserved */
    munit_assert_uint8(((uint8_t*)ptr)[0], ==, 0xAA);
    munit_assert_uint8(((uint8_t*)ptr)[63], ==, 0xAA);

    td_sys_free(ptr);
    return MUNIT_OK;
}

/* ---- strdup ---- */
static MunitResult test_sys_strdup(const void* params, void* data) {
    (void)params; (void)data;

    char* dup = td_sys_strdup("hello");
    munit_assert_not_null(dup);
    munit_assert_string_equal(dup, "hello");
    td_sys_free(dup);

    /* NULL input */
    char* n = td_sys_strdup(NULL);
    munit_assert_null(n);

    return MUNIT_OK;
}

/* ---- stats tracking ---- */
static MunitResult test_sys_stats(const void* params, void* data) {
    (void)params; (void)data;

    int64_t cur0, peak0;
    td_sys_get_stat(&cur0, &peak0);

    void* ptr = td_sys_alloc(4096);
    int64_t cur1, peak1;
    td_sys_get_stat(&cur1, &peak1);
    munit_assert_int64(cur1, >, cur0);
    munit_assert_int64(peak1, >=, cur1);

    td_sys_free(ptr);
    int64_t cur2, peak2;
    td_sys_get_stat(&cur2, &peak2);
    munit_assert_int64(cur2, <, cur1);
    munit_assert_int64(peak2, ==, peak1);  /* peak doesn't decrease */

    return MUNIT_OK;
}

static MunitTest sys_tests[] = {
    { "/alloc_free",   test_sys_alloc_free,   NULL, NULL, 0, NULL },
    { "/realloc_grow", test_sys_realloc_grow,  NULL, NULL, 0, NULL },
    { "/strdup",       test_sys_strdup,        NULL, NULL, 0, NULL },
    { "/stats",        test_sys_stats,         NULL, NULL, 0, NULL },
    { NULL, NULL, NULL, NULL, 0, NULL },
};

MunitSuite test_sys_suite = {
    "/sys", sys_tests, NULL, 1, 0
};
```

**Step 2: Register in test_main.c**

Add extern, child_suites entry, and main() patch.

**Step 3: Build and run**

Run: `cmake --build build && ./build/test_teide --suite /sys`
Expected: PASS

**Step 4: Commit**

```bash
git add test/test_sys.c test/test_main.c
git commit -m "test: add system allocator tests"
```

---

## Task 13: Add test_pipe.c — pipeline node tests [x]

**Files:**
- Create: `test/test_pipe.c`
- Modify: `test/test_main.c` (register suite)

**Step 1: Write test file**

```c
#include "munit.h"
#include <teide/td.h>
#include "../src/ops/pipe.h"

/* ---- new/free ---- */
static MunitResult test_pipe_new_free(const void* params, void* data) {
    (void)params; (void)data;

    td_pipe_t* p = td_pipe_new();
    munit_assert_not_null(p);

    /* Verify zero-init */
    munit_assert_null(p->op);
    munit_assert_null(p->inputs[0]);
    munit_assert_null(p->inputs[1]);
    munit_assert_null(p->materialized);
    munit_assert_int(p->spill_fd, ==, -1);

    td_pipe_free(p);
    return MUNIT_OK;
}

/* ---- free NULL safety ---- */
static MunitResult test_pipe_free_null(const void* params, void* data) {
    (void)params; (void)data;

    /* Should not crash */
    td_pipe_free(NULL);
    return MUNIT_OK;
}

/* ---- multiple alloc/free ---- */
static MunitResult test_pipe_multi(const void* params, void* data) {
    (void)params; (void)data;

    td_pipe_t* pipes[10];
    for (int i = 0; i < 10; i++) {
        pipes[i] = td_pipe_new();
        munit_assert_not_null(pipes[i]);
    }
    for (int i = 0; i < 10; i++) {
        td_pipe_free(pipes[i]);
    }
    return MUNIT_OK;
}

static MunitTest pipe_tests[] = {
    { "/new_free",   test_pipe_new_free,  NULL, NULL, 0, NULL },
    { "/free_null",  test_pipe_free_null, NULL, NULL, 0, NULL },
    { "/multi",      test_pipe_multi,     NULL, NULL, 0, NULL },
    { NULL, NULL, NULL, NULL, 0, NULL },
};

MunitSuite test_pipe_suite = {
    "/pipe", pipe_tests, NULL, 1, 0
};
```

**Step 2: Register in test_main.c**

Add extern, child_suites entry, and main() patch.

**Step 3: Build and run**

Run: `cmake --build build && ./build/test_teide --suite /pipe`
Expected: PASS

**Step 4: Commit**

```bash
git add test/test_pipe.c test/test_main.c
git commit -m "test: add pipeline node tests"
```

---

## Task 14: Final verification and cleanup [x]

**Step 1: Clean rebuild with -Werror**

Run: `cmake -B build_final -DCMAKE_BUILD_TYPE=Debug -DTEIDE_WERROR=ON && cmake --build build_final`
Expected: Clean build, zero warnings.

**Step 2: Run all tests**

Run: `cd build_final && ctest --output-on-failure`
Expected: All tests pass (should be ~230+ tests).

**Step 3: Run release build**

Run: `cmake -B build_release -DCMAKE_BUILD_TYPE=Release && cmake --build build_release && cd build_release && ctest --output-on-failure`
Expected: All tests pass under release optimization.

**Step 4: Move design doc to completed**

```bash
mv docs/plans/2026-03-04-complete-audit-remaining-design.md docs/plans/completed/
mv docs/plans/2026-03-04-complete-audit-remaining-plan.md docs/plans/completed/
git add docs/plans/
git commit -m "move completed plans: complete-audit-remaining"
```
