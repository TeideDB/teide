# Smart Optimizer — Design Document

**Date**: 2026-03-04
**Status**: Approved
**Goal**: Make Teide's optimizer do more heavy lifting so database builders get good query performance without implementing their own optimization passes.

## Scope

Four new optimizer passes, all pure DAG rewrites in `src/ops/opt.c`:

1. Predicate pushdown
2. Projection pushdown
3. Filter reordering
4. Partition pruning

## Pipeline Integration

New passes slot into the existing `td_optimize()` pipeline between factorize and fusion:

```
 1. Type inference        (existing)
 2. Constant folding      (existing)
 3. SIP                   (existing)
 4. Factorize             (existing)
 5. Predicate pushdown    ← NEW
 6. Projection pushdown   ← NEW
 7. Filter reordering     ← NEW
 8. Partition pruning     ← NEW
 9. Fusion                (existing, renumbered)
10. DCE                   (existing, renumbered)
```

**Ordering rationale**:
- Predicate pushdown before projection pushdown: pushdown may move filters below projections, changing which columns are needed at each level.
- Projection pushdown before filter reordering: projection narrows the column set, simplifying filter cost estimation.
- Filter reordering before partition pruning: reordering ensures the partition-key filter is innermost, making it visible to the pruning pass.
- All new passes before fusion: fusion needs the final DAG shape.

## Pass 5: Predicate Pushdown

Move filters as close to the data source as possible so fewer rows flow through expensive operators.

### Rules

**5a. Past PROJECT/SELECT/ALIAS**: Column-renaming/reordering ops don't change row count. Filter moves below with column references remapped through the projection mapping.

```
FILTER(pred, PROJECT(cols, input))  →  PROJECT(cols, FILTER(pred, input))
```

**5b. Past JOIN (one-sided predicates)**: If a filter references only left-side columns, push below the join onto the left input (vice versa for right-only). Predicates referencing both sides stay above.

```
FILTER(pred_left_only, JOIN(left, right))  →  JOIN(FILTER(pred_left_only, left), right)
```

**5c. Past GROUP (key-only predicates)**: If a filter tests only group-key columns (not aggregates), push below GROUP to reduce input size. Filters on aggregate results stay above.

```
FILTER(pred_on_key, GROUP(keys, aggs, input))  →  GROUP(keys, aggs, FILTER(pred_on_key, input))
```

**5d. Past EXPAND (source-side predicates)**: If a filter tests only source-side columns, push below the expand. Complementary to existing SIP pass (which handles target-side filters).

```
FILTER(pred_on_src, EXPAND(src_scan, rel))  →  EXPAND(FILTER(pred_on_src, src_scan), rel)
```

### Column origin tracking

The pass traces each column reference in the predicate back through the DAG to its source `OP_SCAN` node. If all referenced scans are on one side of a join/expand, the predicate is pushable to that side.

### Exclusions

- No transitive predicate generation.
- No pushing past SORT or WINDOW (order-dependent).
- No OR decomposition.

## Pass 6: Projection Pushdown

Only load/scan columns that are actually referenced downstream.

### Algorithm

**Step 1 — Build column reference sets**: For each node, determine which columns it references:
- `OP_SCAN`: produces all columns
- `OP_GROUP`: references key columns + aggregate input columns
- `OP_JOIN`: references join key columns from both sides
- `OP_SORT`: references sort-key columns
- `OP_PROJECT/SELECT`: references only the projected columns (key pruning point)
- `OP_FILTER`: references columns in the predicate
- `OP_WINDOW`: references partition keys, order keys, function inputs
- `OP_EXPAND`: references source column

**Step 2 — Propagate needed set downward**: Walk from root, tracking which columns are used. At each SCAN, the needed set identifies which table columns matter.

**Step 3 — Insert SELECT nodes**: If a SCAN produces 20 columns but only 3 are needed, insert `OP_SELECT` above the scan. For mmap'd columns, unused ones are never faulted in. For parted columns, entire segment vectors are skipped.

### Exclusions

- No lazy column loading (load-on-first-access).
- No partial column loading (first N rows only).

## Pass 7: Filter Reordering

When multiple filters are chained, reorder so cheapest/most-selective predicates execute first.

### Scoring criteria (priority order)

1. **Constant comparisons first** — `col = 42` before `col1 = col2` (one scan vs two). +0 vs +4.
2. **Narrower types first** — `TD_BOOL`/`TD_U8` (0) → `TD_I16` (1) → `TD_I32`/`TD_DATE` (2) → `TD_I64`/`TD_SYM`/`TD_F64` (3). Better cache utilization per morsel.
3. **Equality before range** — `EQ`/`NE` (+0) before `LT`/`LE`/`GT`/`GE` (+2). Equality is typically more selective.
4. **Simple before complex** — single-column predicates before `LIKE`/`ILIKE`/function calls (+4).

### Algorithm

Walk the DAG top-down looking for chains of `OP_FILTER` nodes. Collect all predicates, score each, rewire the chain so lowest-cost predicate is innermost (executed first).

Also handle AND trees within a single filter: `FILTER(AND(pred_a, pred_b), SCAN)` splits into `FILTER(pred_a, FILTER(pred_b, SCAN))` if reordering would help, then the chain is sorted.

### Exclusions

- No runtime selectivity estimation. Purely static heuristic based on type metadata.

## Pass 8: Partition Pruning

When filtering on the MAPCOMMON key column of a partitioned table, eliminate entire partitions at plan time.

### How partitioned tables work

`td_read_parted()` builds a table where:
- Column 0 is `TD_MAPCOMMON` with `key_values` (sorted partition keys) and `row_counts`
- Data columns have type `TD_PARTED_BASE + base_type`, containing per-partition segment vectors

### Pattern match

```
FILTER(comparison_on_mapcommon_col, SCAN(parted_table))
```

Where comparison is `EQ`, `NE`, `LT`, `LE`, `GT`, `GE` against a constant, and the column matches the MAPCOMMON key.

### Pruning logic

1. Extract constant value and comparison op from filter predicate.
2. Read MAPCOMMON key_values vector (partition keys are sorted).
3. Evaluate which partitions pass:
   - `date = '2025.03.01'` → only matching partition
   - `date >= '2025.01.01'` → binary search, keep tail
   - `date < '2024.06.01'` → binary search, keep head
4. Build `TD_SEL` bitmap on MAPCOMMON, set on scan node via `ext->sel`.
5. Executor already respects `TD_SEL` — pruned partitions are never touched.

### Compound predicates

`AND(date >= X, date < Y)` becomes two chained filters after AND-splitting (Pass 7). Each applies pruning independently; resulting `TD_SEL` bitmaps intersect naturally.

### Exclusions

- No runtime partition pruning (parameterized queries) — static constants only.
- No pruning on non-key columns (would require per-partition zone maps / statistics catalog).
