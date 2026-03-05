# Production Hardening — Design Document

**Status**: Approved
**Date**: 2026-03-05

## Goal

Production-harden Teide: optimize ASOF join to DuckDB-style sort-merge, add fuzz testing, CI/CD pipeline, and benchmarks.

## 1. ASOF Join Optimization

Replace brute-force O(N*M) `exec_window_join` with DuckDB-style sort-merge O(N+M).

### Semantics (matching DuckDB)

- One match per left row: find the most recent right row where `right.time <= left.time`
- Zero or more equality keys for partitioning (e.g., symbol/ticker)
- Inner join (drop unmatched) and left outer join (NULL for unmatched)
- No window bounds, no aggregation over multiple matches
- Result: left columns + matched right columns (excluding duplicate key columns)

### Algorithm

1. Sort both tables by `(equality_keys, time_key)` if not already sorted
2. Two-pointer merge: for each left row, advance right pointer to find last right row where `right.time <= left.time` within the same equality partition
3. O(N+M) after sorting, O(1) extra memory during merge phase

### API Change

Replace current `td_window_join` with simplified `td_asof_join`:

```c
td_op_t* td_asof_join(td_graph_t* g,
                       td_op_t* left_table, td_op_t* right_table,
                       td_op_t* time_key,
                       td_op_t** eq_keys, uint8_t n_eq_keys,
                       uint8_t join_type);  /* 0=inner, 1=left */
```

### Ext struct change

Replace `wjoin` with:
```c
struct {
    td_op_t*   time_key;
    td_op_t**  eq_keys;
    uint8_t    n_eq_keys;
    uint8_t    join_type;  /* 0=inner, 1=left */
} asof;
```

### Files

- `include/teide/td.h` — replace wjoin struct, update API declaration
- `src/ops/graph.c` — rewrite DAG builder + fixup
- `src/ops/exec.c` — rewrite executor with sort-merge
- `src/ops/opt.c` — update traversals
- `src/ops/fuse.c` — update traversal
- `test/test_exec.c` — rewrite window_join tests for new semantics

## 2. Fuzz Testing (libFuzzer)

### Targets

Two fuzz harnesses with ASan+UBSan:
- `fuzz/fuzz_col_load.c` — feeds random bytes to `td_col_load`, catches buffer overflows, integer overflows, and malformed headers in column deserialization
- `fuzz/fuzz_csv_read.c` — feeds random text to CSV parser, catches malformed input handling

### Build Integration

CMake option `TEIDE_FUZZ=ON`:
- Enables `-fsanitize=fuzzer` on fuzz target executables
- Builds fuzz targets as separate executables
- Maintains ASan+UBSan alongside fuzzer sanitizer

### Seed Corpus

- `fuzz/corpus/col/` — valid .col files from test_store tests
- `fuzz/corpus/csv/` — valid .csv files from test_csv tests

## 3. CI/CD (GitHub Actions)

### Workflow: `.github/workflows/ci.yml`

Matrix build:
- OS: ubuntu-latest, macos-latest
- Compiler: gcc, clang (Linux), clang (macOS)
- Build type: Debug, Release

Steps per job:
1. Checkout
2. Configure with CMake
3. Build (capture warnings)
4. Run test suite with `ctest --output-on-failure`

Debug builds enable ASan+UBSan (already configured in CMakeLists.txt).

Triggers:
- Push to master
- All pull requests

### Optional: benchmark tracking

Run benchmarks on push to master, store results as artifacts, compare against previous run.

## 4. Benchmarks

### Microbenchmarks: `bench/bench_teide.c`

Per-operation timing with parametric sizes (1K, 100K, 10M rows):
- `vec_add` — element-wise addition
- `filter` — predicate evaluation + selection
- `sort` — multi-column sort
- `group` — group-by with SUM aggregation
- `join` — hash join
- `asof_join` — ASOF join on time-series data
- `scan` — column scan throughput
- `csv_read` — CSV parse throughput

Output: operation name, row count, elapsed_ns, rows_per_sec.

### End-to-end queries: `bench/bench_queries.c`

Simplified TPC-H-like workloads:
- Q1: scan + filter + group + sum (analytics)
- Q2: join + filter + sort (relational)
- Q3: ASOF join on time-series data

### Build Integration

CMake option `TEIDE_BENCH=ON`:
- Builds benchmark executables
- Not built by default (opt-in)

## Decisions

- **ASOF join**: DuckDB-style (single match, no aggregation, no window bounds)
- **Fuzzing**: libFuzzer with ASan+UBSan coverage guidance
- **CI/CD**: GitHub Actions with Linux+macOS matrix
- **Benchmarks**: Micro (per-op) + end-to-end (TPC-H-like), opt-in build
- **Ordering**: ASOF join first (functional), then fuzz+CI+bench (infrastructure)
