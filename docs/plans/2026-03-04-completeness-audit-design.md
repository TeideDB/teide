# Teide Completeness Audit — Design Document

**Status**: Approved
**Date**: 2026-03-04

## Goal

Bring Teide to full feature completeness: test all critical untested modules, fix known bugs, add missing API wrappers, implement remaining opcodes, and extend serialization. Bottom-up ordering — test existing code first, then fix, then build new.

## Phase 1: Test Harness for Existing Code

Add dedicated test suites for the 6 most critical untested modules. Catches bugs before adding features.

### 1.1 — test_exec.c (executor tests)

Test every implemented opcode end-to-end: build DAG → optimize → execute → verify output.

Coverage areas:
- Unary element-wise ops (NEG, ABS, NOT, SQRT, LOG, EXP, CEIL, FLOOR, ISNULL, CAST)
- Binary element-wise ops (ADD, SUB, MUL, DIV, MOD, EQ, NE, LT, LE, GT, GE, AND, OR, MIN2, MAX2, IF)
- String ops (LIKE, ILIKE, UPPER, LOWER, STRLEN, SUBSTR, REPLACE, TRIM, CONCAT)
- Date/time ops (EXTRACT, DATE_TRUNC)
- Reductions (SUM, PROD, MIN, MAX, COUNT, AVG, FIRST, LAST, STDDEV, STDDEV_POP, VAR, VAR_POP)
- Structural ops (FILTER, SORT, GROUP, JOIN, WINDOW, HEAD, TAIL, SELECT, MATERIALIZE, ALIAS)
- Partitioned table operations and morsel boundary conditions
- NULL propagation for all op categories
- ~40-60 test cases

### 1.2 — test_fvec.c (factorized vectors)

- `td_fvec_t` creation, append, iteration
- `td_ftable_t` cross-product avoidance
- Memory management (retain/release)
- ~10-15 test cases

### 1.3 — test_lftj.c (Leapfrog TrieJoin)

- Iterator init, seek, next
- Leapfrog enumeration over sorted relations
- Triangle queries, 4-clique patterns
- Edge cases: empty iterators, single-element relations
- ~10-15 test cases

### 1.4 — test_fuse.c (fusion pass)

- Fusible operation chains get merged into single bytecode sequences
- Fused bytecode produces identical results to unfused execution
- Fusion barriers (GROUP, JOIN, WINDOW) prevent incorrect fusion
- ~8-10 test cases

### 1.5 — test_csv.c (CSV I/O)

- Read/write roundtrip for all serializable column types
- Edge cases: empty files, missing values, quoted strings with commas/newlines, large files
- Type inference from CSV data
- ~10-12 test cases

### 1.6 — test_sel.c (selection vectors)

- Bitmap creation from predicates
- AND/OR/NOT composition
- Iteration and population count
- ~5-8 test cases

## Phase 2: Bug Fixes & Correctness

### 2.1 — Fix `td_block_copy()` child ref retention

`src/core/block.c:73-76` — copying blocks with STR/LIST/TABLE children doesn't retain child refs. Either expose `td_retain_owned_refs()` from `arena.c` or inline the retention logic in `td_block_copy()`. Add regression tests to `test_cow.c`.

### 2.2 — Fix bugs surfaced by Phase 1

Placeholder — Phase 1 testing will likely surface issues. Each fix gets a regression test.

## Phase 3: Missing API Wrappers

Add public API functions for opcodes that already work internally but have no `td_*()` wrapper.

### 3.1 — `td_stddev()` / `td_stddev_pop()`

- Declare in `include/teide/td.h`
- Implement in `src/ops/graph.c` (DAG builder) — same pattern as `td_sum()`, `td_avg()`
- Add tests in test_exec.c

### 3.2 — `td_var()` / `td_var_pop()`

- Same pattern as 3.1
- Add tests in test_exec.c

## Phase 4: New Feature Implementation

### 4.1 — OP_COUNT_DISTINCT

Implement `exec_count_distinct()` in `src/ops/exec.c`.

- Hash-based distinct counting
- Handle all column types: integers, floats, strings, symbols
- NULL handling: NULLs are not counted (SQL semantics)
- Morsel-driven: accumulate hash set across morsels, emit final count
- Add case in executor switch statement
- Add tests in test_exec.c

### 4.2 — OP_WINDOW_JOIN (ASOF join semantics)

Implement `exec_window_join()` in `src/ops/exec.c`.

ASOF join: for each row in the left table, find the nearest matching row in the right table by a timestamp/ordered key, optionally within a tolerance window.

Algorithm:
1. Both tables must be sorted by the join key
2. For each left row, binary search (or merge-scan) the right table for the nearest key ≤ left key
3. Optional tolerance: reject matches where `left_key - right_key > tolerance`
4. Output: left columns + matched right columns (NULL if no match within tolerance)

API: `td_window_join(graph, left, right, left_key, right_key, tolerance)`

Add tests in test_exec.c covering: exact matches, nearest-before matches, tolerance filtering, NULLs, empty tables.

### 4.3 — Remove OP_PROJECT

- Remove `OP_PROJECT` from opcode enum in `td.h`
- Remove `td_project()` from `td.h` and `src/ops/graph.c`
- Update optimizer passes if they reference OP_PROJECT
- Users should use `td_select()` / OP_SELECT for column projection

## Phase 5: Serialization Extension

### 5.1 — TD_STR column serialization

Extend `src/store/col.c` to support string columns.

On-disk format:
```
[header: type + count]
[offsets array: (count+1) × uint32_t]
[string data: contiguous bytes]
```

- `td_col_save()`: serialize offset + data
- `td_col_load()`: deserialize and reconstruct string vector
- `td_col_mmap()`: memory-map with lazy string materialization
- Add tests in test_store.c

### 5.2 — TD_LIST / TD_TABLE column serialization

Recursive serialization of nested structures. More complex due to heterogeneous element types.

- TD_LIST: serialize element count + recursive serialization of each element
- TD_TABLE: serialize schema + per-column recursive serialization
- Add tests in test_store.c

## Phase 6: Minor Test Coverage

Fill remaining gaps for lower-priority modules:

- `store/meta.c` — metadata read/write roundtrip tests
- `core/types.c` — type utility function tests
- `core/platform.c` — platform abstraction tests
- `mem/sys.c` — system memory allocation tests
- `ops/plan.c` — query plan generation tests
- `ops/pipe.c` — pipelining tests

~3-5 test cases per module.

## Decisions

- **OP_PROJECT**: Remove (redundant with OP_SELECT)
- **OP_WINDOW_JOIN**: ASOF join semantics (nearest match by ordered key)
- **Ordering**: Bottom-up (tests → fixes → new features)
- **Scope**: Everything — full completeness audit
