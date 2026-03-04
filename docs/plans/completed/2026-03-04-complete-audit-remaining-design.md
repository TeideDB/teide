# Complete Remaining Audit Phases — Design Document

**Status**: Approved
**Date**: 2026-03-04

## Goal

Finish the remaining phases of the Teide completeness audit: ASOF window join, string/list/table serialization, and minor test coverage for 6 untested modules.

## Phase 4.2 — OP_WINDOW_JOIN (ASOF Join)

For each row in the left table, find the nearest matching row in the right table by an ordered key, optionally within a tolerance window.

**Algorithm**: Merge-scan. Both tables sorted by join key. For each left row, advance right pointer to nearest key <= left key. Reject if gap exceeds tolerance. Output: left columns + matched right columns (NULL if no match within tolerance).

**API**: `td_window_join(graph, left, right, left_key, right_key, tolerance)`

**Files**:
- `include/teide/td.h` — add `OP_WINDOW_JOIN` opcode, `td_window_join()` declaration
- `src/ops/graph.c` — DAG builder
- `src/ops/exec.c` — `exec_window_join()` implementation
- `src/ops/opt.c` — type inference for new opcode
- `test/test_exec.c` — 5-6 tests (exact match, nearest-before, tolerance, NULLs, empty)

## Phase 5.1 — TD_STR Column Serialization

On-disk format:
```
[header: type + count]
[offsets array: (count+1) × uint32_t]
[string data: contiguous bytes]
```

- `td_col_save()`: serialize offset + data
- `td_col_load()`: deserialize and reconstruct string vector
- `td_col_mmap()`: memory-map with lazy string materialization

**Files**: `src/store/col.c`, `test/test_store.c`

## Phase 5.2 — TD_LIST / TD_TABLE Column Serialization

Recursive serialization of nested structures.

- TD_LIST: serialize element count + recursive serialization of each element
- TD_TABLE: serialize schema + per-column recursive serialization

**Files**: `src/store/col.c`, `test/test_store.c`

## Phase 6 — Minor Test Coverage

3-5 tests per module for the 6 remaining untested modules:

- `test_meta.c` — metadata read/write roundtrip
- `test_types.c` — type utility functions
- `test_platform.c` — platform abstractions
- `test_sys.c` — system memory allocation
- `test_plan.c` — query plan generation
- `test_pipe.c` — pipelining

## Decisions

- **ASOF join algorithm**: Merge-scan (not binary search) — simpler, same O(n+m) for sorted inputs
- **Serialization order**: Strings first (simpler), then lists/tables (recursive)
- **Test scope**: All 6 remaining modules, full coverage
- **Ordering**: ASOF join → serialization → test coverage
