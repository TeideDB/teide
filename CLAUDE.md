# CLAUDE.md

## What is Teide?

Pure C17 zero-dependency columnar dataframe library. Lazy fusion API → operation DAG → optimizer → fused morsel-driven execution.

## Build & Test

```bash
# Debug (ASan + UBSan)
cmake -B build -DCMAKE_BUILD_TYPE=Debug && cmake --build build

# Release
cmake -B build_release -DCMAKE_BUILD_TYPE=Release && cmake --build build_release

# Run all tests
cd build && ctest --output-on-failure

# Run a single test suite
./build/test_teide --suite /vec
```

## Architecture

Core abstraction is `td_t` — a 32-byte block header. Every object (atom, vector, list, table) is a `td_t` with data following at byte 32.

**Memory**: buddy allocator with thread-local arenas, slab cache for small allocations, COW ref counting.

**Execution pipeline**:
1. Build lazy DAG: `td_graph_new(df)` → `td_scan/td_add/td_filter/...` → `td_execute(g, root)`
2. Optimizer: type inference → constant fold → predicate pushdown → CSE → fusion → DCE
3. Fused executor: bytecode over register slots, morsel-by-morsel (1024 elements)

## Code Conventions

- **Prefix**: all public symbols `td_`, internal functions `static`
- **Constants**: `TD_UPPER_SNAKE_CASE`
- **Types**: `td_name_t` (typedef'd structs)
- **Morsel-only processing**: all vector loops chunk through `td_morsel_t` (1024 elements)
- **Error returns**: `td_t*` functions use `TD_ERR_PTR()` / `TD_IS_ERR()`; other functions return `td_err_t`
- **No external deps**: pure C17, single public header `include/teide/td.h`
- **No system allocator**: never use `malloc`/`calloc`/`realloc`/`free`. Use `td_alloc()`/`td_free()`.
