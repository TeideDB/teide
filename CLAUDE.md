# CLAUDE.md

## What is Teide?

Pure C17 zero-dependency columnar dataframe library with native graph engine. Lazy fusion API → operation DAG → optimizer → fused morsel-driven execution. CSR edge indices, graph traversal opcodes, worst-case optimal joins, and sideways information passing — all in the same pipeline.

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
2. Optimizer: type inference → constant fold → SIP → factorize → fusion → DCE
3. Fused executor: bytecode over register slots, morsel-by-morsel (1024 elements)

**Graph engine**: CSR edge indices (`td_csr_t`, `td_rel_t`) alongside columnar tables.
- Storage: double-indexed CSR (forward + reverse), persisted as `.col` files, supports mmap
- Opcodes: `OP_EXPAND` (1-hop), `OP_VAR_EXPAND` (BFS), `OP_SHORTEST_PATH`, `OP_WCO_JOIN` (LFTJ)
- Factorized execution: `td_fvec_t` / `td_ftable_t` avoid materializing cross-products
- Optimizer: SIP pass propagates `TD_SEL` bitmaps backward through `OP_EXPAND` chains

## Code Conventions

- **Prefix**: all public symbols `td_`, internal functions `static`
- **Constants**: `TD_UPPER_SNAKE_CASE`
- **Types**: `td_name_t` (typedef'd structs)
- **Morsel-only processing**: all vector loops chunk through `td_morsel_t` (1024 elements)
- **Error returns**: `td_t*` functions use `TD_ERR_PTR()` / `TD_IS_ERR()`; other functions return `td_err_t`
- **No external deps**: pure C17, single public header `include/teide/td.h`
- **No system allocator**: never use `malloc`/`calloc`/`realloc`/`free`. Use `td_alloc()`/`td_free()`.

## Key File Paths

```
include/teide/td.h         Single public header (all types, opcodes, API)
src/store/csr.{h,c}        CSR storage — build, save, load, mmap, free
src/ops/graph.c             DAG construction (td_expand, td_var_expand, etc.)
src/ops/exec.c              Fused morsel-driven executor (all opcodes)
src/ops/opt.c               Optimizer passes (type inference, SIP, factorize, fusion, DCE)
src/ops/lftj.{h,c}         Leapfrog Triejoin — iterator, search, enumeration
src/ops/fvec.{h,c}         Factorized vectors — td_fvec_t, td_ftable_t
test/test_csr.c             Graph engine tests (42 tests)
```
