# Teide

Pure C17 zero-dependency columnar dataframe engine with lazy fusion execution.

## Features

- **32-byte block header** — unified `td_t` type for atoms, vectors, lists, and tables
- **Buddy allocator** with thread-local arenas, slab cache, COW ref counting
- **Lazy DAG execution** — build operation graph → optimize → fused morsel-driven execution
- **Parallel execution** — morsel-driven thread pool, radix-partitioned hash tables
- **Zero external dependencies** — pure C17, single public header

## Build

```bash
# Debug (ASan + UBSan)
cmake -B build -DCMAKE_BUILD_TYPE=Debug
cmake --build build

# Release
cmake -B build_release -DCMAKE_BUILD_TYPE=Release
cmake --build build_release

# Run tests
cd build && ctest --output-on-failure
```

## API

Single public header: `include/teide/td.h`

```c
#include <teide/td.h>

int main(void) {
    td_heap_init();
    td_sym_init();

    td_t *tbl = td_read_csv("data.csv");
    td_graph_t *g = td_graph_new(tbl);

    td_op_t *key = td_scan(g, "category");
    td_op_t *val = td_scan(g, "amount");
    td_op_t *agg = td_sum(g, val);

    uint16_t ops[] = {50};  // OP_SUM
    td_op_t *keys[] = {key};
    td_op_t *ins[] = {agg};
    td_op_t *grp = td_group(g, keys, 1, ops, ins, 1);

    td_op_t *root = td_optimize(g, grp);
    td_t *result = td_execute(g, root);

    td_graph_free(g);
    td_release(result);
    td_sym_destroy();
    td_heap_destroy();
    return 0;
}
```

## License

MIT
