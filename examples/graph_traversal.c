/*
 * graph_traversal.c -- Teide example: CSR graph + variable-length expansion
 *
 * Creates a directed graph with 5 nodes and 5 edges:
 *   0 -> 1, 0 -> 2, 1 -> 3, 2 -> 3, 3 -> 4
 *
 * Builds a CSR relationship, then performs a variable-length expansion
 * (BFS) from node 0 with depth 1..2 hops.
 *
 * Build:  cmake -B build -DTEIDE_EXAMPLES=ON && cmake --build build
 * Run:    ./build/example_graph_traversal
 */

#include <teide/td.h>
#include <stdio.h>

int main(void) {
    td_heap_init();
    td_sym_init();

    /* --- Build edge table ---------------------------------------------- */

    int64_t src_data[] = {0, 0, 1, 2, 3};
    int64_t dst_data[] = {1, 2, 3, 3, 4};
    int64_t n_edges = 5;

    td_t* src_vec = td_vec_from_raw(TD_I64, src_data, n_edges);
    td_t* dst_vec = td_vec_from_raw(TD_I64, dst_data, n_edges);

    int64_t sym_src = td_sym_intern("src", 3);
    int64_t sym_dst = td_sym_intern("dst", 3);

    td_t* edge_tbl = td_table_new(2);
    edge_tbl = td_table_add_col(edge_tbl, sym_src, src_vec);
    edge_tbl = td_table_add_col(edge_tbl, sym_dst, dst_vec);
    td_release(src_vec);
    td_release(dst_vec);

    printf("Edge table: %lld edges\n", (long long)td_table_nrows(edge_tbl));

    /* --- Build CSR relationship ---------------------------------------- */

    int64_t n_nodes = 5;
    td_rel_t* rel = td_rel_from_edges(edge_tbl, "src", "dst",
                                       n_nodes, n_nodes, false);
    if (!rel) {
        printf("ERROR: failed to build CSR relationship\n");
        td_release(edge_tbl);
        td_sym_destroy();
        td_heap_destroy();
        return 1;
    }
    printf("CSR relationship built for %lld nodes\n", (long long)n_nodes);

    /* --- Variable-length expansion from node 0, depth 1..2 ------------- */

    int64_t start_data[] = {0};
    td_t* start_vec = td_vec_from_raw(TD_I64, start_data, 1);

    td_graph_t* g = td_graph_new(NULL);
    td_op_t* src_op = td_const_vec(g, start_vec);

    /* direction=0 (forward), min_depth=1, max_depth=2, track_path=false */
    td_op_t* var_exp = td_var_expand(g, src_op, rel, 0, 1, 2, false);

    td_t* result = td_execute(g, var_exp);
    if (TD_IS_ERR(result)) {
        printf("ERROR: %s\n", td_err_str(TD_ERR_CODE(result)));
        td_graph_free(g);
        td_release(start_vec);
        td_rel_free(rel);
        td_release(edge_tbl);
        td_sym_destroy();
        td_heap_destroy();
        return 1;
    }

    /* --- Print results ------------------------------------------------- */

    int64_t nrows = td_table_nrows(result);
    int64_t ncols = td_table_ncols(result);
    printf("\nVar-expand from node 0, depth 1..2:\n");
    printf("  Result: %lld rows, %lld cols\n",
           (long long)nrows, (long long)ncols);

    /* Expected reachable nodes:
     *   depth 1: 0->1, 0->2          => nodes 1, 2
     *   depth 2: 1->3, 2->3 (dedup)  => node  3
     *   Total: 3 reachable nodes */
    printf("  Expected 3 reachable nodes (1, 2 at depth 1; 3 at depth 2)\n");

    /* Result columns: _start (source node), _end (reached node), _depth */
    td_t* start_col = td_table_get_col_idx(result, 0);
    td_t* end_col   = td_table_get_col_idx(result, 1);
    td_t* depth_col = td_table_get_col_idx(result, 2);

    if (start_col && end_col && depth_col) {
        int64_t* starts = (int64_t*)td_data(start_col);
        int64_t* ends   = (int64_t*)td_data(end_col);
        int64_t* depths = (int64_t*)td_data(depth_col);
        printf("\n  %-8s %-8s %s\n", "start", "end", "depth");
        printf("  %-8s %-8s %s\n", "-----", "---", "-----");
        for (int64_t i = 0; i < nrows; i++) {
            printf("  %-8lld %-8lld %lld\n",
                   (long long)starts[i],
                   (long long)ends[i],
                   (long long)depths[i]);
        }
    }

    /* --- Cleanup ------------------------------------------------------- */

    td_release(result);
    td_graph_free(g);
    td_release(start_vec);
    td_rel_free(rel);
    td_release(edge_tbl);
    td_sym_destroy();
    td_heap_destroy();

    printf("\nDone.\n");
    return 0;
}
