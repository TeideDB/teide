/*
 * analytics.c -- Teide example: group-by + sum aggregation
 *
 * Creates a 12-row sales table (region, category, amount), groups by
 * region, and computes sum(amount) per region.
 *
 * Build:  cmake -B build -DTEIDE_EXAMPLES=ON && cmake --build build
 * Run:    ./build/example_analytics
 */

#include <teide/td.h>
#include <stdio.h>

int main(void) {
    td_heap_init();
    td_sym_init();

    /* --- Build a 12-row sales table ------------------------------------ */

    int64_t region_data[]   = {0, 0, 0, 0, 1, 1, 1, 1, 2, 2, 2, 2};
    int64_t amount_data[]   = {100, 200, 150, 300, 400, 50, 250, 350, 175, 225, 125, 275};
    int64_t n = 12;

    td_t* region_vec = td_vec_from_raw(TD_I64, region_data, n);
    td_t* amount_vec = td_vec_from_raw(TD_I64, amount_data, n);

    int64_t sym_region = td_sym_intern("region", 6);
    int64_t sym_amount = td_sym_intern("amount", 6);

    td_t* tbl = td_table_new(2);
    tbl = td_table_add_col(tbl, sym_region, region_vec);
    tbl = td_table_add_col(tbl, sym_amount, amount_vec);

    td_release(region_vec);
    td_release(amount_vec);

    printf("Input table: %lld rows, %lld cols\n",
           (long long)td_table_nrows(tbl),
           (long long)td_table_ncols(tbl));

    /* --- Group by region, sum(amount) ---------------------------------- */

    td_graph_t* g = td_graph_new(tbl);

    td_op_t* reg_op = td_scan(g, "region");
    td_op_t* amt_op = td_scan(g, "amount");

    td_op_t* keys[]    = { reg_op };
    td_op_t* agg_ins[] = { amt_op };
    uint16_t agg_ops[] = { OP_SUM };

    td_op_t* grp = td_group(g, keys, 1, agg_ops, agg_ins, 1);

    /* --- Execute ------------------------------------------------------- */

    td_t* result = td_execute(g, grp);
    if (TD_IS_ERR(result)) {
        printf("ERROR: %s\n", td_err_str(TD_ERR_CODE(result)));
        td_graph_free(g);
        td_release(tbl);
        td_sym_destroy();
        td_heap_destroy();
        return 1;
    }

    /* --- Print results ------------------------------------------------- */

    int64_t nrows = td_table_nrows(result);
    int64_t ncols = td_table_ncols(result);
    printf("Result table: %lld rows, %lld cols\n",
           (long long)nrows, (long long)ncols);

    td_t* key_col = td_table_get_col_idx(result, 0);
    td_t* sum_col = td_table_get_col_idx(result, 1);
    int64_t* regions = (int64_t*)td_data(key_col);
    int64_t* sums    = (int64_t*)td_data(sum_col);

    /* Expected sums:
     *   region 0: 100+200+150+300 = 750
     *   region 1: 400+50+250+350  = 1050
     *   region 2: 175+225+125+275 = 800 */
    printf("\nGroup by region, sum(amount):\n");
    printf("  %-10s %s\n", "region", "sum(amount)");
    printf("  %-10s %s\n", "------", "-----------");
    for (int64_t i = 0; i < nrows; i++) {
        printf("  %-10lld %lld\n", (long long)regions[i], (long long)sums[i]);
    }

    /* --- Cleanup ------------------------------------------------------- */

    td_release(result);
    td_graph_free(g);
    td_release(tbl);
    td_sym_destroy();
    td_heap_destroy();

    printf("\nDone.\n");
    return 0;
}
