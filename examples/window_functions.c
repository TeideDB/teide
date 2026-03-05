/*
 * window_functions.c -- Teide example: RANK window function
 *
 * Creates a 6-row table (dept, revenue) and computes RANK() partitioned
 * by dept, ordered by revenue DESC.
 *
 * Build:  cmake -B build -DTEIDE_EXAMPLES=ON && cmake --build build
 * Run:    ./build/example_window_functions
 */

#include <teide/td.h>
#include <stdio.h>

int main(void) {
    td_heap_init();
    td_sym_init();

    /* --- Build a 6-row table ------------------------------------------- */

    int64_t dept_data[]    = {1, 1, 1, 2, 2, 2};
    int64_t revenue_data[] = {300, 100, 200, 500, 400, 600};
    int64_t n = 6;

    td_t* dept_vec    = td_vec_from_raw(TD_I64, dept_data,    n);
    td_t* revenue_vec = td_vec_from_raw(TD_I64, revenue_data, n);

    int64_t sym_dept    = td_sym_intern("dept",    4);
    int64_t sym_revenue = td_sym_intern("revenue", 7);

    td_t* tbl = td_table_new(2);
    tbl = td_table_add_col(tbl, sym_dept,    dept_vec);
    tbl = td_table_add_col(tbl, sym_revenue, revenue_vec);
    td_release(dept_vec);
    td_release(revenue_vec);

    printf("Input table: %lld rows, %lld cols\n",
           (long long)td_table_nrows(tbl),
           (long long)td_table_ncols(tbl));

    /* --- Window: RANK() PARTITION BY dept ORDER BY revenue DESC -------- */

    td_graph_t* g = td_graph_new(tbl);

    td_op_t* tbl_op     = td_const_table(g, tbl);
    td_op_t* dept_op    = td_scan(g, "dept");
    td_op_t* revenue_op = td_scan(g, "revenue");

    td_op_t* parts[]       = { dept_op };
    td_op_t* orders[]      = { revenue_op };
    uint8_t  order_descs[] = { 1 };            /* 1 = descending */
    uint8_t  func_kinds[]  = { TD_WIN_RANK };
    td_op_t* func_inputs[] = { revenue_op };
    int64_t  func_params[] = { 0 };

    td_op_t* win = td_window_op(g, tbl_op,
                                parts, 1,
                                orders, order_descs, 1,
                                func_kinds, func_inputs, func_params, 1,
                                TD_FRAME_ROWS,
                                TD_BOUND_UNBOUNDED_PRECEDING,
                                TD_BOUND_UNBOUNDED_FOLLOWING,
                                0, 0);

    td_t* result = td_execute(g, win);
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

    /* The window op appends a rank column to the original 2 columns */
    printf("  (original 2 cols + 1 window column = %lld cols)\n",
           (long long)ncols);

    /* Print all columns */
    if (nrows > 0 && ncols >= 3) {
        td_t* d_col = td_table_get_col_idx(result, 0);
        td_t* r_col = td_table_get_col_idx(result, 1);
        td_t* w_col = td_table_get_col_idx(result, 2);

        if (d_col && r_col && w_col) {
            int64_t* depts    = (int64_t*)td_data(d_col);
            int64_t* revenues = (int64_t*)td_data(r_col);
            int64_t* ranks    = (int64_t*)td_data(w_col);

            printf("\n  %-6s %-10s %s\n", "dept", "revenue", "rank");
            printf("  %-6s %-10s %s\n", "----", "-------", "----");
            for (int64_t i = 0; i < nrows; i++) {
                printf("  %-6lld %-10lld %lld\n",
                       (long long)depts[i],
                       (long long)revenues[i],
                       (long long)ranks[i]);
            }
        }
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
