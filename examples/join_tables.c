/*
 * join_tables.c -- Teide example: inner join of two tables
 *
 * Creates an orders table (5 rows: order_id, customer_id, amount) and
 * a customers table (3 rows: customer_id, score). Performs an inner
 * join on customer_id and prints the result shape.
 *
 * Build:  cmake -B build -DTEIDE_EXAMPLES=ON && cmake --build build
 * Run:    ./build/example_join_tables
 */

#include <teide/td.h>
#include <stdio.h>

int main(void) {
    td_heap_init();
    td_sym_init();

    /* --- Orders table: 5 rows ------------------------------------------ */

    int64_t oid_data[] = {1, 2, 3, 4, 5};
    int64_t cid_data[] = {10, 20, 10, 30, 20};
    int64_t amt_data[] = {500, 300, 700, 200, 450};
    int64_t n_orders = 5;

    td_t* oid_vec = td_vec_from_raw(TD_I64, oid_data, n_orders);
    td_t* cid_vec = td_vec_from_raw(TD_I64, cid_data, n_orders);
    td_t* amt_vec = td_vec_from_raw(TD_I64, amt_data, n_orders);

    int64_t sym_oid = td_sym_intern("order_id",    8);
    int64_t sym_cid = td_sym_intern("customer_id", 11);
    int64_t sym_amt = td_sym_intern("amount",      6);

    td_t* orders = td_table_new(3);
    orders = td_table_add_col(orders, sym_oid, oid_vec);
    orders = td_table_add_col(orders, sym_cid, cid_vec);
    orders = td_table_add_col(orders, sym_amt, amt_vec);
    td_release(oid_vec);
    td_release(cid_vec);
    td_release(amt_vec);

    printf("Orders table:    %lld rows, %lld cols\n",
           (long long)td_table_nrows(orders),
           (long long)td_table_ncols(orders));

    /* --- Customers table: 3 rows --------------------------------------- */

    int64_t cust_cid_data[]   = {10, 20, 30};
    int64_t cust_score_data[] = {85, 92, 78};
    int64_t n_customers = 3;

    td_t* cust_cid_vec   = td_vec_from_raw(TD_I64, cust_cid_data,   n_customers);
    td_t* cust_score_vec = td_vec_from_raw(TD_I64, cust_score_data, n_customers);

    int64_t sym_score = td_sym_intern("score", 5);

    td_t* customers = td_table_new(2);
    customers = td_table_add_col(customers, sym_cid, cust_cid_vec);
    customers = td_table_add_col(customers, sym_score, cust_score_vec);
    td_release(cust_cid_vec);
    td_release(cust_score_vec);

    printf("Customers table: %lld rows, %lld cols\n",
           (long long)td_table_nrows(customers),
           (long long)td_table_ncols(customers));

    /* --- Inner join on customer_id ------------------------------------- */

    td_graph_t* g = td_graph_new(orders);

    td_op_t* left_op  = td_const_table(g, orders);
    td_op_t* right_op = td_const_table(g, customers);

    td_op_t* key_op     = td_scan(g, "customer_id");
    td_op_t* lk_arr[]   = { key_op };
    td_op_t* rk_arr[]   = { key_op };

    /* join_type 0 = inner join */
    td_op_t* join_op = td_join(g, left_op, lk_arr, right_op, rk_arr, 1, 0);

    td_t* result = td_execute(g, join_op);
    if (TD_IS_ERR(result)) {
        printf("ERROR: %s\n", td_err_str(TD_ERR_CODE(result)));
        td_graph_free(g);
        td_release(orders);
        td_release(customers);
        td_sym_destroy();
        td_heap_destroy();
        return 1;
    }

    /* --- Print results ------------------------------------------------- */

    int64_t nrows = td_table_nrows(result);
    int64_t ncols = td_table_ncols(result);
    printf("\nInner join result: %lld rows, %lld cols\n",
           (long long)nrows, (long long)ncols);

    /* Every order has a matching customer (all cids 10,20,30 exist),
     * so we expect 5 result rows with columns from both tables. */
    printf("  Expected: 5 rows (every order matched a customer)\n");

    /* Print joined data */
    if (nrows > 0) {
        td_t* res_cid = td_table_get_col(result, sym_cid);
        td_t* res_amt = td_table_get_col(result, sym_amt);
        td_t* res_score = td_table_get_col(result, sym_score);

        if (res_cid && res_amt && res_score) {
            int64_t* cids   = (int64_t*)td_data(res_cid);
            int64_t* amts   = (int64_t*)td_data(res_amt);
            int64_t* scores = (int64_t*)td_data(res_score);

            printf("\n  %-14s %-8s %s\n", "customer_id", "amount", "score");
            printf("  %-14s %-8s %s\n", "-----------", "------", "-----");
            for (int64_t i = 0; i < nrows; i++) {
                printf("  %-14lld %-8lld %lld\n",
                       (long long)cids[i],
                       (long long)amts[i],
                       (long long)scores[i]);
            }
        }
    }

    /* --- Cleanup ------------------------------------------------------- */

    td_release(result);
    td_graph_free(g);
    td_release(orders);
    td_release(customers);
    td_sym_destroy();
    td_heap_destroy();

    printf("\nDone.\n");
    return 0;
}
