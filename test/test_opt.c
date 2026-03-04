#include "munit.h"
#include <teide/td.h>
#include <string.h>

/* Helper: create a test table with columns id1(I64), v1(I64), v3(F64) */
static td_t* make_test_table(void) {
    td_sym_init();

    int64_t n = 10;
    int64_t id1_data[] = {1, 1, 2, 2, 3, 3, 1, 2, 3, 1};
    int64_t v1_data[]  = {10, 20, 30, 40, 50, 60, 70, 80, 90, 100};
    double  v3_data[]  = {1.5, 2.5, 3.5, 4.5, 5.5, 6.5, 7.5, 8.5, 9.5, 10.5};

    td_t* id1_vec = td_vec_from_raw(TD_I64, id1_data, n);
    td_t* v1_vec  = td_vec_from_raw(TD_I64, v1_data, n);
    td_t* v3_vec  = td_vec_from_raw(TD_F64, v3_data, n);

    int64_t name_id1 = td_sym_intern("id1", 3);
    int64_t name_v1  = td_sym_intern("v1", 2);
    int64_t name_v3  = td_sym_intern("v3", 2);

    td_t* tbl = td_table_new(3);
    tbl = td_table_add_col(tbl, name_id1, id1_vec);
    tbl = td_table_add_col(tbl, name_v1, v1_vec);
    tbl = td_table_add_col(tbl, name_v3, v3_vec);

    td_release(id1_vec);
    td_release(v1_vec);
    td_release(v3_vec);

    return tbl;
}

/*
 * Test: filter with AND-combined predicates in "wrong" order.
 *
 * DAG: FILTER(AND(id1_eq, v3_gt), SCAN(v1))
 *   - id1_eq is cheap (I64 eq const) but listed first
 *   - v3_gt is expensive (F64 range cmp) but listed second
 *   - Optimizer should later split AND and reorder so cheap is innermost
 *
 * Baseline correctness: id1=1 AND v3>5.0 → count=2
 */
static MunitResult test_filter_reorder_by_type(const void* params, void* data) {
    (void)params; (void)data;
    td_heap_init();

    td_t* tbl = make_test_table();
    td_graph_t* g = td_graph_new(tbl);

    td_op_t* v1    = td_scan(g, "v1");
    td_op_t* id1   = td_scan(g, "id1");
    td_op_t* v3    = td_scan(g, "v3");
    td_op_t* c1    = td_const_i64(g, 1);
    td_op_t* c5    = td_const_f64(g, 5.0);

    td_op_t* id1_eq = td_eq(g, id1, c1);    /* cheap: const cmp + eq */
    td_op_t* v3_gt  = td_gt(g, v3, c5);     /* more expensive: range */

    /* AND with "wrong" order: cheap pred first, expensive second */
    td_op_t* combined = td_and(g, id1_eq, v3_gt);
    td_op_t* filt = td_filter(g, v1, combined);
    td_op_t* cnt = td_count(g, filt);

    /* Execute and verify correctness: id1=1 AND v3>5.0
     * Rows: id1={1,1,2,2,3,3,1,2,3,1}, v3={1.5,2.5,...,10.5}
     * id1=1 rows: indices 0,1,6,9 → v3={1.5,2.5,7.5,10.5}
     * v3>5.0 from those: indices 6,9 → count=2 */
    td_t* result = td_execute(g, cnt);
    munit_assert_false(TD_IS_ERR(result));
    munit_assert_int(result->i64, ==, 2);

    td_release(result);
    td_graph_free(g);
    td_release(tbl);
    td_sym_destroy();
    td_heap_destroy();
    return MUNIT_OK;
}

/*
 * Test: AND(pred_a, pred_b) in a single filter gets split into
 * two chained filters for independent reordering.
 *
 * DAG: FILTER(AND(v3 > 5.0, id1 = 1), SCAN(v1))
 * After: FILTER(v3 > 5.0, FILTER(id1 = 1, SCAN(v1)))
 * Verify via correctness — same result as test above.
 */
static MunitResult test_filter_and_split(const void* params, void* data) {
    (void)params; (void)data;
    td_heap_init();

    td_t* tbl = make_test_table();
    td_graph_t* g = td_graph_new(tbl);

    td_op_t* v1    = td_scan(g, "v1");
    td_op_t* id1   = td_scan(g, "id1");
    td_op_t* v3    = td_scan(g, "v3");
    td_op_t* c1    = td_const_i64(g, 1);
    td_op_t* c5    = td_const_f64(g, 5.0);

    td_op_t* id1_eq = td_eq(g, id1, c1);
    td_op_t* v3_gt  = td_gt(g, v3, c5);
    td_op_t* combined = td_and(g, v3_gt, id1_eq);

    td_op_t* filt = td_filter(g, v1, combined);
    td_op_t* cnt = td_count(g, filt);

    td_t* result = td_execute(g, cnt);
    munit_assert_false(TD_IS_ERR(result));
    munit_assert_int(result->i64, ==, 2);

    td_release(result);
    td_graph_free(g);
    td_release(tbl);
    td_sym_destroy();
    td_heap_destroy();
    return MUNIT_OK;
}

static MunitTest tests[] = {
    { "/filter_reorder_type", test_filter_reorder_by_type, NULL, NULL, 0, NULL },
    { "/filter_and_split",    test_filter_and_split,       NULL, NULL, 0, NULL },
    { NULL, NULL, NULL, NULL, 0, NULL }
};

MunitSuite test_opt_suite = {
    "/opt", tests, NULL, 1, 0
};
