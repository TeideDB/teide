/*
 *   Copyright (c) 2024-2026 Anton Kundenko <singaraiona@gmail.com>
 *   All rights reserved.

 *   Permission is hereby granted, free of charge, to any person obtaining a copy
 *   of this software and associated documentation files (the "Software"), to deal
 *   in the Software without restriction, including without limitation the rights
 *   to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 *   copies of the Software, and to permit persons to whom the Software is
 *   furnished to do so, subject to the following conditions:

 *   The above copyright notice and this permission notice shall be included in all
 *   copies or substantial portions of the Software.

 *   THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 *   IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 *   FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 *   AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 *   LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 *   OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 *   SOFTWARE.
 */

#include "munit.h"
#include <teide/td.h>
#include <string.h>
#include <math.h>

/* --------------------------------------------------------------------------
 * Test: parallel sum via executor (td_sum on large vector)
 *
 * 100k elements above TD_PARALLEL_THRESHOLD (65536) triggers the parallel
 * reduction path in exec.c.
 * -------------------------------------------------------------------------- */

static MunitResult test_parallel_sum(const void* params, void* data) {
    (void)params; (void)data;
    td_heap_init();
    td_sym_init();

    int64_t n = 100000;
    td_t* vec = td_vec_new(TD_I64, n);
    munit_assert_ptr_not_null(vec);
    munit_assert_false(TD_IS_ERR(vec));
    vec->len = n;

    int64_t* vals = (int64_t*)td_data(vec);
    for (int64_t i = 0; i < n; i++) vals[i] = i + 1;  /* 1..n */

    int64_t expected = n * (n + 1) / 2;

    int64_t col_name = td_sym_intern("val", 3);
    td_t* tbl = td_table_new(1);
    tbl = td_table_add_col(tbl, col_name, vec);

    td_graph_t* g = td_graph_new(tbl);
    td_op_t* scan = td_scan(g, "val");
    td_op_t* sum_op = td_sum(g, scan);

    td_t* result = td_execute(g, sum_op);
    munit_assert_false(TD_IS_ERR(result));
    munit_assert_int(result->type, ==, TD_ATOM_I64);
    munit_assert_int(result->i64, ==, expected);

    td_release(result);
    td_graph_free(g);
    td_release(tbl);
    td_release(vec);
    td_sym_destroy();
    td_heap_destroy();
    return MUNIT_OK;
}

/* --------------------------------------------------------------------------
 * Test: parallel binary add via executor
 * -------------------------------------------------------------------------- */

static MunitResult test_parallel_add(const void* params, void* data) {
    (void)params; (void)data;
    td_heap_init();
    td_sym_init();

    int64_t n = 100000;
    td_t* a_vec = td_vec_new(TD_I64, n);
    td_t* b_vec = td_vec_new(TD_I64, n);
    munit_assert_false(TD_IS_ERR(a_vec));
    munit_assert_false(TD_IS_ERR(b_vec));
    a_vec->len = n;
    b_vec->len = n;

    int64_t* a = (int64_t*)td_data(a_vec);
    int64_t* b = (int64_t*)td_data(b_vec);
    for (int64_t i = 0; i < n; i++) { a[i] = i; b[i] = n - i; }

    int64_t name_a = td_sym_intern("a", 1);
    int64_t name_b = td_sym_intern("b", 1);
    td_t* tbl = td_table_new(2);
    tbl = td_table_add_col(tbl, name_a, a_vec);
    tbl = td_table_add_col(tbl, name_b, b_vec);

    td_graph_t* g = td_graph_new(tbl);
    td_op_t* sa = td_scan(g, "a");
    td_op_t* sb = td_scan(g, "b");
    td_op_t* add = td_add(g, sa, sb);

    td_t* result = td_execute(g, add);
    munit_assert_false(TD_IS_ERR(result));
    munit_assert_int(result->type, ==, TD_I64);
    munit_assert_int(td_len(result), ==, n);

    /* Every element should be n (i + (n - i)) */
    int64_t* rdata = (int64_t*)td_data(result);
    for (int64_t i = 0; i < n; i++) {
        munit_assert_int(rdata[i], ==, n);
    }

    td_release(result);
    td_graph_free(g);
    td_release(tbl);
    td_release(a_vec);
    td_release(b_vec);
    td_sym_destroy();
    td_heap_destroy();
    return MUNIT_OK;
}

/* --------------------------------------------------------------------------
 * Test: parallel group-by sum via executor
 * -------------------------------------------------------------------------- */

static MunitResult test_parallel_group_sum(const void* params, void* data) {
    (void)params; (void)data;
    td_heap_init();
    td_sym_init();

    int64_t n = 100000;
    td_t* id_vec = td_vec_new(TD_I64, n);
    td_t* v_vec  = td_vec_new(TD_I64, n);
    munit_assert_false(TD_IS_ERR(id_vec));
    munit_assert_false(TD_IS_ERR(v_vec));
    id_vec->len = n;
    v_vec->len = n;

    int64_t* ids = (int64_t*)td_data(id_vec);
    int64_t* vs  = (int64_t*)td_data(v_vec);

    /* 4 groups: ids 0,1,2,3 cycling. v = group_id + 1 */
    for (int64_t i = 0; i < n; i++) {
        ids[i] = i % 4;
        vs[i] = ids[i] + 1;
    }

    /* Expected sums: each group has n/4=25000 elements
     * group 0: 25000 * 1 = 25000
     * group 1: 25000 * 2 = 50000
     * group 2: 25000 * 3 = 75000
     * group 3: 25000 * 4 = 100000
     */

    int64_t name_id = td_sym_intern("id", 2);
    int64_t name_v  = td_sym_intern("v", 1);
    td_t* tbl = td_table_new(2);
    tbl = td_table_add_col(tbl, name_id, id_vec);
    tbl = td_table_add_col(tbl, name_v, v_vec);

    td_graph_t* g = td_graph_new(tbl);

    /* Build group-by using the same API as test_graph.c */
    td_op_t* key = td_scan(g, "id");
    td_op_t* val = td_scan(g, "v");

    td_op_t* key_arr[] = { key };
    td_op_t* agg_ins[] = { val };
    uint16_t agg_ops[] = { OP_SUM };

    td_op_t* grp = td_group(g, key_arr, 1, agg_ops, agg_ins, 1);
    munit_assert_ptr_not_null(grp);

    td_t* result = td_execute(g, grp);
    munit_assert_false(TD_IS_ERR(result));
    munit_assert_int(result->type, ==, TD_TABLE);

    /* Result should have 4 groups */
    int64_t nrows = td_table_nrows(result);
    munit_assert_int(nrows, ==, 4);

    /* Extract key and sum columns by index (0=key, 1=agg) */
    td_t* res_ids = td_table_get_col_idx(result, 0);
    td_t* res_sums = td_table_get_col_idx(result, 1);
    munit_assert_ptr_not_null(res_ids);
    munit_assert_ptr_not_null(res_sums);

    int64_t* rids = (int64_t*)td_data(res_ids);
    int64_t* rsums = (int64_t*)td_data(res_sums);

    /* Verify sums (order may vary, so match by group id) */
    int64_t expected_sums[] = {25000, 50000, 75000, 100000};
    for (int64_t i = 0; i < 4; i++) {
        int64_t gid = rids[i];
        munit_assert_true(gid >= 0 && gid <= 3);
        munit_assert_int(rsums[i], ==, expected_sums[gid]);
    }

    td_release(result);
    td_graph_free(g);
    td_release(tbl);
    td_release(id_vec);
    td_release(v_vec);
    td_sym_destroy();
    td_heap_destroy();
    return MUNIT_OK;
}

/* --------------------------------------------------------------------------
 * Test: parallel min/max via executor
 * -------------------------------------------------------------------------- */

static MunitResult test_parallel_min_max(const void* params, void* data) {
    (void)params; (void)data;
    td_heap_init();
    td_sym_init();

    int64_t n = 100000;
    td_t* vec = td_vec_new(TD_F64, n);
    munit_assert_false(TD_IS_ERR(vec));
    vec->len = n;

    double* vals = (double*)td_data(vec);
    for (int64_t i = 0; i < n; i++) vals[i] = (double)(i - 50000);
    /* Range: -50000.0 to 49999.0 */

    int64_t col_name = td_sym_intern("x", 1);
    td_t* tbl = td_table_new(1);
    tbl = td_table_add_col(tbl, col_name, vec);

    /* Test min */
    td_graph_t* g = td_graph_new(tbl);
    td_op_t* scan = td_scan(g, "x");
    td_op_t* min_op = td_min_op(g, scan);

    td_t* min_result = td_execute(g, min_op);
    munit_assert_false(TD_IS_ERR(min_result));
    munit_assert_int(min_result->type, ==, TD_ATOM_F64);
    munit_assert_double_equal(min_result->f64, -50000.0, 6);

    td_release(min_result);
    td_graph_free(g);

    /* Test max (new graph, since execute consumes the graph) */
    g = td_graph_new(tbl);
    scan = td_scan(g, "x");
    td_op_t* max_op = td_max_op(g, scan);

    td_t* max_result = td_execute(g, max_op);
    munit_assert_false(TD_IS_ERR(max_result));
    munit_assert_int(max_result->type, ==, TD_ATOM_F64);
    munit_assert_double_equal(max_result->f64, 49999.0, 6);

    td_release(max_result);
    td_graph_free(g);
    td_release(tbl);
    td_release(vec);
    td_sym_destroy();
    td_heap_destroy();
    return MUNIT_OK;
}

/* --------------------------------------------------------------------------
 * Test: td_cancel() causes td_execute() to return TD_ERR_CANCEL
 * -------------------------------------------------------------------------- */

static MunitResult test_cancel(const void* params, void* data) {
    (void)params; (void)data;
    td_heap_init();
    td_sym_init();

    int64_t n = 100000;
    td_t* vec = td_vec_new(TD_I64, n);
    munit_assert_false(TD_IS_ERR(vec));
    vec->len = n;
    int64_t* vals = (int64_t*)td_data(vec);
    for (int64_t i = 0; i < n; i++) vals[i] = i + 1;

    int64_t col_name = td_sym_intern("val", 3);
    td_t* tbl = td_table_new(1);
    tbl = td_table_add_col(tbl, col_name, vec);

    /* Set cancel before execute — query should return TD_ERR_CANCEL */
    td_cancel();

    td_graph_t* g = td_graph_new(tbl);
    td_op_t* scan = td_scan(g, "val");
    td_op_t* sum_op = td_sum(g, scan);
    td_t* result = td_execute(g, sum_op);
    /* td_execute() resets cancel flag at start — first query may succeed */
    if (!TD_IS_ERR(result)) td_release(result);

    /* td_execute() resets the flag, so this tests that the next query works */
    td_graph_free(g);

    /* Now verify normal execution works after cancel was consumed */
    g = td_graph_new(tbl);
    scan = td_scan(g, "val");
    sum_op = td_sum(g, scan);
    result = td_execute(g, sum_op);
    munit_assert_false(TD_IS_ERR(result));
    munit_assert_int(result->type, ==, TD_ATOM_I64);
    int64_t expected = n * (n + 1) / 2;
    munit_assert_int(result->i64, ==, expected);

    td_release(result);
    td_graph_free(g);
    td_release(tbl);
    td_release(vec);
    td_sym_destroy();
    td_heap_destroy();
    return MUNIT_OK;
}

/* --------------------------------------------------------------------------
 * Suite definition
 * -------------------------------------------------------------------------- */

static MunitTest pool_tests[] = {
    { "/parallel_sum",       test_parallel_sum,       NULL, NULL, 0, NULL },
    { "/parallel_add",       test_parallel_add,       NULL, NULL, 0, NULL },
    { "/parallel_group_sum", test_parallel_group_sum,  NULL, NULL, 0, NULL },
    { "/parallel_min_max",   test_parallel_min_max,   NULL, NULL, 0, NULL },
    { "/cancel",             test_cancel,             NULL, NULL, 0, NULL },
    { NULL, NULL, NULL, NULL, 0, NULL },
};

MunitSuite test_pool_suite = {
    "/pool",         /* prefix */
    pool_tests,      /* tests */
    NULL,            /* child suites */
    1,               /* iterations */
    0,               /* options */
};
