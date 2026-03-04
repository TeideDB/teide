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

static MunitResult test_sel_new(const void* params, void* data) {
    (void)params; (void)data;
    td_heap_init();

    td_t* sel = td_sel_new(100);
    munit_assert_ptr_not_null(sel);
    munit_assert_false(TD_IS_ERR(sel));
    munit_assert_int(sel->type, ==, TD_SEL);

    td_release(sel);
    td_heap_destroy();
    return MUNIT_OK;
}

static MunitResult test_sel_from_pred(const void* params, void* data) {
    (void)params; (void)data;
    td_heap_init();
    td_sym_init();

    /* Create bool vector: [true, false, true, false, true] */
    uint8_t bools[] = {1, 0, 1, 0, 1};
    td_t* bvec = td_vec_from_raw(TD_BOOL, bools, 5);
    munit_assert_ptr_not_null(bvec);

    td_t* sel = td_sel_from_pred(bvec);
    munit_assert_ptr_not_null(sel);
    munit_assert_false(TD_IS_ERR(sel));
    munit_assert_int(sel->type, ==, TD_SEL);

    td_release(sel);
    td_release(bvec);
    td_sym_destroy();
    td_heap_destroy();
    return MUNIT_OK;
}

static MunitResult test_sel_and(const void* params, void* data) {
    (void)params; (void)data;
    td_heap_init();

    uint8_t a_data[] = {1, 1, 0, 0, 1};
    uint8_t b_data[] = {1, 0, 1, 0, 1};
    td_t* a_vec = td_vec_from_raw(TD_BOOL, a_data, 5);
    td_t* b_vec = td_vec_from_raw(TD_BOOL, b_data, 5);

    td_t* sel_a = td_sel_from_pred(a_vec);
    td_t* sel_b = td_sel_from_pred(b_vec);
    td_t* sel_and = td_sel_and(sel_a, sel_b);

    munit_assert_ptr_not_null(sel_and);
    munit_assert_false(TD_IS_ERR(sel_and));
    munit_assert_int(sel_and->type, ==, TD_SEL);

    td_release(sel_and);
    td_release(sel_a);
    td_release(sel_b);
    td_release(a_vec);
    td_release(b_vec);
    td_heap_destroy();
    return MUNIT_OK;
}

static MunitResult test_sel_filter_integration(const void* params, void* data) {
    (void)params; (void)data;
    td_heap_init();
    td_sym_init();

    /* Test that selection vectors work end-to-end through executor */
    int64_t vals[] = {10, 20, 30, 40, 50};
    td_t* vec = td_vec_from_raw(TD_I64, vals, 5);
    int64_t name = td_sym_intern("x", 1);
    td_t* tbl = td_table_new(1);
    tbl = td_table_add_col(tbl, name, vec);
    td_release(vec);

    td_graph_t* g = td_graph_new(tbl);
    td_op_t* x = td_scan(g, "x");
    td_op_t* c25 = td_const_i64(g, 25);
    td_op_t* pred = td_gt(g, x, c25);
    td_op_t* filtered = td_filter(g, x, pred);
    td_op_t* s = td_sum(g, filtered);

    td_t* result = td_execute(g, s);
    munit_assert_false(TD_IS_ERR(result));
    munit_assert_int(result->i64, ==, 120);  /* 30+40+50 */

    td_release(result);
    td_graph_free(g);
    td_release(tbl);
    td_sym_destroy();
    td_heap_destroy();
    return MUNIT_OK;
}

static MunitTest sel_tests[] = {
    { "/new",              test_sel_new,               NULL, NULL, 0, NULL },
    { "/from_pred",        test_sel_from_pred,         NULL, NULL, 0, NULL },
    { "/and",              test_sel_and,               NULL, NULL, 0, NULL },
    { "/filter_integration", test_sel_filter_integration, NULL, NULL, 0, NULL },
    { NULL, NULL, NULL, NULL, 0, NULL }
};

MunitSuite test_sel_suite = {
    "/sel", sel_tests, NULL, 1, 0
};
