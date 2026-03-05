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

/* Helper: create table with id1(I64), v1(I64), v3(F64) — 10 rows */
static td_t* make_exec_table(void) {
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

/* ---- NEG ---- */
static MunitResult test_exec_neg_i64(const void* params, void* data) {
    (void)params; (void)data;
    td_heap_init();
    td_t* tbl = make_exec_table();
    td_graph_t* g = td_graph_new(tbl);

    td_op_t* v1 = td_scan(g, "v1");
    td_op_t* neg_op = td_neg(g, v1);
    td_op_t* s = td_sum(g, neg_op);

    td_t* result = td_execute(g, s);
    munit_assert_false(TD_IS_ERR(result));
    munit_assert_int(result->i64, ==, -550);

    td_release(result);
    td_graph_free(g);
    td_release(tbl);
    td_sym_destroy();
    td_heap_destroy();
    return MUNIT_OK;
}

static MunitResult test_exec_neg_f64(const void* params, void* data) {
    (void)params; (void)data;
    td_heap_init();
    td_t* tbl = make_exec_table();
    td_graph_t* g = td_graph_new(tbl);

    td_op_t* v3 = td_scan(g, "v3");
    td_op_t* neg_op = td_neg(g, v3);
    td_op_t* s = td_sum(g, neg_op);

    td_t* result = td_execute(g, s);
    munit_assert_false(TD_IS_ERR(result));
    munit_assert_double_equal(result->f64, -60.0, 6);

    td_release(result);
    td_graph_free(g);
    td_release(tbl);
    td_sym_destroy();
    td_heap_destroy();
    return MUNIT_OK;
}

/* ---- ABS ---- */
static MunitResult test_exec_abs(const void* params, void* data) {
    (void)params; (void)data;
    td_heap_init();
    td_t* tbl = make_exec_table();
    td_graph_t* g = td_graph_new(tbl);

    /* abs(neg(v1)) should equal v1 */
    td_op_t* v1 = td_scan(g, "v1");
    td_op_t* neg_op = td_neg(g, v1);
    td_op_t* abs_op = td_abs(g, neg_op);
    td_op_t* s = td_sum(g, abs_op);

    td_t* result = td_execute(g, s);
    munit_assert_false(TD_IS_ERR(result));
    munit_assert_int(result->i64, ==, 550);

    td_release(result);
    td_graph_free(g);
    td_release(tbl);
    td_sym_destroy();
    td_heap_destroy();
    return MUNIT_OK;
}

/* ---- NOT ---- */
static MunitResult test_exec_not(const void* params, void* data) {
    (void)params; (void)data;
    td_heap_init();
    td_t* tbl = make_exec_table();
    td_graph_t* g = td_graph_new(tbl);

    td_op_t* v1 = td_scan(g, "v1");
    td_op_t* threshold = td_const_i64(g, 50);
    td_op_t* pred = td_ge(g, v1, threshold);
    td_op_t* not_pred = td_not(g, pred);
    td_op_t* filtered = td_filter(g, v1, not_pred);
    td_op_t* cnt = td_count(g, filtered);

    td_t* result = td_execute(g, cnt);
    munit_assert_false(TD_IS_ERR(result));
    munit_assert_int(result->i64, ==, 4);  /* 10,20,30,40 */

    td_release(result);
    td_graph_free(g);
    td_release(tbl);
    td_sym_destroy();
    td_heap_destroy();
    return MUNIT_OK;
}

/* ---- ISNULL ---- */
static MunitResult test_exec_isnull(const void* params, void* data) {
    (void)params; (void)data;
    td_heap_init();
    td_sym_init();

    /* Create vector with no nulls */
    int64_t raw[] = {10, 20, 30, 40, 50};
    td_t* vec = td_vec_from_raw(TD_I64, raw, 5);
    int64_t name = td_sym_intern("x", 1);
    td_t* tbl = td_table_new(1);
    tbl = td_table_add_col(tbl, name, vec);
    td_release(vec);

    td_graph_t* g = td_graph_new(tbl);
    td_op_t* x = td_scan(g, "x");
    td_op_t* is_null = td_isnull(g, x);
    td_op_t* filtered = td_filter(g, x, is_null);
    td_op_t* cnt = td_count(g, filtered);

    td_t* result = td_execute(g, cnt);
    munit_assert_false(TD_IS_ERR(result));
    munit_assert_int(result->i64, ==, 0);  /* no nulls in raw data */

    td_release(result);
    td_graph_free(g);
    td_release(tbl);
    td_sym_destroy();
    td_heap_destroy();
    return MUNIT_OK;
}

/* ---- SQRT / LOG / EXP ---- */
static MunitResult test_exec_math_ops(const void* params, void* data) {
    (void)params; (void)data;
    td_heap_init();
    td_sym_init();

    double raw[] = {1.0, 4.0, 9.0, 16.0, 25.0};
    td_t* vec = td_vec_from_raw(TD_F64, raw, 5);
    int64_t name = td_sym_intern("x", 1);
    td_t* tbl = td_table_new(1);
    tbl = td_table_add_col(tbl, name, vec);
    td_release(vec);

    /* sqrt(x) -> sum should be 1+2+3+4+5 = 15 */
    td_graph_t* g = td_graph_new(tbl);
    td_op_t* x = td_scan(g, "x");
    td_op_t* sq = td_sqrt_op(g, x);
    td_op_t* s = td_sum(g, sq);
    td_t* result = td_execute(g, s);
    munit_assert_false(TD_IS_ERR(result));
    munit_assert_double_equal(result->f64, 15.0, 6);
    td_release(result);
    td_graph_free(g);

    /* exp(log(x)) should roundtrip -> sum = 55 */
    g = td_graph_new(tbl);
    x = td_scan(g, "x");
    td_op_t* lg = td_log_op(g, x);
    td_op_t* ex = td_exp_op(g, lg);
    s = td_sum(g, ex);
    result = td_execute(g, s);
    munit_assert_false(TD_IS_ERR(result));
    munit_assert_double_equal(result->f64, 55.0, 3);
    td_release(result);
    td_graph_free(g);

    td_release(tbl);
    td_sym_destroy();
    td_heap_destroy();
    return MUNIT_OK;
}

/* ---- CEIL / FLOOR ---- */
static MunitResult test_exec_ceil_floor(const void* params, void* data) {
    (void)params; (void)data;
    td_heap_init();
    td_sym_init();

    double raw[] = {1.1, 2.5, 3.9, -1.1, -2.9};
    td_t* vec = td_vec_from_raw(TD_F64, raw, 5);
    int64_t name = td_sym_intern("x", 1);
    td_t* tbl = td_table_new(1);
    tbl = td_table_add_col(tbl, name, vec);
    td_release(vec);

    /* ceil: 2+3+4+(-1)+(-2) = 6 */
    td_graph_t* g = td_graph_new(tbl);
    td_op_t* x = td_scan(g, "x");
    td_op_t* c = td_ceil_op(g, x);
    td_op_t* s = td_sum(g, c);
    td_t* result = td_execute(g, s);
    munit_assert_false(TD_IS_ERR(result));
    munit_assert_double_equal(result->f64, 6.0, 6);
    td_release(result);
    td_graph_free(g);

    /* floor: 1+2+3+(-2)+(-3) = 1 */
    g = td_graph_new(tbl);
    x = td_scan(g, "x");
    td_op_t* f = td_floor_op(g, x);
    s = td_sum(g, f);
    result = td_execute(g, s);
    munit_assert_false(TD_IS_ERR(result));
    munit_assert_double_equal(result->f64, 1.0, 6);
    td_release(result);
    td_graph_free(g);

    td_release(tbl);
    td_sym_destroy();
    td_heap_destroy();
    return MUNIT_OK;
}

/* ======================================================================
 * Binary element-wise ops
 * ====================================================================== */

static MunitResult test_exec_binary_arithmetic(const void* params, void* data) {
    (void)params; (void)data;
    td_heap_init();
    td_t* tbl = make_exec_table();
    td_graph_t* g = td_graph_new(tbl);

    td_op_t* v1 = td_scan(g, "v1");
    td_op_t* id1 = td_scan(g, "id1");

    /* v1 + id1 -> sum */
    td_op_t* add_op = td_add(g, v1, id1);
    td_op_t* s = td_sum(g, add_op);
    td_t* result = td_execute(g, s);
    munit_assert_false(TD_IS_ERR(result));
    /* sum(v1)=550, sum(id1)=19, sum(v1+id1)=569 */
    munit_assert_int(result->i64, ==, 569);
    td_release(result);
    td_graph_free(g);

    /* v1 - id1 -> sum */
    g = td_graph_new(tbl);
    v1 = td_scan(g, "v1");
    id1 = td_scan(g, "id1");
    td_op_t* sub_op = td_sub(g, v1, id1);
    s = td_sum(g, sub_op);
    result = td_execute(g, s);
    munit_assert_false(TD_IS_ERR(result));
    munit_assert_int(result->i64, ==, 531);
    td_release(result);
    td_graph_free(g);

    /* v1 * id1 -> sum */
    g = td_graph_new(tbl);
    v1 = td_scan(g, "v1");
    id1 = td_scan(g, "id1");
    td_op_t* mul_op = td_mul(g, v1, id1);
    s = td_sum(g, mul_op);
    result = td_execute(g, s);
    munit_assert_false(TD_IS_ERR(result));
    /* 10*1+20*1+30*2+40*2+50*3+60*3+70*1+80*2+90*3+100*1 = 1100 */
    munit_assert_int(result->i64, ==, 1100);
    td_release(result);
    td_graph_free(g);

    /* v1 / id1 -> sum (OP_DIV always promotes to f64) */
    g = td_graph_new(tbl);
    v1 = td_scan(g, "v1");
    id1 = td_scan(g, "id1");
    td_op_t* div_op = td_div(g, v1, id1);
    s = td_sum(g, div_op);
    result = td_execute(g, s);
    munit_assert_false(TD_IS_ERR(result));
    /* 10/1+20/1+15+20+50/3+60/3+70/1+40+30+100/1 = 341.666... */
    munit_assert_double_equal(result->f64, 341.666, 2);
    td_release(result);
    td_graph_free(g);

    /* v1 % id1 -> sum */
    g = td_graph_new(tbl);
    v1 = td_scan(g, "v1");
    id1 = td_scan(g, "id1");
    td_op_t* mod_op = td_mod(g, v1, id1);
    s = td_sum(g, mod_op);
    result = td_execute(g, s);
    munit_assert_false(TD_IS_ERR(result));
    /* 10%1+20%1+30%2+40%2+50%3+60%3+70%1+80%2+90%3+100%1 = 2 */
    munit_assert_int(result->i64, ==, 2);
    td_release(result);
    td_graph_free(g);

    td_release(tbl);
    td_sym_destroy();
    td_heap_destroy();
    return MUNIT_OK;
}

/* ---- Comparison ops ---- */
static MunitResult test_exec_comparisons(const void* params, void* data) {
    (void)params; (void)data;
    td_heap_init();
    td_t* tbl = make_exec_table();

    /* EQ: count where v1 == 50 */
    td_graph_t* g = td_graph_new(tbl);
    td_op_t* v1 = td_scan(g, "v1");
    td_op_t* c50 = td_const_i64(g, 50);
    td_op_t* pred = td_eq(g, v1, c50);
    td_op_t* filtered = td_filter(g, v1, pred);
    td_op_t* cnt = td_count(g, filtered);
    td_t* result = td_execute(g, cnt);
    munit_assert_false(TD_IS_ERR(result));
    munit_assert_int(result->i64, ==, 1);
    td_release(result);
    td_graph_free(g);

    /* NE: count where v1 != 50 */
    g = td_graph_new(tbl);
    v1 = td_scan(g, "v1");
    c50 = td_const_i64(g, 50);
    pred = td_ne(g, v1, c50);
    filtered = td_filter(g, v1, pred);
    cnt = td_count(g, filtered);
    result = td_execute(g, cnt);
    munit_assert_false(TD_IS_ERR(result));
    munit_assert_int(result->i64, ==, 9);
    td_release(result);
    td_graph_free(g);

    /* LT: count where v1 < 50 */
    g = td_graph_new(tbl);
    v1 = td_scan(g, "v1");
    c50 = td_const_i64(g, 50);
    pred = td_lt(g, v1, c50);
    filtered = td_filter(g, v1, pred);
    cnt = td_count(g, filtered);
    result = td_execute(g, cnt);
    munit_assert_false(TD_IS_ERR(result));
    munit_assert_int(result->i64, ==, 4);  /* 10,20,30,40 */
    td_release(result);
    td_graph_free(g);

    /* LE: count where v1 <= 50 */
    g = td_graph_new(tbl);
    v1 = td_scan(g, "v1");
    c50 = td_const_i64(g, 50);
    pred = td_le(g, v1, c50);
    filtered = td_filter(g, v1, pred);
    cnt = td_count(g, filtered);
    result = td_execute(g, cnt);
    munit_assert_false(TD_IS_ERR(result));
    munit_assert_int(result->i64, ==, 5);
    td_release(result);
    td_graph_free(g);

    /* GT: count where v1 > 50 */
    g = td_graph_new(tbl);
    v1 = td_scan(g, "v1");
    c50 = td_const_i64(g, 50);
    pred = td_gt(g, v1, c50);
    filtered = td_filter(g, v1, pred);
    cnt = td_count(g, filtered);
    result = td_execute(g, cnt);
    munit_assert_false(TD_IS_ERR(result));
    munit_assert_int(result->i64, ==, 5);  /* 60,70,80,90,100 */
    td_release(result);
    td_graph_free(g);

    /* AND: v1 > 20 AND v1 < 80 */
    g = td_graph_new(tbl);
    v1 = td_scan(g, "v1");
    td_op_t* c20 = td_const_i64(g, 20);
    td_op_t* c80 = td_const_i64(g, 80);
    td_op_t* gt20 = td_gt(g, v1, c20);
    td_op_t* lt80 = td_lt(g, v1, c80);
    td_op_t* both = td_and(g, gt20, lt80);
    filtered = td_filter(g, v1, both);
    cnt = td_count(g, filtered);
    result = td_execute(g, cnt);
    munit_assert_false(TD_IS_ERR(result));
    munit_assert_int(result->i64, ==, 5);  /* 30,40,50,60,70 */
    td_release(result);
    td_graph_free(g);

    /* OR: v1 < 20 OR v1 > 90 */
    g = td_graph_new(tbl);
    v1 = td_scan(g, "v1");
    c20 = td_const_i64(g, 20);
    td_op_t* c90 = td_const_i64(g, 90);
    td_op_t* lt20 = td_lt(g, v1, c20);
    td_op_t* gt90 = td_gt(g, v1, c90);
    td_op_t* either = td_or(g, lt20, gt90);
    filtered = td_filter(g, v1, either);
    cnt = td_count(g, filtered);
    result = td_execute(g, cnt);
    munit_assert_false(TD_IS_ERR(result));
    munit_assert_int(result->i64, ==, 2);  /* 10, 100 */
    td_release(result);
    td_graph_free(g);

    td_release(tbl);
    td_sym_destroy();
    td_heap_destroy();
    return MUNIT_OK;
}

/* ---- MIN2 / MAX2 ---- */
static MunitResult test_exec_min2_max2(const void* params, void* data) {
    (void)params; (void)data;
    td_heap_init();
    td_t* tbl = make_exec_table();

    td_graph_t* g = td_graph_new(tbl);
    td_op_t* v1 = td_scan(g, "v1");
    td_op_t* id1 = td_scan(g, "id1");
    td_op_t* mn = td_min2(g, v1, id1);
    td_op_t* s = td_sum(g, mn);
    td_t* result = td_execute(g, s);
    munit_assert_false(TD_IS_ERR(result));
    /* min2(v1,id1) per row: 1,1,2,2,3,3,1,2,3,1 = sum(id1) = 19 */
    munit_assert_int(result->i64, ==, 19);
    td_release(result);
    td_graph_free(g);

    g = td_graph_new(tbl);
    v1 = td_scan(g, "v1");
    id1 = td_scan(g, "id1");
    td_op_t* mx = td_max2(g, v1, id1);
    s = td_sum(g, mx);
    result = td_execute(g, s);
    munit_assert_false(TD_IS_ERR(result));
    /* max2(v1,id1) per row: all v1 values since v1 > id1 -> sum = 550 */
    munit_assert_int(result->i64, ==, 550);
    td_release(result);
    td_graph_free(g);

    td_release(tbl);
    td_sym_destroy();
    td_heap_destroy();
    return MUNIT_OK;
}

/* ---- IF (ternary) ---- */
static MunitResult test_exec_if(const void* params, void* data) {
    (void)params; (void)data;
    td_heap_init();
    td_t* tbl = make_exec_table();

    td_graph_t* g = td_graph_new(tbl);
    td_op_t* v1 = td_scan(g, "v1");
    td_op_t* c50 = td_const_i64(g, 50);
    td_op_t* pred = td_gt(g, v1, c50);
    td_op_t* c1 = td_const_i64(g, 1);
    td_op_t* c0 = td_const_i64(g, 0);
    td_op_t* if_op = td_if(g, pred, c1, c0);
    td_op_t* s = td_sum(g, if_op);

    td_t* result = td_execute(g, s);
    munit_assert_false(TD_IS_ERR(result));
    munit_assert_int(result->i64, ==, 5);  /* 5 values > 50 */

    td_release(result);
    td_graph_free(g);
    td_release(tbl);
    td_sym_destroy();
    td_heap_destroy();
    return MUNIT_OK;
}

/* ======================================================================
 * Reduction ops
 * ====================================================================== */

static MunitResult test_exec_reductions(const void* params, void* data) {
    (void)params; (void)data;
    td_heap_init();
    td_t* tbl = make_exec_table();

    /* PROD on small values to avoid overflow */
    td_graph_t* g = td_graph_new(tbl);
    td_op_t* id1 = td_scan(g, "id1");
    td_op_t* prod_op = td_prod(g, id1);
    td_t* result = td_execute(g, prod_op);
    munit_assert_false(TD_IS_ERR(result));
    /* id1 = {1,1,2,2,3,3,1,2,3,1} -> prod = 216 */
    munit_assert_int(result->i64, ==, 216);
    td_release(result);
    td_graph_free(g);

    /* MIN */
    g = td_graph_new(tbl);
    td_op_t* v1 = td_scan(g, "v1");
    td_op_t* min_op = td_min_op(g, v1);
    result = td_execute(g, min_op);
    munit_assert_false(TD_IS_ERR(result));
    munit_assert_int(result->i64, ==, 10);
    td_release(result);
    td_graph_free(g);

    /* MAX */
    g = td_graph_new(tbl);
    v1 = td_scan(g, "v1");
    td_op_t* max_op = td_max_op(g, v1);
    result = td_execute(g, max_op);
    munit_assert_false(TD_IS_ERR(result));
    munit_assert_int(result->i64, ==, 100);
    td_release(result);
    td_graph_free(g);

    /* AVG */
    g = td_graph_new(tbl);
    v1 = td_scan(g, "v1");
    td_op_t* avg_op = td_avg(g, v1);
    result = td_execute(g, avg_op);
    munit_assert_false(TD_IS_ERR(result));
    munit_assert_double_equal(result->f64, 55.0, 6);
    td_release(result);
    td_graph_free(g);

    /* FIRST */
    g = td_graph_new(tbl);
    v1 = td_scan(g, "v1");
    td_op_t* first_op = td_first(g, v1);
    result = td_execute(g, first_op);
    munit_assert_false(TD_IS_ERR(result));
    munit_assert_int(result->i64, ==, 10);
    td_release(result);
    td_graph_free(g);

    /* LAST */
    g = td_graph_new(tbl);
    v1 = td_scan(g, "v1");
    td_op_t* last_op = td_last(g, v1);
    result = td_execute(g, last_op);
    munit_assert_false(TD_IS_ERR(result));
    munit_assert_int(result->i64, ==, 100);
    td_release(result);
    td_graph_free(g);

    td_release(tbl);
    td_sym_destroy();
    td_heap_destroy();
    return MUNIT_OK;
}

/* ---- SORT ---- */
static MunitResult test_exec_sort(const void* params, void* data) {
    (void)params; (void)data;
    td_heap_init();
    td_t* tbl = make_exec_table();

    /* Ascending sort on v1 -> produces sorted table */
    td_graph_t* g = td_graph_new(tbl);
    td_op_t* tbl_op = td_const_table(g, tbl);
    td_op_t* v1 = td_scan(g, "v1");
    td_op_t* keys[] = { v1 };
    uint8_t descs[] = { 0 };       /* ascending */
    uint8_t nulls_first[] = { 0 };
    td_op_t* sort_op = td_sort_op(g, tbl_op, keys, descs, nulls_first, 1);

    td_t* result = td_execute(g, sort_op);
    munit_assert_false(TD_IS_ERR(result));
    munit_assert_int(result->type, ==, TD_TABLE);
    munit_assert_int(td_table_nrows(result), ==, 10);

    /* Verify ascending order */
    td_t* sorted_col = td_table_get_col_idx(result, 1); /* v1 is col 1 */
    int64_t* sdata = (int64_t*)td_data(sorted_col);
    for (int i = 0; i < 9; i++) {
        munit_assert_true(sdata[i] <= sdata[i + 1]);
    }

    td_release(result);
    td_graph_free(g);

    /* Descending sort on v1 */
    g = td_graph_new(tbl);
    tbl_op = td_const_table(g, tbl);
    v1 = td_scan(g, "v1");
    td_op_t* keys2[] = { v1 };
    uint8_t descs2[] = { 1 };      /* descending */
    uint8_t nulls_first2[] = { 0 };
    sort_op = td_sort_op(g, tbl_op, keys2, descs2, nulls_first2, 1);
    result = td_execute(g, sort_op);
    munit_assert_false(TD_IS_ERR(result));
    munit_assert_int(result->type, ==, TD_TABLE);
    munit_assert_int(td_table_nrows(result), ==, 10);

    /* Verify descending order */
    sorted_col = td_table_get_col_idx(result, 1);
    sdata = (int64_t*)td_data(sorted_col);
    for (int i = 0; i < 9; i++) {
        munit_assert_true(sdata[i] >= sdata[i + 1]);
    }

    td_release(result);
    td_graph_free(g);
    td_release(tbl);
    td_sym_destroy();
    td_heap_destroy();
    return MUNIT_OK;
}

/* ---- HEAD / TAIL ---- */
static MunitResult test_exec_head_tail(const void* params, void* data) {
    (void)params; (void)data;
    td_heap_init();
    td_t* tbl = make_exec_table();

    /* HEAD 3 */
    td_graph_t* g = td_graph_new(tbl);
    td_op_t* v1 = td_scan(g, "v1");
    td_op_t* head_op = td_head(g, v1, 3);
    td_op_t* s = td_sum(g, head_op);
    td_t* result = td_execute(g, s);
    munit_assert_false(TD_IS_ERR(result));
    munit_assert_int(result->i64, ==, 60);  /* 10+20+30 */
    td_release(result);
    td_graph_free(g);

    /* TAIL 3 */
    g = td_graph_new(tbl);
    v1 = td_scan(g, "v1");
    td_op_t* tail_op = td_tail(g, v1, 3);
    s = td_sum(g, tail_op);
    result = td_execute(g, s);
    munit_assert_false(TD_IS_ERR(result));
    munit_assert_int(result->i64, ==, 270);  /* 80+90+100 */
    td_release(result);
    td_graph_free(g);

    td_release(tbl);
    td_sym_destroy();
    td_heap_destroy();
    return MUNIT_OK;
}

/* ---- JOIN ---- */
static MunitResult test_exec_join(const void* params, void* data) {
    (void)params; (void)data;
    td_heap_init();
    td_sym_init();

    /* Left table: id(I64), val(I64) */
    int64_t lid[] = {1, 2, 3};
    int64_t lval[] = {10, 20, 30};
    td_t* lid_v = td_vec_from_raw(TD_I64, lid, 3);
    td_t* lval_v = td_vec_from_raw(TD_I64, lval, 3);
    int64_t n_id = td_sym_intern("id", 2);
    int64_t n_val = td_sym_intern("val", 3);
    td_t* left = td_table_new(2);
    left = td_table_add_col(left, n_id, lid_v);
    left = td_table_add_col(left, n_val, lval_v);
    td_release(lid_v);
    td_release(lval_v);

    /* Right table: id(I64), score(I64) */
    int64_t rid[] = {1, 2, 2, 3};
    int64_t rscore[] = {100, 200, 201, 300};
    td_t* rid_v = td_vec_from_raw(TD_I64, rid, 4);
    td_t* rscore_v = td_vec_from_raw(TD_I64, rscore, 4);
    int64_t n_score = td_sym_intern("score", 5);
    td_t* right = td_table_new(2);
    right = td_table_add_col(right, n_id, rid_v);
    right = td_table_add_col(right, n_score, rscore_v);
    td_release(rid_v);
    td_release(rscore_v);

    td_graph_t* g = td_graph_new(left);
    td_op_t* left_op = td_const_table(g, left);
    td_op_t* right_op = td_const_table(g, right);
    td_op_t* lk = td_scan(g, "id");
    td_op_t* lk_arr[] = { lk };
    td_op_t* rk_arr[] = { lk };  /* same key name */
    td_op_t* join_op = td_join(g, left_op, lk_arr, right_op, rk_arr, 1, 0);

    td_t* result = td_execute(g, join_op);
    munit_assert_false(TD_IS_ERR(result));
    munit_assert_int(result->type, ==, TD_TABLE);
    /* 1->1, 2->2(twice), 3->1 = 4 result rows */
    munit_assert_int(td_table_nrows(result), ==, 4);
    /* Joined table should have columns from both sides */
    munit_assert_true(td_table_ncols(result) >= 3);

    td_release(result);
    td_graph_free(g);
    td_release(left);
    td_release(right);
    td_sym_destroy();
    td_heap_destroy();
    return MUNIT_OK;
}

/* ---- LARGE JOIN (radix-partitioned path) ---- */
static MunitResult test_exec_join_large(const void* params, void* data) {
    (void)params; (void)data;
    td_heap_init();
    td_sym_init();

    /* Create left table: 100K rows, id = i % 50000, val = i
     * Each key appears exactly twice on the left side. */
    int64_t n_left = 100000;
    td_t* lid_v = td_vec_new(TD_I64, n_left);
    lid_v->len = n_left;
    td_t* lval_v = td_vec_new(TD_I64, n_left);
    lval_v->len = n_left;
    int64_t* lid = (int64_t*)td_data(lid_v);
    int64_t* lval = (int64_t*)td_data(lval_v);
    for (int64_t i = 0; i < n_left; i++) {
        lid[i] = i % 50000;
        lval[i] = i;
    }

    /* Right table: 100K rows, id = i % 50000, score = i * 10
     * Each key appears exactly twice on the right side. */
    int64_t n_right = 100000;
    td_t* rid_v = td_vec_new(TD_I64, n_right);
    rid_v->len = n_right;
    td_t* rscore_v = td_vec_new(TD_I64, n_right);
    rscore_v->len = n_right;
    int64_t* rid = (int64_t*)td_data(rid_v);
    int64_t* rscore = (int64_t*)td_data(rscore_v);
    for (int64_t i = 0; i < n_right; i++) {
        rid[i] = i % 50000;
        rscore[i] = i * 10;
    }

    int64_t n_id = td_sym_intern("id", 2);
    int64_t n_val = td_sym_intern("val", 3);
    int64_t n_score = td_sym_intern("score", 5);

    td_t* left = td_table_new(2);
    left = td_table_add_col(left, n_id, lid_v);
    left = td_table_add_col(left, n_val, lval_v);
    td_release(lid_v);
    td_release(lval_v);

    td_t* right = td_table_new(2);
    right = td_table_add_col(right, n_id, rid_v);
    right = td_table_add_col(right, n_score, rscore_v);
    td_release(rid_v);
    td_release(rscore_v);

    td_graph_t* g = td_graph_new(left);
    td_op_t* left_op = td_const_table(g, left);
    td_op_t* right_op = td_const_table(g, right);
    td_op_t* lk = td_scan(g, "id");
    td_op_t* lk_arr[] = { lk };
    td_op_t* rk_arr[] = { lk };
    td_op_t* join_op = td_join(g, left_op, lk_arr, right_op, rk_arr, 1, 0);

    td_t* result = td_execute(g, join_op);
    munit_assert_false(TD_IS_ERR(result));
    munit_assert_int(result->type, ==, TD_TABLE);
    /* 50K keys, 2 left x 2 right = 4 matches per key, total = 200K rows */
    munit_assert_int(td_table_nrows(result), ==, 200000);
    munit_assert_true(td_table_ncols(result) >= 3);

    td_release(result);
    td_graph_free(g);
    td_release(left);
    td_release(right);
    td_sym_destroy();
    td_heap_destroy();
    return MUNIT_OK;
}

/* ---- JOIN FALLBACK (chained HT when radix OOM) ---- */
static MunitResult test_exec_join_fallback(const void* params, void* data) {
    (void)params; (void)data;
    td_heap_init();
    td_sym_init();

    /* Use a small join — right_rows (4) < TD_PARALLEL_THRESHOLD so this
     * always exercises the chained HT fallback path, never the radix path. */
    int64_t lid[] = {1, 2, 3};
    int64_t lval[] = {10, 20, 30};
    td_t* lid_v = td_vec_from_raw(TD_I64, lid, 3);
    td_t* lval_v = td_vec_from_raw(TD_I64, lval, 3);
    int64_t n_id = td_sym_intern("id", 2);
    int64_t n_val = td_sym_intern("val", 3);
    td_t* left = td_table_new(2);
    left = td_table_add_col(left, n_id, lid_v);
    left = td_table_add_col(left, n_val, lval_v);
    td_release(lid_v);
    td_release(lval_v);

    int64_t rid[] = {1, 2, 2, 3};
    int64_t rscore[] = {100, 200, 201, 300};
    td_t* rid_v = td_vec_from_raw(TD_I64, rid, 4);
    td_t* rscore_v = td_vec_from_raw(TD_I64, rscore, 4);
    int64_t n_score = td_sym_intern("score", 5);
    td_t* right = td_table_new(2);
    right = td_table_add_col(right, n_id, rid_v);
    right = td_table_add_col(right, n_score, rscore_v);
    td_release(rid_v);
    td_release(rscore_v);

    td_graph_t* g = td_graph_new(left);
    td_op_t* left_op = td_const_table(g, left);
    td_op_t* right_op = td_const_table(g, right);
    td_op_t* lk = td_scan(g, "id");
    td_op_t* lk_arr[] = { lk };
    td_op_t* rk_arr[] = { lk };
    td_op_t* join_op = td_join(g, left_op, lk_arr, right_op, rk_arr, 1, 0);

    td_t* result = td_execute(g, join_op);
    munit_assert_false(TD_IS_ERR(result));
    munit_assert_int(result->type, ==, TD_TABLE);
    /* 1->1, 2->2(twice), 3->1 = 4 result rows */
    munit_assert_int(td_table_nrows(result), ==, 4);
    munit_assert_true(td_table_ncols(result) >= 3);

    td_release(result);
    td_graph_free(g);
    td_release(left);
    td_release(right);
    td_sym_destroy();
    td_heap_destroy();
    return MUNIT_OK;
}

/* ---- WINDOW ---- */
static MunitResult test_exec_window(const void* params, void* data) {
    (void)params; (void)data;
    td_heap_init();
    td_sym_init();

    int64_t n = 6;
    int64_t grp_data[] = {1, 1, 1, 2, 2, 2};
    int64_t val_data[] = {10, 20, 30, 40, 50, 60};
    td_t* grp_v = td_vec_from_raw(TD_I64, grp_data, n);
    td_t* val_v = td_vec_from_raw(TD_I64, val_data, n);
    int64_t n_grp = td_sym_intern("grp", 3);
    int64_t n_val = td_sym_intern("val", 3);
    td_t* tbl = td_table_new(2);
    tbl = td_table_add_col(tbl, n_grp, grp_v);
    tbl = td_table_add_col(tbl, n_val, val_v);
    td_release(grp_v);
    td_release(val_v);

    td_graph_t* g = td_graph_new(tbl);
    td_op_t* tbl_op = td_const_table(g, tbl);
    td_op_t* grp_op = td_scan(g, "grp");
    td_op_t* val_op = td_scan(g, "val");

    td_op_t* parts[] = { grp_op };
    td_op_t* orders[] = { val_op };
    uint8_t order_descs[] = { 0 };  /* ascending */
    uint8_t func_kinds[] = { TD_WIN_ROW_NUMBER };
    td_op_t* func_inputs[] = { val_op };
    int64_t func_params[] = { 0 };
    td_op_t* win = td_window_op(g, tbl_op,
                                parts, 1,
                                orders, order_descs, 1,
                                func_kinds, func_inputs, func_params, 1,
                                TD_FRAME_ROWS,
                                TD_BOUND_UNBOUNDED_PRECEDING,
                                TD_BOUND_UNBOUNDED_FOLLOWING,
                                0, 0);

    td_t* result = td_execute(g, win);
    munit_assert_false(TD_IS_ERR(result));
    munit_assert_int(result->type, ==, TD_TABLE);
    munit_assert_int(td_table_nrows(result), ==, 6);
    /* Window adds a column (row_number) to the 2-col input */
    munit_assert_true(td_table_ncols(result) >= 3);

    td_release(result);
    td_graph_free(g);
    td_release(tbl);
    td_sym_destroy();
    td_heap_destroy();
    return MUNIT_OK;
}

/* ---- SELECT (column projection) ---- */
static MunitResult test_exec_select(const void* params, void* data) {
    (void)params; (void)data;
    td_heap_init();
    td_t* tbl = make_exec_table();

    td_graph_t* g = td_graph_new(tbl);
    td_op_t* tbl_op = td_const_table(g, tbl);
    td_op_t* v1 = td_scan(g, "v1");
    td_op_t* id1 = td_scan(g, "id1");
    td_op_t* cols[] = { v1, id1 };
    td_op_t* sel = td_select(g, tbl_op, cols, 2);

    td_t* result = td_execute(g, sel);
    munit_assert_false(TD_IS_ERR(result));
    munit_assert_int(result->type, ==, TD_TABLE);
    munit_assert_int(td_table_ncols(result), ==, 2);
    munit_assert_int(td_table_nrows(result), ==, 10);

    /* Verify first column is v1 (I64) and has correct values */
    td_t* c0 = td_table_get_col_idx(result, 0);
    munit_assert_ptr_not_null(c0);
    munit_assert_int(c0->type, ==, TD_I64);
    int64_t* c0_data = (int64_t*)td_data(c0);
    munit_assert_int(c0_data[0], ==, 10);

    td_release(result);
    td_graph_free(g);
    td_release(tbl);
    td_sym_destroy();
    td_heap_destroy();
    return MUNIT_OK;
}

/* ---- STDDEV / VAR ---- */
static MunitResult test_exec_stddev(const void* params, void* data) {
    (void)params; (void)data;
    td_heap_init();
    td_sym_init();

    double vals[] = {2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0};
    td_t* vec = td_vec_from_raw(TD_F64, vals, 8);
    int64_t name = td_sym_intern("x", 1);
    td_t* tbl = td_table_new(1);
    tbl = td_table_add_col(tbl, name, vec);
    td_release(vec);

    /* VAR_POP = 4.0, STDDEV_POP = 2.0 for this dataset */
    td_graph_t* g = td_graph_new(tbl);
    td_op_t* x = td_scan(g, "x");
    td_op_t* var_op = td_var_pop(g, x);
    td_t* result = td_execute(g, var_op);
    munit_assert_false(TD_IS_ERR(result));
    munit_assert_double_equal(result->f64, 4.0, 6);
    td_release(result);
    td_graph_free(g);

    g = td_graph_new(tbl);
    x = td_scan(g, "x");
    td_op_t* stddev_op = td_stddev_pop(g, x);
    result = td_execute(g, stddev_op);
    munit_assert_false(TD_IS_ERR(result));
    munit_assert_double_equal(result->f64, 2.0, 6);
    td_release(result);
    td_graph_free(g);

    /* Sample variance: var_pop * n/(n-1) = 4.0 * 8/7 = 32/7 ≈ 4.571429 */
    g = td_graph_new(tbl);
    x = td_scan(g, "x");
    td_op_t* var_s = td_var(g, x);
    result = td_execute(g, var_s);
    munit_assert_false(TD_IS_ERR(result));
    munit_assert_double_equal(result->f64, 32.0 / 7.0, 5);
    td_release(result);
    td_graph_free(g);

    /* Sample stddev: sqrt(32/7) ≈ 2.138090 */
    g = td_graph_new(tbl);
    x = td_scan(g, "x");
    td_op_t* stddev_s = td_stddev(g, x);
    result = td_execute(g, stddev_s);
    munit_assert_false(TD_IS_ERR(result));
    munit_assert_double_equal(result->f64, sqrt(32.0 / 7.0), 5);
    td_release(result);
    td_graph_free(g);

    td_release(tbl);
    td_sym_destroy();
    td_heap_destroy();
    return MUNIT_OK;
}

/* ---- COUNT_DISTINCT ---- */
static MunitResult test_exec_count_distinct(const void* params, void* data) {
    (void)params; (void)data;
    td_heap_init();
    td_t* tbl = make_exec_table();

    /* id1 has values {1,1,2,2,3,3,1,2,3,1} → 3 distinct */
    td_graph_t* g = td_graph_new(tbl);
    td_op_t* id1 = td_scan(g, "id1");
    td_op_t* cd = td_count_distinct(g, id1);
    td_t* result = td_execute(g, cd);
    munit_assert_false(TD_IS_ERR(result));
    munit_assert_int(result->i64, ==, 3);
    td_release(result);
    td_graph_free(g);

    /* v1 has values {10,20,...,100} → 10 distinct */
    g = td_graph_new(tbl);
    td_op_t* v1 = td_scan(g, "v1");
    cd = td_count_distinct(g, v1);
    result = td_execute(g, cd);
    munit_assert_false(TD_IS_ERR(result));
    munit_assert_int(result->i64, ==, 10);
    td_release(result);
    td_graph_free(g);

    /* F64 column: {1.5, 2.5, 1.5, 2.5, 3.5} → 3 distinct */
    td_sym_init();
    double fvals[] = {1.5, 2.5, 1.5, 2.5, 3.5};
    td_t* fvec = td_vec_from_raw(TD_F64, fvals, 5);
    int64_t fname = td_sym_intern("f", 1);
    td_t* ftbl = td_table_new(1);
    ftbl = td_table_add_col(ftbl, fname, fvec);
    td_release(fvec);

    g = td_graph_new(ftbl);
    td_op_t* fop = td_scan(g, "f");
    cd = td_count_distinct(g, fop);
    result = td_execute(g, cd);
    munit_assert_false(TD_IS_ERR(result));
    munit_assert_int(result->i64, ==, 3);
    td_release(result);
    td_graph_free(g);
    td_release(ftbl);

    /* Single value repeated → 1 distinct */
    int64_t ones[] = {1, 1, 1, 1, 1};
    td_t* ones_v = td_vec_from_raw(TD_I64, ones, 5);
    int64_t name = td_sym_intern("x", 1);
    td_t* tbl2 = td_table_new(1);
    tbl2 = td_table_add_col(tbl2, name, ones_v);
    td_release(ones_v);

    g = td_graph_new(tbl2);
    td_op_t* x = td_scan(g, "x");
    cd = td_count_distinct(g, x);
    result = td_execute(g, cd);
    munit_assert_false(TD_IS_ERR(result));
    munit_assert_int(result->i64, ==, 1);
    td_release(result);
    td_graph_free(g);

    td_release(tbl2);
    td_release(tbl);
    td_sym_destroy();
    td_heap_destroy();
    return MUNIT_OK;
}

/* ---- ASOF JOIN ---- */
static MunitResult test_exec_asof_join(const void* params, void* data) {
    (void)params; (void)data;
    td_heap_init();
    td_sym_init();

    /* Left: trades — time(I64), sym(I64), price(F64) */
    int64_t ltime[]  = {100, 200, 300, 400, 500};
    int64_t lsym[]   = {1, 1, 2, 1, 2};
    double  lprice[] = {10.0, 20.0, 30.0, 40.0, 50.0};

    td_t* lt_v = td_vec_from_raw(TD_I64, ltime, 5);
    td_t* ls_v = td_vec_from_raw(TD_I64, lsym, 5);
    td_t* lp_v = td_vec_from_raw(TD_F64, lprice, 5);

    int64_t n_time  = td_sym_intern("time", 4);
    int64_t n_sym   = td_sym_intern("sym", 3);
    int64_t n_price = td_sym_intern("price", 5);

    td_t* left = td_table_new(3);
    left = td_table_add_col(left, n_time, lt_v);
    left = td_table_add_col(left, n_sym, ls_v);
    left = td_table_add_col(left, n_price, lp_v);
    td_release(lt_v); td_release(ls_v); td_release(lp_v);

    /* Right: quotes — time(I64), sym(I64), bid(F64) */
    int64_t rtime[] = {90, 150, 250, 350, 450};
    int64_t rsym[]  = {1, 1, 2, 1, 2};
    double  rbid[]  = {9.5, 15.0, 25.0, 35.0, 45.0};

    td_t* rt_v = td_vec_from_raw(TD_I64, rtime, 5);
    td_t* rs_v = td_vec_from_raw(TD_I64, rsym, 5);
    td_t* rb_v = td_vec_from_raw(TD_F64, rbid, 5);

    int64_t n_bid = td_sym_intern("bid", 3);

    td_t* right = td_table_new(3);
    right = td_table_add_col(right, n_time, rt_v);
    right = td_table_add_col(right, n_sym, rs_v);
    right = td_table_add_col(right, n_bid, rb_v);
    td_release(rt_v); td_release(rs_v); td_release(rb_v);

    td_graph_t* g = td_graph_new(left);
    td_op_t* left_op  = td_const_table(g, left);
    td_op_t* right_op = td_const_table(g, right);
    td_op_t* tkey = td_scan(g, "time");
    td_op_t* skey = td_scan(g, "sym");
    td_op_t* eq_keys[] = { skey };

    /* Inner ASOF join */
    td_op_t* aj = td_asof_join(g, left_op, right_op, tkey, eq_keys, 1, 0);

    td_t* result = td_execute(g, aj);
    munit_assert_false(TD_IS_ERR(result));
    munit_assert_int(result->type, ==, TD_TABLE);
    /* All 5 left rows should have matches */
    munit_assert_int(td_table_nrows(result), ==, 5);
    /* Should have left cols + bid (time/sym deduplicated) */
    munit_assert_int(td_table_ncols(result), ==, 4);  /* time, sym, price, bid */

    /* Verify bid values — DuckDB semantics: best right.time <= left.time per partition */
    td_t* bid_col = td_table_get_col(result, n_bid);
    munit_assert_ptr_not_null(bid_col);
    double* bid_data = (double*)td_data(bid_col);
    /* Sorted output order is by (sym, time): sym=1 rows first, then sym=2 */
    /* sym=1,t=100: right sym=1,t=90 -> bid=9.5 */
    munit_assert_double(bid_data[0], ==, 9.5);
    /* sym=1,t=200: right sym=1,t=150 -> bid=15.0 */
    munit_assert_double(bid_data[1], ==, 15.0);
    /* sym=1,t=400: right sym=1,t=350 -> bid=35.0 */
    munit_assert_double(bid_data[2], ==, 35.0);
    /* sym=2,t=300: right sym=2,t=250 -> bid=25.0 */
    munit_assert_double(bid_data[3], ==, 25.0);
    /* sym=2,t=500: right sym=2,t=450 -> bid=45.0 */
    munit_assert_double(bid_data[4], ==, 45.0);

    td_release(result);
    td_graph_free(g);
    td_release(left);
    td_release(right);
    td_sym_destroy();
    td_heap_destroy();
    return MUNIT_OK;
}

/* ---- ASOF LEFT JOIN ---- */
static MunitResult test_exec_asof_left_join(const void* params, void* data) {
    (void)params; (void)data;
    td_heap_init();
    td_sym_init();

    /* Left has a row with time=50 that's before any right row */
    int64_t ltime[] = {50, 100, 200};
    double  lval[]  = {1.0, 2.0, 3.0};
    td_t* lt_v = td_vec_from_raw(TD_I64, ltime, 3);
    td_t* lv_v = td_vec_from_raw(TD_F64, lval, 3);
    int64_t n_time = td_sym_intern("time", 4);
    int64_t n_val  = td_sym_intern("val", 3);
    td_t* left = td_table_new(2);
    left = td_table_add_col(left, n_time, lt_v);
    left = td_table_add_col(left, n_val, lv_v);
    td_release(lt_v); td_release(lv_v);

    int64_t rtime[] = {80, 150};
    double  rbid[]  = {0.8, 1.5};
    td_t* rt_v = td_vec_from_raw(TD_I64, rtime, 2);
    td_t* rb_v = td_vec_from_raw(TD_F64, rbid, 2);
    int64_t n_bid = td_sym_intern("bid", 3);
    td_t* right = td_table_new(2);
    right = td_table_add_col(right, n_time, rt_v);
    right = td_table_add_col(right, n_bid, rb_v);
    td_release(rt_v); td_release(rb_v);

    td_graph_t* g = td_graph_new(left);
    td_op_t* left_op  = td_const_table(g, left);
    td_op_t* right_op = td_const_table(g, right);
    td_op_t* tkey = td_scan(g, "time");

    /* Left outer ASOF join, no eq keys */
    td_op_t* aj = td_asof_join(g, left_op, right_op, tkey, NULL, 0, 1);

    td_t* result = td_execute(g, aj);
    munit_assert_false(TD_IS_ERR(result));
    /* Left outer: all 3 left rows preserved */
    munit_assert_int(td_table_nrows(result), ==, 3);
    /* Verify: time=50 has no match (before any right row), bid should be 0 (NULL fill) */
    td_t* bid_col = td_table_get_col(result, n_bid);
    munit_assert_ptr_not_null(bid_col);
    double* bid_data = (double*)td_data(bid_col);
    munit_assert_double(bid_data[0], ==, 0.0);   /* t=50: no match */
    munit_assert_double(bid_data[1], ==, 0.8);   /* t=100: right t=80 */
    munit_assert_double(bid_data[2], ==, 1.5);   /* t=200: right t=150 */

    td_release(result);
    td_graph_free(g);
    td_release(left);
    td_release(right);
    td_sym_destroy();
    td_heap_destroy();
    return MUNIT_OK;
}

/* ---- ASOF JOIN: empty right ---- */
static MunitResult test_exec_asof_empty(const void* params, void* data) {
    (void)params; (void)data;
    td_heap_init();
    td_sym_init();

    int64_t ltime[] = {100, 200};
    double  lval[]  = {1.0, 2.0};
    td_t* lt_v = td_vec_from_raw(TD_I64, ltime, 2);
    td_t* lv_v = td_vec_from_raw(TD_F64, lval, 2);
    int64_t n_time = td_sym_intern("time", 4);
    int64_t n_val  = td_sym_intern("val", 3);
    td_t* left = td_table_new(2);
    left = td_table_add_col(left, n_time, lt_v);
    left = td_table_add_col(left, n_val, lv_v);
    td_release(lt_v); td_release(lv_v);

    int64_t n_bid = td_sym_intern("bid", 3);
    td_t* right = td_table_new(2);
    td_t* rt_v = td_vec_new(TD_I64, 0);
    td_t* rb_v = td_vec_new(TD_F64, 0);
    right = td_table_add_col(right, n_time, rt_v);
    right = td_table_add_col(right, n_bid, rb_v);
    td_release(rt_v); td_release(rb_v);

    td_graph_t* g = td_graph_new(left);
    td_op_t* left_op  = td_const_table(g, left);
    td_op_t* right_op = td_const_table(g, right);
    td_op_t* tkey = td_scan(g, "time");

    /* Inner ASOF with empty right → 0 rows */
    td_op_t* aj = td_asof_join(g, left_op, right_op, tkey, NULL, 0, 0);
    td_t* result = td_execute(g, aj);
    munit_assert_false(TD_IS_ERR(result));
    munit_assert_int(td_table_nrows(result), ==, 0);

    td_release(result);
    td_graph_free(g);
    td_release(left);
    td_release(right);
    td_sym_destroy();
    td_heap_destroy();
    return MUNIT_OK;
}

/* ======================================================================
 * Suite
 * ====================================================================== */

static MunitTest exec_tests[] = {
    { "/neg_i64",        test_exec_neg_i64,           NULL, NULL, 0, NULL },
    { "/neg_f64",        test_exec_neg_f64,           NULL, NULL, 0, NULL },
    { "/abs",            test_exec_abs,               NULL, NULL, 0, NULL },
    { "/not",            test_exec_not,               NULL, NULL, 0, NULL },
    { "/isnull",         test_exec_isnull,            NULL, NULL, 0, NULL },
    { "/math_ops",       test_exec_math_ops,          NULL, NULL, 0, NULL },
    { "/ceil_floor",     test_exec_ceil_floor,        NULL, NULL, 0, NULL },
    { "/binary_arith",   test_exec_binary_arithmetic, NULL, NULL, 0, NULL },
    { "/comparisons",    test_exec_comparisons,       NULL, NULL, 0, NULL },
    { "/min2_max2",      test_exec_min2_max2,         NULL, NULL, 0, NULL },
    { "/if",             test_exec_if,                NULL, NULL, 0, NULL },
    { "/reductions",     test_exec_reductions,        NULL, NULL, 0, NULL },
    { "/sort",           test_exec_sort,              NULL, NULL, 0, NULL },
    { "/head_tail",      test_exec_head_tail,         NULL, NULL, 0, NULL },
    { "/join",           test_exec_join,              NULL, NULL, 0, NULL },
    { "/join_large",     test_exec_join_large,        NULL, NULL, 0, NULL },
    { "/join_fallback",  test_exec_join_fallback,     NULL, NULL, 0, NULL },
    { "/window",         test_exec_window,            NULL, NULL, 0, NULL },
    { "/select",         test_exec_select,            NULL, NULL, 0, NULL },
    { "/stddev",         test_exec_stddev,            NULL, NULL, 0, NULL },
    { "/count_distinct", test_exec_count_distinct,    NULL, NULL, 0, NULL },
    { "/asof_join",      test_exec_asof_join,      NULL, NULL, 0, NULL },
    { "/asof_left_join", test_exec_asof_left_join,  NULL, NULL, 0, NULL },
    { "/asof_empty",     test_exec_asof_empty,      NULL, NULL, 0, NULL },
    { NULL, NULL, NULL, NULL, 0, NULL }
};

MunitSuite test_exec_suite = {
    "/exec", exec_tests, NULL, 1, 0
};
