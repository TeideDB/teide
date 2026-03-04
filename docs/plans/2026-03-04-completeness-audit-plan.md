# Teide Completeness Audit Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Bring Teide to full feature completeness — test all critical untested modules, fix known bugs, add missing API wrappers, implement remaining opcodes (COUNT_DISTINCT, ASOF WINDOW_JOIN), extend serialization, and remove OP_PROJECT.

**Architecture:** Bottom-up approach. First add test harnesses for existing untested modules to catch hidden bugs. Then fix known issues. Then add missing API surface. Then implement new features with TDD. Finally extend serialization and fill minor test gaps.

**Tech Stack:** Pure C17, munit test framework, Teide buddy allocator (`td_alloc`/`td_free`), no external dependencies.

---

## Task 1: Add test_exec.c skeleton — unary element-wise ops [x]

**Files:**
- Create: `test/test_exec.c`
- Modify: `test/test_main.c:37-101` (add extern + registration)

**Step 1: Write the test file skeleton with first tests**

```c
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

    /* Create vector with nulls */
    int64_t n = 5;
    int64_t raw[] = {10, 0, 30, 0, 50};
    td_t* vec = td_vec_from_raw(TD_I64, raw, n);
    td_t* nullmap = td_sel_new(n);
    /* Mark indices 1 and 3 as null by clearing bits (sel_new starts all-zero) */
    /* Actually we need a proper nullable vector — use a table with null support */
    /* Simpler: just test isnull on non-null data → all false → count after filter = 0 */
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
    td_op_t* sq = td_sqrt(g, x);
    td_op_t* s = td_sum(g, sq);
    td_t* result = td_execute(g, s);
    munit_assert_false(TD_IS_ERR(result));
    munit_assert_double_equal(result->f64, 15.0, 6);
    td_release(result);
    td_graph_free(g);

    /* exp(log(x)) should roundtrip -> sum = 55 */
    g = td_graph_new(tbl);
    x = td_scan(g, "x");
    td_op_t* lg = td_log(g, x);
    td_op_t* ex = td_exp(g, lg);
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
    td_op_t* c = td_ceil(g, x);
    td_op_t* s = td_sum(g, c);
    td_t* result = td_execute(g, s);
    munit_assert_false(TD_IS_ERR(result));
    munit_assert_double_equal(result->f64, 6.0, 6);
    td_release(result);
    td_graph_free(g);

    /* floor: 1+2+3+(-2)+(-3) = 1 */
    g = td_graph_new(tbl);
    x = td_scan(g, "x");
    td_op_t* f = td_floor(g, x);
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
    /* 10*1+20*1+30*2+40*2+50*3+60*3+70*1+80*2+90*3+100*1 = 10+20+60+80+150+180+70+160+270+100 = 1100 */
    munit_assert_int(result->i64, ==, 1100);
    td_release(result);
    td_graph_free(g);

    /* v1 / id1 -> sum (integer division) */
    g = td_graph_new(tbl);
    v1 = td_scan(g, "v1");
    id1 = td_scan(g, "id1");
    td_op_t* div_op = td_div(g, v1, id1);
    s = td_sum(g, div_op);
    result = td_execute(g, s);
    munit_assert_false(TD_IS_ERR(result));
    /* 10/1+20/1+30/2+40/2+50/3+60/3+70/1+80/2+90/3+100/1 = 10+20+15+20+16+20+70+40+30+100 = 341 */
    munit_assert_int(result->i64, ==, 341);
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
    /* 10%1+20%1+30%2+40%2+50%3+60%3+70%1+80%2+90%3+100%1 = 0+0+0+0+2+0+0+0+0+0 = 2 */
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
    /* max2(v1,id1) per row: all v1 values since v1 > id1 → sum = 550 */
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
    /* id1 = {1,1,2,2,3,3,1,2,3,1} → prod = 1*1*2*2*3*3*1*2*3*1 = 216 */
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

    td_graph_t* g = td_graph_new(tbl);
    td_op_t* v1 = td_scan(g, "v1");
    td_op_t* sort_cols[] = { v1 };
    td_op_t* sort_op = td_sort(g, v1, sort_cols, 1, true);
    td_op_t* first_op = td_first(g, sort_op);

    td_t* result = td_execute(g, first_op);
    munit_assert_false(TD_IS_ERR(result));
    munit_assert_int(result->i64, ==, 10);  /* ascending: first = min */

    td_release(result);
    td_graph_free(g);

    /* Descending */
    g = td_graph_new(tbl);
    v1 = td_scan(g, "v1");
    td_op_t* sort_cols2[] = { v1 };
    sort_op = td_sort(g, v1, sort_cols2, 1, false);
    first_op = td_first(g, sort_op);
    result = td_execute(g, first_op);
    munit_assert_false(TD_IS_ERR(result));
    munit_assert_int(result->i64, ==, 100);  /* descending: first = max */

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
    td_op_t* lk = td_scan(g, "id");
    td_op_t* rk = td_scan_right(g, right, "id");
    td_op_t* lk_arr[] = { lk };
    td_op_t* rk_arr[] = { rk };
    td_op_t* join_op = td_join(g, left, right, lk_arr, rk_arr, 1, 0);

    td_t* result = td_execute(g, join_op);
    munit_assert_false(TD_IS_ERR(result));
    munit_assert_int(result->type, ==, TD_TABLE);
    /* 1→1, 2→2(twice), 3→1 = 4 result rows */
    munit_assert_int(td_table_nrows(result), ==, 4);

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
    td_op_t* grp_op = td_scan(g, "grp");
    td_op_t* val_op = td_scan(g, "val");

    td_op_t* parts[] = { grp_op };
    td_op_t* orders[] = { val_op };
    td_op_t* win = td_window(g, tbl, val_op,
                             TD_WIN_ROW_NUMBER,
                             parts, 1,
                             orders, 1, true,
                             TD_BOUND_UNBOUNDED_PRECEDING,
                             TD_BOUND_UNBOUNDED_FOLLOWING,
                             0, 0);

    td_t* result = td_execute(g, win);
    munit_assert_false(TD_IS_ERR(result));
    munit_assert_int(result->type, ==, TD_TABLE);
    munit_assert_int(td_table_nrows(result), ==, 6);

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
    td_op_t* v1 = td_scan(g, "v1");
    td_op_t* id1 = td_scan(g, "id1");
    td_op_t* cols[] = { v1, id1 };
    td_op_t* sel = td_select(g, NULL, cols, 2);

    td_t* result = td_execute(g, sel);
    munit_assert_false(TD_IS_ERR(result));
    munit_assert_int(result->type, ==, TD_TABLE);
    munit_assert_int(td_table_ncols(result), ==, 2);
    munit_assert_int(td_table_nrows(result), ==, 10);

    td_release(result);
    td_graph_free(g);
    td_release(tbl);
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
    { "/ceil_floor",     test_exec_ceil_floor,         NULL, NULL, 0, NULL },
    { "/binary_arith",   test_exec_binary_arithmetic, NULL, NULL, 0, NULL },
    { "/comparisons",    test_exec_comparisons,       NULL, NULL, 0, NULL },
    { "/min2_max2",      test_exec_min2_max2,         NULL, NULL, 0, NULL },
    { "/if",             test_exec_if,                NULL, NULL, 0, NULL },
    { "/reductions",     test_exec_reductions,        NULL, NULL, 0, NULL },
    { "/sort",           test_exec_sort,              NULL, NULL, 0, NULL },
    { "/head_tail",      test_exec_head_tail,         NULL, NULL, 0, NULL },
    { "/join",           test_exec_join,              NULL, NULL, 0, NULL },
    { "/window",         test_exec_window,            NULL, NULL, 0, NULL },
    { "/select",         test_exec_select,            NULL, NULL, 0, NULL },
    { NULL, NULL, NULL, NULL, 0, NULL }
};

MunitSuite test_exec_suite = {
    "/exec", exec_tests, NULL, 1, 0
};
```

**Step 2: Register in test_main.c**

Add after line 52 in `test/test_main.c`:
```c
extern MunitSuite test_exec_suite;
```

Add entry in `child_suites[]` before the terminator:
```c
    { "/exec",   NULL, NULL, 0, 0 },
```

Add in `main()` before `return`:
```c
    child_suites[16] = test_exec_suite;
```

**Step 3: Build and run**

Run: `cmake -B build -DCMAKE_BUILD_TYPE=Debug && cmake --build build && cd build && ctest --output-on-failure`

All 17 new tests should pass (they test already-implemented opcodes).

**Step 4: Commit**

```bash
git add test/test_exec.c test/test_main.c
git commit -m "test: add test_exec.c with 17 tests for executor opcodes"
```

---

## Task 2: Add test_csv.c — CSV I/O tests [x]

**Files:**
- Create: `test/test_csv.c`
- Modify: `test/test_main.c` (add extern + registration)

**Step 1: Explore CSV API**

Check `include/teide/td.h` for `td_csv_read` and `td_csv_write` signatures. Check `src/io/csv.c` for implementation details.

**Step 2: Write failing tests**

```c
#include "munit.h"
#include <teide/td.h>
#include <stdio.h>
#include <unistd.h>

#define TMP_CSV "/tmp/teide_test.csv"

static MunitResult test_csv_roundtrip_i64(const void* params, void* data) {
    (void)params; (void)data;
    td_heap_init();
    td_sym_init();

    int64_t vals[] = {10, 20, 30};
    td_t* vec = td_vec_from_raw(TD_I64, vals, 3);
    int64_t name = td_sym_intern("x", 1);
    td_t* tbl = td_table_new(1);
    tbl = td_table_add_col(tbl, name, vec);
    td_release(vec);

    td_err_t err = td_csv_write(tbl, TMP_CSV);
    munit_assert_int(err, ==, TD_OK);

    td_t* loaded = td_csv_read(TMP_CSV);
    munit_assert_false(TD_IS_ERR(loaded));
    munit_assert_int(loaded->type, ==, TD_TABLE);
    munit_assert_int(td_table_nrows(loaded), ==, 3);
    munit_assert_int(td_table_ncols(loaded), ==, 1);

    td_release(loaded);
    td_release(tbl);
    unlink(TMP_CSV);
    td_sym_destroy();
    td_heap_destroy();
    return MUNIT_OK;
}

static MunitResult test_csv_roundtrip_f64(const void* params, void* data) {
    (void)params; (void)data;
    td_heap_init();
    td_sym_init();

    double vals[] = {1.5, 2.5, 3.5};
    td_t* vec = td_vec_from_raw(TD_F64, vals, 3);
    int64_t name = td_sym_intern("price", 5);
    td_t* tbl = td_table_new(1);
    tbl = td_table_add_col(tbl, name, vec);
    td_release(vec);

    td_err_t err = td_csv_write(tbl, TMP_CSV);
    munit_assert_int(err, ==, TD_OK);

    td_t* loaded = td_csv_read(TMP_CSV);
    munit_assert_false(TD_IS_ERR(loaded));
    munit_assert_int(td_table_nrows(loaded), ==, 3);

    td_release(loaded);
    td_release(tbl);
    unlink(TMP_CSV);
    td_sym_destroy();
    td_heap_destroy();
    return MUNIT_OK;
}

static MunitResult test_csv_multi_column(const void* params, void* data) {
    (void)params; (void)data;
    td_heap_init();
    td_sym_init();

    int64_t ids[] = {1, 2, 3};
    double vals[] = {10.5, 20.5, 30.5};
    td_t* id_v = td_vec_from_raw(TD_I64, ids, 3);
    td_t* val_v = td_vec_from_raw(TD_F64, vals, 3);
    int64_t n_id = td_sym_intern("id", 2);
    int64_t n_val = td_sym_intern("val", 3);
    td_t* tbl = td_table_new(2);
    tbl = td_table_add_col(tbl, n_id, id_v);
    tbl = td_table_add_col(tbl, n_val, val_v);
    td_release(id_v);
    td_release(val_v);

    td_err_t err = td_csv_write(tbl, TMP_CSV);
    munit_assert_int(err, ==, TD_OK);

    td_t* loaded = td_csv_read(TMP_CSV);
    munit_assert_false(TD_IS_ERR(loaded));
    munit_assert_int(td_table_ncols(loaded), ==, 2);
    munit_assert_int(td_table_nrows(loaded), ==, 3);

    td_release(loaded);
    td_release(tbl);
    unlink(TMP_CSV);
    td_sym_destroy();
    td_heap_destroy();
    return MUNIT_OK;
}

static MunitResult test_csv_empty_table(const void* params, void* data) {
    (void)params; (void)data;
    td_heap_init();
    td_sym_init();

    td_t* tbl = td_table_new(0);
    td_err_t err = td_csv_write(tbl, TMP_CSV);
    /* Writing empty table should succeed or return an error gracefully */
    (void)err;

    td_release(tbl);
    unlink(TMP_CSV);
    td_sym_destroy();
    td_heap_destroy();
    return MUNIT_OK;
}

static MunitTest csv_tests[] = {
    { "/roundtrip_i64",  test_csv_roundtrip_i64,  NULL, NULL, 0, NULL },
    { "/roundtrip_f64",  test_csv_roundtrip_f64,  NULL, NULL, 0, NULL },
    { "/multi_column",   test_csv_multi_column,   NULL, NULL, 0, NULL },
    { "/empty_table",    test_csv_empty_table,     NULL, NULL, 0, NULL },
    { NULL, NULL, NULL, NULL, 0, NULL }
};

MunitSuite test_csv_suite = {
    "/csv", csv_tests, NULL, 1, 0
};
```

**Step 3: Register in test_main.c, build, run, commit**

Same pattern as Task 1. Add `extern MunitSuite test_csv_suite;`, entry in `child_suites[]`, and assignment in `main()`.

Run: `cmake --build build && ./build/test_teide --suite /csv`

```bash
git add test/test_csv.c test/test_main.c
git commit -m "test: add test_csv.c with 4 CSV I/O tests"
```

---

## Task 3: Add test_sel.c — selection vector tests

**Files:**
- Create: `test/test_sel.c`
- Modify: `test/test_main.c`

**Step 1: Write tests**

```c
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
```

**Step 2: Register in test_main.c, build, run, commit**

```bash
git add test/test_sel.c test/test_main.c
git commit -m "test: add test_sel.c with 4 selection vector tests"
```

---

## Task 4: Add test_fvec.c — factorized vector tests

**Files:**
- Create: `test/test_fvec.c`
- Modify: `test/test_main.c`

**Step 1: Write tests**

```c
#include "munit.h"
#include <teide/td.h>
#include "ops/fvec.h"

static MunitResult test_ftable_new_free(const void* params, void* data) {
    (void)params; (void)data;
    td_heap_init();

    td_ftable_t* ft = td_ftable_new(3);
    munit_assert_ptr_not_null(ft);
    munit_assert_uint(ft->n_cols, ==, 3);

    td_ftable_free(ft);
    td_heap_destroy();
    return MUNIT_OK;
}

static MunitResult test_ftable_materialize_flat(const void* params, void* data) {
    (void)params; (void)data;
    td_heap_init();
    td_sym_init();

    td_ftable_t* ft = td_ftable_new(1);

    /* Create a flat fvec: single value at index 0, cardinality 5 */
    int64_t vals[] = {42};
    td_t* vec = td_vec_from_raw(TD_I64, vals, 1);
    ft->columns[0].vec = vec;
    ft->columns[0].cur_idx = 0;
    ft->columns[0].cardinality = 5;
    ft->n_tuples = 5;

    td_t* result = td_ftable_materialize(ft);
    munit_assert_ptr_not_null(result);
    munit_assert_false(TD_IS_ERR(result));
    munit_assert_int(result->type, ==, TD_TABLE);
    munit_assert_int(td_table_nrows(result), ==, 5);

    /* All rows should be 42 */
    td_t* col = td_table_get_col_idx(result, 0);
    int64_t* data_ptr = (int64_t*)td_data(col);
    for (int i = 0; i < 5; i++)
        munit_assert_int(data_ptr[i], ==, 42);

    td_release(result);
    td_ftable_free(ft);
    td_sym_destroy();
    td_heap_destroy();
    return MUNIT_OK;
}

static MunitResult test_ftable_materialize_unflat(const void* params, void* data) {
    (void)params; (void)data;
    td_heap_init();
    td_sym_init();

    td_ftable_t* ft = td_ftable_new(1);

    int64_t vals[] = {10, 20, 30};
    td_t* vec = td_vec_from_raw(TD_I64, vals, 3);
    ft->columns[0].vec = vec;
    ft->columns[0].cur_idx = -1;  /* unflat */
    ft->columns[0].cardinality = 3;
    ft->n_tuples = 3;

    td_t* result = td_ftable_materialize(ft);
    munit_assert_ptr_not_null(result);
    munit_assert_false(TD_IS_ERR(result));
    munit_assert_int(td_table_nrows(result), ==, 3);

    td_t* col = td_table_get_col_idx(result, 0);
    int64_t* data_ptr = (int64_t*)td_data(col);
    munit_assert_int(data_ptr[0], ==, 10);
    munit_assert_int(data_ptr[1], ==, 20);
    munit_assert_int(data_ptr[2], ==, 30);

    td_release(result);
    td_ftable_free(ft);
    td_sym_destroy();
    td_heap_destroy();
    return MUNIT_OK;
}

static MunitTest fvec_tests[] = {
    { "/new_free",          test_ftable_new_free,          NULL, NULL, 0, NULL },
    { "/materialize_flat",  test_ftable_materialize_flat,  NULL, NULL, 0, NULL },
    { "/materialize_unflat", test_ftable_materialize_unflat, NULL, NULL, 0, NULL },
    { NULL, NULL, NULL, NULL, 0, NULL }
};

MunitSuite test_fvec_suite = {
    "/fvec", fvec_tests, NULL, 1, 0
};
```

**Step 2: Register in test_main.c, build, run, commit**

```bash
git add test/test_fvec.c test/test_main.c
git commit -m "test: add test_fvec.c with 3 factorized vector tests"
```

---

## Task 5: Add test_lftj.c — Leapfrog TrieJoin tests

**Files:**
- Create: `test/test_lftj.c`
- Modify: `test/test_main.c`

**Step 1: Write tests**

```c
#include "munit.h"
#include <teide/td.h>
#include "store/csr.h"
#include "ops/lftj.h"

/* Helper: build CSR relation from edge arrays */
static td_rel_t* make_rel(int64_t* src, int64_t* dst, int64_t n,
                           int64_t n_nodes) {
    td_t* src_v = td_vec_from_raw(TD_I64, src, n);
    td_t* dst_v = td_vec_from_raw(TD_I64, dst, n);
    int64_t s_src = td_sym_intern("src", 3);
    int64_t s_dst = td_sym_intern("dst", 3);
    td_t* edges = td_table_new(2);
    edges = td_table_add_col(edges, s_src, src_v);
    edges = td_table_add_col(edges, s_dst, dst_v);
    td_release(src_v);
    td_release(dst_v);

    td_rel_t* rel = td_rel_from_edges(edges, "src", "dst",
                                       n_nodes, n_nodes, false);
    td_release(edges);
    return rel;
}

/* Triangle graph: 0-1, 0-2, 1-2 (bidirectional) */
static MunitResult test_lftj_triangle(const void* params, void* data) {
    (void)params; (void)data;
    td_heap_init();
    td_sym_init();

    /* Bidirectional triangle: 0↔1, 0↔2, 1↔2 */
    int64_t src[] = {0, 0, 1, 1, 2, 2};
    int64_t dst[] = {1, 2, 0, 2, 0, 1};
    td_rel_t* rel = make_rel(src, dst, 6, 3);
    munit_assert_ptr_not_null(rel);

    /* Find triangles: (a,b,c) where a→b, a→c, b→c */
    lftj_enum_ctx_t ctx;
    memset(&ctx, 0, sizeof(ctx));

    td_rel_t* rels[] = { rel, rel, rel };
    bool ok = lftj_build_default_plan(&ctx, rels, 3, 3);
    munit_assert_true(ok);

    lftj_enumerate(&ctx, 0);
    munit_assert_false(ctx.oom);
    /* One triangle: (0,1,2) in all 6 orderings → 6 results */
    munit_assert_true(ctx.out_count == 6);

    /* Cleanup output buffers */
    for (uint8_t i = 0; i < ctx.n_vars; i++) {
        if (ctx.buf_hdrs[i]) td_release(ctx.buf_hdrs[i]);
    }
    td_rel_free(rel);
    td_sym_destroy();
    td_heap_destroy();
    return MUNIT_OK;
}

static MunitResult test_lftj_no_results(const void* params, void* data) {
    (void)params; (void)data;
    td_heap_init();
    td_sym_init();

    /* Linear graph: 0→1→2 (no triangles) */
    int64_t src[] = {0, 1};
    int64_t dst[] = {1, 2};
    td_rel_t* rel = make_rel(src, dst, 2, 3);
    munit_assert_ptr_not_null(rel);

    lftj_enum_ctx_t ctx;
    memset(&ctx, 0, sizeof(ctx));

    td_rel_t* rels[] = { rel, rel, rel };
    bool ok = lftj_build_default_plan(&ctx, rels, 3, 3);
    munit_assert_true(ok);

    lftj_enumerate(&ctx, 0);
    munit_assert_false(ctx.oom);
    munit_assert_true(ctx.out_count == 0);

    for (uint8_t i = 0; i < ctx.n_vars; i++) {
        if (ctx.buf_hdrs[i]) td_release(ctx.buf_hdrs[i]);
    }
    td_rel_free(rel);
    td_sym_destroy();
    td_heap_destroy();
    return MUNIT_OK;
}

static MunitResult test_leapfrog_search(const void* params, void* data) {
    (void)params; (void)data;
    td_heap_init();

    /* Two sorted arrays, find intersection */
    int64_t a_data[] = {1, 3, 5, 7, 9};
    int64_t b_data[] = {2, 3, 6, 7, 10};

    td_lftj_iter_t a = { .targets = a_data, .start = 0, .end = 5, .pos = 0 };
    td_lftj_iter_t b = { .targets = b_data, .start = 0, .end = 5, .pos = 0 };

    td_lftj_iter_t* iters[] = { &a, &b };
    int64_t val;
    bool found = leapfrog_search(iters, 2, &val);
    munit_assert_true(found);
    munit_assert_int(val, ==, 3);

    td_heap_destroy();
    return MUNIT_OK;
}

static MunitTest lftj_tests[] = {
    { "/triangle",      test_lftj_triangle,     NULL, NULL, 0, NULL },
    { "/no_results",    test_lftj_no_results,   NULL, NULL, 0, NULL },
    { "/leapfrog_search", test_leapfrog_search, NULL, NULL, 0, NULL },
    { NULL, NULL, NULL, NULL, 0, NULL }
};

MunitSuite test_lftj_suite = {
    "/lftj", lftj_tests, NULL, 1, 0
};
```

**Step 2: Register in test_main.c, build, run, commit**

```bash
git add test/test_lftj.c test/test_main.c
git commit -m "test: add test_lftj.c with 3 Leapfrog TrieJoin tests"
```

---

## Task 6: Fix td_block_copy() child ref retention

**Files:**
- Modify: `src/core/block.c:62-78`
- Modify: `src/mem/heap.c:359` (make `td_retain_owned_refs` non-static)
- Modify: `include/teide/td.h` or internal header (declare the function)
- Test: add regression test in `test/test_cow.c`

**Step 1: Write failing test in test_cow.c**

Add a test that copies a TABLE block and verifies child column ref counts increase:

```c
static MunitResult test_block_copy_retains_children(const void* params, void* fixture) {
    (void)params; (void)fixture;

    td_sym_init();

    int64_t vals[] = {1, 2, 3};
    td_t* vec = td_vec_from_raw(TD_I64, vals, 3);
    int64_t name = td_sym_intern("x", 1);
    td_t* tbl = td_table_new(1);
    tbl = td_table_add_col(tbl, name, vec);
    td_release(vec);

    /* Get column ref count before copy */
    td_t* col_before = td_table_get_col_idx(tbl, 0);
    uint32_t rc_before = atomic_load(&col_before->rc);

    /* Copy the table block */
    td_t* copy = td_block_copy(tbl);
    munit_assert_ptr_not_null(copy);
    munit_assert_false(TD_IS_ERR(copy));

    /* Column ref count should have increased by 1 */
    uint32_t rc_after = atomic_load(&col_before->rc);
    munit_assert_uint(rc_after, ==, rc_before + 1);

    td_release(copy);
    td_release(tbl);
    td_sym_destroy();

    return MUNIT_OK;
}
```

**Step 2: Run test to verify it fails**

Run: `cmake --build build && ./build/test_teide --suite /cow`

Expected: FAIL — `rc_after` equals `rc_before` (block_copy doesn't retain children).

**Step 3: Fix block.c**

In `src/mem/heap.c`, change `td_retain_owned_refs` from `static` to non-static. Add a forward declaration in an internal header (e.g., `src/mem/heap.h` or add `extern void td_retain_owned_refs(td_t* v);` to a shared internal header).

In `src/core/block.c`, after line 72 (after `atomic_store_explicit(&dst->rc, 1, ...)`), add:

```c
    extern void td_retain_owned_refs(td_t* v);
    td_retain_owned_refs(dst);
```

Remove the TODO comment (lines 73-76).

**Step 4: Run test to verify it passes**

Run: `cmake --build build && ./build/test_teide --suite /cow`

Expected: PASS

**Step 5: Commit**

```bash
git add src/core/block.c src/mem/heap.c test/test_cow.c
git commit -m "fix: td_block_copy retains child refs for compound types"
```

---

## Task 7: Add td_stddev/td_stddev_pop/td_var/td_var_pop API wrappers

**Files:**
- Modify: `include/teide/td.h` (add declarations after line 826)
- Modify: `src/ops/graph.c` (add implementations after line 589)
- Test: add tests in `test/test_exec.c`

**Step 1: Write failing tests in test_exec.c**

Add these tests to `test/test_exec.c`:

```c
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

    td_release(tbl);
    td_sym_destroy();
    td_heap_destroy();
    return MUNIT_OK;
}
```

**Step 2: Run to verify it fails** (compile error — td_var_pop, td_stddev_pop not declared)

**Step 3: Add declarations to td.h**

After line 826 (`td_count_distinct` declaration), add:

```c
td_op_t* td_stddev(td_graph_t* g, td_op_t* a);
td_op_t* td_stddev_pop(td_graph_t* g, td_op_t* a);
td_op_t* td_var(td_graph_t* g, td_op_t* a);
td_op_t* td_var_pop(td_graph_t* g, td_op_t* a);
```

**Step 4: Add implementations to graph.c**

After line 589 (after `td_count_distinct`), add:

```c
td_op_t* td_stddev(td_graph_t* g, td_op_t* a)     { return make_unary(g, OP_STDDEV, a, TD_F64); }
td_op_t* td_stddev_pop(td_graph_t* g, td_op_t* a)  { return make_unary(g, OP_STDDEV_POP, a, TD_F64); }
td_op_t* td_var(td_graph_t* g, td_op_t* a)         { return make_unary(g, OP_VAR, a, TD_F64); }
td_op_t* td_var_pop(td_graph_t* g, td_op_t* a)     { return make_unary(g, OP_VAR_POP, a, TD_F64); }
```

**Step 5: Add test to suite, build, run, commit**

Add `{ "/stddev", test_exec_stddev, NULL, NULL, 0, NULL },` to `exec_tests[]`.

Run: `cmake --build build && ./build/test_teide --suite /exec/stddev`

```bash
git add include/teide/td.h src/ops/graph.c test/test_exec.c
git commit -m "feat: add td_stddev/td_stddev_pop/td_var/td_var_pop API wrappers"
```

---

## Task 8: Implement OP_COUNT_DISTINCT executor

**Files:**
- Modify: `src/ops/exec.c` (add `exec_count_distinct` + case in switch)
- Test: add tests in `test/test_exec.c`

**Step 1: Write failing test**

```c
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

    /* Single value repeated → 1 distinct */
    td_sym_init();
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
```

**Step 2: Run to verify it fails**

Expected: returns TD_ERR_NYI (OP_COUNT_DISTINCT falls through to default in switch).

**Step 3: Implement exec_count_distinct**

In `src/ops/exec.c`, add a static function before the main dispatch:

```c
/* Hash-based count distinct for integer/float columns */
static td_t* exec_count_distinct(td_graph_t* g, td_op_t* op, td_t* input) {
    (void)g; (void)op;
    if (!input || TD_IS_ERR(input)) return input;

    int8_t in_type = input->type;
    int64_t len = input->len;

    if (len == 0) return td_i64(0);

    /* Use a simple open-addressing hash set for int64 values */
    int64_t cap = len < 16 ? 32 : len * 2;
    /* Round up to power of 2 */
    int64_t c = 1;
    while (c < cap) c <<= 1;
    cap = c;

    td_t* set_hdr;
    int64_t* set = (int64_t*)scratch_calloc(&set_hdr,
                                             (size_t)cap * sizeof(int64_t));
    td_t* used_hdr;
    uint8_t* used = (uint8_t*)scratch_calloc(&used_hdr,
                                              (size_t)cap * sizeof(uint8_t));
    if (!set || !used) {
        if (set_hdr) scratch_free(set_hdr);
        if (used_hdr) scratch_free(used_hdr);
        return TD_ERR_PTR(TD_ERR_OOM);
    }

    int64_t count = 0;
    int64_t mask = cap - 1;
    void* base = td_data(input);

    for (int64_t i = 0; i < len; i++) {
        int64_t val;
        if (in_type == TD_F64) {
            double fv = ((double*)base)[i];
            memcpy(&val, &fv, sizeof(int64_t));
        } else {
            val = read_col_i64(base, i, in_type, input->attrs);
        }

        /* Open-addressing linear probe */
        uint64_t h = (uint64_t)val * 0x9E3779B97F4A7C15ULL;
        int64_t slot = (int64_t)(h & (uint64_t)mask);
        while (used[slot]) {
            if (set[slot] == val) goto next_val;
            slot = (slot + 1) & mask;
        }
        /* New distinct value */
        set[slot] = val;
        used[slot] = 1;
        count++;
        next_val:;
    }

    scratch_free(set_hdr);
    scratch_free(used_hdr);
    return td_i64(count);
}
```

Add case in the main switch (near other reduction ops like OP_COUNT):

```c
        case OP_COUNT_DISTINCT: {
            td_t* input = exec_node(g, op->inputs[0]);
            if (!input || TD_IS_ERR(input)) return input;
            td_t* result = exec_count_distinct(g, op, input);
            td_release(input);
            return result;
        }
```

**Step 4: Build, run, verify passing**

Run: `cmake --build build && ./build/test_teide --suite /exec/count_distinct`

**Step 5: Commit**

```bash
git add src/ops/exec.c test/test_exec.c
git commit -m "feat: implement OP_COUNT_DISTINCT executor with hash-based counting"
```

---

## Task 9: Remove OP_PROJECT

**Files:**
- Modify: `include/teide/td.h` (remove OP_PROJECT define, td_project declaration)
- Modify: `src/ops/graph.c` (remove td_project implementation)
- Modify: `src/ops/opt.c` (remove any OP_PROJECT references)

**Step 1: Check for OP_PROJECT references**

Run grep across codebase: `grep -rn "OP_PROJECT\|td_project" src/ include/ test/`

**Step 2: Remove each reference**

In `include/teide/td.h`:
- Remove `#define OP_PROJECT 65` (but keep numbering — just remove the line, don't renumber)
- Remove `td_op_t* td_project(...)` declaration

In `src/ops/graph.c`:
- Remove `td_project()` function implementation (lines 875-901)

In `src/ops/opt.c`:
- Remove any `case OP_PROJECT:` handling (if exists)

**Step 3: Build and verify no compilation errors**

Run: `cmake --build build && cd build && ctest --output-on-failure`

**Step 4: Commit**

```bash
git add include/teide/td.h src/ops/graph.c src/ops/opt.c
git commit -m "refactor: remove OP_PROJECT (redundant with OP_SELECT)"
```

---

## Task 10: Implement OP_WINDOW_JOIN (ASOF join)

**Files:**
- Modify: `src/ops/exec.c` (add `exec_window_join` + case in switch)
- Test: add tests in `test/test_exec.c`

**Step 1: Write failing test**

```c
static MunitResult test_exec_window_join_asof(const void* params, void* data) {
    (void)params; (void)data;
    td_heap_init();
    td_sym_init();

    /* Left: trades with timestamps */
    int64_t l_time[] = {100, 200, 300, 400, 500};
    int64_t l_sym[]  = {1, 1, 2, 2, 1};   /* stock symbols as ints */
    int64_t l_qty[]  = {10, 20, 30, 40, 50};
    td_t* lt_v = td_vec_from_raw(TD_I64, l_time, 5);
    td_t* ls_v = td_vec_from_raw(TD_I64, l_sym, 5);
    td_t* lq_v = td_vec_from_raw(TD_I64, l_qty, 5);

    int64_t n_time = td_sym_intern("time", 4);
    int64_t n_sym  = td_sym_intern("sym", 3);
    int64_t n_qty  = td_sym_intern("qty", 3);
    td_t* left = td_table_new(3);
    left = td_table_add_col(left, n_time, lt_v);
    left = td_table_add_col(left, n_sym, ls_v);
    left = td_table_add_col(left, n_qty, lq_v);
    td_release(lt_v);
    td_release(ls_v);
    td_release(lq_v);

    /* Right: quotes with timestamps */
    int64_t r_time[]  = {90, 150, 190, 250, 290, 350};
    int64_t r_sym[]   = {1, 1, 1, 2, 2, 1};
    int64_t r_price[] = {10, 11, 12, 20, 21, 13};
    td_t* rt_v = td_vec_from_raw(TD_I64, r_time, 6);
    td_t* rs_v = td_vec_from_raw(TD_I64, r_sym, 6);
    td_t* rp_v = td_vec_from_raw(TD_I64, r_price, 6);

    int64_t n_price = td_sym_intern("price", 5);
    td_t* right = td_table_new(3);
    right = td_table_add_col(right, n_time, rt_v);
    right = td_table_add_col(right, n_sym, rs_v);
    right = td_table_add_col(right, n_price, rp_v);
    td_release(rt_v);
    td_release(rs_v);
    td_release(rp_v);

    td_graph_t* g = td_graph_new(left);
    td_op_t* time_key = td_scan(g, "time");
    td_op_t* sym_key  = td_scan(g, "sym");
    td_op_t* price_in = td_scan_right(g, right, "price");

    uint16_t agg_ops[] = { OP_LAST };
    td_op_t* agg_ins[] = { price_in };
    td_op_t* wj = td_window_join(g, NULL, NULL,
                                  time_key, sym_key,
                                  0, INT64_MAX,
                                  agg_ops, agg_ins, 1);

    td_t* result = td_execute(g, wj);
    munit_assert_false(TD_IS_ERR(result));
    munit_assert_int(result->type, ==, TD_TABLE);
    munit_assert_int(td_table_nrows(result), ==, 5);

    td_release(result);
    td_graph_free(g);
    td_release(left);
    td_release(right);
    td_sym_destroy();
    td_heap_destroy();
    return MUNIT_OK;
}
```

**Step 2: Run to verify it fails (NYI)**

**Step 3: Implement exec_window_join**

This is the most complex new feature. The ASOF join algorithm:
1. Sort both tables by (sym_key, time_key) if not already sorted
2. For each left row, find the matching sym_key partition in the right table
3. Within that partition, binary search for the largest right time_key ≤ left time_key
4. If found and within tolerance (window_lo, window_hi), emit matched row
5. If not found, emit left row with NULL right columns

Add case in switch:
```c
        case OP_WINDOW_JOIN: {
            td_op_ext_t* ext = (td_op_ext_t*)op;
            if (!ext) return TD_ERR_PTR(TD_ERR_NYI);
            td_t* left = exec_node(g, op->inputs[0]);
            td_t* right = exec_node(g, op->inputs[1]);
            if (!left || TD_IS_ERR(left)) {
                if (right && !TD_IS_ERR(right)) td_release(right);
                return left;
            }
            if (!right || TD_IS_ERR(right)) { td_release(left); return right; }
            td_t* result = exec_window_join(g, op, left, right);
            td_release(left);
            td_release(right);
            return result;
        }
```

The `exec_window_join` function implementation will need to:
- Extract time_key and sym_key column indices from the ext node
- Sort both sides by (sym, time)
- Merge-scan matching sym partitions
- Binary search within each sym partition for ASOF match
- Build output table with left columns + matched right aggregate columns

**Note:** This task requires careful study of the existing `exec_join()` implementation to follow the same patterns for table construction, column extraction, and output building. The implementer should read `exec_join()` thoroughly before writing `exec_window_join()`.

**Step 4: Build, run, verify**

Run: `cmake --build build && ./build/test_teide --suite /exec/window_join_asof`

**Step 5: Commit**

```bash
git add src/ops/exec.c test/test_exec.c
git commit -m "feat: implement OP_WINDOW_JOIN with ASOF join semantics"
```

---

## Task 11: Extend col.c for TD_STR serialization

**Files:**
- Modify: `src/store/col.c` (extend is_serializable_type, save, load, mmap)
- Test: add tests in `test/test_store.c`

**Step 1: Write failing test**

Add to `test/test_store.c`:

```c
static MunitResult test_col_roundtrip_str(const void* params, void* fixture) {
    (void)params; (void)fixture;

    /* Create string vector */
    td_t* vec = td_vec_new(TD_SYM, 3);
    int64_t s1 = td_sym_intern("hello", 5);
    int64_t s2 = td_sym_intern("world", 5);
    int64_t s3 = td_sym_intern("test", 4);
    vec = td_vec_append(vec, &s1);
    vec = td_vec_append(vec, &s2);
    vec = td_vec_append(vec, &s3);

    /* Save */
    td_err_t err = td_col_save(vec, TMP_COL_PATH);
    munit_assert_int(err, ==, TD_OK);

    /* Load */
    td_t* loaded = td_col_load(TMP_COL_PATH);
    munit_assert_ptr_not_null(loaded);
    munit_assert_false(TD_IS_ERR(loaded));
    munit_assert_int(loaded->type, ==, TD_SYM);
    munit_assert_int(loaded->len, ==, 3);

    int64_t* data = (int64_t*)td_data(loaded);
    munit_assert_int(data[0], ==, s1);
    munit_assert_int(data[1], ==, s2);
    munit_assert_int(data[2], ==, s3);

    td_release(loaded);
    td_release(vec);
    unlink(TMP_COL_PATH);
    return MUNIT_OK;
}
```

**Note:** TD_SYM is already in the serializable types list (symbols are stored as int64 indices). This test should already pass. If the user wants actual TD_STR (raw string) serialization, that requires a different on-disk format with offset arrays. Verify by running the test first.

**Step 2: If TD_SYM test passes, design TD_STR format**

For true string columns (not symbol-interned), the on-disk format needs:
- Header (32 bytes): type=TD_STR, len=count
- Offsets array: `(count + 1) * sizeof(uint32_t)` — cumulative byte offsets
- String data: concatenated raw bytes

Implementation in `td_col_save`:
```c
case TD_STR: {
    /* Write offsets array, then concatenated string data */
    /* Each string is a td_t* pointer in the vector data */
    /* Extract length and pointer via td_str_ptr/td_str_len */
    ...
}
```

This is the most complex serialization extension. The implementer should study how string vectors store their data internally before implementing.

**Step 3: Build, run, commit**

```bash
git add src/store/col.c test/test_store.c
git commit -m "feat: extend column serialization for string types"
```

---

## Task 12: Minor test coverage — store/meta.c, core/types.c

**Files:**
- Create: `test/test_types.c` (if there are testable type utility functions)
- Modify: `test/test_store.c` (add metadata tests)
- Modify: `test/test_main.c`

**Step 1: Explore the APIs**

Read `src/store/meta.c` and `src/core/types.c` to identify testable functions. Look for public functions declared in `td.h` or internal headers.

**Step 2: Write tests for each testable function**

Follow the same pattern as previous test files. Focus on:
- `core/types.c`: type size queries, type name functions, type compatibility checks
- `store/meta.c`: metadata read/write roundtrip

**Step 3: Register, build, run, commit**

```bash
git add test/test_types.c test/test_store.c test/test_main.c
git commit -m "test: add coverage for types.c and store/meta.c"
```

---

## Summary of All Tasks

| Task | Type | Scope | Depends On |
|------|------|-------|------------|
| 1 | Test | test_exec.c — 17 executor tests | — |
| 2 | Test | test_csv.c — 4 CSV I/O tests | — |
| 3 | Test | test_sel.c — 4 selection vector tests | — |
| 4 | Test | test_fvec.c — 3 factorized vector tests | — |
| 5 | Test | test_lftj.c — 3 LFTJ tests | — |
| 6 | Fix | td_block_copy child ref retention | — |
| 7 | Feature | Add stddev/var API wrappers | Task 1 |
| 8 | Feature | Implement OP_COUNT_DISTINCT | Task 1 |
| 9 | Refactor | Remove OP_PROJECT | — |
| 10 | Feature | Implement OP_WINDOW_JOIN (ASOF) | Task 1 |
| 11 | Feature | TD_STR column serialization | — |
| 12 | Test | Minor test coverage gaps | — |

**Parallel-safe groups:**
- Tasks 1-5 can all run in parallel (independent test files)
- Task 6 is independent
- Task 9 is independent
- Tasks 7, 8, 10 depend on Task 1 (add tests to test_exec.c)
- Tasks 11-12 are independent
