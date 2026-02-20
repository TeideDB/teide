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
#include <stdatomic.h>

/* ---- Setup / Teardown -------------------------------------------------- */

static void* cow_setup(const void* params, void* user_data) {
    (void)params; (void)user_data;
    td_heap_init();
    return NULL;
}

static void cow_teardown(void* fixture) {
    (void)fixture;
    td_heap_destroy();
}

/* ---- retain/release basic ---------------------------------------------- */

static MunitResult test_retain_release(const void* params, void* fixture) {
    (void)params; (void)fixture;

    td_t* v = td_alloc(0);
    munit_assert_ptr_not_null(v);
    munit_assert_false(TD_IS_ERR(v));

    /* rc starts at 1 */
    munit_assert_uint(atomic_load_explicit(&v->rc, memory_order_relaxed), ==, 1);

    /* retain -> rc=2 */
    td_retain(v);
    munit_assert_uint(atomic_load_explicit(&v->rc, memory_order_relaxed), ==, 2);

    /* release -> rc=1 */
    td_release(v);
    munit_assert_uint(atomic_load_explicit(&v->rc, memory_order_relaxed), ==, 1);

    /* release -> rc=0, block freed (don't access v after this) */
    td_release(v);

    return MUNIT_OK;
}

/* ---- cow sole owner ---------------------------------------------------- */

static MunitResult test_cow_sole_owner(const void* params, void* fixture) {
    (void)params; (void)fixture;

    td_t* v = td_alloc(0);
    munit_assert_ptr_not_null(v);
    v->type = TD_ATOM_I64;
    v->i64 = 42;

    /* rc=1, sole owner -> cow returns same pointer */
    td_t* w = td_cow(v);
    munit_assert_ptr_equal(v, w);
    munit_assert_uint(atomic_load_explicit(&w->rc, memory_order_relaxed), ==, 1);

    td_release(w);
    return MUNIT_OK;
}

/* ---- cow shared -------------------------------------------------------- */

static MunitResult test_cow_shared(const void* params, void* fixture) {
    (void)params; (void)fixture;

    td_t* v = td_alloc(0);
    munit_assert_ptr_not_null(v);
    v->type = TD_ATOM_I64;
    v->i64 = 99;

    /* retain to rc=2 (shared) */
    td_retain(v);
    munit_assert_uint(atomic_load_explicit(&v->rc, memory_order_relaxed), ==, 2);

    /* cow on shared object -> returns different pointer */
    td_t* w = td_cow(v);
    munit_assert_ptr_not_null(w);
    munit_assert_false(TD_IS_ERR(w));
    munit_assert_true((void*)w != (void*)v);

    /* Copy should have rc=1 */
    munit_assert_uint(atomic_load_explicit(&w->rc, memory_order_relaxed), ==, 1);

    /* Original should have rc=1 (cow decremented from 2 to 1) */
    munit_assert_uint(atomic_load_explicit(&v->rc, memory_order_relaxed), ==, 1);

    /* Value should be preserved */
    munit_assert_int(w->type, ==, TD_ATOM_I64);
    munit_assert_int(w->i64, ==, 99);

    td_release(v);
    td_release(w);
    return MUNIT_OK;
}

/* ---- null/error safety ------------------------------------------------- */

static MunitResult test_null_error_safety(const void* params, void* fixture) {
    (void)params; (void)fixture;

    /* These should not crash */
    td_retain(NULL);
    td_release(NULL);
    td_t* r = td_cow(NULL);
    munit_assert_null(r);

    /* Error pointers */
    td_t* err = TD_ERR_PTR(TD_ERR_OOM);
    td_retain(err);
    td_release(err);
    td_t* r2 = td_cow(err);
    munit_assert_true(TD_IS_ERR(r2));

    return MUNIT_OK;
}

/* ---- cow with vector data ---------------------------------------------- */

static MunitResult test_cow_vector(const void* params, void* fixture) {
    (void)params; (void)fixture;

    /* Create a vector with actual data */
    size_t data_size = 10 * sizeof(int64_t);
    td_t* v = td_alloc(data_size);
    munit_assert_ptr_not_null(v);
    v->type = TD_I64;
    v->len = 10;
    int64_t* data = (int64_t*)td_data(v);
    for (int i = 0; i < 10; i++) {
        data[i] = (int64_t)(i * 100);
    }

    /* Share and cow */
    td_retain(v);
    td_t* w = td_cow(v);
    munit_assert_ptr_not_null(w);
    munit_assert_true((void*)w != (void*)v);
    munit_assert_int(w->type, ==, TD_I64);
    munit_assert_int(w->len, ==, 10);

    /* Verify data was copied */
    int64_t* wdata = (int64_t*)td_data(w);
    for (int i = 0; i < 10; i++) {
        munit_assert_int(wdata[i], ==, (int64_t)(i * 100));
    }

    /* Modifying copy should not affect original */
    wdata[0] = 999;
    munit_assert_int(data[0], ==, 0);

    td_release(v);
    td_release(w);
    return MUNIT_OK;
}

/* ---- Suite definition -------------------------------------------------- */

static MunitTest cow_tests[] = {
    { "/retain_release",     test_retain_release,    cow_setup, cow_teardown, 0, NULL },
    { "/cow_sole_owner",     test_cow_sole_owner,    cow_setup, cow_teardown, 0, NULL },
    { "/cow_shared",         test_cow_shared,        cow_setup, cow_teardown, 0, NULL },
    { "/null_error_safety",  test_null_error_safety, cow_setup, cow_teardown, 0, NULL },
    { "/cow_vector",         test_cow_vector,        cow_setup, cow_teardown, 0, NULL },
    { NULL, NULL, NULL, NULL, 0, NULL },
};

MunitSuite test_cow_suite = {
    "/cow",
    cow_tests,
    NULL,
    0,
    0,
};
