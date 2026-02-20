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

/* ---- Setup / Teardown -------------------------------------------------- */

static void* morsel_setup(const void* params, void* user_data) {
    (void)params; (void)user_data;
    td_heap_init();
    return NULL;
}

static void morsel_teardown(void* fixture) {
    (void)fixture;
    td_heap_destroy();
}

/* ---- morsel_init ------------------------------------------------------- */

static MunitResult test_morsel_init(const void* params, void* fixture) {
    (void)params; (void)fixture;

    int64_t raw[10];
    for (int i = 0; i < 10; i++) raw[i] = (int64_t)(i * 10);
    td_t* v = td_vec_from_raw(TD_I64, raw, 10);
    munit_assert_ptr_not_null(v);
    munit_assert_false(TD_IS_ERR(v));

    td_morsel_t m;
    td_morsel_init(&m, v);

    munit_assert_ptr_equal(m.vec, v);
    munit_assert_int(m.offset, ==, 0);
    munit_assert_int(m.len, ==, 10);
    munit_assert_uint(m.elem_size, ==, 8);  /* I64 = 8 bytes */
    munit_assert_int(m.morsel_len, ==, 0);
    munit_assert_null(m.morsel_ptr);
    munit_assert_null(m.null_bits);

    td_release(v);
    return MUNIT_OK;
}

/* ---- morsel_single (< 1024 elements) ----------------------------------- */

static MunitResult test_morsel_single(const void* params, void* fixture) {
    (void)params; (void)fixture;

    int64_t raw[5];
    for (int i = 0; i < 5; i++) raw[i] = (int64_t)i;
    td_t* v = td_vec_from_raw(TD_I64, raw, 5);

    td_morsel_t m;
    td_morsel_init(&m, v);

    /* First morsel: should contain all 5 elements */
    munit_assert_true(td_morsel_next(&m));
    munit_assert_int(m.morsel_len, ==, 5);
    munit_assert_int(m.offset, ==, 0);
    munit_assert_ptr_not_null(m.morsel_ptr);

    /* Second call: should return false (exhausted) */
    munit_assert_false(td_morsel_next(&m));

    td_release(v);
    return MUNIT_OK;
}

/* ---- morsel_exact (exactly 1024 elements) ------------------------------ */

static MunitResult test_morsel_exact(const void* params, void* fixture) {
    (void)params; (void)fixture;

    int64_t raw[1024];
    for (int i = 0; i < 1024; i++) raw[i] = (int64_t)i;
    td_t* v = td_vec_from_raw(TD_I64, raw, 1024);

    td_morsel_t m;
    td_morsel_init(&m, v);

    /* First morsel: exactly 1024 elements */
    munit_assert_true(td_morsel_next(&m));
    munit_assert_int(m.morsel_len, ==, 1024);
    munit_assert_int(m.offset, ==, 0);

    /* Second call: exhausted */
    munit_assert_false(td_morsel_next(&m));

    td_release(v);
    return MUNIT_OK;
}

/* ---- morsel_multiple (2500 elements = 1024+1024+452) ------------------- */

static MunitResult test_morsel_multiple(const void* params, void* fixture) {
    (void)params; (void)fixture;

    int64_t raw[2500];
    for (int i = 0; i < 2500; i++) raw[i] = (int64_t)i;
    td_t* v = td_vec_from_raw(TD_I64, raw, 2500);

    td_morsel_t m;
    td_morsel_init(&m, v);

    /* Morsel 1: 1024 elements */
    munit_assert_true(td_morsel_next(&m));
    munit_assert_int(m.morsel_len, ==, 1024);
    munit_assert_int(m.offset, ==, 0);

    /* Morsel 2: 1024 elements */
    munit_assert_true(td_morsel_next(&m));
    munit_assert_int(m.morsel_len, ==, 1024);
    munit_assert_int(m.offset, ==, 1024);

    /* Morsel 3: 452 elements */
    munit_assert_true(td_morsel_next(&m));
    munit_assert_int(m.morsel_len, ==, 452);
    munit_assert_int(m.offset, ==, 2048);

    /* Exhausted */
    munit_assert_false(td_morsel_next(&m));

    td_release(v);
    return MUNIT_OK;
}

/* ---- morsel_empty (0 elements) ----------------------------------------- */

static MunitResult test_morsel_empty(const void* params, void* fixture) {
    (void)params; (void)fixture;

    td_t* v = td_vec_new(TD_I64, 0);
    munit_assert_ptr_not_null(v);
    munit_assert_false(TD_IS_ERR(v));

    td_morsel_t m;
    td_morsel_init(&m, v);

    /* Should return false immediately */
    munit_assert_false(td_morsel_next(&m));

    td_release(v);
    return MUNIT_OK;
}

/* ---- morsel_data_access (verify I64 data through morsel_ptr) ----------- */

static MunitResult test_morsel_data_access(const void* params, void* fixture) {
    (void)params; (void)fixture;

    int64_t raw[2500];
    for (int i = 0; i < 2500; i++) raw[i] = (int64_t)(i * 3);
    td_t* v = td_vec_from_raw(TD_I64, raw, 2500);

    td_morsel_t m;
    td_morsel_init(&m, v);

    int64_t total_checked = 0;

    while (td_morsel_next(&m)) {
        int64_t* data = (int64_t*)m.morsel_ptr;
        for (int64_t i = 0; i < m.morsel_len; i++) {
            int64_t global_idx = m.offset + i;
            munit_assert_int(data[i], ==, global_idx * 3);
            total_checked++;
        }
    }

    munit_assert_int(total_checked, ==, 2500);

    td_release(v);
    return MUNIT_OK;
}

/* ---- morsel_f64 (verify F64 data through morsel_ptr) ------------------- */

static MunitResult test_morsel_f64(const void* params, void* fixture) {
    (void)params; (void)fixture;

    double raw[2000];
    for (int i = 0; i < 2000; i++) raw[i] = (double)i * 1.5;
    td_t* v = td_vec_from_raw(TD_F64, raw, 2000);

    td_morsel_t m;
    td_morsel_init(&m, v);
    munit_assert_uint(m.elem_size, ==, 8);  /* F64 = 8 bytes */

    int64_t total_checked = 0;

    while (td_morsel_next(&m)) {
        double* data = (double*)m.morsel_ptr;
        for (int64_t i = 0; i < m.morsel_len; i++) {
            int64_t global_idx = m.offset + i;
            munit_assert_double(data[i], ==, (double)global_idx * 1.5);
            total_checked++;
        }
    }

    munit_assert_int(total_checked, ==, 2000);

    td_release(v);
    return MUNIT_OK;
}

/* ---- morsel_bool (verify BOOL data through morsel_ptr) ----------------- */

static MunitResult test_morsel_bool(const void* params, void* fixture) {
    (void)params; (void)fixture;

    uint8_t raw[50];
    for (int i = 0; i < 50; i++) raw[i] = (uint8_t)(i % 2);
    td_t* v = td_vec_from_raw(TD_BOOL, raw, 50);

    td_morsel_t m;
    td_morsel_init(&m, v);
    munit_assert_uint(m.elem_size, ==, 1);  /* BOOL = 1 byte */

    /* Single morsel (50 < 1024) */
    munit_assert_true(td_morsel_next(&m));
    munit_assert_int(m.morsel_len, ==, 50);

    uint8_t* data = (uint8_t*)m.morsel_ptr;
    for (int i = 0; i < 50; i++) {
        munit_assert_uint(data[i], ==, (uint8_t)(i % 2));
    }

    /* Exhausted */
    munit_assert_false(td_morsel_next(&m));

    td_release(v);
    return MUNIT_OK;
}

/* ---- Suite definition -------------------------------------------------- */

static MunitTest morsel_tests[] = {
    { "/init",         test_morsel_init,         morsel_setup, morsel_teardown, 0, NULL },
    { "/single",       test_morsel_single,       morsel_setup, morsel_teardown, 0, NULL },
    { "/exact",        test_morsel_exact,         morsel_setup, morsel_teardown, 0, NULL },
    { "/multiple",     test_morsel_multiple,      morsel_setup, morsel_teardown, 0, NULL },
    { "/empty",        test_morsel_empty,         morsel_setup, morsel_teardown, 0, NULL },
    { "/data_access",  test_morsel_data_access,   morsel_setup, morsel_teardown, 0, NULL },
    { "/f64",          test_morsel_f64,            morsel_setup, morsel_teardown, 0, NULL },
    { "/bool",         test_morsel_bool,           morsel_setup, morsel_teardown, 0, NULL },
    { NULL, NULL, NULL, NULL, 0, NULL },
};

MunitSuite test_morsel_suite = {
    "/morsel",
    morsel_tests,
    NULL,
    0,
    0,
};
