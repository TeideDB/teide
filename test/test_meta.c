/*
 *   Copyright (c) 2024-2026 Anton Kundenko <singaraiona@gmail.com>
 *   All rights reserved.
 *
 *   Permission is hereby granted, free of charge, to any person obtaining a copy
 *   of this software and associated documentation files (the "Software"), to deal
 *   in the Software without restriction, including without limitation the rights
 *   to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 *   copies of the Software, and to permit persons to whom the Software is
 *   furnished to do so, subject to the following conditions:
 *
 *   The above copyright notice and this permission notice shall be included in all
 *   copies or substantial portions of the Software.
 *
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
#include <unistd.h>

#define TMP_META_PATH "/tmp/teide_test_meta.d"

/* ---- Setup / Teardown -------------------------------------------------- */

static void* meta_setup(const void* params, void* user_data) {
    (void)params; (void)user_data;
    td_heap_init();
    td_sym_init();
    return NULL;
}

static void meta_teardown(void* fixture) {
    (void)fixture;
    unlink(TMP_META_PATH);
    td_sym_destroy();
    td_heap_destroy();
}

/* ---- test_meta_save_load_roundtrip ------------------------------------- */

static MunitResult test_meta_save_load_roundtrip(const void* params, void* fixture) {
    (void)params; (void)fixture;

    /* Build a small I64 schema vector */
    int64_t ids[] = {100, 200, 300};
    td_t* schema = td_vec_from_raw(TD_I64, ids, 3);
    munit_assert_ptr_not_null(schema);
    munit_assert_false(TD_IS_ERR(schema));

    /* Save */
    td_err_t err = td_meta_save_d(schema, TMP_META_PATH);
    munit_assert_int(err, ==, TD_OK);

    /* Load back */
    td_t* loaded = td_meta_load_d(TMP_META_PATH);
    munit_assert_ptr_not_null(loaded);
    munit_assert_false(TD_IS_ERR(loaded));

    /* Verify contents */
    munit_assert_int(td_type(loaded), ==, TD_I64);
    munit_assert_int(td_len(loaded), ==, 3);

    int64_t* out = (int64_t*)td_data(loaded);
    munit_assert_int(out[0], ==, 100);
    munit_assert_int(out[1], ==, 200);
    munit_assert_int(out[2], ==, 300);

    td_free(schema);
    td_free(loaded);
    return MUNIT_OK;
}

/* ---- test_meta_save_null_returns_error --------------------------------- */

static MunitResult test_meta_save_null_returns_error(const void* params, void* fixture) {
    (void)params; (void)fixture;

    td_err_t err = td_meta_save_d(NULL, TMP_META_PATH);
    munit_assert_int(err, !=, TD_OK);

    return MUNIT_OK;
}

/* ---- test_meta_save_err_ptr_returns_error ------------------------------ */

static MunitResult test_meta_save_err_ptr_returns_error(const void* params, void* fixture) {
    (void)params; (void)fixture;

    td_t* bad = TD_ERR_PTR(TD_ERR_TYPE);
    td_err_t err = td_meta_save_d(bad, TMP_META_PATH);
    munit_assert_int(err, !=, TD_OK);

    return MUNIT_OK;
}

/* ---- test_meta_load_nonexistent --------------------------------------- */

static MunitResult test_meta_load_nonexistent(const void* params, void* fixture) {
    (void)params; (void)fixture;

    td_t* loaded = td_meta_load_d("/tmp/teide_no_such_file_meta.d");
    /* Should return NULL or error pointer for missing file */
    munit_assert_true(loaded == NULL || TD_IS_ERR(loaded));

    return MUNIT_OK;
}

/* ---- Suite definition -------------------------------------------------- */

static MunitTest meta_tests[] = {
    { "/save_load_roundtrip",     test_meta_save_load_roundtrip,     meta_setup, meta_teardown, 0, NULL },
    { "/save_null_returns_error", test_meta_save_null_returns_error, meta_setup, meta_teardown, 0, NULL },
    { "/save_err_ptr_returns_error", test_meta_save_err_ptr_returns_error, meta_setup, meta_teardown, 0, NULL },
    { "/load_nonexistent",        test_meta_load_nonexistent,        meta_setup, meta_teardown, 0, NULL },
    { NULL, NULL, NULL, NULL, 0, NULL },
};

MunitSuite test_meta_suite = {
    "/meta",
    meta_tests,
    NULL,
    0,
    0,
};
