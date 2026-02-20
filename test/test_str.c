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

static void* str_setup(const void* params, void* user_data) {
    (void)params; (void)user_data;
    td_heap_init();
    return NULL;
}

static void str_teardown(void* fixture) {
    (void)fixture;
    td_heap_destroy();
}

/* ---- str_ptr SSO ------------------------------------------------------- */

static MunitResult test_str_ptr_sso(const void* params, void* fixture) {
    (void)params; (void)fixture;

    td_t* s = td_str("hello", 5);
    munit_assert_ptr_not_null(s);

    const char* p = td_str_ptr(s);
    munit_assert_ptr_not_null(p);
    munit_assert_memory_equal(5, p, "hello");

    td_release(s);
    return MUNIT_OK;
}

/* ---- str_ptr long ------------------------------------------------------ */

static MunitResult test_str_ptr_long(const void* params, void* fixture) {
    (void)params; (void)fixture;

    const char* text = "this is a longer string";
    size_t len = strlen(text);
    td_t* s = td_str(text, len);
    munit_assert_ptr_not_null(s);

    const char* p = td_str_ptr(s);
    munit_assert_ptr_not_null(p);
    munit_assert_memory_equal(len, p, text);

    td_release(s);
    return MUNIT_OK;
}

/* ---- str_len ----------------------------------------------------------- */

static MunitResult test_str_len(const void* params, void* fixture) {
    (void)params; (void)fixture;

    /* SSO */
    td_t* s1 = td_str("abc", 3);
    munit_assert_size(td_str_len(s1), ==, 3);
    td_release(s1);

    /* Empty SSO */
    td_t* s2 = td_str("", 0);
    munit_assert_size(td_str_len(s2), ==, 0);
    td_release(s2);

    /* Long */
    const char* text = "a longer string for testing";
    size_t len = strlen(text);
    td_t* s3 = td_str(text, len);
    munit_assert_size(td_str_len(s3), ==, len);
    td_release(s3);

    return MUNIT_OK;
}

/* ---- str_cmp equal ----------------------------------------------------- */

static MunitResult test_str_cmp_equal(const void* params, void* fixture) {
    (void)params; (void)fixture;

    td_t* a = td_str("hello", 5);
    td_t* b = td_str("hello", 5);

    munit_assert_int(td_str_cmp(a, b), ==, 0);

    td_release(a);
    td_release(b);
    return MUNIT_OK;
}

/* ---- str_cmp different ------------------------------------------------- */

static MunitResult test_str_cmp_different(const void* params, void* fixture) {
    (void)params; (void)fixture;

    td_t* a = td_str("abc", 3);
    td_t* b = td_str("abd", 3);

    int cmp = td_str_cmp(a, b);
    munit_assert_int(cmp, <, 0);  /* 'c' < 'd' */

    int cmp2 = td_str_cmp(b, a);
    munit_assert_int(cmp2, >, 0);

    td_release(a);
    td_release(b);
    return MUNIT_OK;
}

/* ---- str_cmp prefix ---------------------------------------------------- */

static MunitResult test_str_cmp_prefix(const void* params, void* fixture) {
    (void)params; (void)fixture;

    td_t* a = td_str("abc", 3);
    td_t* b = td_str("abcde", 5);

    int cmp = td_str_cmp(a, b);
    munit_assert_int(cmp, <, 0);  /* shorter sorts first */

    int cmp2 = td_str_cmp(b, a);
    munit_assert_int(cmp2, >, 0);

    td_release(a);
    td_release(b);
    return MUNIT_OK;
}

/* ---- Suite definition -------------------------------------------------- */

static MunitTest str_tests[] = {
    { "/ptr_sso",       test_str_ptr_sso,       str_setup, str_teardown, 0, NULL },
    { "/ptr_long",      test_str_ptr_long,       str_setup, str_teardown, 0, NULL },
    { "/len",           test_str_len,             str_setup, str_teardown, 0, NULL },
    { "/cmp_equal",     test_str_cmp_equal,       str_setup, str_teardown, 0, NULL },
    { "/cmp_different", test_str_cmp_different,   str_setup, str_teardown, 0, NULL },
    { "/cmp_prefix",    test_str_cmp_prefix,      str_setup, str_teardown, 0, NULL },
    { NULL, NULL, NULL, NULL, 0, NULL },
};

MunitSuite test_str_suite = {
    "/str",
    str_tests,
    NULL,
    0,
    0,
};
