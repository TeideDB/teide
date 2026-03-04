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
#include "mem/sys.h"
#include <string.h>

/* ---- test_sys_alloc_free ----------------------------------------------- */

static MunitResult test_sys_alloc_free(const void* params, void* fixture) {
    (void)params; (void)fixture;

    void* p = td_sys_alloc(128);
    munit_assert_ptr_not_null(p);

    /* Should be writable */
    memset(p, 0x42, 128);

    td_sys_free(p);

    /* Free NULL should be safe */
    td_sys_free(NULL);

    return MUNIT_OK;
}

/* ---- test_sys_realloc -------------------------------------------------- */

static MunitResult test_sys_realloc(const void* params, void* fixture) {
    (void)params; (void)fixture;

    /* realloc(NULL, n) should behave like alloc */
    void* p = td_sys_realloc(NULL, 64);
    munit_assert_ptr_not_null(p);
    memset(p, 0xAA, 64);

    /* Grow the allocation */
    void* p2 = td_sys_realloc(p, 8192);
    munit_assert_ptr_not_null(p2);

    /* First 64 bytes should be preserved */
    uint8_t* bytes = (uint8_t*)p2;
    for (int i = 0; i < 64; i++) {
        munit_assert_uint(bytes[i], ==, 0xAA);
    }

    td_sys_free(p2);

    /* realloc(ptr, 0) should free and return NULL */
    void* p3 = td_sys_alloc(32);
    munit_assert_ptr_not_null(p3);
    void* p4 = td_sys_realloc(p3, 0);
    munit_assert_null(p4);

    return MUNIT_OK;
}

/* ---- test_sys_strdup --------------------------------------------------- */

static MunitResult test_sys_strdup(const void* params, void* fixture) {
    (void)params; (void)fixture;

    char* dup = td_sys_strdup("hello");
    munit_assert_ptr_not_null(dup);
    munit_assert_string_equal(dup, "hello");
    td_sys_free(dup);

    /* NULL input should return NULL */
    munit_assert_null(td_sys_strdup(NULL));

    /* Empty string */
    char* empty = td_sys_strdup("");
    munit_assert_ptr_not_null(empty);
    munit_assert_string_equal(empty, "");
    td_sys_free(empty);

    return MUNIT_OK;
}

/* ---- test_sys_get_stat ------------------------------------------------- */

static MunitResult test_sys_get_stat(const void* params, void* fixture) {
    (void)params; (void)fixture;

    int64_t current_before, peak_before;
    td_sys_get_stat(&current_before, &peak_before);

    void* p = td_sys_alloc(4096);
    munit_assert_ptr_not_null(p);

    int64_t current_during, peak_during;
    td_sys_get_stat(&current_during, &peak_during);
    munit_assert_int(current_during, >, current_before);
    munit_assert_int(peak_during, >=, current_during);

    td_sys_free(p);

    int64_t current_after, peak_after;
    td_sys_get_stat(&current_after, &peak_after);
    munit_assert_int(current_after, <, current_during);
    /* Peak should not decrease */
    munit_assert_int(peak_after, >=, peak_during);

    return MUNIT_OK;
}

/* ---- Suite definition -------------------------------------------------- */

static MunitTest sys_tests[] = {
    { "/alloc_free",  test_sys_alloc_free,  NULL, NULL, 0, NULL },
    { "/realloc",     test_sys_realloc,     NULL, NULL, 0, NULL },
    { "/strdup",      test_sys_strdup,      NULL, NULL, 0, NULL },
    { "/get_stat",    test_sys_get_stat,    NULL, NULL, 0, NULL },
    { NULL, NULL, NULL, NULL, 0, NULL },
};

MunitSuite test_sys_suite = {
    "/sys",
    sys_tests,
    NULL,
    0,
    0,
};
