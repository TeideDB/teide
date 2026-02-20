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

static void* sym_setup(const void* params, void* user_data) {
    (void)params; (void)user_data;
    td_heap_init();
    td_sym_init();
    return NULL;
}

static void sym_teardown(void* fixture) {
    (void)fixture;
    td_sym_destroy();
    td_heap_destroy();
}

/* ---- sym_init_destroy -------------------------------------------------- */

static MunitResult test_sym_init_destroy(const void* params, void* fixture) {
    (void)params; (void)fixture;

    /* After init, count should be 0 */
    munit_assert_uint(td_sym_count(), ==, 0);

    return MUNIT_OK;
}

/* ---- sym_intern_basic -------------------------------------------------- */

static MunitResult test_sym_intern_basic(const void* params, void* fixture) {
    (void)params; (void)fixture;

    int64_t id = td_sym_intern("hello", 5);
    munit_assert_int(id, >=, 0);
    munit_assert_uint(td_sym_count(), ==, 1);

    return MUNIT_OK;
}

/* ---- sym_intern_duplicate ---------------------------------------------- */

static MunitResult test_sym_intern_duplicate(const void* params, void* fixture) {
    (void)params; (void)fixture;

    int64_t id1 = td_sym_intern("hello", 5);
    int64_t id2 = td_sym_intern("hello", 5);
    munit_assert_int(id1, ==, id2);
    munit_assert_uint(td_sym_count(), ==, 1);

    return MUNIT_OK;
}

/* ---- sym_find_existing ------------------------------------------------- */

static MunitResult test_sym_find_existing(const void* params, void* fixture) {
    (void)params; (void)fixture;

    int64_t id = td_sym_intern("world", 5);
    int64_t found = td_sym_find("world", 5);
    munit_assert_int(found, ==, id);

    return MUNIT_OK;
}

/* ---- sym_find_missing -------------------------------------------------- */

static MunitResult test_sym_find_missing(const void* params, void* fixture) {
    (void)params; (void)fixture;

    int64_t found = td_sym_find("nonexistent", 11);
    munit_assert_int(found, ==, -1);

    return MUNIT_OK;
}

/* ---- sym_str_roundtrip ------------------------------------------------- */

static MunitResult test_sym_str_roundtrip(const void* params, void* fixture) {
    (void)params; (void)fixture;

    int64_t id = td_sym_intern("roundtrip", 9);
    munit_assert_int(id, >=, 0);

    td_t* s = td_sym_str(id);
    munit_assert_ptr_not_null(s);
    munit_assert_size(td_str_len(s), ==, 9);
    munit_assert_memory_equal(9, td_str_ptr(s), "roundtrip");

    return MUNIT_OK;
}

/* ---- sym_count --------------------------------------------------------- */

static MunitResult test_sym_count(const void* params, void* fixture) {
    (void)params; (void)fixture;

    munit_assert_uint(td_sym_count(), ==, 0);

    td_sym_intern("a", 1);
    munit_assert_uint(td_sym_count(), ==, 1);

    td_sym_intern("b", 1);
    munit_assert_uint(td_sym_count(), ==, 2);

    td_sym_intern("c", 1);
    munit_assert_uint(td_sym_count(), ==, 3);

    /* Duplicate should not increase count */
    td_sym_intern("a", 1);
    munit_assert_uint(td_sym_count(), ==, 3);

    return MUNIT_OK;
}

/* ---- sym_many ---------------------------------------------------------- */

static MunitResult test_sym_many(const void* params, void* fixture) {
    (void)params; (void)fixture;

    /* Intern 1000 unique symbols */
    int64_t ids[1000];
    char buf[32];
    for (int i = 0; i < 1000; i++) {
        int len = snprintf(buf, sizeof(buf), "sym_%d", i);
        ids[i] = td_sym_intern(buf, (size_t)len);
        munit_assert_int(ids[i], >=, 0);
    }

    munit_assert_uint(td_sym_count(), ==, 1000);

    /* Verify all are distinct IDs */
    for (int i = 0; i < 1000; i++) {
        for (int j = i + 1; j < 1000; j++) {
            munit_assert_int(ids[i], !=, ids[j]);
        }
    }

    /* Verify all are retrievable with correct strings */
    for (int i = 0; i < 1000; i++) {
        int len = snprintf(buf, sizeof(buf), "sym_%d", i);
        td_t* s = td_sym_str(ids[i]);
        munit_assert_ptr_not_null(s);
        munit_assert_size(td_str_len(s), ==, (size_t)len);
        munit_assert_memory_equal((size_t)len, td_str_ptr(s), buf);
    }

    /* Re-interning should return same IDs */
    for (int i = 0; i < 1000; i++) {
        int len = snprintf(buf, sizeof(buf), "sym_%d", i);
        int64_t id2 = td_sym_intern(buf, (size_t)len);
        munit_assert_int(id2, ==, ids[i]);
    }

    munit_assert_uint(td_sym_count(), ==, 1000);

    return MUNIT_OK;
}

/* ---- Suite definition -------------------------------------------------- */

static MunitTest sym_tests[] = {
    { "/init_destroy",    test_sym_init_destroy,    sym_setup, sym_teardown, 0, NULL },
    { "/intern_basic",    test_sym_intern_basic,    sym_setup, sym_teardown, 0, NULL },
    { "/intern_duplicate", test_sym_intern_duplicate, sym_setup, sym_teardown, 0, NULL },
    { "/find_existing",   test_sym_find_existing,   sym_setup, sym_teardown, 0, NULL },
    { "/find_missing",    test_sym_find_missing,    sym_setup, sym_teardown, 0, NULL },
    { "/str_roundtrip",   test_sym_str_roundtrip,   sym_setup, sym_teardown, 0, NULL },
    { "/count",           test_sym_count,           sym_setup, sym_teardown, 0, NULL },
    { "/many",            test_sym_many,            sym_setup, sym_teardown, 0, NULL },
    { NULL, NULL, NULL, NULL, 0, NULL },
};

MunitSuite test_sym_suite = {
    "/sym",
    sym_tests,
    NULL,
    0,
    0,
};
