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

/* ---- test_type_sizes_known_types --------------------------------------- */

static MunitResult test_type_sizes_known_types(const void* params, void* fixture) {
    (void)params; (void)fixture;

    munit_assert_uint(td_type_sizes[TD_BOOL], ==, 1);
    munit_assert_uint(td_type_sizes[TD_U8],   ==, 1);
    munit_assert_uint(td_type_sizes[TD_CHAR], ==, 1);
    munit_assert_uint(td_type_sizes[TD_I16],  ==, 2);
    munit_assert_uint(td_type_sizes[TD_I32],  ==, 4);
    munit_assert_uint(td_type_sizes[TD_I64],  ==, 8);
    munit_assert_uint(td_type_sizes[TD_F64],  ==, 8);
    munit_assert_uint(td_type_sizes[TD_DATE], ==, 4);
    munit_assert_uint(td_type_sizes[TD_TIME], ==, 4);
    munit_assert_uint(td_type_sizes[TD_TIMESTAMP], ==, 8);
    munit_assert_uint(td_type_sizes[TD_GUID], ==, 16);

    return MUNIT_OK;
}

/* ---- test_elem_size_macro ---------------------------------------------- */

static MunitResult test_elem_size_macro(const void* params, void* fixture) {
    (void)params; (void)fixture;

    /* td_elem_size(t) should match td_type_sizes[t] */
    munit_assert_uint(td_elem_size(TD_I64), ==, 8);
    munit_assert_uint(td_elem_size(TD_I32), ==, 4);
    munit_assert_uint(td_elem_size(TD_BOOL), ==, 1);
    munit_assert_uint(td_elem_size(TD_GUID), ==, 16);

    return MUNIT_OK;
}

/* ---- test_type_sizes_pointer_types ------------------------------------- */

static MunitResult test_type_sizes_pointer_types(const void* params, void* fixture) {
    (void)params; (void)fixture;

    /* LIST and TABLE are pointer-sized (8 bytes) */
    munit_assert_uint(td_type_sizes[TD_LIST],  ==, 8);
    munit_assert_uint(td_type_sizes[TD_TABLE], ==, 8);

    /* SYM default width is 8 (W64) */
    munit_assert_uint(td_type_sizes[TD_SYM],   ==, 8);

    /* SEL has no fixed element size */
    munit_assert_uint(td_type_sizes[TD_SEL],   ==, 0);

    return MUNIT_OK;
}

/* ---- Suite definition -------------------------------------------------- */

static MunitTest types_tests[] = {
    { "/sizes_known_types",  test_type_sizes_known_types,  NULL, NULL, 0, NULL },
    { "/elem_size_macro",    test_elem_size_macro,         NULL, NULL, 0, NULL },
    { "/sizes_pointer_types", test_type_sizes_pointer_types, NULL, NULL, 0, NULL },
    { NULL, NULL, NULL, NULL, 0, NULL },
};

MunitSuite test_types_suite = {
    "/types",
    types_tests,
    NULL,
    0,
    0,
};
