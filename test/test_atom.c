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
#include <string.h>

/* ---- Setup / Teardown -------------------------------------------------- */

static void* atom_setup(const void* params, void* user_data) {
    (void)params; (void)user_data;
    td_heap_init();
    return NULL;
}

static void atom_teardown(void* fixture) {
    (void)fixture;
    td_heap_destroy();
}

/* ---- Bool atom --------------------------------------------------------- */

static MunitResult test_atom_bool(const void* params, void* fixture) {
    (void)params; (void)fixture;

    td_t* t = td_bool(true);
    munit_assert_ptr_not_null(t);
    munit_assert_false(TD_IS_ERR(t));
    munit_assert_true(td_is_atom(t));
    munit_assert_int(t->type, ==, TD_ATOM_BOOL);
    munit_assert_uint(t->b8, ==, 1);
    td_release(t);

    td_t* f = td_bool(false);
    munit_assert_int(f->type, ==, TD_ATOM_BOOL);
    munit_assert_uint(f->b8, ==, 0);
    td_release(f);

    return MUNIT_OK;
}

/* ---- U8 atom ----------------------------------------------------------- */

static MunitResult test_atom_u8(const void* params, void* fixture) {
    (void)params; (void)fixture;

    td_t* v = td_u8(255);
    munit_assert_ptr_not_null(v);
    munit_assert_true(td_is_atom(v));
    munit_assert_int(v->type, ==, TD_ATOM_U8);
    munit_assert_uint(v->u8, ==, 255);
    td_release(v);

    return MUNIT_OK;
}

/* ---- Char atom --------------------------------------------------------- */

static MunitResult test_atom_char(const void* params, void* fixture) {
    (void)params; (void)fixture;

    td_t* v = td_char('Z');
    munit_assert_ptr_not_null(v);
    munit_assert_true(td_is_atom(v));
    munit_assert_int(v->type, ==, TD_ATOM_CHAR);
    munit_assert_int(v->c8, ==, 'Z');
    td_release(v);

    return MUNIT_OK;
}

/* ---- I16 atom ---------------------------------------------------------- */

static MunitResult test_atom_i16(const void* params, void* fixture) {
    (void)params; (void)fixture;

    td_t* v = td_i16(-1234);
    munit_assert_ptr_not_null(v);
    munit_assert_true(td_is_atom(v));
    munit_assert_int(v->type, ==, TD_ATOM_I16);
    munit_assert_int(v->i16, ==, -1234);
    td_release(v);

    return MUNIT_OK;
}

/* ---- I32 atom ---------------------------------------------------------- */

static MunitResult test_atom_i32(const void* params, void* fixture) {
    (void)params; (void)fixture;

    td_t* v = td_i32(1000000);
    munit_assert_ptr_not_null(v);
    munit_assert_true(td_is_atom(v));
    munit_assert_int(v->type, ==, TD_ATOM_I32);
    munit_assert_int(v->i32, ==, 1000000);
    td_release(v);

    return MUNIT_OK;
}

/* ---- I64 atom ---------------------------------------------------------- */

static MunitResult test_atom_i64(const void* params, void* fixture) {
    (void)params; (void)fixture;

    td_t* v = td_i64(9876543210LL);
    munit_assert_ptr_not_null(v);
    munit_assert_true(td_is_atom(v));
    munit_assert_int(v->type, ==, TD_ATOM_I64);
    munit_assert_int(v->i64, ==, 9876543210LL);
    td_release(v);

    return MUNIT_OK;
}

/* ---- F64 atom ---------------------------------------------------------- */

static MunitResult test_atom_f64(const void* params, void* fixture) {
    (void)params; (void)fixture;

    td_t* v = td_f64(3.14159265358979);
    munit_assert_ptr_not_null(v);
    munit_assert_true(td_is_atom(v));
    munit_assert_int(v->type, ==, TD_ATOM_F64);
    munit_assert_double(v->f64, ==, 3.14159265358979);
    td_release(v);

    return MUNIT_OK;
}

/* ---- String SSO (short) ------------------------------------------------ */

static MunitResult test_atom_str_sso(const void* params, void* fixture) {
    (void)params; (void)fixture;

    const char* s = "hello";
    td_t* v = td_str(s, 5);
    munit_assert_ptr_not_null(v);
    munit_assert_false(TD_IS_ERR(v));
    munit_assert_true(td_is_atom(v));
    munit_assert_int(v->type, ==, TD_ATOM_STR);
    munit_assert_uint(v->slen, ==, 5);
    munit_assert_memory_equal(5, v->sdata, "hello");
    td_release(v);

    /* Empty string */
    td_t* e = td_str("", 0);
    munit_assert_int(e->type, ==, TD_ATOM_STR);
    munit_assert_uint(e->slen, ==, 0);
    td_release(e);

    /* Exactly 7 bytes — uses long-string path (no room for NUL in sdata[7]) */
    td_t* m = td_str("1234567", 7);
    munit_assert_int(m->type, ==, TD_ATOM_STR);
    munit_assert_size(td_str_len(m), ==, 7);
    munit_assert_memory_equal(7, td_str_ptr(m), "1234567");
    td_release(m);

    return MUNIT_OK;
}

/* ---- String long (> 7 bytes) ------------------------------------------- */

static MunitResult test_atom_str_long(const void* params, void* fixture) {
    (void)params; (void)fixture;

    const char* s = "hello world!";
    size_t len = strlen(s);
    td_t* v = td_str(s, len);
    munit_assert_ptr_not_null(v);
    munit_assert_false(TD_IS_ERR(v));
    munit_assert_true(td_is_atom(v));
    munit_assert_int(v->type, ==, TD_ATOM_STR);

    /* For long strings, obj points to a CHAR vector */
    td_t* chars = v->obj;
    munit_assert_ptr_not_null(chars);
    munit_assert_int(chars->type, ==, TD_CHAR);
    munit_assert_int(chars->len, ==, (int64_t)len);
    munit_assert_memory_equal(len, td_data(chars), s);

    /* Keep one guard ref so we can observe atom-owned release. */
    td_retain(chars);
    munit_assert_uint(atomic_load_explicit(&chars->rc, memory_order_relaxed), ==, 2);

    td_release(v);
    munit_assert_uint(atomic_load_explicit(&chars->rc, memory_order_relaxed), ==, 1);
    td_release(chars);

    return MUNIT_OK;
}

/* ---- Symbol atom ------------------------------------------------------- */

static MunitResult test_atom_sym(const void* params, void* fixture) {
    (void)params; (void)fixture;

    td_t* v = td_sym(42);
    munit_assert_ptr_not_null(v);
    munit_assert_true(td_is_atom(v));
    munit_assert_int(v->type, ==, TD_ATOM_SYM);
    munit_assert_int(v->i64, ==, 42);
    td_release(v);

    return MUNIT_OK;
}

/* ---- Date atom --------------------------------------------------------- */

static MunitResult test_atom_date(const void* params, void* fixture) {
    (void)params; (void)fixture;

    td_t* v = td_date(19700);  /* days since epoch */
    munit_assert_ptr_not_null(v);
    munit_assert_true(td_is_atom(v));
    munit_assert_int(v->type, ==, -TD_DATE);
    munit_assert_int(v->i64, ==, 19700);
    td_release(v);

    return MUNIT_OK;
}

/* ---- Time atom --------------------------------------------------------- */

static MunitResult test_atom_time(const void* params, void* fixture) {
    (void)params; (void)fixture;

    td_t* v = td_time(43200000);  /* milliseconds since midnight (12:00:00.000) */
    munit_assert_ptr_not_null(v);
    munit_assert_true(td_is_atom(v));
    munit_assert_int(v->type, ==, -TD_TIME);
    munit_assert_int(v->i64, ==, 43200000);
    td_release(v);

    return MUNIT_OK;
}

/* ---- Timestamp atom ---------------------------------------------------- */

static MunitResult test_atom_timestamp(const void* params, void* fixture) {
    (void)params; (void)fixture;

    td_t* v = td_timestamp(1700000000000000000LL);
    munit_assert_ptr_not_null(v);
    munit_assert_true(td_is_atom(v));
    munit_assert_int(v->type, ==, -TD_TIMESTAMP);
    munit_assert_int(v->i64, ==, 1700000000000000000LL);
    td_release(v);

    return MUNIT_OK;
}

/* ---- GUID atom --------------------------------------------------------- */

static MunitResult test_atom_guid(const void* params, void* fixture) {
    (void)params; (void)fixture;

    uint8_t bytes[16] = {
        0x01, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF,
        0xFE, 0xDC, 0xBA, 0x98, 0x76, 0x54, 0x32, 0x10
    };
    td_t* v = td_guid(bytes);
    munit_assert_ptr_not_null(v);
    munit_assert_false(TD_IS_ERR(v));
    munit_assert_true(td_is_atom(v));
    munit_assert_int(v->type, ==, -TD_GUID);

    /* obj points to a U8 vector of length 16 */
    td_t* vec = v->obj;
    munit_assert_ptr_not_null(vec);
    munit_assert_int(vec->type, ==, TD_U8);
    munit_assert_int(vec->len, ==, 16);
    munit_assert_memory_equal(16, td_data(vec), bytes);

    td_retain(vec);
    munit_assert_uint(atomic_load_explicit(&vec->rc, memory_order_relaxed), ==, 2);

    td_release(v);
    munit_assert_uint(atomic_load_explicit(&vec->rc, memory_order_relaxed), ==, 1);
    td_release(vec);

    return MUNIT_OK;
}

/* ---- is_atom correctness ----------------------------------------------- */

static MunitResult test_is_atom(const void* params, void* fixture) {
    (void)params; (void)fixture;

    td_t* a = td_i64(0);
    munit_assert_true(td_is_atom(a));
    munit_assert_false(td_is_vec(a));
    td_release(a);

    /* A raw alloc with type 0 is not an atom (LIST) */
    td_t* b = td_alloc(0);
    munit_assert_false(td_is_atom(b));
    td_free(b);

    return MUNIT_OK;
}

/* ---- Suite definition -------------------------------------------------- */

static MunitTest atom_tests[] = {
    { "/bool",      test_atom_bool,      atom_setup, atom_teardown, 0, NULL },
    { "/u8",        test_atom_u8,        atom_setup, atom_teardown, 0, NULL },
    { "/char",      test_atom_char,      atom_setup, atom_teardown, 0, NULL },
    { "/i16",       test_atom_i16,       atom_setup, atom_teardown, 0, NULL },
    { "/i32",       test_atom_i32,       atom_setup, atom_teardown, 0, NULL },
    { "/i64",       test_atom_i64,       atom_setup, atom_teardown, 0, NULL },
    { "/f64",       test_atom_f64,       atom_setup, atom_teardown, 0, NULL },
    { "/str_sso",   test_atom_str_sso,   atom_setup, atom_teardown, 0, NULL },
    { "/str_long",  test_atom_str_long,  atom_setup, atom_teardown, 0, NULL },
    { "/sym",       test_atom_sym,       atom_setup, atom_teardown, 0, NULL },
    { "/date",      test_atom_date,      atom_setup, atom_teardown, 0, NULL },
    { "/time",      test_atom_time,      atom_setup, atom_teardown, 0, NULL },
    { "/timestamp", test_atom_timestamp, atom_setup, atom_teardown, 0, NULL },
    { "/guid",      test_atom_guid,      atom_setup, atom_teardown, 0, NULL },
    { "/is_atom",   test_is_atom,        atom_setup, atom_teardown, 0, NULL },
    { NULL, NULL, NULL, NULL, 0, NULL },
};

MunitSuite test_atom_suite = {
    "/atom",
    atom_tests,
    NULL,
    0,
    0,
};
