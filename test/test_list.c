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

static void* list_setup(const void* params, void* user_data) {
    (void)params; (void)user_data;
    td_heap_init();
    return NULL;
}

static void list_teardown(void* fixture) {
    (void)fixture;
    td_heap_destroy();
}

/* ---- list_new ---------------------------------------------------------- */

static MunitResult test_list_new(const void* params, void* fixture) {
    (void)params; (void)fixture;

    td_t* list = td_list_new(4);
    munit_assert_ptr_not_null(list);
    munit_assert_false(TD_IS_ERR(list));
    munit_assert_int(list->type, ==, TD_LIST);
    munit_assert_int(list->len, ==, 0);
    munit_assert_false(td_is_atom(list));
    munit_assert_false(td_is_vec(list));  /* type==0, neither atom nor vec */

    td_release(list);
    return MUNIT_OK;
}

/* ---- list_append_get --------------------------------------------------- */

static MunitResult test_list_append_get(const void* params, void* fixture) {
    (void)params; (void)fixture;

    td_t* list = td_list_new(4);

    td_t* a = td_i64(42);
    td_t* b = td_f64(3.14);

    list = td_list_append(list, a);
    munit_assert_false(TD_IS_ERR(list));
    munit_assert_int(list->len, ==, 1);

    list = td_list_append(list, b);
    munit_assert_false(TD_IS_ERR(list));
    munit_assert_int(list->len, ==, 2);

    td_t* got0 = td_list_get(list, 0);
    munit_assert_ptr_equal(got0, a);
    munit_assert_int(got0->i64, ==, 42);

    td_t* got1 = td_list_get(list, 1);
    munit_assert_ptr_equal(got1, b);
    munit_assert_double(got1->f64, ==, 3.14);

    /* Out of range */
    td_t* oob = td_list_get(list, 2);
    munit_assert_null(oob);

    /* Release items, then list.
     * list_append retained a and b, so we release our original refs. */
    td_release(a);
    td_release(b);
    /* Now the list holds the only refs. Destroy arena cleans up. */

    td_release(list);
    return MUNIT_OK;
}

/* ---- list_set ---------------------------------------------------------- */

static MunitResult test_list_set(const void* params, void* fixture) {
    (void)params; (void)fixture;

    td_t* list = td_list_new(4);
    td_t* a = td_i64(10);
    td_t* b = td_i64(20);
    td_t* c = td_i64(30);

    list = td_list_append(list, a);
    list = td_list_append(list, b);
    munit_assert_int(list->len, ==, 2);

    /* Replace index 0 with c */
    list = td_list_set(list, 0, c);
    munit_assert_false(TD_IS_ERR(list));

    td_t* got = td_list_get(list, 0);
    munit_assert_ptr_equal(got, c);
    munit_assert_int(got->i64, ==, 30);

    /* Out of range */
    td_t* err = td_list_set(list, 5, a);
    munit_assert_true(TD_IS_ERR(err));

    td_release(a);
    td_release(b);
    td_release(c);
    td_release(list);
    return MUNIT_OK;
}

/* ---- list_grow --------------------------------------------------------- */

static MunitResult test_list_grow(const void* params, void* fixture) {
    (void)params; (void)fixture;

    td_t* list = td_list_new(1);

    /* Append many items to force reallocation */
    td_t* items[20];
    for (int i = 0; i < 20; i++) {
        items[i] = td_i64((int64_t)i);
        list = td_list_append(list, items[i]);
        munit_assert_false(TD_IS_ERR(list));
    }

    munit_assert_int(list->len, ==, 20);

    /* Verify all items */
    for (int i = 0; i < 20; i++) {
        td_t* got = td_list_get(list, (int64_t)i);
        munit_assert_ptr_not_null(got);
        munit_assert_int(got->i64, ==, (int64_t)i);
    }

    for (int i = 0; i < 20; i++) td_release(items[i]);
    td_release(list);
    return MUNIT_OK;
}

/* ---- list_empty -------------------------------------------------------- */

static MunitResult test_list_empty(const void* params, void* fixture) {
    (void)params; (void)fixture;

    td_t* list = td_list_new(0);
    munit_assert_ptr_not_null(list);
    munit_assert_false(TD_IS_ERR(list));
    munit_assert_int(list->len, ==, 0);

    td_t* got = td_list_get(list, 0);
    munit_assert_null(got);

    td_release(list);
    return MUNIT_OK;
}

/* ---- list_mixed_types -------------------------------------------------- */

static MunitResult test_list_mixed_types(const void* params, void* fixture) {
    (void)params; (void)fixture;

    td_t* list = td_list_new(4);

    td_t* a = td_i64(42);
    td_t* b = td_f64(2.718);
    td_t* c = td_bool(true);
    td_t* d = td_str("hi", 2);

    list = td_list_append(list, a);
    list = td_list_append(list, b);
    list = td_list_append(list, c);
    list = td_list_append(list, d);

    munit_assert_int(list->len, ==, 4);

    td_t* g0 = td_list_get(list, 0);
    munit_assert_int(g0->type, ==, TD_ATOM_I64);
    munit_assert_int(g0->i64, ==, 42);

    td_t* g1 = td_list_get(list, 1);
    munit_assert_int(g1->type, ==, TD_ATOM_F64);
    munit_assert_double(g1->f64, ==, 2.718);

    td_t* g2 = td_list_get(list, 2);
    munit_assert_int(g2->type, ==, TD_ATOM_BOOL);
    munit_assert_uint(g2->b8, ==, 1);

    td_t* g3 = td_list_get(list, 3);
    munit_assert_int(g3->type, ==, TD_ATOM_STR);

    td_release(a);
    td_release(b);
    td_release(c);
    td_release(d);
    td_release(list);
    return MUNIT_OK;
}

/* ---- list_release_drops_item_ref ---------------------------------------- */

static MunitResult test_list_release_drops_item_ref(const void* params, void* fixture) {
    (void)params; (void)fixture;

    td_t* list = td_list_new(1);
    munit_assert_ptr_not_null(list);
    munit_assert_false(TD_IS_ERR(list));

    td_t* item = td_i64(42);
    munit_assert_ptr_not_null(item);
    munit_assert_false(TD_IS_ERR(item));

    list = td_list_append(list, item);
    munit_assert_ptr_not_null(list);
    munit_assert_false(TD_IS_ERR(list));
    munit_assert_uint(atomic_load_explicit(&item->rc, memory_order_relaxed), ==, 2);

    td_release(list);
    munit_assert_uint(atomic_load_explicit(&item->rc, memory_order_relaxed), ==, 1);

    td_release(item);
    return MUNIT_OK;
}

/* ---- Suite definition -------------------------------------------------- */

static MunitTest list_tests[] = {
    { "/new",          test_list_new,          list_setup, list_teardown, 0, NULL },
    { "/append_get",   test_list_append_get,   list_setup, list_teardown, 0, NULL },
    { "/set",          test_list_set,          list_setup, list_teardown, 0, NULL },
    { "/grow",         test_list_grow,         list_setup, list_teardown, 0, NULL },
    { "/empty",        test_list_empty,        list_setup, list_teardown, 0, NULL },
    { "/mixed_types",  test_list_mixed_types,  list_setup, list_teardown, 0, NULL },
    { "/release_drops_item_ref", test_list_release_drops_item_ref, list_setup, list_teardown, 0, NULL },
    { NULL, NULL, NULL, NULL, 0, NULL },
};

MunitSuite test_list_suite = {
    "/list",
    list_tests,
    NULL,
    0,
    0,
};
