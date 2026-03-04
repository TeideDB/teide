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
#include <stdatomic.h>
#include <string.h>

/* ---- test_vm_alloc_free ------------------------------------------------ */

static MunitResult test_vm_alloc_free(const void* params, void* fixture) {
    (void)params; (void)fixture;

    size_t size = 4096;
    void* p = td_vm_alloc(size);
    munit_assert_ptr_not_null(p);

    /* Should be writable */
    memset(p, 0xAB, size);

    td_vm_free(p, size);
    return MUNIT_OK;
}

/* ---- test_vm_alloc_aligned --------------------------------------------- */

static MunitResult test_vm_alloc_aligned(const void* params, void* fixture) {
    (void)params; (void)fixture;

    size_t alignment = 64 * 1024;  /* 64 KB alignment */
    size_t size = 4096;
    void* p = td_vm_alloc_aligned(size, alignment);
    munit_assert_ptr_not_null(p);

    /* Verify alignment */
    munit_assert_size((uintptr_t)p % alignment, ==, 0);

    /* Should be writable */
    memset(p, 0xCD, size);

    td_vm_free(p, size);
    return MUNIT_OK;
}

/* ---- test_thread_count ------------------------------------------------- */

static MunitResult test_thread_count(const void* params, void* fixture) {
    (void)params; (void)fixture;

    uint32_t count = td_thread_count();
    munit_assert_uint(count, >=, 1);

    return MUNIT_OK;
}

/* ---- test_thread_create_join ------------------------------------------- */

static _Atomic(int) g_thread_ran = 0;

static void thread_fn(void* arg) {
    (void)arg;
    atomic_store(&g_thread_ran, 1);
}

static MunitResult test_thread_create_join(const void* params, void* fixture) {
    (void)params; (void)fixture;

    atomic_store(&g_thread_ran, 0);

    td_thread_t t;
    td_err_t err = td_thread_create(&t, thread_fn, NULL);
    munit_assert_int(err, ==, TD_OK);

    err = td_thread_join(t);
    munit_assert_int(err, ==, TD_OK);

    munit_assert_int(atomic_load(&g_thread_ran), ==, 1);

    return MUNIT_OK;
}

/* ---- Suite definition -------------------------------------------------- */

static MunitTest platform_tests[] = {
    { "/vm_alloc_free",      test_vm_alloc_free,      NULL, NULL, 0, NULL },
    { "/vm_alloc_aligned",   test_vm_alloc_aligned,   NULL, NULL, 0, NULL },
    { "/thread_count",       test_thread_count,       NULL, NULL, 0, NULL },
    { "/thread_create_join", test_thread_create_join,  NULL, NULL, 0, NULL },
    { NULL, NULL, NULL, NULL, 0, NULL },
};

MunitSuite test_platform_suite = {
    "/platform",
    platform_tests,
    NULL,
    0,
    0,
};
