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
#include "ops/pipe.h"

/* ---- test_pipe_new_defaults -------------------------------------------- */

static MunitResult test_pipe_new_defaults(const void* params, void* fixture) {
    (void)params; (void)fixture;

    td_pipe_t* p = td_pipe_new();
    munit_assert_ptr_not_null(p);

    /* All fields should be zero-initialized */
    munit_assert_null(p->op);
    munit_assert_null(p->inputs[0]);
    munit_assert_null(p->inputs[1]);
    munit_assert_null(p->materialized);

    /* spill_fd should be -1 (no spill file) */
    munit_assert_int(p->spill_fd, ==, -1);

    td_pipe_free(p);
    return MUNIT_OK;
}

/* ---- test_pipe_free_null_safe ------------------------------------------ */

static MunitResult test_pipe_free_null_safe(const void* params, void* fixture) {
    (void)params; (void)fixture;

    /* Freeing NULL should not crash */
    td_pipe_free(NULL);

    return MUNIT_OK;
}

/* ---- test_pipe_multiple_alloc_free ------------------------------------- */

static MunitResult test_pipe_multiple_alloc_free(const void* params, void* fixture) {
    (void)params; (void)fixture;

    /* Allocate several pipes, verify independence, free them */
    td_pipe_t* p1 = td_pipe_new();
    td_pipe_t* p2 = td_pipe_new();
    td_pipe_t* p3 = td_pipe_new();

    munit_assert_ptr_not_null(p1);
    munit_assert_ptr_not_null(p2);
    munit_assert_ptr_not_null(p3);

    /* They should be distinct allocations */
    munit_assert_true(p1 != p2);
    munit_assert_true(p2 != p3);
    munit_assert_true(p1 != p3);

    /* Wire p1 as input to p2 */
    p2->inputs[0] = p1;
    munit_assert_true(p2->inputs[0] == p1);

    /* Free in reverse; td_pipe_free does NOT recurse into inputs */
    td_pipe_free(p3);
    td_pipe_free(p2);
    td_pipe_free(p1);

    return MUNIT_OK;
}

/* ---- Suite definition -------------------------------------------------- */

static MunitTest pipe_tests[] = {
    { "/new_defaults",        test_pipe_new_defaults,        NULL, NULL, 0, NULL },
    { "/free_null_safe",      test_pipe_free_null_safe,      NULL, NULL, 0, NULL },
    { "/multiple_alloc_free", test_pipe_multiple_alloc_free, NULL, NULL, 0, NULL },
    { NULL, NULL, NULL, NULL, 0, NULL },
};

MunitSuite test_pipe_suite = {
    "/pipe",
    pipe_tests,
    NULL,
    0,
    0,
};
