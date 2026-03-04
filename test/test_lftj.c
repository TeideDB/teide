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
#include "store/csr.h"
#include "ops/lftj.h"
#include <string.h>

/* Helper: build CSR relation from edge arrays */
static td_rel_t* make_rel(int64_t* src, int64_t* dst, int64_t n,
                           int64_t n_nodes) {
    td_t* src_v = td_vec_from_raw(TD_I64, src, n);
    td_t* dst_v = td_vec_from_raw(TD_I64, dst, n);
    int64_t s_src = td_sym_intern("src", 3);
    int64_t s_dst = td_sym_intern("dst", 3);
    td_t* edges = td_table_new(2);
    edges = td_table_add_col(edges, s_src, src_v);
    edges = td_table_add_col(edges, s_dst, dst_v);
    td_release(src_v);
    td_release(dst_v);

    td_rel_t* rel = td_rel_from_edges(edges, "src", "dst",
                                       n_nodes, n_nodes, true);
    td_release(edges);
    return rel;
}

/* Helper: set up enumeration context output buffers after build_plan */
static void init_enum_output(lftj_enum_ctx_t* ctx, int64_t** col_ptrs) {
    int64_t cap = 64;
    ctx->col_data  = col_ptrs;
    ctx->out_count = 0;
    ctx->out_cap   = cap;
    ctx->oom       = false;
    for (uint8_t v = 0; v < ctx->n_vars; v++) {
        td_t* h = td_alloc((size_t)cap * sizeof(int64_t));
        ctx->buf_hdrs[v] = h;
        col_ptrs[v] = (int64_t*)td_data(h);
    }
}

/* Triangle graph: 0-1, 0-2, 1-2 (bidirectional) */
static MunitResult test_lftj_triangle(const void* params, void* data) {
    (void)params; (void)data;
    td_heap_init();
    td_sym_init();

    /* Bidirectional triangle: 0↔1, 0↔2, 1↔2 */
    int64_t src[] = {0, 0, 1, 1, 2, 2};
    int64_t dst[] = {1, 2, 0, 2, 0, 1};
    td_rel_t* rel = make_rel(src, dst, 6, 3);
    munit_assert_ptr_not_null(rel);

    /* Find triangles: (a,b,c) where a→b, a→c, b→c */
    lftj_enum_ctx_t ctx;
    memset(&ctx, 0, sizeof(ctx));

    td_rel_t* rels[] = { rel, rel, rel };
    bool ok = lftj_build_default_plan(&ctx, rels, 3, 3);
    munit_assert_true(ok);

    int64_t* col_ptrs[LFTJ_MAX_VARS];
    init_enum_output(&ctx, col_ptrs);

    lftj_enumerate(&ctx, 0);
    munit_assert_false(ctx.oom);
    /* One triangle: (0,1,2) in all 6 orderings → 6 results */
    munit_assert_true(ctx.out_count == 6);

    /* Cleanup output buffers */
    for (uint8_t i = 0; i < ctx.n_vars; i++) {
        if (ctx.buf_hdrs[i]) td_free(ctx.buf_hdrs[i]);
    }
    td_rel_free(rel);
    td_sym_destroy();
    td_heap_destroy();
    return MUNIT_OK;
}

static MunitResult test_lftj_no_results(const void* params, void* data) {
    (void)params; (void)data;
    td_heap_init();
    td_sym_init();

    /* Linear graph: 0→1→2 (no triangles) */
    int64_t src[] = {0, 1};
    int64_t dst[] = {1, 2};
    td_rel_t* rel = make_rel(src, dst, 2, 3);
    munit_assert_ptr_not_null(rel);

    lftj_enum_ctx_t ctx;
    memset(&ctx, 0, sizeof(ctx));

    td_rel_t* rels[] = { rel, rel, rel };
    bool ok = lftj_build_default_plan(&ctx, rels, 3, 3);
    munit_assert_true(ok);

    int64_t* col_ptrs[LFTJ_MAX_VARS];
    init_enum_output(&ctx, col_ptrs);

    lftj_enumerate(&ctx, 0);
    munit_assert_false(ctx.oom);
    munit_assert_true(ctx.out_count == 0);

    for (uint8_t i = 0; i < ctx.n_vars; i++) {
        if (ctx.buf_hdrs[i]) td_free(ctx.buf_hdrs[i]);
    }
    td_rel_free(rel);
    td_sym_destroy();
    td_heap_destroy();
    return MUNIT_OK;
}

static MunitResult test_leapfrog_search(const void* params, void* data) {
    (void)params; (void)data;
    td_heap_init();

    /* Two sorted arrays, find intersection */
    int64_t a_data[] = {1, 3, 5, 7, 9};
    int64_t b_data[] = {2, 3, 6, 7, 10};

    td_lftj_iter_t a = { .targets = a_data, .start = 0, .end = 5, .pos = 0 };
    td_lftj_iter_t b = { .targets = b_data, .start = 0, .end = 5, .pos = 0 };

    td_lftj_iter_t* iters[] = { &a, &b };
    int64_t val;
    bool found = leapfrog_search(iters, 2, &val);
    munit_assert_true(found);
    munit_assert_int(val, ==, 3);

    /* Advance both iterators past 3 and find second intersection (7) */
    a.pos = 3;  /* points to 7 in a_data */
    b.pos = 3;  /* points to 7 in b_data */
    found = leapfrog_search(iters, 2, &val);
    munit_assert_true(found);
    munit_assert_int(val, ==, 7);

    /* Advance past 7 -- no more intersections */
    a.pos = 4;  /* points to 9 */
    b.pos = 4;  /* points to 10 */
    found = leapfrog_search(iters, 2, &val);
    munit_assert_false(found);

    td_heap_destroy();
    return MUNIT_OK;
}

static MunitTest lftj_tests[] = {
    { "/triangle",        test_lftj_triangle,   NULL, NULL, 0, NULL },
    { "/no_results",      test_lftj_no_results, NULL, NULL, 0, NULL },
    { "/leapfrog_search", test_leapfrog_search,  NULL, NULL, 0, NULL },
    { NULL, NULL, NULL, NULL, 0, NULL }
};

MunitSuite test_lftj_suite = {
    "/lftj", lftj_tests, NULL, 1, 0
};
