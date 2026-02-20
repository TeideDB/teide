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
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>

#define TMP_COL_PATH  "/tmp/teide_test_col.dat"
#define TMP_SPLAY_DIR "/tmp/teide_test_splay"

/* ---- Setup / Teardown -------------------------------------------------- */

static void* store_setup(const void* params, void* user_data) {
    (void)params; (void)user_data;
    td_heap_init();
    td_sym_init();
    return NULL;
}

static void store_teardown(void* fixture) {
    (void)fixture;
    td_sym_destroy();
    td_heap_destroy();
}

/* ---- test_col_mmap_i64 ------------------------------------------------- */

static MunitResult test_col_mmap_i64(const void* params, void* fixture) {
    (void)params; (void)fixture;

    int64_t raw[] = {10, 20, 30, 40, 50};
    td_t* vec = td_vec_from_raw(TD_I64, raw, 5);
    munit_assert_ptr_not_null(vec);
    munit_assert_false(TD_IS_ERR(vec));

    /* Save to file */
    td_err_t err = td_col_save(vec, TMP_COL_PATH);
    munit_assert_int(err, ==, TD_OK);

    /* Load via mmap */
    td_t* mapped = td_col_mmap(TMP_COL_PATH);
    munit_assert_ptr_not_null(mapped);
    munit_assert_false(TD_IS_ERR(mapped));

    /* Verify mmod==1 */
    munit_assert_uint(mapped->mmod, ==, 1);

    /* Verify type, len, data */
    munit_assert_int(mapped->type, ==, TD_I64);
    munit_assert_int(mapped->len, ==, 5);

    int64_t* data = (int64_t*)td_data(mapped);
    for (int i = 0; i < 5; i++) {
        munit_assert_int(data[i], ==, raw[i]);
    }

    td_release(mapped);
    td_release(vec);
    unlink(TMP_COL_PATH);
    return MUNIT_OK;
}

/* ---- test_col_mmap_f64 ------------------------------------------------- */

static MunitResult test_col_mmap_f64(const void* params, void* fixture) {
    (void)params; (void)fixture;

    double raw[] = {1.1, 2.2, 3.3, 4.4};
    td_t* vec = td_vec_from_raw(TD_F64, raw, 4);
    munit_assert_ptr_not_null(vec);
    munit_assert_false(TD_IS_ERR(vec));

    td_err_t err = td_col_save(vec, TMP_COL_PATH);
    munit_assert_int(err, ==, TD_OK);

    td_t* mapped = td_col_mmap(TMP_COL_PATH);
    munit_assert_ptr_not_null(mapped);
    munit_assert_false(TD_IS_ERR(mapped));

    munit_assert_uint(mapped->mmod, ==, 1);
    munit_assert_int(mapped->type, ==, TD_F64);
    munit_assert_int(mapped->len, ==, 4);

    double* data = (double*)td_data(mapped);
    for (int i = 0; i < 4; i++) {
        munit_assert_double(data[i], ==, raw[i]);
    }

    td_release(mapped);
    td_release(vec);
    unlink(TMP_COL_PATH);
    return MUNIT_OK;
}

/* ---- test_col_mmap_cow ------------------------------------------------- */

static MunitResult test_col_mmap_cow(const void* params, void* fixture) {
    (void)params; (void)fixture;

    int64_t raw[] = {100, 200, 300};
    td_t* vec = td_vec_from_raw(TD_I64, raw, 3);
    munit_assert_false(TD_IS_ERR(vec));

    td_err_t err = td_col_save(vec, TMP_COL_PATH);
    munit_assert_int(err, ==, TD_OK);

    td_t* mapped = td_col_mmap(TMP_COL_PATH);
    munit_assert_false(TD_IS_ERR(mapped));
    munit_assert_uint(mapped->mmod, ==, 1);

    /* Retain so rc==2, forcing td_cow to make a real copy */
    td_retain(mapped);
    munit_assert_uint(atomic_load_explicit(&mapped->rc, memory_order_relaxed), ==, 2);

    /* COW: td_cow should produce a buddy-allocated copy */
    td_t* copy = td_cow(mapped);
    munit_assert_ptr_not_null(copy);
    munit_assert_false(TD_IS_ERR(copy));
    munit_assert_uint(copy->mmod, ==, 0);

    /* td_cow called td_release on mapped (rc 2->1), so mapped still alive */

    /* Verify data in copy */
    int64_t* data = (int64_t*)td_data(copy);
    for (int i = 0; i < 3; i++) {
        munit_assert_int(data[i], ==, raw[i]);
    }

    td_release(copy);
    td_release(mapped);
    td_release(vec);
    unlink(TMP_COL_PATH);
    return MUNIT_OK;
}

/* ---- test_col_mmap_refcount -------------------------------------------- */

static MunitResult test_col_mmap_refcount(const void* params, void* fixture) {
    (void)params; (void)fixture;

    int64_t raw[] = {7, 8, 9};
    td_t* vec = td_vec_from_raw(TD_I64, raw, 3);
    munit_assert_false(TD_IS_ERR(vec));

    td_err_t err = td_col_save(vec, TMP_COL_PATH);
    munit_assert_int(err, ==, TD_OK);

    td_t* mapped = td_col_mmap(TMP_COL_PATH);
    munit_assert_false(TD_IS_ERR(mapped));
    munit_assert_uint(atomic_load_explicit(&mapped->rc, memory_order_relaxed), ==, 1);

    /* Retain: rc should be 2 */
    td_retain(mapped);
    munit_assert_uint(atomic_load_explicit(&mapped->rc, memory_order_relaxed), ==, 2);

    /* Release once: rc==1, still readable */
    td_release(mapped);
    munit_assert_uint(atomic_load_explicit(&mapped->rc, memory_order_relaxed), ==, 1);

    int64_t* data = (int64_t*)td_data(mapped);
    munit_assert_int(data[0], ==, 7);
    munit_assert_int(data[1], ==, 8);
    munit_assert_int(data[2], ==, 9);

    /* Release again: munmap */
    td_release(mapped);

    td_release(vec);
    unlink(TMP_COL_PATH);
    return MUNIT_OK;
}

/* ---- test_col_mmap_corrupt --------------------------------------------- */

static MunitResult test_col_mmap_corrupt(const void* params, void* fixture) {
    (void)params; (void)fixture;

    /* Write a 16-byte file (too small for a valid column header) */
    FILE* f = fopen(TMP_COL_PATH, "wb");
    munit_assert_ptr_not_null(f);
    uint8_t junk[16] = {0};
    fwrite(junk, 1, 16, f);
    fclose(f);

    td_t* result = td_col_mmap(TMP_COL_PATH);
    munit_assert_true(TD_IS_ERR(result));
    munit_assert_int(TD_ERR_CODE(result), ==, TD_ERR_CORRUPT);

    unlink(TMP_COL_PATH);
    return MUNIT_OK;
}

/* ---- test_col_mmap_nofile ---------------------------------------------- */

static MunitResult test_col_mmap_nofile(const void* params, void* fixture) {
    (void)params; (void)fixture;

    td_t* result = td_col_mmap("/tmp/teide_nonexistent_file_xyz.dat");
    munit_assert_true(TD_IS_ERR(result));
    munit_assert_int(TD_ERR_CODE(result), ==, TD_ERR_IO);

    return MUNIT_OK;
}

/* ---- test_splay_open_roundtrip ----------------------------------------- */

static MunitResult test_splay_open_roundtrip(const void* params, void* fixture) {
    (void)params; (void)fixture;

    /* Clean up any leftover splay dir */
    (void)!system("rm -rf " TMP_SPLAY_DIR);

    /* Build a 3-column table: I64, F64, I32 */
    td_t* tbl = td_table_new(4);
    munit_assert_ptr_not_null(tbl);
    munit_assert_false(TD_IS_ERR(tbl));

    int64_t id_a = td_sym_intern("col_a", 5);
    int64_t id_b = td_sym_intern("col_b", 5);
    int64_t id_c = td_sym_intern("col_c", 5);

    int64_t raw_a[] = {1, 2, 3, 4, 5};
    double  raw_b[] = {1.5, 2.5, 3.5, 4.5, 5.5};
    int32_t raw_c[] = {10, 20, 30, 40, 50};

    td_t* col_a = td_vec_from_raw(TD_I64, raw_a, 5);
    td_t* col_b = td_vec_from_raw(TD_F64, raw_b, 5);
    td_t* col_c = td_vec_from_raw(TD_I32, raw_c, 5);
    munit_assert_false(TD_IS_ERR(col_a));
    munit_assert_false(TD_IS_ERR(col_b));
    munit_assert_false(TD_IS_ERR(col_c));

    tbl = td_table_add_col(tbl, id_a, col_a);
    munit_assert_false(TD_IS_ERR(tbl));
    tbl = td_table_add_col(tbl, id_b, col_b);
    munit_assert_false(TD_IS_ERR(tbl));
    tbl = td_table_add_col(tbl, id_c, col_c);
    munit_assert_false(TD_IS_ERR(tbl));

    /* Save to splay directory */
    td_err_t err = td_splay_save(tbl, TMP_SPLAY_DIR, NULL);
    munit_assert_int(err, ==, TD_OK);

    /* Open via mmap (zero-copy) */
    td_t* loaded = td_read_splayed(TMP_SPLAY_DIR, NULL);
    munit_assert_ptr_not_null(loaded);
    munit_assert_false(TD_IS_ERR(loaded));

    /* Verify ncols and nrows */
    munit_assert_int(td_table_ncols(loaded), ==, 3);
    munit_assert_int(td_table_nrows(loaded), ==, 5);

    /* Verify column mmod==1 (mmap'd) */
    td_t* la = td_table_get_col(loaded, id_a);
    td_t* lb = td_table_get_col(loaded, id_b);
    td_t* lc = td_table_get_col(loaded, id_c);
    munit_assert_ptr_not_null(la);
    munit_assert_ptr_not_null(lb);
    munit_assert_ptr_not_null(lc);

    munit_assert_uint(la->mmod, ==, 1);
    munit_assert_uint(lb->mmod, ==, 1);
    munit_assert_uint(lc->mmod, ==, 1);

    /* Verify data */
    int64_t* da = (int64_t*)td_data(la);
    double*  db = (double*)td_data(lb);
    int32_t* dc = (int32_t*)td_data(lc);

    for (int i = 0; i < 5; i++) {
        munit_assert_int(da[i], ==, raw_a[i]);
        munit_assert_double(db[i], ==, raw_b[i]);
        munit_assert_int(dc[i], ==, raw_c[i]);
    }

    td_release(loaded);
    td_release(col_a);
    td_release(col_b);
    td_release(col_c);
    td_release(tbl);

    /* Cleanup */
    (void)!system("rm -rf " TMP_SPLAY_DIR);
    return MUNIT_OK;
}

/* ---- test_parted_nrows ------------------------------------------------- */

static MunitResult test_parted_nrows(const void* params, void* fixture) {
    (void)params; (void)fixture;

    /* Build 3 segment vectors: 100, 200, 300 rows */
    td_t* seg0 = td_vec_new(TD_I64, 100);
    td_t* seg1 = td_vec_new(TD_I64, 200);
    td_t* seg2 = td_vec_new(TD_I64, 300);
    munit_assert_false(TD_IS_ERR(seg0));
    munit_assert_false(TD_IS_ERR(seg1));
    munit_assert_false(TD_IS_ERR(seg2));
    seg0->len = 100;
    seg1->len = 200;
    seg2->len = 300;

    /* Build a parted column: type = TD_PARTED_BASE + TD_I64, len = 3 segments */
    size_t data_size = 3 * sizeof(td_t*);
    td_t* parted = td_alloc(data_size);
    munit_assert_ptr_not_null(parted);
    munit_assert_false(TD_IS_ERR(parted));
    parted->type = TD_PARTED_BASE + TD_I64;
    parted->len = 3;
    parted->attrs = 0;
    memset(parted->nullmap, 0, 16);

    td_t** segs = (td_t**)td_data(parted);
    segs[0] = seg0; td_retain(seg0);
    segs[1] = seg1; td_retain(seg1);
    segs[2] = seg2; td_retain(seg2);

    /* Verify td_parted_nrows returns 600 */
    int64_t total = td_parted_nrows(parted);
    munit_assert_int(total, ==, 600);

    /* Non-parted vector falls through to v->len */
    munit_assert_int(td_parted_nrows(seg0), ==, 100);

    td_release(parted);
    td_release(seg0);
    td_release(seg1);
    td_release(seg2);
    return MUNIT_OK;
}

/* ---- test_table_nrows_parted ------------------------------------------- */

static MunitResult test_table_nrows_parted(const void* params, void* fixture) {
    (void)params; (void)fixture;

    /* Build 2 segment vectors: 50 and 75 rows */
    td_t* seg0 = td_vec_new(TD_I64, 50);
    td_t* seg1 = td_vec_new(TD_I64, 75);
    munit_assert_false(TD_IS_ERR(seg0));
    munit_assert_false(TD_IS_ERR(seg1));
    seg0->len = 50;
    seg1->len = 75;

    /* Build a parted column */
    size_t data_size = 2 * sizeof(td_t*);
    td_t* parted = td_alloc(data_size);
    munit_assert_false(TD_IS_ERR(parted));
    parted->type = TD_PARTED_BASE + TD_I64;
    parted->len = 2;
    parted->attrs = 0;
    memset(parted->nullmap, 0, 16);

    td_t** segs = (td_t**)td_data(parted);
    segs[0] = seg0; td_retain(seg0);
    segs[1] = seg1; td_retain(seg1);

    /* Build a table with this parted column */
    int64_t name_id = td_sym_intern("pcol", 4);
    td_t* tbl = td_table_new(2);
    munit_assert_false(TD_IS_ERR(tbl));
    tbl = td_table_add_col(tbl, name_id, parted);
    munit_assert_false(TD_IS_ERR(tbl));

    /* Verify td_table_nrows returns 125 */
    munit_assert_int(td_table_nrows(tbl), ==, 125);

    td_release(tbl);
    td_release(parted);
    td_release(seg0);
    td_release(seg1);
    return MUNIT_OK;
}

/* ---- test_parted_release ----------------------------------------------- */

static MunitResult test_parted_release(const void* params, void* fixture) {
    (void)params; (void)fixture;

    /* Build 2 segment vectors */
    td_t* seg0 = td_vec_new(TD_I64, 10);
    td_t* seg1 = td_vec_new(TD_I64, 20);
    munit_assert_false(TD_IS_ERR(seg0));
    munit_assert_false(TD_IS_ERR(seg1));
    seg0->len = 10;
    seg1->len = 20;

    /* Build a parted column */
    size_t data_size = 2 * sizeof(td_t*);
    td_t* parted = td_alloc(data_size);
    munit_assert_false(TD_IS_ERR(parted));
    parted->type = TD_PARTED_BASE + TD_I64;
    parted->len = 2;
    parted->attrs = 0;
    memset(parted->nullmap, 0, 16);

    td_t** segs = (td_t**)td_data(parted);
    segs[0] = seg0; td_retain(seg0);
    segs[1] = seg1; td_retain(seg1);

    /* Segments should have rc=2 (original + parted ref) */
    munit_assert_uint(atomic_load_explicit(&seg0->rc, memory_order_relaxed), ==, 2);
    munit_assert_uint(atomic_load_explicit(&seg1->rc, memory_order_relaxed), ==, 2);

    /* Release parted column — segments' rc should drop to 1 */
    td_release(parted);
    munit_assert_uint(atomic_load_explicit(&seg0->rc, memory_order_relaxed), ==, 1);
    munit_assert_uint(atomic_load_explicit(&seg1->rc, memory_order_relaxed), ==, 1);

    td_release(seg0);
    td_release(seg1);
    return MUNIT_OK;
}

/* ---- test_part_open ---------------------------------------------------- */

#define TMP_PART_DB "/tmp/teide_test_parted_db"
#define TMP_TABLE_NAME "test_tbl"

static MunitResult test_part_open(const void* params, void* fixture) {
    (void)params; (void)fixture;

    /* Setup: create a 2-partition db with 2 columns each */
    (void)!system("rm -rf " TMP_PART_DB);
    (void)!system("mkdir -p " TMP_PART_DB "/2024.01.01/" TMP_TABLE_NAME);
    (void)!system("mkdir -p " TMP_PART_DB "/2024.01.02/" TMP_TABLE_NAME);

    /* Partition 1: 3 rows */
    int64_t raw_a1[] = {10, 20, 30};
    double  raw_b1[] = {1.1, 2.2, 3.3};
    td_t* a1 = td_vec_from_raw(TD_I64, raw_a1, 3);
    td_t* b1 = td_vec_from_raw(TD_F64, raw_b1, 3);

    td_t* tbl1 = td_table_new(3);
    int64_t name_a = td_sym_intern("a", 1);
    int64_t name_b = td_sym_intern("b", 1);
    tbl1 = td_table_add_col(tbl1, name_a, a1);
    tbl1 = td_table_add_col(tbl1, name_b, b1);
    td_err_t err = td_splay_save(tbl1, TMP_PART_DB "/2024.01.01/" TMP_TABLE_NAME, NULL);
    munit_assert_int(err, ==, TD_OK);

    /* Partition 2: 5 rows */
    int64_t raw_a2[] = {40, 50, 60, 70, 80};
    double  raw_b2[] = {4.4, 5.5, 6.6, 7.7, 8.8};
    td_t* a2 = td_vec_from_raw(TD_I64, raw_a2, 5);
    td_t* b2 = td_vec_from_raw(TD_F64, raw_b2, 5);

    td_t* tbl2 = td_table_new(3);
    tbl2 = td_table_add_col(tbl2, name_a, a2);
    tbl2 = td_table_add_col(tbl2, name_b, b2);
    err = td_splay_save(tbl2, TMP_PART_DB "/2024.01.02/" TMP_TABLE_NAME, NULL);
    munit_assert_int(err, ==, TD_OK);

    /* Save symfile */
    err = td_sym_save(TMP_PART_DB "/sym");
    munit_assert_int(err, ==, TD_OK);

    /* Cleanup in-memory tables */
    td_release(a1); td_release(b1); td_release(tbl1);
    td_release(a2); td_release(b2); td_release(tbl2);

    /* Open via td_read_parted */
    td_t* parted = td_read_parted(TMP_PART_DB, TMP_TABLE_NAME);
    munit_assert_ptr_not_null(parted);
    munit_assert_false(TD_IS_ERR(parted));

    /* Should have 3 columns: date (MAPCOMMON), a (parted I64), b (parted F64) */
    int64_t ncols = td_table_ncols(parted);
    munit_assert_int(ncols, ==, 3);

    /* Total rows should be 8 */
    int64_t nrows = td_table_nrows(parted);
    munit_assert_int(nrows, ==, 8);

    /* Verify first column is MAPCOMMON (date-inferred) */
    td_t* mapcommon = td_table_get_col_idx(parted, 0);
    munit_assert_ptr_not_null(mapcommon);
    munit_assert_int(mapcommon->type, ==, TD_MAPCOMMON);
    munit_assert_uint(mapcommon->attrs, ==, TD_MC_DATE);

    /* MAPCOMMON: [key_values (TD_DATE), row_counts (TD_I64)] */
    td_t** mc_ptrs = (td_t**)td_data(mapcommon);
    td_t* key_values = mc_ptrs[0];
    td_t* row_counts = mc_ptrs[1];

    /* key_values should be TD_DATE with parsed days-since-2000 */
    munit_assert_int(key_values->type, ==, TD_DATE);
    munit_assert_int(key_values->len, ==, 2);
    int32_t* kv_data = (int32_t*)td_data(key_values);
    /* 2024.01.01 = 8766 days since 2000-01-01 */
    munit_assert_int(kv_data[0], ==, 8766);
    /* 2024.01.02 = 8767 */
    munit_assert_int(kv_data[1], ==, 8767);

    munit_assert_int(row_counts->len, ==, 2);
    int64_t* rc_data = (int64_t*)td_data(row_counts);
    munit_assert_int(rc_data[0], ==, 3);
    munit_assert_int(rc_data[1], ==, 5);

    /* Verify second column is parted I64 */
    td_t* col_a = td_table_get_col_idx(parted, 1);
    munit_assert_ptr_not_null(col_a);
    munit_assert_true(TD_IS_PARTED(col_a->type));
    munit_assert_int(TD_PARTED_BASETYPE(col_a->type), ==, TD_I64);
    munit_assert_int(col_a->len, ==, 2);

    /* Verify segment 0 has 3 rows, mmod=1 (mmap'd) */
    td_t** segs_a = (td_t**)td_data(col_a);
    munit_assert_int(segs_a[0]->len, ==, 3);
    munit_assert_uint(segs_a[0]->mmod, ==, 1);
    munit_assert_int(segs_a[1]->len, ==, 5);
    munit_assert_uint(segs_a[1]->mmod, ==, 1);

    /* Verify data in segment 0 */
    int64_t* data_a0 = (int64_t*)td_data(segs_a[0]);
    munit_assert_int(data_a0[0], ==, 10);
    munit_assert_int(data_a0[2], ==, 30);

    /* Verify third column is parted F64 */
    td_t* col_b = td_table_get_col_idx(parted, 2);
    munit_assert_true(TD_IS_PARTED(col_b->type));
    munit_assert_int(TD_PARTED_BASETYPE(col_b->type), ==, TD_F64);

    /* Release — should unmap all segments */
    td_release(parted);

    (void)!system("rm -rf " TMP_PART_DB);
    return MUNIT_OK;
}

/* ---- test_group_parted ------------------------------------------------- */

static MunitResult test_group_parted(const void* params, void* fixture) {
    (void)params; (void)fixture;

    /* Build a 2-partition parted table with columns id1 (I64) and v1 (I64).
     * Partition 0: id1=[0,0,1,1,2], v1=[10,20,30,40,50]
     * Partition 1: id1=[0,1,1,2,2], v1=[60,70,80,90,100]
     * GROUP BY id1 SUM(v1) should give:
     *   id1=0: 10+20+60 = 90
     *   id1=1: 30+40+70+80 = 220
     *   id1=2: 50+90+100 = 240
     */

    /* Build segment vectors */
    td_t* id1_0 = td_vec_new(TD_I64, 5);
    td_t* v1_0  = td_vec_new(TD_I64, 5);
    munit_assert_ptr_not_null(id1_0);
    munit_assert_ptr_not_null(v1_0);
    id1_0->len = v1_0->len = 5;
    int64_t id1_0_data[] = {0,0,1,1,2};
    int64_t v1_0_data[]  = {10,20,30,40,50};
    memcpy(td_data(id1_0), id1_0_data, sizeof(id1_0_data));
    memcpy(td_data(v1_0),  v1_0_data,  sizeof(v1_0_data));

    td_t* id1_1 = td_vec_new(TD_I64, 5);
    td_t* v1_1  = td_vec_new(TD_I64, 5);
    munit_assert_ptr_not_null(id1_1);
    munit_assert_ptr_not_null(v1_1);
    id1_1->len = v1_1->len = 5;
    int64_t id1_1_data[] = {0,1,1,2,2};
    int64_t v1_1_data[]  = {60,70,80,90,100};
    memcpy(td_data(id1_1), id1_1_data, sizeof(id1_1_data));
    memcpy(td_data(v1_1),  v1_1_data,  sizeof(v1_1_data));

    /* Build parted columns (2 segments each) */
    td_t* id1_parted = td_alloc(2 * sizeof(td_t*));
    munit_assert_ptr_not_null(id1_parted);
    id1_parted->type = TD_PARTED_BASE + TD_I64;
    id1_parted->len = 2;
    ((td_t**)td_data(id1_parted))[0] = id1_0;
    ((td_t**)td_data(id1_parted))[1] = id1_1;

    td_t* v1_parted = td_alloc(2 * sizeof(td_t*));
    munit_assert_ptr_not_null(v1_parted);
    v1_parted->type = TD_PARTED_BASE + TD_I64;
    v1_parted->len = 2;
    ((td_t**)td_data(v1_parted))[0] = v1_0;
    ((td_t**)td_data(v1_parted))[1] = v1_1;

    /* Build parted table */
    int64_t sym_id1 = td_sym_intern("id1", 3);
    int64_t sym_v1  = td_sym_intern("v1",  2);

    td_t* tbl = td_table_new(2);
    munit_assert_ptr_not_null(tbl);
    tbl = td_table_add_col(tbl, sym_id1, id1_parted);
    tbl = td_table_add_col(tbl, sym_v1,  v1_parted);
    munit_assert_int(td_table_nrows(tbl), ==, 10);

    /* Build graph: GROUP BY id1 SUM(v1) */
    td_graph_t* g = td_graph_new(tbl);
    munit_assert_ptr_not_null(g);
    td_op_t* scan_id1 = td_scan(g, "id1");
    td_op_t* scan_v1  = td_scan(g, "v1");
    td_op_t* keys[] = { scan_id1 };
    uint16_t ops[]  = { OP_SUM };
    td_op_t* ins[]  = { scan_v1 };
    td_op_t* root = td_group(g, keys, 1, ops, ins, 1);
    root = td_optimize(g, root);
    td_t* result = td_execute(g, root);

    munit_assert_ptr_not_null(result);
    munit_assert_false(TD_IS_ERR(result));
    munit_assert_int(td_table_nrows(result), ==, 3); /* groups: 0, 1, 2 */

    /* Verify sums — extract key and agg columns, match by key value */
    td_t* rk = td_table_get_col_idx(result, 0); /* id1 key column */
    td_t* rv = td_table_get_col_idx(result, 1); /* v1_sum agg column */
    munit_assert_ptr_not_null(rk);
    munit_assert_ptr_not_null(rv);

    int64_t* rk_data = (int64_t*)td_data(rk);
    int64_t* rv_data = (int64_t*)td_data(rv);
    int64_t expected_sums[3] = {0, 0, 0}; /* for keys 0, 1, 2 */
    for (int i = 0; i < 3; i++) {
        int64_t key = rk_data[i];
        munit_assert_true(key >= 0 && key <= 2);
        expected_sums[key] = rv_data[i];
    }
    munit_assert_int(expected_sums[0], ==, 90);
    munit_assert_int(expected_sums[1], ==, 220);
    munit_assert_int(expected_sums[2], ==, 240);

    td_release(result);
    td_graph_free(g);
    td_release(id1_parted);
    td_release(v1_parted);
    td_release(tbl);
    return MUNIT_OK;
}

/* ---- test_col_ext_nullmap_roundtrip ------------------------------------- */

#define EXT_NM_LEN 256  /* >128 to trigger ext_nullmap */

static MunitResult test_col_ext_nullmap_roundtrip(const void* params, void* fixture) {
    (void)params; (void)fixture;

    /* Create a 256-element I64 vector with nulls at various positions */
    td_t* vec = td_vec_new(TD_I64, EXT_NM_LEN);
    munit_assert_ptr_not_null(vec);
    munit_assert_false(TD_IS_ERR(vec));
    vec->len = EXT_NM_LEN;

    int64_t* data = (int64_t*)td_data(vec);
    for (int i = 0; i < EXT_NM_LEN; i++) data[i] = i * 10;

    /* Set nulls at positions: 0, 5, 127, 128, 200, 255 */
    int null_positions[] = { 0, 5, 127, 128, 200, 255 };
    int n_nulls = (int)(sizeof(null_positions) / sizeof(null_positions[0]));
    for (int i = 0; i < n_nulls; i++)
        td_vec_set_null(vec, null_positions[i], true);

    /* Verify ext_nullmap was created (>128 elements forces external) */
    munit_assert_true((vec->attrs & TD_ATTR_HAS_NULLS) != 0);
    munit_assert_true((vec->attrs & TD_ATTR_NULLMAP_EXT) != 0);
    munit_assert_ptr_not_null(vec->ext_nullmap);

    /* --- Round-trip via td_col_load --- */
    td_err_t err = td_col_save(vec, TMP_COL_PATH);
    munit_assert_int(err, ==, TD_OK);

    td_t* loaded = td_col_load(TMP_COL_PATH);
    munit_assert_ptr_not_null(loaded);
    munit_assert_false(TD_IS_ERR(loaded));

    munit_assert_int(loaded->type, ==, TD_I64);
    munit_assert_int(loaded->len, ==, EXT_NM_LEN);
    munit_assert_true((loaded->attrs & TD_ATTR_HAS_NULLS) != 0);
    munit_assert_true((loaded->attrs & TD_ATTR_NULLMAP_EXT) != 0);
    munit_assert_ptr_not_null(loaded->ext_nullmap);

    /* Verify null positions preserved */
    for (int i = 0; i < n_nulls; i++)
        munit_assert_true(td_vec_is_null(loaded, null_positions[i]));

    /* Verify non-null positions */
    munit_assert_false(td_vec_is_null(loaded, 1));
    munit_assert_false(td_vec_is_null(loaded, 129));
    munit_assert_false(td_vec_is_null(loaded, 254));

    /* Verify data values at non-null positions */
    int64_t* ld = (int64_t*)td_data(loaded);
    munit_assert_int(ld[1], ==, 10);
    munit_assert_int(ld[129], ==, 1290);
    munit_assert_int(ld[254], ==, 2540);

    td_release(loaded);

    /* --- Round-trip via td_col_mmap --- */
    td_t* mapped = td_col_mmap(TMP_COL_PATH);
    munit_assert_ptr_not_null(mapped);
    munit_assert_false(TD_IS_ERR(mapped));

    munit_assert_uint(mapped->mmod, ==, 1);
    munit_assert_int(mapped->type, ==, TD_I64);
    munit_assert_int(mapped->len, ==, EXT_NM_LEN);
    munit_assert_true((mapped->attrs & TD_ATTR_HAS_NULLS) != 0);
    munit_assert_true((mapped->attrs & TD_ATTR_NULLMAP_EXT) != 0);
    munit_assert_ptr_not_null(mapped->ext_nullmap);

    /* Verify null positions preserved in mmap path */
    for (int i = 0; i < n_nulls; i++)
        munit_assert_true(td_vec_is_null(mapped, null_positions[i]));

    munit_assert_false(td_vec_is_null(mapped, 1));
    munit_assert_false(td_vec_is_null(mapped, 129));

    /* Verify data */
    int64_t* md = (int64_t*)td_data(mapped);
    munit_assert_int(md[1], ==, 10);
    munit_assert_int(md[129], ==, 1290);

    td_release(mapped);
    td_release(vec);
    unlink(TMP_COL_PATH);
    return MUNIT_OK;
}

/* ---- Suite definition -------------------------------------------------- */

static MunitTest store_tests[] = {
    { "/col_mmap_i64",         test_col_mmap_i64,         store_setup, store_teardown, 0, NULL },
    { "/col_mmap_f64",         test_col_mmap_f64,         store_setup, store_teardown, 0, NULL },
    { "/col_mmap_cow",         test_col_mmap_cow,         store_setup, store_teardown, 0, NULL },
    { "/col_mmap_refcount",    test_col_mmap_refcount,    store_setup, store_teardown, 0, NULL },
    { "/col_mmap_corrupt",     test_col_mmap_corrupt,     store_setup, store_teardown, 0, NULL },
    { "/col_mmap_nofile",      test_col_mmap_nofile,      store_setup, store_teardown, 0, NULL },
    { "/splay_open_roundtrip", test_splay_open_roundtrip, store_setup, store_teardown, 0, NULL },
    { "/parted_nrows",        test_parted_nrows,         store_setup, store_teardown, 0, NULL },
    { "/table_nrows_parted",  test_table_nrows_parted,   store_setup, store_teardown, 0, NULL },
    { "/parted_release",      test_parted_release,        store_setup, store_teardown, 0, NULL },
    { "/part_open",            test_part_open,            store_setup, store_teardown, 0, NULL },
    { "/group_parted",         test_group_parted,         store_setup, store_teardown, 0, NULL },
    { "/col_ext_nullmap_roundtrip", test_col_ext_nullmap_roundtrip, store_setup, store_teardown, 0, NULL },
    { NULL, NULL, NULL, NULL, 0, NULL },
};

MunitSuite test_store_suite = {
    "/store",
    store_tests,
    NULL,
    0,
    0,
};
