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
#include <stdio.h>
#include <unistd.h>

static char tmp_csv_path[64];
static const char* tmp_csv(void) {
    if (!tmp_csv_path[0])
        snprintf(tmp_csv_path, sizeof(tmp_csv_path),
                 "/tmp/teide_test_%d.csv", (int)getpid());
    return tmp_csv_path;
}
#define TMP_CSV tmp_csv()

static MunitResult test_csv_roundtrip_i64(const void* params, void* data) {
    (void)params; (void)data;
    td_heap_init();
    td_sym_init();

    int64_t vals[] = {10, 20, 30};
    td_t* vec = td_vec_from_raw(TD_I64, vals, 3);
    int64_t name = td_sym_intern("x", 1);
    td_t* tbl = td_table_new(1);
    tbl = td_table_add_col(tbl, name, vec);
    td_release(vec);

    td_err_t err = td_write_csv(tbl, TMP_CSV);
    munit_assert_int(err, ==, TD_OK);

    td_t* loaded = td_read_csv(TMP_CSV);
    munit_assert_false(TD_IS_ERR(loaded));
    munit_assert_int(loaded->type, ==, TD_TABLE);
    munit_assert_int(td_table_nrows(loaded), ==, 3);
    munit_assert_int(td_table_ncols(loaded), ==, 1);

    /* Verify actual data values survived the roundtrip */
    td_t* col = td_table_get_col_idx(loaded, 0);
    munit_assert_ptr_not_null(col);
    int64_t* loaded_data = (int64_t*)td_data(col);
    munit_assert_int(loaded_data[0], ==, 10);
    munit_assert_int(loaded_data[1], ==, 20);
    munit_assert_int(loaded_data[2], ==, 30);

    td_release(loaded);
    td_release(tbl);
    unlink(TMP_CSV);
    td_sym_destroy();
    td_heap_destroy();
    return MUNIT_OK;
}

static MunitResult test_csv_roundtrip_f64(const void* params, void* data) {
    (void)params; (void)data;
    td_heap_init();
    td_sym_init();

    double vals[] = {1.5, 2.5, 3.5};
    td_t* vec = td_vec_from_raw(TD_F64, vals, 3);
    int64_t name = td_sym_intern("price", 5);
    td_t* tbl = td_table_new(1);
    tbl = td_table_add_col(tbl, name, vec);
    td_release(vec);

    td_err_t err = td_write_csv(tbl, TMP_CSV);
    munit_assert_int(err, ==, TD_OK);

    td_t* loaded = td_read_csv(TMP_CSV);
    munit_assert_false(TD_IS_ERR(loaded));
    munit_assert_int(loaded->type, ==, TD_TABLE);
    munit_assert_int(td_table_nrows(loaded), ==, 3);

    /* Verify F64 values survived the roundtrip */
    td_t* col = td_table_get_col_idx(loaded, 0);
    munit_assert_ptr_not_null(col);
    munit_assert_int(col->type, ==, TD_F64);
    double* loaded_data = (double*)td_data(col);
    munit_assert_double_equal(loaded_data[0], 1.5, 6);
    munit_assert_double_equal(loaded_data[1], 2.5, 6);
    munit_assert_double_equal(loaded_data[2], 3.5, 6);

    td_release(loaded);
    td_release(tbl);
    unlink(TMP_CSV);
    td_sym_destroy();
    td_heap_destroy();
    return MUNIT_OK;
}

static MunitResult test_csv_multi_column(const void* params, void* data) {
    (void)params; (void)data;
    td_heap_init();
    td_sym_init();

    int64_t ids[] = {1, 2, 3};
    double vals[] = {10.5, 20.5, 30.5};
    td_t* id_v = td_vec_from_raw(TD_I64, ids, 3);
    td_t* val_v = td_vec_from_raw(TD_F64, vals, 3);
    int64_t n_id = td_sym_intern("id", 2);
    int64_t n_val = td_sym_intern("val", 3);
    td_t* tbl = td_table_new(2);
    tbl = td_table_add_col(tbl, n_id, id_v);
    tbl = td_table_add_col(tbl, n_val, val_v);
    td_release(id_v);
    td_release(val_v);

    td_err_t err = td_write_csv(tbl, TMP_CSV);
    munit_assert_int(err, ==, TD_OK);

    td_t* loaded = td_read_csv(TMP_CSV);
    munit_assert_false(TD_IS_ERR(loaded));
    munit_assert_int(td_table_ncols(loaded), ==, 2);
    munit_assert_int(td_table_nrows(loaded), ==, 3);

    /* Verify both columns' data values */
    td_t* id_col = td_table_get_col_idx(loaded, 0);
    munit_assert_ptr_not_null(id_col);
    int64_t* id_data = (int64_t*)td_data(id_col);
    munit_assert_int(id_data[0], ==, 1);
    munit_assert_int(id_data[1], ==, 2);
    munit_assert_int(id_data[2], ==, 3);
    td_t* val_col = td_table_get_col_idx(loaded, 1);
    munit_assert_ptr_not_null(val_col);
    double* val_data = (double*)td_data(val_col);
    munit_assert_double_equal(val_data[0], 10.5, 6);
    munit_assert_double_equal(val_data[1], 20.5, 6);
    munit_assert_double_equal(val_data[2], 30.5, 6);

    td_release(loaded);
    td_release(tbl);
    unlink(TMP_CSV);
    td_sym_destroy();
    td_heap_destroy();
    return MUNIT_OK;
}

static MunitResult test_csv_empty_table(const void* params, void* data) {
    (void)params; (void)data;
    td_heap_init();
    td_sym_init();

    td_t* tbl = td_table_new(0);
    td_err_t err = td_write_csv(tbl, TMP_CSV);
    /* Empty table (0 cols) should return TD_ERR_TYPE */
    munit_assert_int(err, ==, TD_ERR_TYPE);

    td_release(tbl);
    unlink(TMP_CSV);
    td_sym_destroy();
    td_heap_destroy();
    return MUNIT_OK;
}

static MunitTest csv_tests[] = {
    { "/roundtrip_i64",  test_csv_roundtrip_i64,  NULL, NULL, 0, NULL },
    { "/roundtrip_f64",  test_csv_roundtrip_f64,  NULL, NULL, 0, NULL },
    { "/multi_column",   test_csv_multi_column,   NULL, NULL, 0, NULL },
    { "/empty_table",    test_csv_empty_table,     NULL, NULL, 0, NULL },
    { NULL, NULL, NULL, NULL, 0, NULL }
};

MunitSuite test_csv_suite = {
    "/csv", csv_tests, NULL, 1, 0
};
