/*
 *   Copyright (c) 2024-2026 Anton Kundenko <singaraiona@gmail.com>
 *   All rights reserved.
 *
 *   Licensed under the MIT License (see LICENSE or td.h header).
 */

#include "munit.h"
#include <teide/td.h>

/* --------------------------------------------------------------------------
 * Test: OP_UNION_ALL row-unions two same-schema tables
 *
 * Strategy: bind a 3-row table to a graph. Build two GROUP BY ops that each
 * produce a 3-row result table, then union them. The result must have
 * nrows(left) + nrows(right) = 6 rows and 2 columns.
 * -------------------------------------------------------------------------- */

static MunitResult test_union_all(const void* params, void* data) {
    (void)params; (void)data;
    td_heap_init();
    td_sym_init();

    /* Build a small 3-row table: id=[1,2,3], val=[10,20,30] */
    int64_t s_id  = td_sym_intern("id",  2);
    int64_t s_val = td_sym_intern("val", 3);

    td_t* id_vec  = td_vec_from_raw(TD_I64, (int64_t[]){1, 2, 3}, 3);
    td_t* val_vec = td_vec_from_raw(TD_I64, (int64_t[]){10, 20, 30}, 3);
    td_t* tbl = td_table_new(2);
    tbl = td_table_add_col(tbl, s_id,  id_vec);
    tbl = td_table_add_col(tbl, s_val, val_vec);
    td_release(id_vec);
    td_release(val_vec);

    /* Build a graph with tbl as the bound table */
    td_graph_t* g = td_graph_new(tbl);
    munit_assert_ptr_not_null(g);

    /* Left input: GROUP BY id, SUM(val) — produces 3 rows */
    td_op_t* key_l     = td_scan(g, "id");
    td_op_t* val_l     = td_scan(g, "val");
    td_op_t* keys_l[]  = { key_l };
    uint16_t agg_ops[] = { OP_SUM };
    td_op_t* agg_l[]   = { val_l };
    td_op_t* grp_l = td_group(g, keys_l, 1, agg_ops, agg_l, 1);
    munit_assert_ptr_not_null(grp_l);

    /* Right input: GROUP BY id, SUM(val) — produces 3 rows (same data) */
    td_op_t* key_r    = td_scan(g, "id");
    td_op_t* val_r    = td_scan(g, "val");
    td_op_t* keys_r[] = { key_r };
    td_op_t* agg_r[]  = { val_r };
    td_op_t* grp_r = td_group(g, keys_r, 1, agg_ops, agg_r, 1);
    munit_assert_ptr_not_null(grp_r);

    /* Union them */
    td_op_t* u = td_union_all(g, grp_l, grp_r);
    munit_assert_ptr_not_null(u);
    munit_assert_int(u->opcode, ==, OP_UNION_ALL);

    /* Execute */
    td_t* out = td_execute(g, u);
    munit_assert_ptr_not_null(out);
    munit_assert_false(TD_IS_ERR(out));
    munit_assert_int(out->type, ==, TD_TABLE);

    /* 3 rows from left + 3 rows from right = 6 total */
    munit_assert_int(td_table_nrows(out), ==, 6);
    munit_assert_int(td_table_ncols(out), ==, 2);

    td_release(out);
    td_graph_free(g);
    td_release(tbl);
    td_sym_destroy();
    td_heap_destroy();
    return MUNIT_OK;
}

/* --------------------------------------------------------------------------
 * Suite
 * -------------------------------------------------------------------------- */

static MunitTest tests[] = {
    { "/union_all", test_union_all, NULL, NULL, 0, NULL },
    { NULL, NULL, NULL, NULL, 0, NULL }
};

MunitSuite test_union_all_suite = {
    "/union_all", tests, NULL, 1, 0
};
