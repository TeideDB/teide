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
#include <string.h>

/* --------------------------------------------------------------------------
 * Helper: create a simple graph with edges
 *
 *   0 -> 1, 0 -> 2, 1 -> 2, 1 -> 3, 2 -> 3, 3 -> 0
 *   (6 edges, 4 nodes — a cycle)
 * -------------------------------------------------------------------------- */

static td_t* make_edge_table(void) {
    int64_t src_data[] = {0, 0, 1, 1, 2, 3};
    int64_t dst_data[] = {1, 2, 2, 3, 3, 0};
    int64_t n = 6;

    td_t* src_vec = td_vec_from_raw(TD_I64, src_data, n);
    td_t* dst_vec = td_vec_from_raw(TD_I64, dst_data, n);

    int64_t src_sym = td_sym_intern("src", 3);
    int64_t dst_sym = td_sym_intern("dst", 3);

    td_t* tbl = td_table_new(2);
    tbl = td_table_add_col(tbl, src_sym, src_vec);
    tbl = td_table_add_col(tbl, dst_sym, dst_vec);

    td_release(src_vec);
    td_release(dst_vec);
    return tbl;
}

/* --------------------------------------------------------------------------
 * Test: CSR build from edges
 * -------------------------------------------------------------------------- */

static MunitResult test_csr_build(const void* params, void* data) {
    (void)params; (void)data;
    td_heap_init();
    td_sym_init();

    td_t* edges = make_edge_table();
    munit_assert_ptr_not_null(edges);

    td_rel_t* rel = td_rel_from_edges(edges, "src", "dst", 4, 4, false);
    munit_assert_ptr_not_null(rel);

    /* Forward CSR: check degrees */
    munit_assert(rel->fwd.n_nodes == 4);
    munit_assert(rel->fwd.n_edges == 6);
    munit_assert(td_csr_degree(&rel->fwd, 0) == 2);  /* 0->1, 0->2 */
    munit_assert(td_csr_degree(&rel->fwd, 1) == 2);  /* 1->2, 1->3 */
    munit_assert(td_csr_degree(&rel->fwd, 2) == 1);  /* 2->3 */
    munit_assert(td_csr_degree(&rel->fwd, 3) == 1);  /* 3->0 */

    /* Check neighbors of node 0 */
    int64_t cnt;
    int64_t* nbrs = td_csr_neighbors(&rel->fwd, 0, &cnt);
    munit_assert(cnt == 2);
    /* Neighbors should be 1 and 2 (order may vary) */
    munit_assert(nbrs[0] == 1 || nbrs[0] == 2);
    munit_assert(nbrs[1] == 1 || nbrs[1] == 2);
    munit_assert(nbrs[0] != nbrs[1]);

    /* Reverse CSR */
    munit_assert(rel->rev.n_nodes == 4);
    munit_assert(rel->rev.n_edges == 6);
    munit_assert(td_csr_degree(&rel->rev, 0) == 1);  /* 3->0 */
    munit_assert(td_csr_degree(&rel->rev, 1) == 1);  /* 0->1 */
    munit_assert(td_csr_degree(&rel->rev, 2) == 2);  /* 0->2, 1->2 */
    munit_assert(td_csr_degree(&rel->rev, 3) == 2);  /* 1->3, 2->3 */

    td_rel_free(rel);
    td_release(edges);
    td_sym_destroy();
    td_heap_destroy();
    return MUNIT_OK;
}

/* --------------------------------------------------------------------------
 * Test: CSR sorted (for LFTJ)
 * -------------------------------------------------------------------------- */

static MunitResult test_csr_sorted(const void* params, void* data) {
    (void)params; (void)data;
    td_heap_init();
    td_sym_init();

    td_t* edges = make_edge_table();
    td_rel_t* rel = td_rel_from_edges(edges, "src", "dst", 4, 4, true);
    munit_assert_ptr_not_null(rel);
    munit_assert_true(rel->fwd.sorted);
    munit_assert_true(rel->rev.sorted);

    /* Check sorted adjacency list for node 0 (fwd) */
    int64_t cnt;
    int64_t* nbrs = td_csr_neighbors(&rel->fwd, 0, &cnt);
    munit_assert(cnt == 2);
    munit_assert(nbrs[0] == 1);
    munit_assert(nbrs[1] == 2);

    td_rel_free(rel);
    td_release(edges);
    td_sym_destroy();
    td_heap_destroy();
    return MUNIT_OK;
}

/* --------------------------------------------------------------------------
 * Test: OP_EXPAND (1-hop forward)
 * -------------------------------------------------------------------------- */

static MunitResult test_expand(const void* params, void* data) {
    (void)params; (void)data;
    td_heap_init();
    td_sym_init();

    td_t* edges = make_edge_table();
    td_rel_t* rel = td_rel_from_edges(edges, "src", "dst", 4, 4, false);
    munit_assert_ptr_not_null(rel);

    /* Expand from nodes {0, 1} forward */
    int64_t start_data[] = {0, 1};
    td_t* start_vec = td_vec_from_raw(TD_I64, start_data, 2);

    td_graph_t* g = td_graph_new(NULL);
    munit_assert_ptr_not_null(g);

    td_op_t* src = td_const_vec(g, start_vec);
    td_op_t* expand = td_expand(g, src, rel, 0);
    munit_assert_ptr_not_null(expand);

    td_t* result = td_execute(g, expand);
    munit_assert_false(TD_IS_ERR(result));
    munit_assert_int(result->type, ==, TD_TABLE);

    /* Node 0 has 2 outgoing, node 1 has 2 outgoing = 4 total */
    munit_assert(td_table_nrows(result) == 4);

    td_release(result);
    td_graph_free(g);
    td_release(start_vec);
    td_rel_free(rel);
    td_release(edges);
    td_sym_destroy();
    td_heap_destroy();
    return MUNIT_OK;
}

/* --------------------------------------------------------------------------
 * Test: OP_EXPAND (reverse)
 * -------------------------------------------------------------------------- */

static MunitResult test_expand_reverse(const void* params, void* data) {
    (void)params; (void)data;
    td_heap_init();
    td_sym_init();

    td_t* edges = make_edge_table();
    td_rel_t* rel = td_rel_from_edges(edges, "src", "dst", 4, 4, false);

    /* Expand from node 3 reverse — should find nodes pointing TO 3: {1, 2} */
    int64_t start_data[] = {3};
    td_t* start_vec = td_vec_from_raw(TD_I64, start_data, 1);

    td_graph_t* g = td_graph_new(NULL);
    td_op_t* src = td_const_vec(g, start_vec);
    td_op_t* expand = td_expand(g, src, rel, 1);  /* direction=1: reverse */

    td_t* result = td_execute(g, expand);
    munit_assert_false(TD_IS_ERR(result));
    munit_assert_int(result->type, ==, TD_TABLE);
    munit_assert(td_table_nrows(result) == 2);  /* 1->3, 2->3 */

    td_release(result);
    td_graph_free(g);
    td_release(start_vec);
    td_rel_free(rel);
    td_release(edges);
    td_sym_destroy();
    td_heap_destroy();
    return MUNIT_OK;
}

/* --------------------------------------------------------------------------
 * Test: OP_VAR_EXPAND (variable-length BFS)
 * -------------------------------------------------------------------------- */

static MunitResult test_var_expand(const void* params, void* data) {
    (void)params; (void)data;
    td_heap_init();
    td_sym_init();

    td_t* edges = make_edge_table();
    td_rel_t* rel = td_rel_from_edges(edges, "src", "dst", 4, 4, false);

    int64_t start_data[] = {0};
    td_t* start_vec = td_vec_from_raw(TD_I64, start_data, 1);

    td_graph_t* g = td_graph_new(NULL);
    td_op_t* src = td_const_vec(g, start_vec);
    td_op_t* var_exp = td_var_expand(g, src, rel, 0, 1, 3, false);

    td_t* result = td_execute(g, var_exp);
    munit_assert_false(TD_IS_ERR(result));
    munit_assert_int(result->type, ==, TD_TABLE);

    /* From node 0 with depth 1..3:
     * depth 1: 0->1, 0->2 (2 results)
     * depth 2: 1->3, 2->3 (but 3 visited only once) => 1 result
     * depth 3: 3->0 (but 0 already visited) => no results
     * Total reachable: nodes 1, 2, 3 at depths 1, 1, 2 = 3 results */
    munit_assert(td_table_nrows(result) == 3);

    td_release(result);
    td_graph_free(g);
    td_release(start_vec);
    td_rel_free(rel);
    td_release(edges);
    td_sym_destroy();
    td_heap_destroy();
    return MUNIT_OK;
}

/* --------------------------------------------------------------------------
 * Test: OP_SHORTEST_PATH
 * -------------------------------------------------------------------------- */

static MunitResult test_shortest_path(const void* params, void* data) {
    (void)params; (void)data;
    td_heap_init();
    td_sym_init();

    td_t* edges = make_edge_table();
    td_rel_t* rel = td_rel_from_edges(edges, "src", "dst", 4, 4, false);

    td_graph_t* g = td_graph_new(NULL);
    td_op_t* src = td_const_i64(g, 0);
    td_op_t* dst = td_const_i64(g, 3);
    td_op_t* sp = td_shortest_path(g, src, dst, rel, 10);

    td_t* result = td_execute(g, sp);
    munit_assert_false(TD_IS_ERR(result));
    munit_assert_int(result->type, ==, TD_TABLE);

    /* Shortest path 0->3: 0->1->3 (length 3 nodes) or 0->2->3 */
    int64_t nrows = td_table_nrows(result);
    munit_assert(nrows == 3);  /* 3 nodes in path */

    /* First node should be 0, last should be 3 */
    int64_t node_sym = td_sym_intern("_node", 5);
    td_t* node_col = td_table_get_col(result, node_sym);
    munit_assert_ptr_not_null(node_col);
    int64_t* nodes = (int64_t*)td_data(node_col);
    munit_assert(nodes[0] == 0);
    munit_assert(nodes[nrows - 1] == 3);

    td_release(result);
    td_graph_free(g);
    td_rel_free(rel);
    td_release(edges);
    td_sym_destroy();
    td_heap_destroy();
    return MUNIT_OK;
}

/* --------------------------------------------------------------------------
 * Test: OP_SHORTEST_PATH (no path)
 * -------------------------------------------------------------------------- */

static MunitResult test_shortest_path_no_path(const void* params, void* data) {
    (void)params; (void)data;
    td_heap_init();
    td_sym_init();

    /* Build a graph with no path from 0 to 3: only 0->1 */
    int64_t src_data[] = {0};
    int64_t dst_data[] = {1};
    td_t* s = td_vec_from_raw(TD_I64, src_data, 1);
    td_t* d = td_vec_from_raw(TD_I64, dst_data, 1);
    int64_t src_sym = td_sym_intern("src", 3);
    int64_t dst_sym = td_sym_intern("dst", 3);
    td_t* edges = td_table_new(2);
    edges = td_table_add_col(edges, src_sym, s);
    edges = td_table_add_col(edges, dst_sym, d);
    td_release(s); td_release(d);

    td_rel_t* rel = td_rel_from_edges(edges, "src", "dst", 4, 4, false);

    td_graph_t* g = td_graph_new(NULL);
    td_op_t* src_op = td_const_i64(g, 0);
    td_op_t* dst_op = td_const_i64(g, 3);
    td_op_t* sp = td_shortest_path(g, src_op, dst_op, rel, 10);

    td_t* result = td_execute(g, sp);
    munit_assert_true(TD_IS_ERR(result));
    munit_assert_int(TD_ERR_CODE(result), ==, TD_ERR_RANGE);

    td_graph_free(g);
    td_rel_free(rel);
    td_release(edges);
    td_sym_destroy();
    td_heap_destroy();
    return MUNIT_OK;
}

/* --------------------------------------------------------------------------
 * Test: OP_WCO_JOIN (triangle enumeration)
 * -------------------------------------------------------------------------- */

static MunitResult test_wco_join_triangle(const void* params, void* data) {
    (void)params; (void)data;
    td_heap_init();
    td_sym_init();

    /* Complete graph K3: 0<->1, 1<->2, 0<->2 */
    int64_t src_data[] = {0, 0, 1, 1, 2, 2};
    int64_t dst_data[] = {1, 2, 0, 2, 0, 1};
    int64_t n = 6;

    td_t* sv = td_vec_from_raw(TD_I64, src_data, n);
    td_t* dv = td_vec_from_raw(TD_I64, dst_data, n);
    int64_t src_sym = td_sym_intern("src", 3);
    int64_t dst_sym = td_sym_intern("dst", 3);
    td_t* edges = td_table_new(2);
    edges = td_table_add_col(edges, src_sym, sv);
    edges = td_table_add_col(edges, dst_sym, dv);
    td_release(sv); td_release(dv);

    /* Build with sorted=true (required for LFTJ) */
    td_rel_t* rel = td_rel_from_edges(edges, "src", "dst", 3, 3, true);
    munit_assert_ptr_not_null(rel);
    munit_assert_true(rel->fwd.sorted);

    /* Triangle pattern: 3 vars, 3 rels (all same rel for K3) */
    td_rel_t* rels[3] = {rel, rel, rel};

    td_graph_t* g = td_graph_new(NULL);
    td_op_t* wco = td_wco_join(g, rels, 3, 3);
    munit_assert_ptr_not_null(wco);

    td_t* result = td_execute(g, wco);
    munit_assert_false(TD_IS_ERR(result));
    munit_assert_int(result->type, ==, TD_TABLE);

    /* K3 has directed triangles */
    munit_assert(td_table_nrows(result) >= 1);

    td_release(result);
    td_graph_free(g);
    td_rel_free(rel);
    td_release(edges);
    td_sym_destroy();
    td_heap_destroy();
    return MUNIT_OK;
}

/* --------------------------------------------------------------------------
 * Test: Multi-table graph (td_graph_add_table + td_scan_table)
 * -------------------------------------------------------------------------- */

static MunitResult test_multi_table(const void* params, void* data) {
    (void)params; (void)data;
    td_heap_init();
    td_sym_init();

    /* Table 1: persons */
    int64_t ages[] = {25, 30, 35, 40};
    td_t* age_vec = td_vec_from_raw(TD_I64, ages, 4);
    int64_t age_sym = td_sym_intern("age", 3);
    td_t* persons = td_table_new(1);
    persons = td_table_add_col(persons, age_sym, age_vec);
    td_release(age_vec);

    /* Table 2: tasks */
    int64_t priorities[] = {1, 2, 3};
    td_t* prio_vec = td_vec_from_raw(TD_I64, priorities, 3);
    int64_t prio_sym = td_sym_intern("priority", 8);
    td_t* tasks = td_table_new(1);
    tasks = td_table_add_col(tasks, prio_sym, prio_vec);
    td_release(prio_vec);

    td_graph_t* g = td_graph_new(persons);  /* primary table */
    uint16_t tasks_id = td_graph_add_table(g, tasks);
    munit_assert(tasks_id == 0);

    /* Scan from persons (primary) */
    td_op_t* age_scan = td_scan(g, "age");
    munit_assert_ptr_not_null(age_scan);

    /* Scan from tasks (registered table) */
    td_op_t* prio_scan = td_scan_table(g, tasks_id, "priority");
    munit_assert_ptr_not_null(prio_scan);

    /* Execute scans */
    td_t* age_result = td_execute(g, age_scan);
    munit_assert_false(TD_IS_ERR(age_result));
    munit_assert(age_result->len == 4);
    td_release(age_result);

    td_t* prio_result = td_execute(g, prio_scan);
    munit_assert_false(TD_IS_ERR(prio_result));
    munit_assert(prio_result->len == 3);
    td_release(prio_result);

    td_graph_free(g);
    td_release(persons);
    td_release(tasks);
    td_sym_destroy();
    td_heap_destroy();
    return MUNIT_OK;
}

/* --------------------------------------------------------------------------
 * Test: OP_WCO_JOIN (chain pattern: 3 vars, 2 rels — general LFTJ)
 * -------------------------------------------------------------------------- */

static MunitResult test_wco_join_chain(const void* params, void* data) {
    (void)params; (void)data;
    td_heap_init();
    td_sym_init();

    /* Graph: 0->1, 0->2, 1->2, 1->3, 2->3 (directed, no back edges) */
    int64_t src_data[] = {0, 0, 1, 1, 2};
    int64_t dst_data[] = {1, 2, 2, 3, 3};
    int64_t n = 5;

    td_t* sv = td_vec_from_raw(TD_I64, src_data, n);
    td_t* dv = td_vec_from_raw(TD_I64, dst_data, n);
    int64_t src_sym = td_sym_intern("src", 3);
    int64_t dst_sym = td_sym_intern("dst", 3);
    td_t* edges = td_table_new(2);
    edges = td_table_add_col(edges, src_sym, sv);
    edges = td_table_add_col(edges, dst_sym, dv);
    td_release(sv); td_release(dv);

    td_rel_t* rel = td_rel_from_edges(edges, "src", "dst", 4, 4, true);
    munit_assert_ptr_not_null(rel);

    /* Chain pattern: a->b->c with n_vars=3, n_rels=2
     * rels[0]: a->b, rels[1]: b->c (fallback chain pattern in LFTJ) */
    td_rel_t* rels[2] = {rel, rel};

    td_graph_t* g = td_graph_new(NULL);
    td_op_t* wco = td_wco_join(g, rels, 2, 3);
    munit_assert_ptr_not_null(wco);

    td_t* result = td_execute(g, wco);
    munit_assert_false(TD_IS_ERR(result));
    munit_assert_int(result->type, ==, TD_TABLE);

    /* 2-hop paths: (0,1,2), (0,1,3), (0,2,3), (1,2,3) = 4 paths */
    int64_t nrows = td_table_nrows(result);
    munit_assert(nrows == 4);

    /* Verify we have 3 columns: _v0, _v1, _v2 */
    munit_assert(td_table_ncols(result) == 3);

    td_release(result);
    td_graph_free(g);
    td_rel_free(rel);
    td_release(edges);
    td_sym_destroy();
    td_heap_destroy();
    return MUNIT_OK;
}

/* --------------------------------------------------------------------------
 * Test: Factorized expand (degree counting)
 * -------------------------------------------------------------------------- */

static MunitResult test_expand_factorized(const void* params, void* data) {
    (void)params; (void)data;
    td_heap_init();
    td_sym_init();

    td_t* edges = make_edge_table();
    td_rel_t* rel = td_rel_from_edges(edges, "src", "dst", 4, 4, false);
    munit_assert_ptr_not_null(rel);

    /* Expand nodes {0, 1, 2} forward — factorized should give degree counts */
    int64_t start_data[] = {0, 1, 2};
    td_t* start_vec = td_vec_from_raw(TD_I64, start_data, 3);

    td_graph_t* g = td_graph_new(NULL);
    td_op_t* src = td_const_vec(g, start_vec);
    td_op_t* expand = td_expand(g, src, rel, 0);
    munit_assert_ptr_not_null(expand);

    /* Manually set factorized flag (normally done by optimizer) */
    td_op_ext_t* ext = NULL;
    for (uint32_t i = 0; i < g->ext_count; i++) {
        if (g->ext_nodes[i] && g->ext_nodes[i]->base.id == expand->id) {
            ext = g->ext_nodes[i];
            break;
        }
    }
    munit_assert_ptr_not_null(ext);
    ext->graph.factorized = 1;

    td_t* result = td_execute(g, expand);
    munit_assert_false(TD_IS_ERR(result));
    munit_assert_int(result->type, ==, TD_TABLE);

    /* Should have 2 columns: _src and _count */
    munit_assert(td_table_ncols(result) == 2);

    int64_t cnt_sym = td_sym_intern("_count", 6);
    td_t* cnt_col = td_table_get_col(result, cnt_sym);
    munit_assert_ptr_not_null(cnt_col);

    /* Degrees: node 0=2, node 1=2, node 2=1 */
    int64_t* counts = (int64_t*)td_data(cnt_col);
    int64_t total_deg = 0;
    for (int64_t i = 0; i < cnt_col->len; i++)
        total_deg += counts[i];
    munit_assert(total_deg == 5);  /* 2 + 2 + 1 = 5 */

    td_release(result);
    td_graph_free(g);
    td_release(start_vec);
    td_rel_free(rel);
    td_release(edges);
    td_sym_destroy();
    td_heap_destroy();
    return MUNIT_OK;
}

/* --------------------------------------------------------------------------
 * Test: SIP expand (source-side selection skip)
 * -------------------------------------------------------------------------- */

static MunitResult test_sip_expand(const void* params, void* data) {
    (void)params; (void)data;
    td_heap_init();
    td_sym_init();

    td_t* edges = make_edge_table();
    td_rel_t* rel = td_rel_from_edges(edges, "src", "dst", 4, 4, false);
    munit_assert_ptr_not_null(rel);

    /* Create source-side selection: only allow node 0 (skip nodes 1, 2, 3) */
    td_t* src_sel = td_sel_new(4);
    munit_assert_ptr_not_null(src_sel);
    munit_assert_false(TD_IS_ERR(src_sel));
    uint64_t* sel_bits = td_sel_bits(src_sel);
    TD_SEL_BIT_SET(sel_bits, 0);  /* only node 0 passes */

    /* Expand from nodes {0, 1, 2} forward — but SIP should skip 1, 2 */
    int64_t start_data[] = {0, 1, 2};
    td_t* start_vec = td_vec_from_raw(TD_I64, start_data, 3);

    td_graph_t* g = td_graph_new(NULL);

    td_op_t* src = td_const_vec(g, start_vec);
    td_op_t* expand = td_expand(g, src, rel, 0);

    /* Attach SIP selection to the expand ext node */
    for (uint32_t i = 0; i < g->ext_count; i++) {
        if (g->ext_nodes[i] && g->ext_nodes[i]->base.id == expand->id) {
            g->ext_nodes[i]->graph.sip_sel = src_sel;
            break;
        }
    }

    td_t* result = td_execute(g, expand);
    munit_assert_false(TD_IS_ERR(result));
    munit_assert_int(result->type, ==, TD_TABLE);

    /* Only node 0 should be expanded: degree 2 → 2 output rows */
    munit_assert(td_table_nrows(result) == 2);

    td_release(result);
    td_graph_free(g);
    td_release(src_sel);
    td_release(start_vec);
    td_rel_free(rel);
    td_release(edges);
    td_sym_destroy();
    td_heap_destroy();
    return MUNIT_OK;
}

/* --------------------------------------------------------------------------
 * Test: S-Join semijoin filter in exec_join
 * -------------------------------------------------------------------------- */

static MunitResult test_sjoin_filter(const void* params, void* data) {
    (void)params; (void)data;
    td_heap_init();
    td_sym_init();

    /* Left table: id column with many rows, most not in right side */
    int64_t n_left = 100;
    td_t* left_ids = td_vec_new(TD_I64, n_left);
    munit_assert_ptr_not_null(left_ids);
    munit_assert_false(TD_IS_ERR(left_ids));
    left_ids->len = n_left;
    int64_t* lid = (int64_t*)td_data(left_ids);
    for (int64_t i = 0; i < n_left; i++) lid[i] = i;

    td_t* left_vals = td_vec_new(TD_I64, n_left);
    left_vals->len = n_left;
    int64_t* lv = (int64_t*)td_data(left_vals);
    for (int64_t i = 0; i < n_left; i++) lv[i] = i * 10;

    int64_t id_sym = td_sym_intern("id", 2);
    int64_t val_sym = td_sym_intern("val", 3);
    td_t* left_tbl = td_table_new(2);
    left_tbl = td_table_add_col(left_tbl, id_sym, left_ids);
    left_tbl = td_table_add_col(left_tbl, val_sym, left_vals);
    td_release(left_ids); td_release(left_vals);

    /* Right table: small, only ids 5, 10, 15 */
    int64_t rids[] = {5, 10, 15};
    td_t* right_ids = td_vec_from_raw(TD_I64, rids, 3);
    int64_t rvals[] = {500, 1000, 1500};
    td_t* right_vals = td_vec_from_raw(TD_I64, rvals, 3);

    int64_t rval_sym = td_sym_intern("rval", 4);
    td_t* right_tbl = td_table_new(2);
    right_tbl = td_table_add_col(right_tbl, id_sym, right_ids);
    right_tbl = td_table_add_col(right_tbl, rval_sym, right_vals);
    td_release(right_ids); td_release(right_vals);

    /* Inner join on id — should trigger S-Join (100 > 3*2) */
    td_graph_t* g = td_graph_new(left_tbl);
    uint16_t right_id = td_graph_add_table(g, right_tbl);

    /* Build join: table-producing ops for left/right, key scans for join keys */
    td_op_t* left_tbl_op  = td_const_table(g, left_tbl);
    td_op_t* right_tbl_op = td_const_table(g, right_tbl);
    td_op_t* left_scan    = td_scan(g, "id");
    td_op_t* right_scan   = td_scan_table(g, right_id, "id");

    td_op_t* left_keys[1]  = { left_scan };
    td_op_t* right_keys[1] = { right_scan };

    td_op_t* join = td_join(g, left_tbl_op, left_keys, right_tbl_op, right_keys, 1, 0);
    munit_assert_ptr_not_null(join);

    td_t* result = td_execute(g, join);
    munit_assert_false(TD_IS_ERR(result));
    munit_assert_int(result->type, ==, TD_TABLE);

    /* Should match exactly 3 rows (ids 5, 10, 15) */
    munit_assert(td_table_nrows(result) == 3);

    td_release(result);
    td_graph_free(g);
    td_release(left_tbl);
    td_release(right_tbl);
    td_sym_destroy();
    td_heap_destroy();
    return MUNIT_OK;
}

/* --------------------------------------------------------------------------
 * Suite definition
 * -------------------------------------------------------------------------- */

static MunitTest csr_tests[] = {
    { "/build",            test_csr_build,            NULL, NULL, 0, NULL },
    { "/sorted",           test_csr_sorted,           NULL, NULL, 0, NULL },
    { "/expand",           test_expand,               NULL, NULL, 0, NULL },
    { "/expand_reverse",   test_expand_reverse,       NULL, NULL, 0, NULL },
    { "/var_expand",       test_var_expand,            NULL, NULL, 0, NULL },
    { "/shortest_path",    test_shortest_path,         NULL, NULL, 0, NULL },
    { "/shortest_path_no", test_shortest_path_no_path, NULL, NULL, 0, NULL },
    { "/wco_join",         test_wco_join_triangle,     NULL, NULL, 0, NULL },
    { "/wco_chain",        test_wco_join_chain,        NULL, NULL, 0, NULL },
    { "/factorized",       test_expand_factorized,     NULL, NULL, 0, NULL },
    { "/sip_expand",       test_sip_expand,            NULL, NULL, 0, NULL },
    { "/sjoin",            test_sjoin_filter,           NULL, NULL, 0, NULL },
    { "/multi_table",      test_multi_table,           NULL, NULL, 0, NULL },
    { NULL, NULL, NULL, NULL, 0, NULL },  /* terminator */
};

MunitSuite test_csr_suite = {
    "/csr",          /* prefix */
    csr_tests,       /* tests */
    NULL,            /* suites */
    1,               /* iterations */
    0,               /* options */
};
