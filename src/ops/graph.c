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

#include "graph.h"
#include "store/csr.h"
#include "mem/sys.h"
#include <string.h>

/* --------------------------------------------------------------------------
 * Graph allocation helpers
 * -------------------------------------------------------------------------- */

#define GRAPH_INIT_CAP 4096

static inline td_op_t* graph_fix_ptr(td_op_t* p, ptrdiff_t delta) {
    return p ? (td_op_t*)((char*)p + delta) : NULL;
}

static void graph_fixup_ext_ptrs(td_graph_t* g, ptrdiff_t delta) {
    for (uint32_t i = 0; i < g->ext_count; i++) {
        td_op_ext_t* ext = g->ext_nodes[i];
        if (!ext) continue;

        ext->base.inputs[0] = graph_fix_ptr(ext->base.inputs[0], delta);
        ext->base.inputs[1] = graph_fix_ptr(ext->base.inputs[1], delta);

        switch (ext->base.opcode) {
            case OP_SORT:
                for (uint8_t k = 0; k < ext->sort.n_cols; k++)
                    ext->sort.columns[k] = graph_fix_ptr(ext->sort.columns[k], delta);
                break;
            case OP_GROUP:
                for (uint8_t k = 0; k < ext->n_keys; k++)
                    ext->keys[k] = graph_fix_ptr(ext->keys[k], delta);
                for (uint8_t a = 0; a < ext->n_aggs; a++)
                    ext->agg_ins[a] = graph_fix_ptr(ext->agg_ins[a], delta);
                break;
            case OP_JOIN:
            case OP_WINDOW_JOIN:
                for (uint8_t k = 0; k < ext->join.n_join_keys; k++)
                    ext->join.left_keys[k] = graph_fix_ptr(ext->join.left_keys[k], delta);
                if (ext->join.right_keys) {
                    for (uint8_t k = 0; k < ext->join.n_join_keys; k++)
                        ext->join.right_keys[k] = graph_fix_ptr(ext->join.right_keys[k], delta);
                }
                break;
            case OP_WINDOW:
                for (uint8_t k = 0; k < ext->window.n_part_keys; k++)
                    ext->window.part_keys[k] = graph_fix_ptr(ext->window.part_keys[k], delta);
                for (uint8_t k = 0; k < ext->window.n_order_keys; k++)
                    ext->window.order_keys[k] = graph_fix_ptr(ext->window.order_keys[k], delta);
                for (uint8_t f = 0; f < ext->window.n_funcs; f++)
                    ext->window.func_inputs[f] = graph_fix_ptr(ext->window.func_inputs[f], delta);
                break;
            /* M5: PROJECT/SELECT intentionally alias sort union for column
               storage — same pointer layout (columns[], n_cols). */
            case OP_PROJECT:
            case OP_SELECT:
                for (uint8_t k = 0; k < ext->sort.n_cols; k++)
                    ext->sort.columns[k] = graph_fix_ptr(ext->sort.columns[k], delta);
                break;
            /* Graph ops: no td_op_t* pointers in ext union to fix */
            case OP_EXPAND:
            case OP_VAR_EXPAND:
            case OP_SHORTEST_PATH:
            case OP_WCO_JOIN:
                break;
            default:
                break;
        }
    }
}

/* After realloc moves g->nodes, fix up all stored input pointers.
   old_base is saved as uintptr_t before realloc to avoid GCC 14
   -Wuse-after-free on the stale pointer. */
static void graph_fixup_ptrs(td_graph_t* g, uintptr_t old_base) {
    ptrdiff_t delta = (ptrdiff_t)((uintptr_t)g->nodes - old_base);
    if (delta == 0) return;
    for (uint32_t i = 0; i < g->node_count; i++) {
        g->nodes[i].inputs[0] = graph_fix_ptr(g->nodes[i].inputs[0], delta);
        g->nodes[i].inputs[1] = graph_fix_ptr(g->nodes[i].inputs[1], delta);
    }
    graph_fixup_ext_ptrs(g, delta);
}

/* L3: node_count is uint32_t — theoretical overflow at 2^32 nodes is
   unreachable in practice (would require ~128 GB for the nodes array). */
static td_op_t* graph_alloc_node(td_graph_t* g) {
    if (g->node_count >= g->node_cap) {
        uintptr_t old_base = (uintptr_t)g->nodes;
        /* H2: Overflow guard — if node_cap is already > UINT32_MAX/2,
           doubling would wrap around to a smaller value. */
        if (g->node_cap > UINT32_MAX / 2) return NULL;
        uint32_t new_cap = g->node_cap * 2;
        td_op_t* new_nodes = (td_op_t*)td_sys_realloc(g->nodes,
                                                      new_cap * sizeof(td_op_t));
        if (!new_nodes) return NULL;
        g->nodes = new_nodes;
        g->node_cap = new_cap;
        graph_fixup_ptrs(g, old_base);
    }
    td_op_t* n = &g->nodes[g->node_count];
    memset(n, 0, sizeof(td_op_t));
    n->id = g->node_count;
    g->node_count++;
    return n;
}

static td_op_ext_t* graph_alloc_ext_node_ex(td_graph_t* g, size_t extra) {
    /* Extended nodes are 64 bytes; extra bytes appended for inline arrays */
    td_op_ext_t* ext = (td_op_ext_t*)td_sys_alloc(sizeof(td_op_ext_t) + extra);
    if (!ext) return NULL;
    memset(ext, 0, sizeof(td_op_ext_t) + extra);

    /* Also add a placeholder in the nodes array for ID tracking */
    if (g->node_count >= g->node_cap) {
        if (g->node_cap > UINT32_MAX / 2) { td_sys_free(ext); return NULL; }
        uintptr_t old_base = (uintptr_t)g->nodes;
        uint32_t new_cap = g->node_cap * 2;
        td_op_t* new_nodes = (td_op_t*)td_sys_realloc(g->nodes,
                                                      new_cap * sizeof(td_op_t));
        if (!new_nodes) { td_sys_free(ext); return NULL; }
        g->nodes = new_nodes;
        g->node_cap = new_cap;
        graph_fixup_ptrs(g, old_base);
    }
    ext->base.id = g->node_count;
    /* H4: Do NOT copy ext->base to nodes[] here — the caller fills in
       fields first and then syncs via g->nodes[ext->base.id] = ext->base. */
    memset(&g->nodes[g->node_count], 0, sizeof(td_op_t));
    g->nodes[g->node_count].id = g->node_count;
    g->node_count++;

    /* Track ext node for cleanup */
    if (g->ext_count >= g->ext_cap) {
        if (g->ext_cap > UINT32_MAX / 2) { g->node_count--; td_sys_free(ext); return NULL; }
        uint32_t new_cap = g->ext_cap == 0 ? 16 : g->ext_cap * 2;
        td_op_ext_t** new_exts = (td_op_ext_t**)td_sys_realloc(g->ext_nodes,
                                                               new_cap * sizeof(td_op_ext_t*));
        if (!new_exts) { g->node_count--; td_sys_free(ext); return NULL; }
        g->ext_nodes = new_exts;
        g->ext_cap = new_cap;
    }
    g->ext_nodes[g->ext_count++] = ext;

    return ext;
}

static td_op_ext_t* graph_alloc_ext_node(td_graph_t* g) {
    return graph_alloc_ext_node_ex(g, 0);
}

/* Pointer to trailing bytes after the ext node */
#define EXT_TRAIL(ext) ((char*)((ext) + 1))

/* --------------------------------------------------------------------------
 * td_graph_new / td_graph_free
 * -------------------------------------------------------------------------- */

td_graph_t* td_graph_new(td_t* tbl) {
    td_graph_t* g = (td_graph_t*)td_sys_alloc(sizeof(td_graph_t));
    if (!g) return NULL;

    g->nodes = (td_op_t*)td_sys_alloc(GRAPH_INIT_CAP * sizeof(td_op_t));
    if (!g->nodes) { td_sys_free(g); return NULL; }
    g->node_cap = GRAPH_INIT_CAP;
    g->node_count = 0;
    g->table = tbl;
    if (tbl) td_retain(tbl);

    g->tables = NULL;
    g->n_tables = 0;

    g->ext_nodes = NULL;
    g->ext_count = 0;
    g->ext_cap = 0;
    g->selection = NULL;

    return g;
}

void td_graph_free(td_graph_t* g) {
    if (!g) return;

    /* M6: Release OP_CONST literal values before freeing ext nodes */
    for (uint32_t i = 0; i < g->ext_count; i++) {
        td_op_ext_t* ext = g->ext_nodes[i];
        if (ext && g->nodes[ext->base.id].opcode == OP_CONST && ext->literal) {
            td_release(ext->literal);
        }
        /* Release runtime-built SIP bitmaps on graph traversal nodes */
        if (ext) {
            uint16_t oc = g->nodes[ext->base.id].opcode;
            if ((oc == OP_EXPAND || oc == OP_VAR_EXPAND || oc == OP_SHORTEST_PATH)
                && ext->graph.sip_sel) {
                td_release((td_t*)ext->graph.sip_sel);
            }
        }
    }
    /* Free extended nodes */
    for (uint32_t i = 0; i < g->ext_count; i++) {
        td_sys_free(g->ext_nodes[i]);
    }
    td_sys_free(g->ext_nodes);

    td_sys_free(g->nodes);
    if (g->table) td_release(g->table);

    /* Release table registry */
    if (g->tables) {
        for (uint16_t i = 0; i < g->n_tables; i++) {
            if (g->tables[i]) td_release(g->tables[i]);
        }
        td_sys_free(g->tables);
    }

    if (g->selection) td_release(g->selection);
    td_sys_free(g);
}

/* --------------------------------------------------------------------------
 * Source ops
 * -------------------------------------------------------------------------- */

td_op_t* td_scan(td_graph_t* g, const char* col_name) {
    td_op_ext_t* ext = graph_alloc_ext_node(g);
    if (!ext) return NULL;

    ext->base.opcode = OP_SCAN;
    ext->base.arity = 0;

    /* Intern the column name to get symbol ID */
    int64_t sym_id = td_sym_intern(col_name, strlen(col_name));
    ext->sym = sym_id;

    /* Infer output type from the bound table */
    if (g->table) {
        td_t* col = td_table_get_col(g->table, sym_id);
        if (col) {
            ext->base.out_type = col->type;
            ext->base.est_rows = (uint32_t)col->len;
        }
    }

    /* Update the nodes array with the filled base */
    g->nodes[ext->base.id] = ext->base;
    return &g->nodes[ext->base.id];
}

td_op_t* td_const_f64(td_graph_t* g, double val) {
    td_op_ext_t* ext = graph_alloc_ext_node(g);
    if (!ext) return NULL;

    ext->base.opcode = OP_CONST;
    ext->base.arity = 0;
    ext->base.out_type = TD_F64;
    ext->literal = td_f64(val);
    /* L4: null/error check on allocation result */
    if (!ext->literal || TD_IS_ERR(ext->literal)) ext->literal = NULL;

    g->nodes[ext->base.id] = ext->base;
    return &g->nodes[ext->base.id];
}

td_op_t* td_const_i64(td_graph_t* g, int64_t val) {
    td_op_ext_t* ext = graph_alloc_ext_node(g);
    if (!ext) return NULL;

    ext->base.opcode = OP_CONST;
    ext->base.arity = 0;
    ext->base.out_type = TD_I64;
    ext->literal = td_i64(val);
    /* L4: null/error check on allocation result */
    if (!ext->literal || TD_IS_ERR(ext->literal)) ext->literal = NULL;

    g->nodes[ext->base.id] = ext->base;
    return &g->nodes[ext->base.id];
}

td_op_t* td_const_bool(td_graph_t* g, bool val) {
    td_op_ext_t* ext = graph_alloc_ext_node(g);
    if (!ext) return NULL;

    ext->base.opcode = OP_CONST;
    ext->base.arity = 0;
    ext->base.out_type = TD_BOOL;
    ext->literal = td_bool(val);
    /* L4: null/error check on allocation result */
    if (!ext->literal || TD_IS_ERR(ext->literal)) ext->literal = NULL;

    g->nodes[ext->base.id] = ext->base;
    return &g->nodes[ext->base.id];
}

td_op_t* td_const_str(td_graph_t* g, const char* s) {
    td_op_ext_t* ext = graph_alloc_ext_node(g);
    if (!ext) return NULL;

    ext->base.opcode = OP_CONST;
    ext->base.arity = 0;
    ext->base.out_type = TD_SYM;   /* string constants resolve to SYM at exec time */
    ext->literal = td_str(s, strlen(s));
    /* L4: null/error check on allocation result */
    if (!ext->literal || TD_IS_ERR(ext->literal)) ext->literal = NULL;

    g->nodes[ext->base.id] = ext->base;
    return &g->nodes[ext->base.id];
}

td_op_t* td_const_vec(td_graph_t* g, td_t* vec) {
    td_op_ext_t* ext = graph_alloc_ext_node(g);
    if (!ext) return NULL;

    ext->base.opcode = OP_CONST;
    ext->base.arity = 0;
    ext->base.out_type = vec->type;
    ext->base.est_rows = (uint32_t)vec->len;
    ext->literal = vec;
    td_retain(vec);

    g->nodes[ext->base.id] = ext->base;
    return &g->nodes[ext->base.id];
}

td_op_t* td_const_table(td_graph_t* g, td_t* tbl) {
    td_op_ext_t* ext = graph_alloc_ext_node(g);
    if (!ext) return NULL;

    ext->base.opcode = OP_CONST;
    ext->base.arity = 0;
    ext->base.out_type = TD_TABLE;
    ext->literal = tbl;
    td_retain(tbl);

    g->nodes[ext->base.id] = ext->base;
    return &g->nodes[ext->base.id];
}

/* --------------------------------------------------------------------------
 * Helper: create unary/binary node
 * -------------------------------------------------------------------------- */

static td_op_t* make_unary(td_graph_t* g, uint16_t opcode, td_op_t* a, int8_t out_type) {
    /* Save ID before alloc — realloc may invalidate the pointer */
    uint32_t a_id = a->id;
    uint32_t est = a->est_rows;
    td_op_t* n = graph_alloc_node(g);
    if (!n) return NULL;
    a = &g->nodes[a_id];  /* re-resolve after potential realloc */

    n->opcode = opcode;
    n->arity = 1;
    n->inputs[0] = a;
    n->out_type = out_type;
    n->est_rows = est;
    return n;
}

static td_op_t* make_binary(td_graph_t* g, uint16_t opcode, td_op_t* a, td_op_t* b, int8_t out_type) {
    /* Save IDs before alloc — realloc may invalidate the pointers */
    uint32_t a_id = a->id;
    uint32_t b_id = b->id;
    uint32_t est = a->est_rows > b->est_rows ? a->est_rows : b->est_rows;
    td_op_t* n = graph_alloc_node(g);
    if (!n) return NULL;
    a = &g->nodes[a_id];  /* re-resolve after potential realloc */
    b = &g->nodes[b_id];

    n->opcode = opcode;
    n->arity = 2;
    n->inputs[0] = a;
    n->inputs[1] = b;
    n->out_type = out_type;
    n->est_rows = est;
    return n;
}

/* Type promotion: BOOL < U8 < I16 < I32 < I64 < F64 */
static int8_t promote(int8_t a, int8_t b) {
    if (a == TD_F64 || b == TD_F64) return TD_F64;
    if (a == TD_I64 || b == TD_I64 || a == TD_SYM || b == TD_SYM ||
        a == TD_TIMESTAMP || b == TD_TIMESTAMP) return TD_I64;
    if (a == TD_I32 || b == TD_I32 ||
        a == TD_DATE || b == TD_DATE || a == TD_TIME || b == TD_TIME) return TD_I32;
    if (a == TD_I16 || b == TD_I16) return TD_I16;
    if (a == TD_U8 || b == TD_U8) return TD_U8;
    return TD_BOOL;
}

/* --------------------------------------------------------------------------
 * Unary element-wise ops
 * -------------------------------------------------------------------------- */

td_op_t* td_neg(td_graph_t* g, td_op_t* a)     { return make_unary(g, OP_NEG, a, a->out_type); }
td_op_t* td_abs(td_graph_t* g, td_op_t* a)     { return make_unary(g, OP_ABS, a, a->out_type); }
td_op_t* td_not(td_graph_t* g, td_op_t* a)     { return make_unary(g, OP_NOT, a, TD_BOOL); }
td_op_t* td_sqrt_op(td_graph_t* g, td_op_t* a) { return make_unary(g, OP_SQRT, a, TD_F64); }
td_op_t* td_log_op(td_graph_t* g, td_op_t* a)  { return make_unary(g, OP_LOG, a, TD_F64); }
td_op_t* td_exp_op(td_graph_t* g, td_op_t* a)  { return make_unary(g, OP_EXP, a, TD_F64); }
td_op_t* td_ceil_op(td_graph_t* g, td_op_t* a) { return make_unary(g, OP_CEIL, a, a->out_type); }
td_op_t* td_floor_op(td_graph_t* g, td_op_t* a){ return make_unary(g, OP_FLOOR, a, a->out_type); }
td_op_t* td_isnull(td_graph_t* g, td_op_t* a)  { return make_unary(g, OP_ISNULL, a, TD_BOOL); }

td_op_t* td_cast(td_graph_t* g, td_op_t* a, int8_t target_type) {
    return make_unary(g, OP_CAST, a, target_type);
}

/* --------------------------------------------------------------------------
 * Binary element-wise ops
 * -------------------------------------------------------------------------- */

td_op_t* td_add(td_graph_t* g, td_op_t* a, td_op_t* b) { return make_binary(g, OP_ADD, a, b, promote(a->out_type, b->out_type)); }
td_op_t* td_sub(td_graph_t* g, td_op_t* a, td_op_t* b) { return make_binary(g, OP_SUB, a, b, promote(a->out_type, b->out_type)); }
td_op_t* td_mul(td_graph_t* g, td_op_t* a, td_op_t* b) { return make_binary(g, OP_MUL, a, b, promote(a->out_type, b->out_type)); }
td_op_t* td_div(td_graph_t* g, td_op_t* a, td_op_t* b) { return make_binary(g, OP_DIV, a, b, TD_F64); }
td_op_t* td_mod(td_graph_t* g, td_op_t* a, td_op_t* b) { return make_binary(g, OP_MOD, a, b, promote(a->out_type, b->out_type)); }

td_op_t* td_eq(td_graph_t* g, td_op_t* a, td_op_t* b) { return make_binary(g, OP_EQ, a, b, TD_BOOL); }
td_op_t* td_ne(td_graph_t* g, td_op_t* a, td_op_t* b) { return make_binary(g, OP_NE, a, b, TD_BOOL); }
td_op_t* td_lt(td_graph_t* g, td_op_t* a, td_op_t* b) { return make_binary(g, OP_LT, a, b, TD_BOOL); }
td_op_t* td_le(td_graph_t* g, td_op_t* a, td_op_t* b) { return make_binary(g, OP_LE, a, b, TD_BOOL); }
td_op_t* td_gt(td_graph_t* g, td_op_t* a, td_op_t* b) { return make_binary(g, OP_GT, a, b, TD_BOOL); }
td_op_t* td_ge(td_graph_t* g, td_op_t* a, td_op_t* b) { return make_binary(g, OP_GE, a, b, TD_BOOL); }
td_op_t* td_and(td_graph_t* g, td_op_t* a, td_op_t* b){ return make_binary(g, OP_AND, a, b, TD_BOOL); }
td_op_t* td_or(td_graph_t* g, td_op_t* a, td_op_t* b) { return make_binary(g, OP_OR, a, b, TD_BOOL); }
td_op_t* td_min2(td_graph_t* g, td_op_t* a, td_op_t* b){ return make_binary(g, OP_MIN2, a, b, promote(a->out_type, b->out_type)); }
td_op_t* td_max2(td_graph_t* g, td_op_t* a, td_op_t* b){ return make_binary(g, OP_MAX2, a, b, promote(a->out_type, b->out_type)); }

td_op_t* td_if(td_graph_t* g, td_op_t* cond, td_op_t* then_val, td_op_t* else_val) {
    /* 3-input node: cond, then, else — needs ext node */
    uint32_t cond_id = cond->id;
    uint32_t then_id = then_val->id;
    uint32_t else_id = else_val->id;
    int8_t out_type = promote(then_val->out_type, else_val->out_type);
    /* String types always resolve to SYM (global sym IDs) */
    if (then_val->out_type == TD_SYM || else_val->out_type == TD_SYM)
        out_type = TD_SYM;
    uint32_t est = cond->est_rows;

    td_op_ext_t* ext = graph_alloc_ext_node(g);
    if (!ext) return NULL;

    /* Re-resolve after potential realloc (else_val stored as index, not pointer) */
    cond = &g->nodes[cond_id];
    then_val = &g->nodes[then_id];

    ext->base.opcode = OP_IF;
    ext->base.arity = 2;  /* inputs[0]=cond, inputs[1]=then; else via ext */
    ext->base.inputs[0] = cond;
    ext->base.inputs[1] = then_val;
    ext->base.out_type = out_type;
    ext->base.est_rows = est;
    /* Store else_val as a node ID (not a pointer) in the literal field.
     * Recovered via (uint32_t)(uintptr_t)ext->literal in fuse.c/exec.c. */
    ext->literal = (td_t*)(uintptr_t)else_id;

    g->nodes[ext->base.id] = ext->base;
    return &g->nodes[ext->base.id];
}

td_op_t* td_like(td_graph_t* g, td_op_t* input, td_op_t* pattern) {
    return make_binary(g, OP_LIKE, input, pattern, TD_BOOL);
}

td_op_t* td_ilike(td_graph_t* g, td_op_t* input, td_op_t* pattern) {
    return make_binary(g, OP_ILIKE, input, pattern, TD_BOOL);
}

/* String ops */
td_op_t* td_upper(td_graph_t* g, td_op_t* a)   { return make_unary(g, OP_UPPER, a, TD_SYM); }
td_op_t* td_lower(td_graph_t* g, td_op_t* a)   { return make_unary(g, OP_LOWER, a, TD_SYM); }
td_op_t* td_strlen(td_graph_t* g, td_op_t* a)  { return make_unary(g, OP_STRLEN, a, TD_I64); }
td_op_t* td_trim_op(td_graph_t* g, td_op_t* a) { return make_unary(g, OP_TRIM, a, TD_SYM); }

td_op_t* td_substr(td_graph_t* g, td_op_t* str, td_op_t* start, td_op_t* len) {
    /* 3-input: str=inputs[0], start=inputs[1], len stored via literal field */
    uint32_t s_id = str->id;
    uint32_t st_id = start->id;
    uint32_t l_id = len->id;
    uint32_t est = str->est_rows;

    td_op_ext_t* ext = graph_alloc_ext_node(g);
    if (!ext) return NULL;
    str   = &g->nodes[s_id];
    start = &g->nodes[st_id];

    ext->base.opcode = OP_SUBSTR;
    ext->base.arity = 2;
    ext->base.inputs[0] = str;
    ext->base.inputs[1] = start;
    ext->base.out_type = TD_SYM;
    ext->base.est_rows = est;
    ext->literal = (td_t*)(uintptr_t)l_id;

    g->nodes[ext->base.id] = ext->base;
    return &g->nodes[ext->base.id];
}

td_op_t* td_replace(td_graph_t* g, td_op_t* str, td_op_t* from, td_op_t* to) {
    /* 3-input: str=inputs[0], from=inputs[1], to stored via literal field */
    uint32_t s_id = str->id;
    uint32_t f_id = from->id;
    uint32_t t_id = to->id;
    uint32_t est = str->est_rows;

    td_op_ext_t* ext = graph_alloc_ext_node(g);
    if (!ext) return NULL;
    str  = &g->nodes[s_id];
    from = &g->nodes[f_id];

    ext->base.opcode = OP_REPLACE;
    ext->base.arity = 2;
    ext->base.inputs[0] = str;
    ext->base.inputs[1] = from;
    ext->base.out_type = TD_SYM;
    ext->base.est_rows = est;
    ext->literal = (td_t*)(uintptr_t)t_id;

    g->nodes[ext->base.id] = ext->base;
    return &g->nodes[ext->base.id];
}

td_op_t* td_concat(td_graph_t* g, td_op_t** args, int n) {
    /* Variadic: first 2 in inputs[], rest in trailing IDs */
    if (!args || n < 2) return NULL;
    /* M4: Guard VLA upper bound */
    if (n > 256) return NULL;
    size_t n_args = (size_t)n;
    if (n_args > (SIZE_MAX / sizeof(uint32_t))) return NULL;
    size_t extra = (n > 2) ? (size_t)(n - 2) * sizeof(uint32_t) : 0;

    /* Save IDs before alloc (n is small — bounded by function arity) */
    uint32_t ids[n];
    for (int i = 0; i < n; i++) ids[i] = args[i]->id;
    uint32_t est = args[0]->est_rows;

    td_op_ext_t* ext = graph_alloc_ext_node_ex(g, extra);
    if (!ext) return NULL;

    ext->base.opcode = OP_CONCAT;
    ext->base.arity = 2;
    ext->base.inputs[0] = &g->nodes[ids[0]];
    ext->base.inputs[1] = &g->nodes[ids[1]];
    ext->base.out_type = TD_SYM;
    ext->base.est_rows = est;
    ext->sym = n; /* total arg count stored in sym field */

    /* Extra args in trailing bytes */
    uint32_t* trail = (uint32_t*)EXT_TRAIL(ext);
    for (int i = 2; i < n; i++) trail[i - 2] = ids[i];

    g->nodes[ext->base.id] = ext->base;
    return &g->nodes[ext->base.id];
}

/* --------------------------------------------------------------------------
 * Reduction ops
 * -------------------------------------------------------------------------- */

td_op_t* td_sum(td_graph_t* g, td_op_t* a)    { return make_unary(g, OP_SUM, a, a->out_type == TD_F64 ? TD_F64 : TD_I64); }
td_op_t* td_prod(td_graph_t* g, td_op_t* a)   { return make_unary(g, OP_PROD, a, a->out_type == TD_F64 ? TD_F64 : TD_I64); }
td_op_t* td_min_op(td_graph_t* g, td_op_t* a) { return make_unary(g, OP_MIN, a, a->out_type); }
td_op_t* td_max_op(td_graph_t* g, td_op_t* a) { return make_unary(g, OP_MAX, a, a->out_type); }
td_op_t* td_count(td_graph_t* g, td_op_t* a)  { return make_unary(g, OP_COUNT, a, TD_I64); }
td_op_t* td_avg(td_graph_t* g, td_op_t* a)    { return make_unary(g, OP_AVG, a, TD_F64); }
td_op_t* td_first(td_graph_t* g, td_op_t* a)  { return make_unary(g, OP_FIRST, a, a->out_type); }
td_op_t* td_last(td_graph_t* g, td_op_t* a)   { return make_unary(g, OP_LAST, a, a->out_type); }
td_op_t* td_count_distinct(td_graph_t* g, td_op_t* a) { return make_unary(g, OP_COUNT_DISTINCT, a, TD_I64); }

/* --------------------------------------------------------------------------
 * Structural ops
 * -------------------------------------------------------------------------- */

td_op_t* td_filter(td_graph_t* g, td_op_t* input, td_op_t* predicate) {
    uint32_t input_id = input->id;
    uint32_t pred_id = predicate->id;
    uint32_t est = input->est_rows / 2;  /* estimate: 50% selectivity */

    td_op_t* n = graph_alloc_node(g);
    if (!n) return NULL;

    input = &g->nodes[input_id];
    predicate = &g->nodes[pred_id];

    n->opcode = OP_FILTER;
    n->arity = 2;
    n->inputs[0] = input;
    n->inputs[1] = predicate;
    n->out_type = input->out_type;
    n->est_rows = est;
    return n;
}

td_op_t* td_sort_op(td_graph_t* g, td_op_t* table_node,
                     td_op_t** keys, uint8_t* descs, uint8_t* nulls_first,
                     uint8_t n_cols) {
    uint32_t table_id = table_node->id;
    /* L5: n_cols is uint8_t (max 255) so 256-element array is always sufficient. */
    uint32_t key_ids[256];
    for (uint8_t i = 0; i < n_cols; i++) key_ids[i] = keys[i]->id;

    size_t keys_sz = (size_t)n_cols * sizeof(td_op_t*);
    size_t descs_sz = (size_t)n_cols;
    size_t nf_sz = (size_t)n_cols;
    td_op_ext_t* ext = graph_alloc_ext_node_ex(g, keys_sz + descs_sz + nf_sz);
    if (!ext) return NULL;

    table_node = &g->nodes[table_id];

    ext->base.opcode = OP_SORT;
    ext->base.arity = 1;
    ext->base.inputs[0] = table_node;
    ext->base.out_type = TD_TABLE;
    ext->base.est_rows = table_node->est_rows;

    /* Arrays embedded in trailing space — freed with ext node */
    char* trail = EXT_TRAIL(ext);
    ext->sort.columns = (td_op_t**)trail;
    for (uint8_t i = 0; i < n_cols; i++)
        ext->sort.columns[i] = &g->nodes[key_ids[i]];
    ext->sort.desc = (uint8_t*)(trail + keys_sz);
    memcpy(ext->sort.desc, descs, descs_sz);
    ext->sort.nulls_first = (uint8_t*)(trail + keys_sz + descs_sz);
    if (nulls_first) {
        memcpy(ext->sort.nulls_first, nulls_first, nf_sz);
    } else {
        /* Default: NULLS LAST for ASC, NULLS FIRST for DESC (PostgreSQL convention) */
        for (uint8_t i = 0; i < n_cols; i++)
            ext->sort.nulls_first[i] = descs[i] ? 1 : 0;
    }
    ext->sort.n_cols = n_cols;

    g->nodes[ext->base.id] = ext->base;
    return &g->nodes[ext->base.id];
}

td_op_t* td_group(td_graph_t* g, td_op_t** keys, uint8_t n_keys,
                   uint16_t* agg_ops, td_op_t** agg_ins, uint8_t n_aggs) {
    uint32_t key_ids[256];
    uint32_t agg_ids[256];
    for (uint8_t i = 0; i < n_keys; i++) key_ids[i] = keys[i]->id;
    for (uint8_t i = 0; i < n_aggs; i++) agg_ids[i] = agg_ins[i]->id;

    size_t keys_sz = (size_t)n_keys * sizeof(td_op_t*);
    size_t ops_sz  = (size_t)n_aggs * sizeof(uint16_t);
    size_t ins_sz  = (size_t)n_aggs * sizeof(td_op_t*);
    /* Align ops after keys (pointer-sized), ins after ops (needs ptr alignment) */
    size_t ops_off = keys_sz;
    size_t ins_off = ops_off + ops_sz;
    /* Round ins_off up to pointer alignment */
    ins_off = (ins_off + sizeof(td_op_t*) - 1) & ~(sizeof(td_op_t*) - 1);
    td_op_ext_t* ext = graph_alloc_ext_node_ex(g, ins_off + ins_sz);
    if (!ext) return NULL;

    ext->base.opcode = OP_GROUP;
    ext->base.arity = 0;
    ext->base.out_type = TD_TABLE;
    if (n_keys > 0 && keys[0])
        ext->base.est_rows = g->nodes[key_ids[0]].est_rows / 10;  /* rough estimate */
    ext->base.inputs[0] = n_keys > 0 ? &g->nodes[key_ids[0]] : NULL;

    /* Arrays embedded in trailing space — freed with ext node */
    char* trail = EXT_TRAIL(ext);
    ext->keys = (td_op_t**)trail;
    for (uint8_t i = 0; i < n_keys; i++)
        ext->keys[i] = &g->nodes[key_ids[i]];
    ext->agg_ops = (uint16_t*)(trail + ops_off);
    if (ops_sz > 0) memcpy(ext->agg_ops, agg_ops, ops_sz);
    ext->agg_ins = (td_op_t**)(trail + ins_off);
    for (uint8_t i = 0; i < n_aggs; i++)
        ext->agg_ins[i] = &g->nodes[agg_ids[i]];
    ext->n_keys = n_keys;
    ext->n_aggs = n_aggs;

    g->nodes[ext->base.id] = ext->base;
    return &g->nodes[ext->base.id];
}

td_op_t* td_distinct(td_graph_t* g, td_op_t** keys, uint8_t n_keys) {
    return td_group(g, keys, n_keys, NULL, NULL, 0);
}

td_op_t* td_join(td_graph_t* g,
                  td_op_t* left_table, td_op_t** left_keys,
                  td_op_t* right_table, td_op_t** right_keys,
                  uint8_t n_keys, uint8_t join_type) {
    uint32_t left_table_id = left_table->id;
    uint32_t right_table_id = right_table->id;
    uint32_t lkey_ids[256];
    uint32_t rkey_ids[256];
    for (uint8_t i = 0; i < n_keys; i++) {
        lkey_ids[i] = left_keys[i]->id;
        rkey_ids[i] = right_keys[i]->id;
    }

    size_t keys_sz = (size_t)n_keys * sizeof(td_op_t*);
    td_op_ext_t* ext = graph_alloc_ext_node_ex(g, keys_sz * 2);
    if (!ext) return NULL;

    left_table = &g->nodes[left_table_id];
    right_table = &g->nodes[right_table_id];

    ext->base.opcode = OP_JOIN;
    ext->base.arity = 2;
    ext->base.inputs[0] = left_table;
    ext->base.inputs[1] = right_table;
    ext->base.out_type = TD_TABLE;
    ext->base.est_rows = left_table->est_rows;

    /* Arrays embedded in trailing space — freed with ext node */
    char* trail = EXT_TRAIL(ext);
    ext->join.left_keys = (td_op_t**)trail;
    for (uint8_t i = 0; i < n_keys; i++)
        ext->join.left_keys[i] = &g->nodes[lkey_ids[i]];
    ext->join.right_keys = (td_op_t**)(trail + (size_t)n_keys * sizeof(td_op_t*));
    for (uint8_t i = 0; i < n_keys; i++)
        ext->join.right_keys[i] = &g->nodes[rkey_ids[i]];
    ext->join.n_join_keys = n_keys;
    ext->join.join_type = join_type;

    g->nodes[ext->base.id] = ext->base;
    return &g->nodes[ext->base.id];
}

td_op_t* td_window_join(td_graph_t* g,
                         td_op_t* left_table, td_op_t* right_table,
                         td_op_t* time_key, td_op_t* sym_key,
                         int64_t window_lo, int64_t window_hi,
                         uint16_t* agg_ops, td_op_t** agg_ins,
                         uint8_t n_aggs) {
    uint32_t left_table_id = left_table->id;
    uint32_t right_table_id = right_table->id;
    uint32_t agg_ids[256];
    for (uint8_t i = 0; i < n_aggs; i++) agg_ids[i] = agg_ins[i]->id;

    (void)time_key; (void)sym_key;
    (void)window_lo; (void)window_hi;
    (void)agg_ops;

    size_t ins_sz = (size_t)n_aggs * sizeof(td_op_t*);
    td_op_ext_t* ext = graph_alloc_ext_node_ex(g, ins_sz);
    if (!ext) return NULL;

    left_table = &g->nodes[left_table_id];
    right_table = &g->nodes[right_table_id];

    ext->base.opcode = OP_WINDOW_JOIN;
    ext->base.arity = 2;
    ext->base.inputs[0] = left_table;
    ext->base.inputs[1] = right_table;
    ext->base.out_type = TD_TABLE;
    ext->base.est_rows = left_table->est_rows;

    /* Array embedded in trailing space — freed with ext node */
    ext->join.n_join_keys = n_aggs;
    ext->join.join_type = 0;
    ext->join.left_keys = (td_op_t**)EXT_TRAIL(ext);
    for (uint8_t i = 0; i < n_aggs; i++)
        ext->join.left_keys[i] = &g->nodes[agg_ids[i]];
    ext->join.right_keys = NULL;

    g->nodes[ext->base.id] = ext->base;
    return &g->nodes[ext->base.id];
}

td_op_t* td_window_op(td_graph_t* g, td_op_t* table_node,
                       td_op_t** part_keys, uint8_t n_part,
                       td_op_t** order_keys, uint8_t* order_descs, uint8_t n_order,
                       uint8_t* func_kinds, td_op_t** func_inputs,
                       int64_t* func_params, uint8_t n_funcs,
                       uint8_t frame_type, uint8_t frame_start, uint8_t frame_end,
                       int64_t frame_start_n, int64_t frame_end_n) {
    uint32_t part_ids[256];
    uint32_t order_ids[256];
    uint32_t func_ids[256];
    for (uint8_t i = 0; i < n_part; i++) part_ids[i] = part_keys[i]->id;
    for (uint8_t i = 0; i < n_order; i++) order_ids[i] = order_keys[i]->id;
    for (uint8_t i = 0; i < n_funcs; i++) func_ids[i] = func_inputs[i]->id;

    /* Trailing layout:
     *   [part_keys:   n_part * ptr]
     *   [order_keys:  n_order * ptr]
     *   [order_descs: n_order * 1B]
     *   [padding to ptr alignment]
     *   [func_inputs: n_funcs * ptr]
     *   [func_kinds:  n_funcs * 1B]
     *   [padding to 8B alignment]
     *   [func_params: n_funcs * 8B]
     */
    size_t pk_sz    = (size_t)n_part  * sizeof(td_op_t*);
    size_t ok_sz    = (size_t)n_order * sizeof(td_op_t*);
    size_t od_sz    = (size_t)n_order;
    size_t od_end   = pk_sz + ok_sz + od_sz;
    size_t fi_off   = (od_end + sizeof(td_op_t*) - 1) & ~(sizeof(td_op_t*) - 1);
    size_t fi_sz    = (size_t)n_funcs * sizeof(td_op_t*);
    size_t fk_off   = fi_off + fi_sz;
    size_t fk_sz    = (size_t)n_funcs;
    size_t fp_off   = (fk_off + fk_sz + 7) & ~(size_t)7;
    size_t fp_sz    = (size_t)n_funcs * sizeof(int64_t);
    size_t total    = fp_off + fp_sz;

    /* Save IDs before alloc — realloc may invalidate pointers */
    uint32_t table_id = table_node->id;
    uint32_t est   = table_node->est_rows;

    td_op_ext_t* ext = graph_alloc_ext_node_ex(g, total);
    if (!ext) return NULL;

    /* Re-resolve table_node after potential realloc */
    table_node = &g->nodes[table_id];

    ext->base.opcode   = OP_WINDOW;
    ext->base.arity    = 1;
    ext->base.inputs[0] = table_node;
    ext->base.out_type = TD_TABLE;
    ext->base.est_rows = est;  /* window preserves row count */

    /* Fill trailing arrays */
    char* trail = EXT_TRAIL(ext);
    ext->window.part_keys = (td_op_t**)trail;
    for (uint8_t i = 0; i < n_part; i++)
        ext->window.part_keys[i] = &g->nodes[part_ids[i]];

    ext->window.order_keys = (td_op_t**)(trail + pk_sz);
    for (uint8_t i = 0; i < n_order; i++)
        ext->window.order_keys[i] = &g->nodes[order_ids[i]];

    ext->window.order_descs = (uint8_t*)(trail + pk_sz + ok_sz);
    if (n_order) memcpy(ext->window.order_descs, order_descs, od_sz);

    ext->window.func_inputs = (td_op_t**)(trail + fi_off);
    for (uint8_t i = 0; i < n_funcs; i++)
        ext->window.func_inputs[i] = &g->nodes[func_ids[i]];

    ext->window.func_kinds = (uint8_t*)(trail + fk_off);
    if (n_funcs) memcpy(ext->window.func_kinds, func_kinds, fk_sz);

    ext->window.func_params = (int64_t*)(trail + fp_off);
    if (n_funcs) memcpy(ext->window.func_params, func_params, fp_sz);

    ext->window.n_part_keys   = n_part;
    ext->window.n_order_keys  = n_order;
    ext->window.n_funcs       = n_funcs;
    ext->window.frame_type    = frame_type;
    ext->window.frame_start   = frame_start;
    ext->window.frame_end     = frame_end;
    ext->window.frame_start_n = frame_start_n;
    ext->window.frame_end_n   = frame_end_n;

    g->nodes[ext->base.id] = ext->base;
    return &g->nodes[ext->base.id];
}

td_op_t* td_project(td_graph_t* g, td_op_t* input,
                     td_op_t** cols, uint8_t n_cols) {
    uint32_t input_id = input->id;
    uint32_t col_ids[256];
    for (uint8_t i = 0; i < n_cols; i++) col_ids[i] = cols[i]->id;

    size_t cols_sz = (size_t)n_cols * sizeof(td_op_t*);
    td_op_ext_t* ext = graph_alloc_ext_node_ex(g, cols_sz);
    if (!ext) return NULL;

    input = &g->nodes[input_id];

    ext->base.opcode = OP_PROJECT;
    ext->base.arity = 1;
    ext->base.inputs[0] = input;
    ext->base.out_type = TD_TABLE;
    ext->base.est_rows = input->est_rows;

    /* Array embedded in trailing space — freed with ext node */
    ext->sort.columns = (td_op_t**)EXT_TRAIL(ext);
    for (uint8_t i = 0; i < n_cols; i++)
        ext->sort.columns[i] = &g->nodes[col_ids[i]];
    ext->sort.n_cols = n_cols;

    g->nodes[ext->base.id] = ext->base;
    return &g->nodes[ext->base.id];
}

td_op_t* td_select(td_graph_t* g, td_op_t* input,
                    td_op_t** cols, uint8_t n_cols) {
    uint32_t input_id = input->id;
    uint32_t col_ids[256];
    for (uint8_t i = 0; i < n_cols; i++) col_ids[i] = cols[i]->id;

    size_t cols_sz = (size_t)n_cols * sizeof(td_op_t*);
    td_op_ext_t* ext = graph_alloc_ext_node_ex(g, cols_sz);
    if (!ext) return NULL;

    input = &g->nodes[input_id];

    ext->base.opcode = OP_SELECT;
    ext->base.arity = 1;
    ext->base.inputs[0] = input;
    ext->base.out_type = TD_TABLE;
    ext->base.est_rows = input->est_rows;

    /* Array embedded in trailing space — freed with ext node */
    ext->sort.columns = (td_op_t**)EXT_TRAIL(ext);
    for (uint8_t i = 0; i < n_cols; i++)
        ext->sort.columns[i] = &g->nodes[col_ids[i]];
    ext->sort.n_cols = n_cols;

    g->nodes[ext->base.id] = ext->base;
    return &g->nodes[ext->base.id];
}

/* L6: When n (stored as ext->sym) is 0, HEAD produces an empty result
   with the same schema as the input. */
td_op_t* td_head(td_graph_t* g, td_op_t* input, int64_t n) {
    uint32_t input_id = input->id;
    td_op_ext_t* ext = graph_alloc_ext_node(g);
    if (!ext) return NULL;

    input = &g->nodes[input_id];

    ext->base.opcode = OP_HEAD;
    ext->base.arity = 1;
    ext->base.inputs[0] = input;
    ext->base.out_type = input->out_type;
    ext->base.est_rows = (uint32_t)n;
    ext->sym = n;

    g->nodes[ext->base.id] = ext->base;
    return &g->nodes[ext->base.id];
}

td_op_t* td_tail(td_graph_t* g, td_op_t* input, int64_t n) {
    uint32_t input_id = input->id;
    td_op_ext_t* ext = graph_alloc_ext_node(g);
    if (!ext) return NULL;

    input = &g->nodes[input_id];

    ext->base.opcode = OP_TAIL;
    ext->base.arity = 1;
    ext->base.inputs[0] = input;
    ext->base.out_type = input->out_type;
    ext->base.est_rows = (uint32_t)n;
    ext->sym = n;

    g->nodes[ext->base.id] = ext->base;
    return &g->nodes[ext->base.id];
}

td_op_t* td_alias(td_graph_t* g, td_op_t* input, const char* name) {
    uint32_t input_id = input->id;
    td_op_ext_t* ext = graph_alloc_ext_node(g);
    if (!ext) return NULL;

    input = &g->nodes[input_id];

    ext->base.opcode = OP_ALIAS;
    ext->base.arity = 1;
    ext->base.inputs[0] = input;
    ext->base.out_type = input->out_type;
    ext->base.est_rows = input->est_rows;
    ext->sym = td_sym_intern(name, strlen(name));

    g->nodes[ext->base.id] = ext->base;
    return &g->nodes[ext->base.id];
}

td_op_t* td_extract(td_graph_t* g, td_op_t* col, int64_t field) {
    uint32_t col_id = col->id;
    uint32_t est = col->est_rows;

    td_op_ext_t* ext = graph_alloc_ext_node(g);
    if (!ext) return NULL;
    col = &g->nodes[col_id];  /* re-resolve after potential realloc */

    ext->base.opcode = OP_EXTRACT;
    ext->base.arity = 1;
    ext->base.inputs[0] = col;
    ext->base.out_type = TD_I64;
    ext->base.est_rows = est;
    ext->sym = field;

    g->nodes[ext->base.id] = ext->base;
    return &g->nodes[ext->base.id];
}

td_op_t* td_date_trunc(td_graph_t* g, td_op_t* col, int64_t field) {
    uint32_t col_id = col->id;
    uint32_t est = col->est_rows;

    td_op_ext_t* ext = graph_alloc_ext_node(g);
    if (!ext) return NULL;
    col = &g->nodes[col_id];  /* re-resolve after potential realloc */

    ext->base.opcode = OP_DATE_TRUNC;
    ext->base.arity = 1;
    ext->base.inputs[0] = col;
    ext->base.out_type = TD_TIMESTAMP;  /* returns timestamp (microseconds) */
    ext->base.est_rows = est;
    ext->sym = field;

    g->nodes[ext->base.id] = ext->base;
    return &g->nodes[ext->base.id];
}

td_op_t* td_materialize(td_graph_t* g, td_op_t* input) {
    uint32_t input_id = input->id;
    td_op_t* n = graph_alloc_node(g);
    if (!n) return NULL;

    input = &g->nodes[input_id];

    n->opcode = OP_MATERIALIZE;
    n->arity = 1;
    n->inputs[0] = input;
    n->out_type = input->out_type;
    n->est_rows = input->est_rows;
    return n;
}

/* --------------------------------------------------------------------------
 * Multi-table support
 * -------------------------------------------------------------------------- */

uint16_t td_graph_add_table(td_graph_t* g, td_t* table) {
    uint16_t id = g->n_tables;
    uint16_t new_cap = id + 1;

    td_t** new_tables = (td_t**)td_sys_realloc(g->tables,
                                                (size_t)new_cap * sizeof(td_t*));
    if (!new_tables) return UINT16_MAX;  /* error sentinel */
    g->tables = new_tables;
    g->tables[id] = table;
    td_retain(table);
    g->n_tables = new_cap;

    return id;
}

td_op_t* td_scan_table(td_graph_t* g, uint16_t table_id, const char* col_name) {
    if (table_id >= g->n_tables || !g->tables[table_id]) return NULL;

    td_op_ext_t* ext = graph_alloc_ext_node(g);
    if (!ext) return NULL;

    ext->base.opcode = OP_SCAN;
    ext->base.arity = 0;

    int64_t sym_id = td_sym_intern(col_name, strlen(col_name));
    ext->sym = sym_id;

    /* Store table_id+1 in pad[0..1] as uint16_t (0 = default g->table) */
    uint16_t stored_id = table_id + 1;
    memcpy(ext->base.pad, &stored_id, sizeof(uint16_t));

    /* Infer output type from the specified table */
    td_t* tbl = g->tables[table_id];
    if (tbl) {
        td_t* col = td_table_get_col(tbl, sym_id);
        if (col) {
            ext->base.out_type = col->type;
            ext->base.est_rows = (uint32_t)col->len;
        }
    }

    g->nodes[ext->base.id] = ext->base;
    return &g->nodes[ext->base.id];
}

/* --------------------------------------------------------------------------
 * Graph traversal DAG builders
 * -------------------------------------------------------------------------- */

td_op_t* td_expand(td_graph_t* g, td_op_t* src_nodes,
                    td_rel_t* rel, uint8_t direction) {
    uint32_t src_id = src_nodes->id;
    uint32_t est = src_nodes->est_rows;

    td_op_ext_t* ext = graph_alloc_ext_node(g);
    if (!ext) return NULL;
    src_nodes = &g->nodes[src_id];

    ext->base.opcode = OP_EXPAND;
    ext->base.arity = 1;
    ext->base.inputs[0] = src_nodes;
    ext->base.out_type = TD_TABLE;
    ext->base.est_rows = est * 10;  /* rough estimate: 10x fan-out */
    ext->graph.rel = rel;
    ext->graph.direction = direction;
    ext->graph.min_depth = 1;
    ext->graph.max_depth = 1;
    ext->graph.path_tracking = 0;

    g->nodes[ext->base.id] = ext->base;
    return &g->nodes[ext->base.id];
}

td_op_t* td_var_expand(td_graph_t* g, td_op_t* start_nodes,
                        td_rel_t* rel, uint8_t direction,
                        uint8_t min_depth, uint8_t max_depth,
                        bool track_path) {
    uint32_t src_id = start_nodes->id;
    uint32_t est = start_nodes->est_rows;

    td_op_ext_t* ext = graph_alloc_ext_node(g);
    if (!ext) return NULL;
    start_nodes = &g->nodes[src_id];

    ext->base.opcode = OP_VAR_EXPAND;
    ext->base.arity = 1;
    ext->base.inputs[0] = start_nodes;
    ext->base.out_type = TD_TABLE;
    ext->base.est_rows = est * 100;  /* rough estimate */
    ext->graph.rel = rel;
    ext->graph.direction = direction;
    ext->graph.min_depth = min_depth;
    ext->graph.max_depth = max_depth;
    ext->graph.path_tracking = track_path ? 1 : 0;

    g->nodes[ext->base.id] = ext->base;
    return &g->nodes[ext->base.id];
}

td_op_t* td_shortest_path(td_graph_t* g, td_op_t* src, td_op_t* dst,
                           td_rel_t* rel, uint8_t max_depth) {
    uint32_t src_id = src->id;
    uint32_t dst_id = dst->id;

    td_op_ext_t* ext = graph_alloc_ext_node(g);
    if (!ext) return NULL;
    src = &g->nodes[src_id];
    dst = &g->nodes[dst_id];

    ext->base.opcode = OP_SHORTEST_PATH;
    ext->base.arity = 2;
    ext->base.inputs[0] = src;
    ext->base.inputs[1] = dst;
    ext->base.out_type = TD_TABLE;
    ext->base.est_rows = max_depth;
    ext->graph.rel = rel;
    ext->graph.direction = 0;  /* forward by default */
    ext->graph.min_depth = 0;
    ext->graph.max_depth = max_depth;
    ext->graph.path_tracking = 0;

    g->nodes[ext->base.id] = ext->base;
    return &g->nodes[ext->base.id];
}

td_op_t* td_wco_join(td_graph_t* g,
                      td_rel_t** rels, uint8_t n_rels,
                      uint8_t n_vars) {
    size_t extra = (size_t)n_rels * sizeof(td_rel_t*);
    td_op_ext_t* ext = graph_alloc_ext_node_ex(g, extra);
    if (!ext) return NULL;

    ext->base.opcode = OP_WCO_JOIN;
    ext->base.arity = 0;
    ext->base.out_type = TD_TABLE;
    ext->base.est_rows = 1000;  /* rough estimate */

    /* Copy rels array into trailing bytes */
    td_rel_t** trail = (td_rel_t**)EXT_TRAIL(ext);
    if (n_rels > 0) memcpy(trail, rels, (size_t)n_rels * sizeof(td_rel_t*));
    ext->wco.rels = (void**)trail;
    ext->wco.n_rels = n_rels;
    ext->wco.n_vars = n_vars;

    g->nodes[ext->base.id] = ext->base;
    return &g->nodes[ext->base.id];
}
