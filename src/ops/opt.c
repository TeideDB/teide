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

#include "opt.h"
#include "mem/sys.h"
#include <math.h>
#include <string.h>

/* Forward declaration — defined below, used by type inference and DCE passes. */
static td_op_ext_t* find_ext(td_graph_t* g, uint32_t node_id);

/* --------------------------------------------------------------------------
 * Optimizer passes (v1): Type Inference + Constant Folding + Fusion + DCE
 *
 * Per the spec's staged rollout:
 *   v1: Type Inference + Constant Folding + Fusion + DCE
 *   v2: Predicate/Projection Pushdown + CSE (future)
 *   v3: Op Reordering + Join Optimization (future)
 * -------------------------------------------------------------------------- */

/* --------------------------------------------------------------------------
 * Pass 1: Type inference (bottom-up)
 *
 * Most type inference is done during graph construction (graph.c).
 * This pass validates and propagates any missing types.
 * -------------------------------------------------------------------------- */

static int8_t promote_type(int8_t a, int8_t b) {
    if (a == TD_F64 || b == TD_F64) return TD_F64;
    /* Treat SYM/TIMESTAMP/DATE/TIME as integer-class types */
    if (a == TD_I64 || b == TD_I64 || a == TD_SYM || b == TD_SYM ||
        a == TD_TIMESTAMP || b == TD_TIMESTAMP) return TD_I64;
    if (a == TD_I32 || b == TD_I32 ||
        a == TD_DATE || b == TD_DATE || a == TD_TIME || b == TD_TIME) return TD_I32;
    if (a == TD_I16 || b == TD_I16) return TD_I16;
    if (a == TD_U8 || b == TD_U8) return TD_U8;
    return TD_BOOL;
}

static void infer_type_for_node(td_op_t* node) {
    if (node->out_type == 0 && node->opcode != OP_SCAN && node->opcode != OP_CONST) {
        if (node->arity >= 2 && node->inputs[0] && node->inputs[1]) {
            node->out_type = promote_type(node->inputs[0]->out_type,
                                           node->inputs[1]->out_type);
        } else if (node->arity >= 1 && node->inputs[0]) {
            node->out_type = node->inputs[0]->out_type;
        }
    }
}

static void pass_type_inference(td_graph_t* g, td_op_t* root) {
    if (!root || root->flags & OP_FLAG_DEAD) return;

    /* Iterative post-order: collect nodes into an order array, then
       process in reverse (children before parents). */
    uint32_t nc = g->node_count;
    uint32_t stack_local[256], order_local[256];
    bool visited_stack[256];
    uint32_t *stack = nc <= 256 ? stack_local : (uint32_t*)td_sys_alloc(nc * sizeof(uint32_t));
    uint32_t *order = nc <= 256 ? order_local : (uint32_t*)td_sys_alloc(nc * sizeof(uint32_t));
    bool* visited;
    if (nc <= 256) {
        visited = visited_stack;
    } else {
        visited = (bool*)td_sys_alloc(nc * sizeof(bool));
    }
    if (!stack || !order || !visited) {
        if (nc > 256) { td_sys_free(stack); td_sys_free(order); td_sys_free(visited); }
        return;
    }
    memset(visited, 0, nc * sizeof(bool));

    int sp = 0, oc = 0;
    stack[sp++] = root->id;
    while (sp > 0 && oc < (int)nc) {
        uint32_t nid = stack[--sp];
        td_op_t* n = &g->nodes[nid];
        if (!n || n->flags & OP_FLAG_DEAD) continue;
        if (visited[nid]) continue;
        visited[nid] = true;
        order[oc++] = nid;
        for (int i = 0; i < 2 && i < n->arity; i++) {
            if (n->inputs[i] && sp < (int)nc)
                stack[sp++] = n->inputs[i]->id;
        }
        /* M3: Traverse ext node children so type inference reaches all
           referenced nodes (GROUP keys/aggs, SORT/PROJECT/SELECT columns,
           JOIN keys, WINDOW partition/order/func_inputs). */
        td_op_ext_t* ext = find_ext(g, nid);
        if (ext) {
            switch (n->opcode) {
                case OP_GROUP:
                    for (uint8_t k = 0; k < ext->n_keys; k++)
                        if (ext->keys[k] && !visited[ext->keys[k]->id] && sp < (int)nc)
                            stack[sp++] = ext->keys[k]->id;
                    for (uint8_t a = 0; a < ext->n_aggs; a++)
                        if (ext->agg_ins[a] && !visited[ext->agg_ins[a]->id] && sp < (int)nc)
                            stack[sp++] = ext->agg_ins[a]->id;
                    break;
                case OP_SORT:
                case OP_PROJECT:
                case OP_SELECT:
                    for (uint8_t k = 0; k < ext->sort.n_cols; k++)
                        if (ext->sort.columns[k] && !visited[ext->sort.columns[k]->id] && sp < (int)nc)
                            stack[sp++] = ext->sort.columns[k]->id;
                    break;
                case OP_JOIN:
                case OP_WINDOW_JOIN:
                    for (uint8_t k = 0; k < ext->join.n_join_keys; k++) {
                        if (ext->join.left_keys[k] && !visited[ext->join.left_keys[k]->id] && sp < (int)nc)
                            stack[sp++] = ext->join.left_keys[k]->id;
                        if (ext->join.right_keys && ext->join.right_keys[k] &&
                            !visited[ext->join.right_keys[k]->id] && sp < (int)nc)
                            stack[sp++] = ext->join.right_keys[k]->id;
                    }
                    break;
                case OP_WINDOW:
                    for (uint8_t k = 0; k < ext->window.n_part_keys; k++)
                        if (ext->window.part_keys[k] && !visited[ext->window.part_keys[k]->id] && sp < (int)nc)
                            stack[sp++] = ext->window.part_keys[k]->id;
                    for (uint8_t k = 0; k < ext->window.n_order_keys; k++)
                        if (ext->window.order_keys[k] && !visited[ext->window.order_keys[k]->id] && sp < (int)nc)
                            stack[sp++] = ext->window.order_keys[k]->id;
                    for (uint8_t f = 0; f < ext->window.n_funcs; f++)
                        if (ext->window.func_inputs[f] && !visited[ext->window.func_inputs[f]->id] && sp < (int)nc)
                            stack[sp++] = ext->window.func_inputs[f]->id;
                    break;
                /* M3b: 3-input ops store third operand node ID in ext->literal */
                case OP_IF:
                case OP_SUBSTR:
                case OP_REPLACE: {
                    uint32_t third_id = (uint32_t)(uintptr_t)ext->literal;
                    if (third_id < nc && !visited[third_id] && sp < (int)nc)
                        stack[sp++] = third_id;
                    break;
                }
                /* M3c: OP_CONCAT trailing arg node IDs beyond inputs[0..1] */
                case OP_CONCAT:
                    if (ext->sym >= 2) {
                        int n_args = (int)ext->sym;
                        uint32_t* trail = (uint32_t*)((char*)(ext + 1));
                        for (int j = 2; j < n_args; j++) {
                            uint32_t arg_id = trail[j - 2];
                            if (arg_id < nc && !visited[arg_id] && sp < (int)nc)
                                stack[sp++] = arg_id;
                        }
                    }
                    break;
                default:
                    break;
            }
        }
    }
    /* Process in reverse order (children before parents) */
    for (int i = oc - 1; i >= 0; i--)
        infer_type_for_node(&g->nodes[order[i]]);

    if (nc > 256) { td_sys_free(stack); td_sys_free(order); td_sys_free(visited); }
}

/* --------------------------------------------------------------------------
 * Pass 2: Constant folding
 *
 * If all inputs to an element-wise op are OP_CONST, evaluate immediately
 * and replace the node with a new OP_CONST.
 * -------------------------------------------------------------------------- */

static bool is_const(td_op_t* n) {
    return n && n->opcode == OP_CONST;
}

/* O(ext_count) per call; acceptable for typical graph sizes (tens to
   hundreds of nodes).  L2: intentional duplication to keep files
   self-contained — also present in fuse.c. */
static td_op_ext_t* find_ext(td_graph_t* g, uint32_t node_id) {
    for (uint32_t i = 0; i < g->ext_count; i++) {
        if (g->ext_nodes[i] && g->ext_nodes[i]->base.id == node_id)
            return g->ext_nodes[i];
    }
    return NULL;
}

static bool track_ext_node(td_graph_t* g, td_op_ext_t* ext) {
    if (g->ext_count >= g->ext_cap) {
        if (g->ext_cap > UINT32_MAX / 2) return false;
        uint32_t new_cap = g->ext_cap == 0 ? 16 : g->ext_cap * 2;
        td_op_ext_t** new_exts =
            (td_op_ext_t**)td_sys_realloc(g->ext_nodes, new_cap * sizeof(td_op_ext_t*));
        if (!new_exts) return false;
        g->ext_nodes = new_exts;
        g->ext_cap = new_cap;
    }
    g->ext_nodes[g->ext_count++] = ext;
    return true;
}

static td_op_ext_t* ensure_ext_node(td_graph_t* g, uint32_t node_id) {
    td_op_ext_t* ext = find_ext(g, node_id);
    if (ext) return ext;

    ext = (td_op_ext_t*)td_sys_alloc(sizeof(td_op_ext_t));
    if (!ext) return NULL;
    /* M1: Zero-init to prevent use of uninitialized fields (literal,
       keys, agg_ins, etc.) before the caller populates them. */
    memset(ext, 0, sizeof(*ext));
    ext->base.id = node_id;
    if (!track_ext_node(g, ext)) {
        td_sys_free(ext);
        return NULL;
    }
    return ext;
}

static bool atom_to_numeric(td_t* v, double* out_f, int64_t* out_i, bool* is_f64) {
    if (!v || !td_is_atom(v)) return false;
    switch (v->type) {
        case TD_ATOM_F64:
            *out_f = v->f64;
            *out_i = (int64_t)v->f64;
            *is_f64 = true;
            return true;
        case TD_ATOM_I64:
        case TD_ATOM_SYM:
        case TD_ATOM_DATE:
        case TD_ATOM_TIME:
        case TD_ATOM_TIMESTAMP:
            *out_i = v->i64;
            *out_f = (double)v->i64;
            *is_f64 = false;
            return true;
        case TD_ATOM_I32:
            *out_i = (int64_t)v->i32;
            *out_f = (double)v->i32;
            *is_f64 = false;
            return true;
        case TD_ATOM_I16:
            *out_i = (int64_t)v->i16;
            *out_f = (double)v->i16;
            *is_f64 = false;
            return true;
        case TD_ATOM_U8:
        case TD_ATOM_BOOL:
            *out_i = (int64_t)v->u8;
            *out_f = (double)v->u8;
            *is_f64 = false;
            return true;
        default:
            return false;
    }
}

static bool replace_with_const(td_graph_t* g, td_op_t* node, td_t* literal) {
    /* H3: If the node already has an ext node (GROUP, SORT, JOIN, etc.),
       skip constant replacement — overwriting the ext union would clobber
       structural data.  Structural ops should never be constant-folded. */
    if (find_ext(g, node->id)) return false;

    td_op_ext_t* ext = ensure_ext_node(g, node->id);
    if (!ext) return false;

    ext->base = *node;
    ext->base.opcode = OP_CONST;
    ext->base.arity = 0;
    ext->base.inputs[0] = NULL;
    ext->base.inputs[1] = NULL;
    ext->base.flags &= (uint8_t)~OP_FLAG_FUSED;
    ext->base.out_type = literal->type < 0 ? (int8_t)(-(int)literal->type) : literal->type;
    ext->literal = literal;

    *node = ext->base;
    g->nodes[node->id] = ext->base;
    return true;
}

static bool fold_unary_const(td_graph_t* g, td_op_t* node) {
    td_op_t* operand = node->inputs[0];
    if (!is_const(operand)) return false;

    td_op_ext_t* oe = find_ext(g, operand->id);
    if (!oe || !oe->literal || !td_is_atom(oe->literal)) return false;

    double vf = 0.0;
    int64_t vi = 0;
    bool is_f64 = false;
    if (!atom_to_numeric(oe->literal, &vf, &vi, &is_f64)) return false;

    td_t* folded = NULL;
    switch (node->opcode) {
        case OP_NEG:
            if (is_f64) folded = td_f64(-vf);
            else if (vi == INT64_MIN) return false;  /* -INT64_MIN overflows */
            else folded = td_i64(-vi);
            break;
        case OP_ABS:
            if (is_f64)
                folded = td_f64(fabs(vf));
            else if (vi == INT64_MIN) return false;  /* -INT64_MIN overflows */
            else folded = td_i64(vi < 0 ? -vi : vi);
            break;
        case OP_NOT:
            folded = td_bool(is_f64 ? vf == 0.0 : vi == 0);
            break;
        case OP_SQRT:
            folded = td_f64(sqrt(is_f64 ? vf : (double)vi));
            break;
        case OP_LOG:
            folded = td_f64(log(is_f64 ? vf : (double)vi));
            break;
        case OP_EXP:
            folded = td_f64(exp(is_f64 ? vf : (double)vi));
            break;
        case OP_CEIL:
            folded = is_f64 ? td_f64(ceil(vf)) : td_i64(vi);
            break;
        case OP_FLOOR:
            folded = is_f64 ? td_f64(floor(vf)) : td_i64(vi);
            break;
        default:
            return false;
    }

    if (!folded || TD_IS_ERR(folded)) return false;
    if (!replace_with_const(g, node, folded)) {
        td_release(folded);
        return false;
    }
    return true;
}

static bool fold_binary_const(td_graph_t* g, td_op_t* node) {
    td_op_t* lhs = node->inputs[0];
    td_op_t* rhs = node->inputs[1];
    if (!is_const(lhs) || !is_const(rhs)) return false;

    td_op_ext_t* le = find_ext(g, lhs->id);
    td_op_ext_t* re = find_ext(g, rhs->id);
    if (!le || !re || !le->literal || !re->literal) return false;
    if (!td_is_atom(le->literal) || !td_is_atom(re->literal)) return false;

    double lf = 0.0, rf = 0.0;
    int64_t li = 0, ri = 0;
    bool l_is_f64 = false, r_is_f64 = false;
    if (!atom_to_numeric(le->literal, &lf, &li, &l_is_f64)) return false;
    if (!atom_to_numeric(re->literal, &rf, &ri, &r_is_f64)) return false;

    td_t* folded = NULL;
    switch (node->out_type) {
        case TD_F64: {
            double lv = l_is_f64 ? lf : (double)li;
            double rv = r_is_f64 ? rf : (double)ri;
            double r = 0.0;
            switch (node->opcode) {
                case OP_ADD: r = lv + rv; break;
                case OP_SUB: r = lv - rv; break;
                case OP_MUL: r = lv * rv; break;
                case OP_DIV: r = lv / rv; break;  /* IEEE 754: ±Inf or NaN */
                case OP_MOD: r = fmod(lv, rv); break;  /* IEEE 754: NaN for rv==0 */
                case OP_MIN2: r = fmin(lv, rv); break;  /* NaN-propagating */
                case OP_MAX2: r = fmax(lv, rv); break;  /* NaN-propagating */
                default: return false;
            }
            folded = td_f64(r);
            break;
        }
        case TD_I64: {
            int64_t lv = l_is_f64 ? (int64_t)lf : li;
            int64_t rv = r_is_f64 ? (int64_t)rf : ri;
            int64_t r = 0;
            switch (node->opcode) {
                case OP_ADD: r = (int64_t)((uint64_t)lv + (uint64_t)rv); break;
                case OP_SUB: r = (int64_t)((uint64_t)lv - (uint64_t)rv); break;
                case OP_MUL: r = (int64_t)((uint64_t)lv * (uint64_t)rv); break;
                case OP_DIV:
                    r = (rv != 0 && !(lv == INT64_MIN && rv == -1)) ? lv / rv : 0;
                    break;
                case OP_MOD:
                    r = (rv != 0 && !(lv == INT64_MIN && rv == -1)) ? lv % rv : 0;
                    break;
                case OP_MIN2: r = lv < rv ? lv : rv; break;
                case OP_MAX2: r = lv > rv ? lv : rv; break;
                default: return false;
            }
            folded = td_i64(r);
            break;
        }
        case TD_BOOL: {
            /* NaN comparison follows IEEE 754; SQL NULL handled separately
               in executor. */
            double lv = l_is_f64 ? lf : (double)li;
            double rv = r_is_f64 ? rf : (double)ri;
            bool r = false;
            switch (node->opcode) {
                case OP_EQ:  r = lv == rv; break;
                case OP_NE:  r = lv != rv; break;
                case OP_LT:  r = lv < rv; break;
                case OP_LE:  r = lv <= rv; break;
                case OP_GT:  r = lv > rv; break;
                case OP_GE:  r = lv >= rv; break;
                case OP_AND: r = (lv != 0.0) && (rv != 0.0); break;
                case OP_OR:  r = (lv != 0.0) || (rv != 0.0); break;
                default: return false;
            }
            folded = td_bool(r);
            break;
        }
        default:
            return false;
    }

    if (!folded || TD_IS_ERR(folded)) return false;
    if (!replace_with_const(g, node, folded)) {
        td_release(folded);
        return false;
    }
    return true;
}

static bool atom_to_bool(td_t* v, bool* out) {
    double vf = 0.0;
    int64_t vi = 0;
    bool is_f64 = false;
    if (!atom_to_numeric(v, &vf, &vi, &is_f64)) return false;
    if (is_f64) {
        *out = vf != 0.0;
    } else {
        *out = vi != 0;
    }
    return true;
}

static bool fold_filter_const_predicate(td_graph_t* g, td_op_t* node) {
    if (node->opcode != OP_FILTER || node->arity != 2) return false;
    td_op_t* pred = node->inputs[1];
    if (!is_const(pred)) return false;

    td_op_ext_t* pred_ext = find_ext(g, pred->id);
    if (!pred_ext || !pred_ext->literal || !td_is_atom(pred_ext->literal)) return false;

    bool keep_rows = false;
    if (!atom_to_bool(pred_ext->literal, &keep_rows)) return false;

    if (keep_rows) {
        node->opcode = OP_MATERIALIZE;
        node->arity = 1;
        node->inputs[1] = NULL;
        node->flags &= (uint8_t)~OP_FLAG_FUSED;
        g->nodes[node->id] = *node;
        return true;
    }

    td_op_ext_t* ext = ensure_ext_node(g, node->id);
    if (!ext) return false;
    ext->base = *node;
    ext->base.opcode = OP_HEAD;
    ext->base.arity = 1;
    ext->base.inputs[1] = NULL;
    ext->base.est_rows = 0;
    ext->base.flags &= (uint8_t)~OP_FLAG_FUSED;
    ext->sym = 0;

    *node = ext->base;
    g->nodes[node->id] = ext->base;
    return true;
}

static void fold_node(td_graph_t* g, td_op_t* node) {
    /* Fold unary element-wise ops with constant input */
    if (node->arity == 1 && node->opcode >= OP_NEG && node->opcode <= OP_FLOOR) {
        (void)fold_unary_const(g, node);
    }
    /* Fold binary element-wise ops with two const inputs */
    if (node->arity == 2 && node->opcode >= OP_ADD && node->opcode <= OP_MAX2) {
        (void)fold_binary_const(g, node);
    }
    /* FILTER with constant predicate can be reduced to pass-through/empty. */
    (void)fold_filter_const_predicate(g, node);
}

static void pass_constant_fold(td_graph_t* g, td_op_t* root) {
    if (!root || root->flags & OP_FLAG_DEAD) return;

    /* Iterative post-order: collect nodes, then process in reverse
       (children before parents). */
    uint32_t nc = g->node_count;
    uint32_t stack_local[256], order_local[256];
    bool visited_stack[256];
    uint32_t *stack = nc <= 256 ? stack_local : (uint32_t*)td_sys_alloc(nc * sizeof(uint32_t));
    uint32_t *order = nc <= 256 ? order_local : (uint32_t*)td_sys_alloc(nc * sizeof(uint32_t));
    bool* visited;
    if (nc <= 256) {
        visited = visited_stack;
    } else {
        visited = (bool*)td_sys_alloc(nc * sizeof(bool));
    }
    if (!stack || !order || !visited) {
        if (nc > 256) { td_sys_free(stack); td_sys_free(order); td_sys_free(visited); }
        return;
    }
    memset(visited, 0, nc * sizeof(bool));

    int sp = 0, oc = 0;
    stack[sp++] = root->id;
    while (sp > 0 && oc < (int)nc) {
        uint32_t nid = stack[--sp];
        td_op_t* n = &g->nodes[nid];
        if (!n || n->flags & OP_FLAG_DEAD) continue;
        if (visited[nid]) continue;
        visited[nid] = true;
        order[oc++] = nid;
        for (int i = 0; i < 2 && i < n->arity; i++) {
            if (n->inputs[i] && sp < (int)nc)
                stack[sp++] = n->inputs[i]->id;
        }
        /* H1: Traverse ext-node children so constant folding reaches all
           referenced nodes (GROUP keys/aggs, SORT/PROJECT/SELECT columns,
           JOIN keys, WINDOW partition/order/func_inputs). */
        td_op_ext_t* ext = find_ext(g, nid);
        if (ext) {
            switch (n->opcode) {
                case OP_GROUP:
                    for (uint8_t k = 0; k < ext->n_keys; k++)
                        if (ext->keys[k] && !visited[ext->keys[k]->id] && sp < (int)nc)
                            stack[sp++] = ext->keys[k]->id;
                    for (uint8_t a = 0; a < ext->n_aggs; a++)
                        if (ext->agg_ins[a] && !visited[ext->agg_ins[a]->id] && sp < (int)nc)
                            stack[sp++] = ext->agg_ins[a]->id;
                    break;
                case OP_SORT:
                case OP_PROJECT:
                case OP_SELECT:
                    for (uint8_t k = 0; k < ext->sort.n_cols; k++)
                        if (ext->sort.columns[k] && !visited[ext->sort.columns[k]->id] && sp < (int)nc)
                            stack[sp++] = ext->sort.columns[k]->id;
                    break;
                case OP_JOIN:
                case OP_WINDOW_JOIN:
                    for (uint8_t k = 0; k < ext->join.n_join_keys; k++) {
                        if (ext->join.left_keys[k] && !visited[ext->join.left_keys[k]->id] && sp < (int)nc)
                            stack[sp++] = ext->join.left_keys[k]->id;
                        if (ext->join.right_keys && ext->join.right_keys[k] &&
                            !visited[ext->join.right_keys[k]->id] && sp < (int)nc)
                            stack[sp++] = ext->join.right_keys[k]->id;
                    }
                    break;
                case OP_WINDOW:
                    for (uint8_t k = 0; k < ext->window.n_part_keys; k++)
                        if (ext->window.part_keys[k] && !visited[ext->window.part_keys[k]->id] && sp < (int)nc)
                            stack[sp++] = ext->window.part_keys[k]->id;
                    for (uint8_t k = 0; k < ext->window.n_order_keys; k++)
                        if (ext->window.order_keys[k] && !visited[ext->window.order_keys[k]->id] && sp < (int)nc)
                            stack[sp++] = ext->window.order_keys[k]->id;
                    for (uint8_t f = 0; f < ext->window.n_funcs; f++)
                        if (ext->window.func_inputs[f] && !visited[ext->window.func_inputs[f]->id] && sp < (int)nc)
                            stack[sp++] = ext->window.func_inputs[f]->id;
                    break;
                /* H1b: 3-input ops store third operand node ID in ext->literal */
                case OP_IF:
                case OP_SUBSTR:
                case OP_REPLACE: {
                    uint32_t third_id = (uint32_t)(uintptr_t)ext->literal;
                    if (third_id < nc && !visited[third_id] && sp < (int)nc)
                        stack[sp++] = third_id;
                    break;
                }
                /* H1c: OP_CONCAT trailing arg node IDs beyond inputs[0..1] */
                case OP_CONCAT:
                    if (ext->sym >= 2) {
                        int n_args = (int)ext->sym;
                        uint32_t* trail = (uint32_t*)((char*)(ext + 1));
                        for (int j = 2; j < n_args; j++) {
                            uint32_t arg_id = trail[j - 2];
                            if (arg_id < nc && !visited[arg_id] && sp < (int)nc)
                                stack[sp++] = arg_id;
                        }
                    }
                    break;
                default:
                    break;
            }
        }
    }
    /* Process in reverse order (children before parents) */
    for (int i = oc - 1; i >= 0; i--)
        fold_node(g, &g->nodes[order[i]]);

    if (nc > 256) { td_sys_free(stack); td_sys_free(order); td_sys_free(visited); }
}

/* --------------------------------------------------------------------------
 * Pass 3: Dead code elimination
 *
 * Mark nodes unreachable from root as DEAD.
 * -------------------------------------------------------------------------- */

static void mark_live(td_graph_t* g, td_op_t* root, bool* live) {
    if (!root) return;

    uint32_t nc = g->node_count;
    if (nc > UINT32_MAX / 2) return;
    /* Worst case: each node can contribute up to ~N children (CONCAT trailing),
       but nc*2 is a safe upper bound for the stack. */
    uint32_t stack_cap = nc * 2;
    uint32_t stack_local[256];
    uint32_t *stack = stack_cap <= 256 ? stack_local : (uint32_t*)td_sys_alloc(stack_cap * sizeof(uint32_t));
    if (!stack) return;
    int sp = 0;
    stack[sp++] = root->id;
    while (sp > 0) {
        uint32_t nid = stack[--sp];
        if (live[nid]) continue;
        live[nid] = true;
        td_op_t* n = &g->nodes[nid];
        for (int i = 0; i < 2; i++) {
            if (n->inputs[i] && sp < (int)stack_cap)
                stack[sp++] = n->inputs[i]->id;
        }
        /* H4: 3-input ops (OP_IF, OP_SUBSTR, OP_REPLACE) store the third
           operand node ID as (uintptr_t)ext->literal. */
        if (n->opcode == OP_IF || n->opcode == OP_SUBSTR || n->opcode == OP_REPLACE) {
            td_op_ext_t* ext = find_ext(g, nid);
            if (ext) {
                uint32_t third_id = (uint32_t)(uintptr_t)ext->literal;
                if (third_id < nc && sp < (int)stack_cap)
                    stack[sp++] = third_id;
            }
        }
        /* H5: OP_CONCAT stores extra arg IDs (beyond inputs[0..1]) as
           uint32_t values in trailing bytes after the ext node.
           ext->sym holds the total arg count. */
        if (n->opcode == OP_CONCAT) {
            td_op_ext_t* ext = find_ext(g, nid);
            /* M4: Guard against ext->sym < 2 — trailing uint32_t values
               only exist when there are more than 2 arguments. */
            if (ext && ext->sym >= 2) {
                int n_args = (int)ext->sym;
                uint32_t* trail = (uint32_t*)((char*)(ext + 1));
                for (int i = 2; i < n_args; i++) {
                    uint32_t arg_id = trail[i - 2];
                    if (arg_id < nc && sp < (int)stack_cap)
                        stack[sp++] = arg_id;
                }
            }
        }
        /* H1: Traverse ext node children for structural ops so DCE does
           not incorrectly mark referenced nodes as dead. */
        if (n->opcode == OP_GROUP || n->opcode == OP_SORT ||
            n->opcode == OP_JOIN  || n->opcode == OP_WINDOW_JOIN ||
            n->opcode == OP_WINDOW ||
            n->opcode == OP_PROJECT || n->opcode == OP_SELECT) {
            td_op_ext_t* ext = find_ext(g, nid);
            if (ext) {
                switch (n->opcode) {
                    case OP_GROUP:
                        for (uint8_t k = 0; k < ext->n_keys; k++) {
                            if (ext->keys[k] && !live[ext->keys[k]->id] && sp < (int)stack_cap)
                                stack[sp++] = ext->keys[k]->id;
                        }
                        for (uint8_t a = 0; a < ext->n_aggs; a++) {
                            if (ext->agg_ins[a] && !live[ext->agg_ins[a]->id] && sp < (int)stack_cap)
                                stack[sp++] = ext->agg_ins[a]->id;
                        }
                        break;
                    case OP_SORT:
                    case OP_PROJECT:
                    case OP_SELECT:
                        for (uint8_t k = 0; k < ext->sort.n_cols; k++) {
                            if (ext->sort.columns[k] && !live[ext->sort.columns[k]->id] && sp < (int)stack_cap)
                                stack[sp++] = ext->sort.columns[k]->id;
                        }
                        break;
                    case OP_JOIN:
                    case OP_WINDOW_JOIN:
                        for (uint8_t k = 0; k < ext->join.n_join_keys; k++) {
                            if (ext->join.left_keys[k] && !live[ext->join.left_keys[k]->id] && sp < (int)stack_cap)
                                stack[sp++] = ext->join.left_keys[k]->id;
                            if (ext->join.right_keys && ext->join.right_keys[k] &&
                                !live[ext->join.right_keys[k]->id] && sp < (int)stack_cap)
                                stack[sp++] = ext->join.right_keys[k]->id;
                        }
                        break;
                    case OP_WINDOW:
                        for (uint8_t k = 0; k < ext->window.n_part_keys; k++) {
                            if (ext->window.part_keys[k] && !live[ext->window.part_keys[k]->id] && sp < (int)stack_cap)
                                stack[sp++] = ext->window.part_keys[k]->id;
                        }
                        for (uint8_t k = 0; k < ext->window.n_order_keys; k++) {
                            if (ext->window.order_keys[k] && !live[ext->window.order_keys[k]->id] && sp < (int)stack_cap)
                                stack[sp++] = ext->window.order_keys[k]->id;
                        }
                        for (uint8_t f = 0; f < ext->window.n_funcs; f++) {
                            if (ext->window.func_inputs[f] && !live[ext->window.func_inputs[f]->id] && sp < (int)stack_cap)
                                stack[sp++] = ext->window.func_inputs[f]->id;
                        }
                        break;
                    default:
                        break;
                }
            }
        }
    }
    if (stack_cap > 256) td_sys_free(stack);
}

static void pass_dce(td_graph_t* g, td_op_t* root) {
    uint32_t nc = g->node_count;
    bool* live;
    bool live_stack[256];
    if (nc <= 256) {
        live = live_stack;
    } else {
        live = (bool*)td_sys_alloc(nc * sizeof(bool));
        if (!live) return;
    }
    memset(live, 0, nc * sizeof(bool));

    mark_live(g, root, live);

    for (uint32_t i = 0; i < nc; i++) {
        if (!live[i]) {
            g->nodes[i].flags |= OP_FLAG_DEAD;
        }
    }
    if (nc > 256) td_sys_free(live);
}

/* --------------------------------------------------------------------------
 * Pass: SIP (Sideways Information Passing)
 *
 * Bottom-up DAG walk. For each OP_EXPAND:
 *   1. Find downstream filter on target side
 *   2. Reverse-CSR: mark source nodes that have any passing target -> TD_SEL
 *   3. Attach source_sel to upstream scan
 *
 * Currently a no-op placeholder — activated when graph ops are present.
 * -------------------------------------------------------------------------- */

/* Find downstream consumer of a node (first node that uses it as input) */
static td_op_t* find_consumer(td_graph_t* g, uint32_t node_id) {
    for (uint32_t i = 0; i < g->node_count; i++) {
        td_op_t* n = &g->nodes[i];
        if (n->flags & OP_FLAG_DEAD) continue;
        for (int j = 0; j < n->arity && j < 2; j++) {
            if (n->inputs[j] && n->inputs[j]->id == node_id)
                return n;
        }
    }
    return NULL;
}

/* Find upstream OP_SCAN that feeds into a node via input chain (iterative) */
static td_op_t* find_upstream_scan(td_graph_t* g, td_op_t* node) {
    uint32_t limit = g ? g->node_count : 1024;
    for (uint32_t steps = 0; node && steps < limit; steps++) {
        if (node->opcode == OP_SCAN) return node;
        if (node->arity > 0 && node->inputs[0])
            node = node->inputs[0];
        else return NULL;
    }
    return NULL;
}

static void sip_pass(td_graph_t* g, td_op_t* root) {
    if (!g || !root) return;

    uint32_t nc = g->node_count;

    /* Collect graph traversal nodes (bottom-up for chained SIP) */
    uint32_t expand_ids[64];
    uint32_t n_expands = 0;
    for (uint32_t i = 0; i < nc && n_expands < 64; i++) {
        td_op_t* n = &g->nodes[i];
        if (n->flags & OP_FLAG_DEAD) continue;
        if (n->opcode != OP_EXPAND && n->opcode != OP_VAR_EXPAND
            && n->opcode != OP_SHORTEST_PATH) continue;
        expand_ids[n_expands++] = i;
    }

    /* Process bottom-up (deepest expand first — process in reverse ID order
     * since deeper nodes in the pipeline tend to have higher IDs) */
    for (int ei = (int)n_expands - 1; ei >= 0; ei--) {
        td_op_t* expand = &g->nodes[expand_ids[ei]];
        td_op_ext_t* ext = find_ext(g, expand->id);
        if (!ext || !ext->graph.rel) continue;

        /* 1. Find downstream consumer — look for OP_FILTER on target side */
        td_op_t* consumer = find_consumer(g, expand->id);
        if (!consumer) continue;

        /* If the consumer is OP_FILTER, we can extract a semijoin.
         * The filter's condition restricts which target nodes pass.
         * We reverse-propagate through the CSR to mark which source
         * nodes could produce any passing target. */
        if (consumer->opcode != OP_FILTER) continue;

        /* 2. Find the input scan to this expand (source side) */
        td_op_t* src_scan = NULL;
        if (expand->arity > 0 && expand->inputs[0])
            src_scan = find_upstream_scan(g, expand->inputs[0]);

        if (!src_scan) continue;

        /* 3. Propagate backward: attach selection hint to the expand node.
         * The executor will use this to build a TD_SEL bitmap at runtime
         * by evaluating the filter condition, reverse-CSR propagating,
         * and applying the resulting source-side selection.
         *
         * We store the filter node ID in the expand's ext pad bytes
         * so the executor can find the downstream filter for runtime SIP. */
        /* pad[2] = 1 signals the executor to build SIP bitmap at runtime.
         * Note: pad is only 3 bytes (pad[0..2]) — do NOT write uint16_t
         * at pad+2 as that overflows into the 'id' field at offset 8. */
        ext->base.pad[2] = 1;
    }
}

/* --------------------------------------------------------------------------
 * Pass: Factorized detection
 *
 * Detect OP_EXPAND → OP_GROUP patterns where factorized execution
 * avoids materializing the full cross-product.
 * -------------------------------------------------------------------------- */
static void factorize_pass(td_graph_t* g, td_op_t* root) {
    if (!g || !root) return;

    uint32_t nc = g->node_count;
    for (uint32_t i = 0; i < nc; i++) {
        td_op_t* n = &g->nodes[i];
        if (n->flags & OP_FLAG_DEAD) continue;
        if (n->opcode != OP_EXPAND) continue;

        td_op_ext_t* ext = find_ext(g, n->id);
        if (!ext || ext->graph.factorized) continue;  /* already set by SIP pass */

        /* Look for immediate OP_GROUP consumer with _src as group key */
        td_op_t* consumer = find_consumer(g, n->id);
        if (!consumer || consumer->opcode != OP_GROUP) continue;

        td_op_ext_t* grp_ext = find_ext(g, consumer->id);
        if (!grp_ext || grp_ext->n_keys != 1 || !grp_ext->keys[0]) continue;

        td_op_ext_t* key_ext = find_ext(g, grp_ext->keys[0]->id);
        if (!key_ext || key_ext->base.opcode != OP_SCAN) continue;

        int64_t src_sym = td_sym_intern("_src", 4);
        if (key_ext->sym == src_sym) {
            ext->graph.factorized = 1;
        }
    }
}

/* --------------------------------------------------------------------------
 * Pass: Filter reordering
 *
 * Reorder chained OP_FILTER nodes so cheapest predicates execute first.
 * Also splits AND trees into separate chained filters.
 * -------------------------------------------------------------------------- */

/* Allocate a new node in the graph (for use during optimization passes).
 * Same logic as graph_alloc_node in graph.c but local to opt.c. */
static td_op_t* graph_alloc_node_opt(td_graph_t* g) {
    if (g->node_count >= g->node_cap) {
        if (g->node_cap > UINT32_MAX / 2) return NULL;
        uint32_t new_cap = g->node_cap * 2;
        uintptr_t old_base = (uintptr_t)g->nodes;
        td_op_t* new_nodes = (td_op_t*)td_sys_realloc(g->nodes,
                                                       new_cap * sizeof(td_op_t));
        if (!new_nodes) return NULL;
        g->nodes = new_nodes;
        g->node_cap = new_cap;
        /* Fix up all input pointers after realloc */
        ptrdiff_t delta = (ptrdiff_t)((uintptr_t)g->nodes - old_base);
        if (delta != 0) {
            for (uint32_t i = 0; i < g->node_count; i++) {
                if (g->nodes[i].inputs[0])
                    g->nodes[i].inputs[0] = (td_op_t*)((char*)g->nodes[i].inputs[0] + delta);
                if (g->nodes[i].inputs[1])
                    g->nodes[i].inputs[1] = (td_op_t*)((char*)g->nodes[i].inputs[1] + delta);
            }
            /* Fix ext node input pointers */
            for (uint32_t i = 0; i < g->ext_count; i++) {
                if (g->ext_nodes[i]) {
                    if (g->ext_nodes[i]->base.inputs[0])
                        g->ext_nodes[i]->base.inputs[0] =
                            (td_op_t*)((char*)g->ext_nodes[i]->base.inputs[0] + delta);
                    if (g->ext_nodes[i]->base.inputs[1])
                        g->ext_nodes[i]->base.inputs[1] =
                            (td_op_t*)((char*)g->ext_nodes[i]->base.inputs[1] + delta);
                    /* Fix structural op column pointers */
                    switch (g->ext_nodes[i]->base.opcode) {
                        case OP_GROUP:
                            for (uint8_t k = 0; k < g->ext_nodes[i]->n_keys; k++)
                                if (g->ext_nodes[i]->keys[k])
                                    g->ext_nodes[i]->keys[k] =
                                        (td_op_t*)((char*)g->ext_nodes[i]->keys[k] + delta);
                            for (uint8_t a = 0; a < g->ext_nodes[i]->n_aggs; a++)
                                if (g->ext_nodes[i]->agg_ins[a])
                                    g->ext_nodes[i]->agg_ins[a] =
                                        (td_op_t*)((char*)g->ext_nodes[i]->agg_ins[a] + delta);
                            break;
                        case OP_SORT:
                        case OP_PROJECT:
                        case OP_SELECT:
                            for (uint8_t k = 0; k < g->ext_nodes[i]->sort.n_cols; k++)
                                if (g->ext_nodes[i]->sort.columns[k])
                                    g->ext_nodes[i]->sort.columns[k] =
                                        (td_op_t*)((char*)g->ext_nodes[i]->sort.columns[k] + delta);
                            break;
                        case OP_JOIN:
                        case OP_WINDOW_JOIN:
                            for (uint8_t k = 0; k < g->ext_nodes[i]->join.n_join_keys; k++) {
                                if (g->ext_nodes[i]->join.left_keys[k])
                                    g->ext_nodes[i]->join.left_keys[k] =
                                        (td_op_t*)((char*)g->ext_nodes[i]->join.left_keys[k] + delta);
                                if (g->ext_nodes[i]->join.right_keys &&
                                    g->ext_nodes[i]->join.right_keys[k])
                                    g->ext_nodes[i]->join.right_keys[k] =
                                        (td_op_t*)((char*)g->ext_nodes[i]->join.right_keys[k] + delta);
                            }
                            break;
                        case OP_WINDOW:
                            for (uint8_t k = 0; k < g->ext_nodes[i]->window.n_part_keys; k++)
                                if (g->ext_nodes[i]->window.part_keys[k])
                                    g->ext_nodes[i]->window.part_keys[k] =
                                        (td_op_t*)((char*)g->ext_nodes[i]->window.part_keys[k] + delta);
                            for (uint8_t k = 0; k < g->ext_nodes[i]->window.n_order_keys; k++)
                                if (g->ext_nodes[i]->window.order_keys[k])
                                    g->ext_nodes[i]->window.order_keys[k] =
                                        (td_op_t*)((char*)g->ext_nodes[i]->window.order_keys[k] + delta);
                            for (uint8_t f = 0; f < g->ext_nodes[i]->window.n_funcs; f++)
                                if (g->ext_nodes[i]->window.func_inputs[f])
                                    g->ext_nodes[i]->window.func_inputs[f] =
                                        (td_op_t*)((char*)g->ext_nodes[i]->window.func_inputs[f] + delta);
                            break;
                        default:
                            break;
                    }
                }
            }
        }
    }
    td_op_t* n = &g->nodes[g->node_count];
    memset(n, 0, sizeof(td_op_t));
    n->id = g->node_count;
    g->node_count++;
    return n;
}

/* --------------------------------------------------------------------------
 * Pass: Predicate pushdown
 *
 * Move OP_FILTER nodes below PROJECT/SELECT, GROUP (key-only), JOIN
 * (one-sided), and EXPAND (source-only) to reduce rows flowing through
 * expensive operators.
 * -------------------------------------------------------------------------- */

/* Collect all OP_SCAN node IDs referenced by a predicate subtree.
 * Returns count (max 64). */
static int collect_pred_scans(td_graph_t* g, td_op_t* pred,
                              uint32_t* scan_ids, int max) {
    if (!pred || max <= 0) return 0;
    int n = 0;

    uint32_t stack[64];
    int sp = 0;
    stack[sp++] = pred->id;

    bool visited[4096];
    uint32_t nc = g->node_count;
    if (nc > 4096) return 0;  /* safety: skip for huge graphs */
    memset(visited, 0, nc * sizeof(bool));

    while (sp > 0 && n < max) {
        uint32_t nid = stack[--sp];
        if (nid >= nc || visited[nid]) continue;
        visited[nid] = true;
        td_op_t* node = &g->nodes[nid];
        if (node->flags & OP_FLAG_DEAD) continue;

        if (node->opcode == OP_SCAN) {
            scan_ids[n++] = nid;
            continue;
        }
        for (int i = 0; i < node->arity && i < 2; i++) {
            if (node->inputs[i] && sp < 64)
                stack[sp++] = node->inputs[i]->id;
        }
    }
    return n;
}

/* Redirect all consumers of old_id to point to new_target instead.
 * Skips nodes with IDs skip_a and skip_b (the swapped pair). */
static void redirect_consumers(td_graph_t* g, uint32_t old_id,
                               td_op_t* new_target,
                               uint32_t skip_a, uint32_t skip_b) {
    uint32_t nc = g->node_count;
    for (uint32_t j = 0; j < nc; j++) {
        td_op_t* c = &g->nodes[j];
        if (c->flags & OP_FLAG_DEAD || j == skip_a || j == skip_b) continue;
        for (int k = 0; k < c->arity && k < 2; k++) {
            if (c->inputs[k] && c->inputs[k]->id == old_id)
                c->inputs[k] = new_target;
        }
    }
}

static void pass_predicate_pushdown(td_graph_t* g, td_op_t* root) {
    if (!g || !root) return;

    /* Multiple iterations: pushdown may enable further pushdowns */
    for (int iter = 0; iter < 4; iter++) {
        bool changed = false;
        uint32_t nc = g->node_count;

        for (uint32_t i = 0; i < nc; i++) {
            td_op_t* n = &g->nodes[i];
            if (n->flags & OP_FLAG_DEAD) continue;
            if (n->opcode != OP_FILTER || n->arity != 2) continue;

            td_op_t* child = n->inputs[0];
            td_op_t* pred  = n->inputs[1];
            if (!child || !pred) continue;

            /* Push past PROJECT/SELECT/ALIAS */
            if (child->opcode == OP_PROJECT || child->opcode == OP_SELECT ||
                child->opcode == OP_ALIAS) {
                /* Swap: FILTER(pred, PROJ(x)) -> PROJ(FILTER(pred, x)) */
                td_op_t* proj_input = child->inputs[0];
                n->inputs[0] = proj_input;
                child->inputs[0] = n;
                redirect_consumers(g, n->id, child, child->id, n->id);
                changed = true;
                continue;
            }

            /* GROUP pushdown disabled: the executor's key/agg scans
             * bypass the filter, producing wrong results. Needs executor
             * support for filtered scan propagation before enabling. */

            /* Push past EXPAND (source-side predicates) */
            if (child->opcode == OP_EXPAND) {
                uint32_t scan_ids[64];
                int n_scans = collect_pred_scans(g, pred, scan_ids, 64);
                if (n_scans == 0) continue;

                /* All scans must be reachable from the expand's input side.
                 * Simple heuristic: check if all scans are above (lower ID than)
                 * the expand node. */
                bool all_source = true;
                for (int s = 0; s < n_scans; s++) {
                    if (scan_ids[s] >= child->id) {
                        all_source = false;
                        break;
                    }
                }
                if (!all_source) continue;

                /* Swap: FILTER(pred, EXPAND(src, rel)) -> EXPAND(FILTER(pred, src), rel) */
                td_op_t* expand_src = child->inputs[0];
                n->inputs[0] = expand_src;
                child->inputs[0] = n;
                redirect_consumers(g, n->id, child, child->id, n->id);
                changed = true;
                continue;
            }
        }
        if (!changed) break;
    }
}

/* Score a predicate subtree: lower = cheaper = execute first. */
static int filter_cost(td_graph_t* g, td_op_t* pred) {
    (void)g;
    if (!pred) return 99;
    int cost = 0;

    /* Constant comparison: one input is OP_CONST */
    bool has_const = false;
    for (int i = 0; i < pred->arity && i < 2; i++) {
        if (pred->inputs[i] && pred->inputs[i]->opcode == OP_CONST)
            has_const = true;
    }
    if (!has_const) cost += 4;  /* col-col comparison */

    /* Type width cost */
    int8_t t = pred->out_type;
    if (pred->arity >= 1 && pred->inputs[0])
        t = pred->inputs[0]->out_type;
    switch (t) {
        case TD_BOOL: case TD_U8:  cost += 0; break;
        case TD_I16:               cost += 1; break;
        case TD_I32:  case TD_DATE: case TD_TIME: cost += 2; break;
        default:                   cost += 3; break;  /* I64, F64, SYM, STR */
    }

    /* Comparison type cost */
    switch (pred->opcode) {
        case OP_EQ: case OP_NE:    cost += 0; break;
        case OP_LT: case OP_LE:
        case OP_GT: case OP_GE:    cost += 2; break;
        case OP_LIKE: case OP_ILIKE: cost += 4; break;
        default:                   cost += 1; break;
    }

    return cost;
}

/* Split FILTER(AND(a, b), input) into FILTER(a, FILTER(b, input)).
 * Returns the new outer filter node, or the original if no split. */
static td_op_t* split_and_filter(td_graph_t* g, td_op_t* filter_node) {
    if (!filter_node || filter_node->opcode != OP_FILTER) return filter_node;
    if (filter_node->arity != 2) return filter_node;

    td_op_t* pred = filter_node->inputs[1];
    if (!pred || pred->opcode != OP_AND || pred->arity != 2) return filter_node;

    td_op_t* pred_a = pred->inputs[0];
    td_op_t* pred_b = pred->inputs[1];
    td_op_t* input  = filter_node->inputs[0];
    if (!pred_a || !pred_b || !input) return filter_node;

    /* Save IDs before potential realloc */
    uint32_t filter_id = filter_node->id;
    uint32_t pred_b_id = pred_b->id;

    /* Rewrite: filter_node becomes FILTER(pred_a, input) */
    filter_node->inputs[1] = pred_a;
    g->nodes[filter_node->id] = *filter_node;

    /* Allocate new outer filter */
    td_op_t* outer = graph_alloc_node_opt(g);
    if (!outer) return &g->nodes[filter_id];  /* OOM: leave unsplit */

    /* Re-fetch after potential realloc */
    filter_node = &g->nodes[filter_id];
    pred_b = &g->nodes[pred_b_id];

    outer->opcode = OP_FILTER;
    outer->arity = 2;
    outer->inputs[0] = filter_node;
    outer->inputs[1] = pred_b;
    outer->out_type = filter_node->out_type;
    outer->est_rows = filter_node->est_rows;

    return outer;
}

/* Collect a chain of OP_FILTER nodes. Returns count (max 64). */
static int collect_filter_chain(td_op_t* top, td_op_t** chain, int max) {
    int n = 0;
    td_op_t* cur = top;
    while (cur && cur->opcode == OP_FILTER && n < max) {
        chain[n++] = cur;
        cur = cur->inputs[0];
    }
    return n;
}

static void pass_filter_reorder(td_graph_t* g, td_op_t* root) {
    if (!g || !root) return;

    uint32_t nc = g->node_count;

    /* First pass: split AND predicates in filters */
    for (uint32_t i = 0; i < nc; i++) {
        td_op_t* n = &g->nodes[i];
        if (n->flags & OP_FLAG_DEAD) continue;
        if (n->opcode != OP_FILTER) continue;
        if (n->arity != 2 || !n->inputs[1]) continue;
        if (n->inputs[1]->opcode != OP_AND) continue;

        /* Split AND and update consumers to point to new outer */
        td_op_t* new_outer = split_and_filter(g, n);
        if (new_outer != n) {
            /* Update all consumers of old node to point to new outer */
            uint32_t new_nc = g->node_count;
            for (uint32_t j = 0; j < new_nc; j++) {
                td_op_t* c = &g->nodes[j];
                if (c->flags & OP_FLAG_DEAD) continue;
                for (int k = 0; k < c->arity && k < 2; k++) {
                    if (c->inputs[k] && c->inputs[k]->id == n->id &&
                        c->id != new_outer->id) {
                        c->inputs[k] = new_outer;
                        g->nodes[j] = *c;
                    }
                }
            }
        }
    }

    /* Second pass: reorder filter chains by cost.
     * Use insertion sort on chain arrays (chains are typically short). */
    nc = g->node_count;  /* may have grown from splits */
    bool* visited = NULL;
    bool visited_stack[256];
    if (nc <= 256) {
        visited = visited_stack;
    } else {
        visited = (bool*)td_sys_alloc(nc * sizeof(bool));
        if (!visited) return;
    }
    memset(visited, 0, nc * sizeof(bool));

    for (uint32_t i = 0; i < nc; i++) {
        td_op_t* n = &g->nodes[i];
        if (n->flags & OP_FLAG_DEAD) continue;
        if (n->opcode != OP_FILTER) continue;
        if (visited[i]) continue;

        /* Collect the filter chain starting at this node */
        td_op_t* chain[64];
        int chain_len = collect_filter_chain(n, chain, 64);
        if (chain_len < 2) {
            for (int c = 0; c < chain_len; c++) visited[chain[c]->id] = true;
            continue;
        }

        /* Mark all as visited */
        for (int c = 0; c < chain_len; c++) visited[chain[c]->id] = true;

        /* Score each filter's predicate */
        int costs[64];
        for (int c = 0; c < chain_len; c++)
            costs[c] = filter_cost(g, chain[c]->inputs[1]);

        /* Insertion sort predicates by cost descending (stable: preserves
         * original order for equal costs). Expensive predicates go to
         * chain[0] (outer, runs last), cheap go to chain[N-1] (inner,
         * runs first). We swap predicates, not filter nodes. */
        for (int c = 1; c < chain_len; c++) {
            td_op_t* pred = chain[c]->inputs[1];
            int cost = costs[c];
            int j = c - 1;
            while (j >= 0 && costs[j] < cost) {
                chain[j + 1]->inputs[1] = chain[j]->inputs[1];
                g->nodes[chain[j + 1]->id].inputs[1] = chain[j]->inputs[1];
                costs[j + 1] = costs[j];
                j--;
            }
            chain[j + 1]->inputs[1] = pred;
            g->nodes[chain[j + 1]->id].inputs[1] = pred;
            costs[j + 1] = cost;
        }
    }

    if (nc > 256) td_sys_free(visited);
}

/* --------------------------------------------------------------------------
 * td_optimize — run all passes in order, return (possibly updated) root
 * -------------------------------------------------------------------------- */

td_op_t* td_optimize(td_graph_t* g, td_op_t* root) {
    if (!g || !root) return root;

    /* Pass 1: Type inference */
    pass_type_inference(g, root);

    /* Pass 2: Constant folding */
    pass_constant_fold(g, root);

    /* Pass 3: SIP (graph-aware sideways information passing) */
    sip_pass(g, root);

    /* Pass 4: Factorized detection (OP_EXPAND → OP_GROUP optimization) */
    factorize_pass(g, root);

    /* Pass 5: Predicate pushdown */
    pass_predicate_pushdown(g, root);

    /* Pass 6: Filter reordering (split ANDs + reorder by cost) */
    pass_filter_reorder(g, root);

    /* Pass 7: Fusion */
    td_fuse_pass(g, root);

    /* Pass 8: DCE */
    pass_dce(g, root);

    /* Return root — may have been replaced during folding.
       Use g->nodes[root_id] pattern for safety. */
    return &g->nodes[root->id];
}
