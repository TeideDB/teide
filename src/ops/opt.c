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
            folded = is_f64 ? td_f64(-vf) : td_i64(-vi);
            break;
        case OP_ABS:
            if (is_f64)
                folded = td_f64(fabs(vf));
            else
                folded = td_i64(vi < 0 ? -vi : vi);
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

static void sip_pass(td_graph_t* g, td_op_t* root) {
    if (!g || !root) return;

    /* Walk DAG looking for OP_EXPAND nodes. For now, this is a simple
     * pass that marks graph ops as reachable. Full SIP with backward
     * TD_SEL propagation requires knowing the target table and downstream
     * filter structure, which the planner provides. */
    uint32_t nc = g->node_count;
    for (uint32_t i = 0; i < nc; i++) {
        td_op_t* n = &g->nodes[i];
        if (n->flags & OP_FLAG_DEAD) continue;
        if (n->opcode != OP_EXPAND) continue;

        /* TODO: Full SIP implementation:
         * 1. Find downstream filter on target side
         * 2. Evaluate filter -> TD_SEL on target table
         * 3. Reverse-CSR: mark source nodes with passing targets
         * 4. Attach source_sel to upstream OP_SCAN */
    }
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

    /* Pass 4: Fusion */
    td_fuse_pass(g, root);

    /* Pass 5: DCE */
    pass_dce(g, root);

    /* Return root — may have been replaced during folding.
       Use g->nodes[root_id] pattern for safety. */
    return &g->nodes[root->id];
}
