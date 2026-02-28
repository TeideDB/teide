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

#include "exec.h"
#include "hash.h"
#include "pool.h"
#include "store/csr.h"
#include "lftj.h"
#include "mem/heap.h"
#include <string.h>
#include <math.h>
#include <stdio.h>
#include <float.h>
#include <ctype.h>

/* --------------------------------------------------------------------------
 * Arena-based scratch allocation helpers
 *
 * All temporary buffers use the buddy allocator instead of malloc/free.
 * td_alloc() returns a td_t* header; data starts at td_data(hdr).
 * -------------------------------------------------------------------------- */

/* Allocate zero-initialized scratch buffer, returns data pointer.
 * *hdr_out receives the td_t* header for later td_free(). */
static inline void* scratch_calloc(td_t** hdr_out, size_t nbytes) {
    td_t* h = td_alloc(nbytes);
    if (!h) { *hdr_out = NULL; return NULL; }
    void* p = td_data(h);
    memset(p, 0, nbytes);
    *hdr_out = h;
    return p;
}

/* Allocate uninitialized scratch buffer. */
static inline void* scratch_alloc(td_t** hdr_out, size_t nbytes) {
    td_t* h = td_alloc(nbytes);
    if (!h) { *hdr_out = NULL; return NULL; }
    *hdr_out = h;
    return td_data(h);
}

/* Reallocate: alloc new, copy old, free old. Returns new data pointer. */
static inline void* scratch_realloc(td_t** hdr_out, size_t old_bytes, size_t new_bytes) {
    td_t* old_h = *hdr_out;
    td_t* new_h = td_alloc(new_bytes);
    if (!new_h) return NULL;
    void* new_p = td_data(new_h);
    if (old_h) {
        memcpy(new_p, td_data(old_h), old_bytes < new_bytes ? old_bytes : new_bytes);
        td_free(old_h);
    }
    *hdr_out = new_h;
    return new_p;
}

/* Free a scratch buffer (NULL-safe). */
static inline void scratch_free(td_t* hdr) {
    if (!hdr) return;
    td_free(hdr);
}

/* --------------------------------------------------------------------------
 * Unified column read/write helpers
 *
 * Read any integer-representable column value as int64_t.
 * TD_SYM dispatches on attrs for adaptive width (W8/W16/W32/W64).
 * F64 is NOT handled — caller must check for F64 separately.
 * -------------------------------------------------------------------------- */

static inline int64_t read_col_i64(const void* data, int64_t row,
                                    int8_t type, uint8_t attrs) {
    switch (type) {
    case TD_I64: case TD_TIMESTAMP:
        return ((const int64_t*)data)[row];
    case TD_SYM:
        switch (attrs & TD_SYM_W_MASK) {
        case TD_SYM_W8:  return (int64_t)((const uint8_t*)data)[row];
        case TD_SYM_W16: return (int64_t)((const uint16_t*)data)[row];
        case TD_SYM_W32: return (int64_t)((const uint32_t*)data)[row];
        default:         return ((const int64_t*)data)[row];
        }
    case TD_I32: case TD_DATE: case TD_TIME:
        return (int64_t)((const int32_t*)data)[row];
    case TD_I16:
        return (int64_t)((const int16_t*)data)[row];
    default: /* TD_BOOL, TD_U8 */
        return (int64_t)((const uint8_t*)data)[row];
    }
}

static inline void write_col_i64(void* data, int64_t row, int64_t val,
                                  int8_t type, uint8_t attrs) {
    switch (type) {
    case TD_I64: case TD_TIMESTAMP:
        ((int64_t*)data)[row] = val; return;
    case TD_SYM:
        td_write_sym(data, row, (uint64_t)val, type, attrs); return;
    case TD_I32: case TD_DATE: case TD_TIME:
        ((int32_t*)data)[row] = (int32_t)val; return;
    case TD_I16:
        ((int16_t*)data)[row] = (int16_t)val; return;
    default: /* TD_BOOL, TD_U8 */
        ((uint8_t*)data)[row] = (uint8_t)val; return;
    }
}

/* --------------------------------------------------------------------------
 * TD_SYM-aware column helpers
 *
 * col_esz():      element size respecting TD_SYM adaptive width
 * col_vec_new():  create vector matching source column's type + width
 * -------------------------------------------------------------------------- */

static inline uint8_t col_esz(const td_t* col) {
    return td_sym_elem_size(col->type, col->attrs);
}

/* Fast key reader for DA/sort hot loops: elem_size is pre-computed and
 * loop-invariant, so the switch is always perfectly predicted.  Avoids the
 * td_read_sym → type dispatch chain (3+ branches per element). */
static inline int64_t read_by_esz(const void* data, int64_t row, uint8_t esz) {
    switch (esz) {
    case 1:  return (int64_t)((const uint8_t*)data)[row];
    case 2:  return (int64_t)((const uint16_t*)data)[row];
    case 4:  return (int64_t)((const uint32_t*)data)[row];
    default: return ((const int64_t*)data)[row];
    }
}

static inline td_t* col_vec_new(const td_t* src, int64_t cap) {
    if (src->type == TD_SYM)
        return td_sym_vec_new(src->attrs & TD_SYM_W_MASK, cap);
    return td_vec_new(src->type, cap);
}

/* Same but from explicit type + attrs (for parted base type, etc.) */
static inline td_t* typed_vec_new(int8_t type, uint8_t attrs, int64_t cap) {
    if (type == TD_SYM)
        return td_sym_vec_new(attrs & TD_SYM_W_MASK, cap);
    return td_vec_new(type, cap);
}

/* --------------------------------------------------------------------------
 * Cancellation check: returns true if the current query was cancelled.
 * Uses relaxed load — zero cost on x86 (piggybacks on existing cache line).
 * -------------------------------------------------------------------------- */

static inline bool pool_cancelled(td_pool_t* pool) {
    return pool && TD_UNLIKELY(atomic_load_explicit(&pool->cancelled,
                                                     memory_order_relaxed));
}

#define CHECK_CANCEL(pool)                                \
    do { if (pool_cancelled(pool))                        \
             return TD_ERR_PTR(TD_ERR_CANCEL); } while(0)

#define CHECK_CANCEL_GOTO(pool, lbl)                      \
    do { if (pool_cancelled(pool)) {                      \
             result = TD_ERR_PTR(TD_ERR_CANCEL);          \
             goto lbl;                                    \
         }                                                \
    } while(0)

/* --------------------------------------------------------------------------
 * Helper: find the extended node for a given base node ID.
 * O(ext_count) linear scan; acceptable for typical graph sizes (<100 ext nodes).
 * -------------------------------------------------------------------------- */

static td_op_ext_t* find_ext(td_graph_t* g, uint32_t node_id) {
    for (uint32_t i = 0; i < g->ext_count; i++) {
        if (g->ext_nodes[i] && g->ext_nodes[i]->base.id == node_id)
            return g->ext_nodes[i];
    }
    return NULL;
}

/* --------------------------------------------------------------------------
 * Materialize a MAPCOMMON column into a flat TD_SYM vector.
 * Expands key_values × row_counts into one SYM ID per row.
 * -------------------------------------------------------------------------- */
static td_t* materialize_mapcommon(td_t* mc) {
    td_t** mc_ptrs = (td_t**)td_data(mc);
    td_t* kv = mc_ptrs[0];   /* key_values: typed vec (DATE/I64/SYM) */
    td_t* rc = mc_ptrs[1];   /* row_counts: TD_I64 vec of n_parts */
    int64_t n_parts = kv->len;
    int8_t kv_type = kv->type;
    size_t esz = (size_t)td_sym_elem_size(kv_type, kv->attrs);
    const char* kdata = (const char*)td_data(kv);
    const int64_t* counts = (const int64_t*)td_data(rc);

    int64_t total = 0;
    for (int64_t p = 0; p < n_parts; p++) total += counts[p];

    td_t* flat = td_vec_new(kv_type, total);
    if (!flat || TD_IS_ERR(flat)) return TD_ERR_PTR(TD_ERR_OOM);
    flat->len = total;

    /* Pattern-fill: broadcast each partition's key value across its row range.
     * Typed fill avoids per-element memcpy overhead. */
    char* out = (char*)td_data(flat);
    int64_t off = 0;
    for (int64_t p = 0; p < n_parts; p++) {
        int64_t cnt = counts[p];
        if (esz == 8) {
            uint64_t v;
            memcpy(&v, kdata + (size_t)p * 8, 8);
            uint64_t* dst = (uint64_t*)(out + off * 8);
            for (int64_t r = 0; r < cnt; r++) dst[r] = v;
        } else if (esz == 4) {
            uint32_t v;
            memcpy(&v, kdata + (size_t)p * 4, 4);
            uint32_t* dst = (uint32_t*)(out + off * 4);
            for (int64_t r = 0; r < cnt; r++) dst[r] = v;
        } else {
            for (int64_t r = 0; r < cnt; r++)
                memcpy(out + (off + r) * esz, kdata + (size_t)p * esz, esz);
        }
        off += cnt;
    }
    return flat;
}

/* Materialize first N rows of a MAPCOMMON column into a flat typed vector. */
static td_t* materialize_mapcommon_head(td_t* mc, int64_t n) {
    td_t** mc_ptrs = (td_t**)td_data(mc);
    td_t* kv = mc_ptrs[0];
    td_t* rc = mc_ptrs[1];
    int64_t n_parts = kv->len;
    int8_t kv_type = kv->type;
    size_t esz = (size_t)td_sym_elem_size(kv_type, kv->attrs);
    const char* kdata = (const char*)td_data(kv);
    const int64_t* counts = (const int64_t*)td_data(rc);

    td_t* flat = td_vec_new(kv_type, n);
    if (!flat || TD_IS_ERR(flat)) return TD_ERR_PTR(TD_ERR_OOM);
    flat->len = n;

    char* out = (char*)td_data(flat);
    int64_t off = 0;
    for (int64_t p = 0; p < n_parts && off < n; p++) {
        int64_t take = counts[p];
        if (take > n - off) take = n - off;
        if (esz == 8) {
            uint64_t v;
            memcpy(&v, kdata + (size_t)p * 8, 8);
            uint64_t* dst = (uint64_t*)(out + off * 8);
            for (int64_t r = 0; r < take; r++) dst[r] = v;
        } else if (esz == 4) {
            uint32_t v;
            memcpy(&v, kdata + (size_t)p * 4, 4);
            uint32_t* dst = (uint32_t*)(out + off * 4);
            for (int64_t r = 0; r < take; r++) dst[r] = v;
        } else {
            for (int64_t r = 0; r < take; r++)
                memcpy(out + (off + r) * esz, kdata + (size_t)p * esz, esz);
        }
        off += take;
    }
    return flat;
}

/* Materialize MAPCOMMON through a boolean filter predicate. */
static td_t* materialize_mapcommon_filter(td_t* mc, td_t* pred, int64_t pass_count) {
    td_t** mc_ptrs = (td_t**)td_data(mc);
    td_t* kv = mc_ptrs[0];
    td_t* rc = mc_ptrs[1];
    int64_t n_parts = kv->len;
    int8_t kv_type = kv->type;
    size_t esz = (size_t)td_sym_elem_size(kv_type, kv->attrs);
    const char* kdata = (const char*)td_data(kv);
    const int64_t* counts = (const int64_t*)td_data(rc);

    td_t* flat = td_vec_new(kv_type, pass_count);
    if (!flat || TD_IS_ERR(flat)) return TD_ERR_PTR(TD_ERR_OOM);
    flat->len = pass_count;

    char* out = (char*)td_data(flat);
    int64_t out_idx = 0;
    int64_t row = 0;
    int64_t part_idx = 0;
    int64_t part_end = counts[0];

    td_morsel_t mp;
    td_morsel_init(&mp, pred);
    while (td_morsel_next(&mp)) {
        const uint8_t* bits = (const uint8_t*)mp.morsel_ptr;
        for (int64_t i = 0; i < mp.morsel_len; i++, row++) {
            while (part_idx < n_parts - 1 && row >= part_end) {
                part_idx++;
                part_end += counts[part_idx];
            }
            if (bits[i])
                memcpy(out + (size_t)out_idx++ * esz,
                       kdata + (size_t)part_idx * esz, esz);
        }
    }
    return flat;
}

typedef struct {
    bool    enabled;
    double  bias_f64;
    int64_t bias_i64;
} agg_affine_t;

#define AGG_LINEAR_MAX_TERMS 8

typedef struct {
    bool    enabled;
    uint8_t n_terms;
    void*   term_ptrs[AGG_LINEAR_MAX_TERMS];
    int8_t  term_types[AGG_LINEAR_MAX_TERMS];
    int64_t coeff_i64[AGG_LINEAR_MAX_TERMS];
    int64_t bias_i64;
} agg_linear_t;

typedef struct {
    uint8_t n_terms;
    int64_t syms[AGG_LINEAR_MAX_TERMS];
    int64_t coeff_i64[AGG_LINEAR_MAX_TERMS];
    int64_t bias_i64;
} linear_expr_i64_t;

static bool atom_to_numeric(td_t* atom, double* out_f, int64_t* out_i, bool* out_is_f64) {
    if (!atom || !td_is_atom(atom)) return false;
    switch (atom->type) {
        case TD_ATOM_F64:
            *out_f = atom->f64;
            *out_i = (int64_t)atom->f64;
            *out_is_f64 = true;
            return true;
        case TD_ATOM_I64:
        case TD_ATOM_SYM:
        case TD_ATOM_DATE:
        case TD_ATOM_TIME:
        case TD_ATOM_TIMESTAMP:
            *out_i = atom->i64;
            *out_f = (double)atom->i64;
            *out_is_f64 = false;
            return true;
        case TD_ATOM_I32:
            *out_i = (int64_t)atom->i32;
            *out_f = (double)atom->i32;
            *out_is_f64 = false;
            return true;
        case TD_ATOM_I16:
            *out_i = (int64_t)atom->i16;
            *out_f = (double)atom->i16;
            *out_is_f64 = false;
            return true;
        case TD_ATOM_U8:
        case TD_ATOM_BOOL:
            *out_i = (int64_t)atom->u8;
            *out_f = (double)atom->u8;
            *out_is_f64 = false;
            return true;
        default:
            return false;
    }
}

/* Evaluate a numeric constant sub-expression from op graph.
 * Supports CONST and arithmetic trees over constant children. */
static bool eval_const_numeric_expr(td_graph_t* g, td_op_t* op,
                                    double* out_f, int64_t* out_i, bool* out_is_f64) {
    if (!g || !op || !out_f || !out_i || !out_is_f64) return false;

    if (op->opcode == OP_CONST) {
        td_op_ext_t* ext = find_ext(g, op->id);
        if (!ext || !ext->literal) return false;
        return atom_to_numeric(ext->literal, out_f, out_i, out_is_f64);
    }

    if ((op->opcode == OP_NEG || op->opcode == OP_ABS) && op->arity == 1 && op->inputs[0]) {
        double af = 0.0;
        int64_t ai = 0;
        bool a_is_f64 = false;
        if (!eval_const_numeric_expr(g, op->inputs[0], &af, &ai, &a_is_f64)) return false;
        if (a_is_f64 || op->out_type == TD_F64) {
            double v = a_is_f64 ? af : (double)ai;
            double r = (op->opcode == OP_NEG) ? -v : fabs(v);
            *out_f = r;
            *out_i = (int64_t)r;
            *out_is_f64 = true;
            return true;
        }
        int64_t v = ai;
        /* Unsigned negation avoids UB on INT64_MIN */
        int64_t r = (op->opcode == OP_NEG)
                  ? (int64_t)(-(uint64_t)v)
                  : (v < 0 ? (int64_t)(-(uint64_t)v) : v);
        *out_i = r;
        *out_f = (double)r;
        *out_is_f64 = false;
        return true;
    }

    if (op->arity != 2 || !op->inputs[0] || !op->inputs[1]) return false;
    if (op->opcode < OP_ADD || op->opcode > OP_MAX2) return false;

    double lf = 0.0, rf = 0.0;
    int64_t li = 0, ri = 0;
    bool l_is_f64 = false, r_is_f64 = false;
    if (!eval_const_numeric_expr(g, op->inputs[0], &lf, &li, &l_is_f64)) return false;
    if (!eval_const_numeric_expr(g, op->inputs[1], &rf, &ri, &r_is_f64)) return false;

    if (op->out_type == TD_F64 || l_is_f64 || r_is_f64 || op->opcode == OP_DIV) {
        double lv = l_is_f64 ? lf : (double)li;
        double rv = r_is_f64 ? rf : (double)ri;
        double r = 0.0;
        switch (op->opcode) {
            case OP_ADD: r = lv + rv; break;
            case OP_SUB: r = lv - rv; break;
            case OP_MUL: r = lv * rv; break;
            case OP_DIV: r = rv != 0.0 ? lv / rv : 0.0; break;
            case OP_MOD: r = rv != 0.0 ? fmod(lv, rv) : 0.0; break;
            case OP_MIN2: r = lv < rv ? lv : rv; break;
            case OP_MAX2: r = lv > rv ? lv : rv; break;
            default: return false;
        }
        *out_f = r;
        *out_i = (int64_t)r;
        *out_is_f64 = true;
        return true;
    }

    int64_t r = 0;
    switch (op->opcode) {
        /* Use uint64_t casts to get defined wrapping on overflow */
        case OP_ADD: r = (int64_t)((uint64_t)li + (uint64_t)ri); break;
        case OP_SUB: r = (int64_t)((uint64_t)li - (uint64_t)ri); break;
        case OP_MUL: r = (int64_t)((uint64_t)li * (uint64_t)ri); break;
        case OP_DIV: r = (ri != 0 && !(li == INT64_MIN && ri == -1)) ? li / ri : 0; break;
        case OP_MOD: r = (ri != 0 && !(li == INT64_MIN && ri == -1)) ? li % ri : 0; break;
        case OP_MIN2: r = li < ri ? li : ri; break;
        case OP_MAX2: r = li > ri ? li : ri; break;
        default: return false;
    }
    *out_i = r;
    *out_f = (double)r;
    *out_is_f64 = false;
    return true;
}

static bool const_expr_to_i64(td_graph_t* g, td_op_t* op, int64_t* out) {
    if (!g || !op || !out) return false;
    double c_f = 0.0;
    int64_t c_i = 0;
    bool c_is_f64 = false;
    if (!eval_const_numeric_expr(g, op, &c_f, &c_i, &c_is_f64)) return false;
    if (!c_is_f64) {
        *out = c_i;
        return true;
    }
    if (!isfinite(c_f)) return false;
    double ip = 0.0;
    if (modf(c_f, &ip) != 0.0) return false;
    if (ip > (double)INT64_MAX || ip < (double)INT64_MIN) return false;
    *out = (int64_t)ip;
    return true;
}

static inline bool type_is_linear_i64_col(int8_t t) {
    return t == TD_I64 || t == TD_TIMESTAMP ||
           t == TD_I32 || t == TD_DATE || t == TD_TIME || t == TD_I16 ||
           t == TD_U8 || t == TD_BOOL || TD_IS_SYM(t);
}

static bool linear_expr_add_term(linear_expr_i64_t* e, int64_t sym, int64_t coeff) {
    if (!e) return false;
    if (coeff == 0) return true;
    for (uint8_t i = 0; i < e->n_terms; i++) {
        if (e->syms[i] != sym) continue;
        int64_t next = e->coeff_i64[i] + coeff;
        if (next != 0) {
            e->coeff_i64[i] = next;
            return true;
        }
        for (uint8_t j = i + 1; j < e->n_terms; j++) {
            e->syms[j - 1] = e->syms[j];
            e->coeff_i64[j - 1] = e->coeff_i64[j];
        }
        e->n_terms--;
        return true;
    }
    if (e->n_terms >= AGG_LINEAR_MAX_TERMS) return false;
    e->syms[e->n_terms] = sym;
    e->coeff_i64[e->n_terms] = coeff;
    e->n_terms++;
    return true;
}

static void linear_expr_scale(linear_expr_i64_t* e, int64_t k) {
    if (!e || k == 1) return;
    e->bias_i64 *= k;
    for (uint8_t i = 0; i < e->n_terms; i++)
        e->coeff_i64[i] *= k;
}

static bool linear_expr_add_scaled(linear_expr_i64_t* dst, const linear_expr_i64_t* src, int64_t scale) {
    if (!dst || !src) return false;
    dst->bias_i64 += src->bias_i64 * scale;
    for (uint8_t i = 0; i < src->n_terms; i++) {
        if (!linear_expr_add_term(dst, src->syms[i], src->coeff_i64[i] * scale))
            return false;
    }
    return true;
}

/* Parse an expression tree into integer linear form:
 *   sum(coeff[i] * scan(sym[i])) + bias
 * Supports +, -, unary -, and multiplication by integer constants. */
static bool parse_linear_i64_expr(td_graph_t* g, td_op_t* op, linear_expr_i64_t* out) {
    if (!g || !op || !out) return false;
    memset(out, 0, sizeof(*out));

    int64_t c = 0;
    if (const_expr_to_i64(g, op, &c)) {
        out->bias_i64 = c;
        return true;
    }

    if (op->opcode == OP_SCAN) {
        td_op_ext_t* ext = find_ext(g, op->id);
        if (!ext || ext->base.opcode != OP_SCAN) return false;
        out->n_terms = 1;
        out->syms[0] = ext->sym;
        out->coeff_i64[0] = 1;
        return true;
    }

    if (op->opcode == OP_NEG && op->arity == 1 && op->inputs[0]) {
        linear_expr_i64_t inner;
        if (!parse_linear_i64_expr(g, op->inputs[0], &inner)) return false;
        linear_expr_scale(&inner, -1);
        *out = inner;
        return true;
    }

    if ((op->opcode == OP_ADD || op->opcode == OP_SUB) &&
        op->arity == 2 && op->inputs[0] && op->inputs[1]) {
        linear_expr_i64_t lhs;
        linear_expr_i64_t rhs;
        if (!parse_linear_i64_expr(g, op->inputs[0], &lhs)) return false;
        if (!parse_linear_i64_expr(g, op->inputs[1], &rhs)) return false;
        *out = lhs;
        return linear_expr_add_scaled(out, &rhs, op->opcode == OP_ADD ? 1 : -1);
    }

    if (op->opcode == OP_MUL && op->arity == 2 && op->inputs[0] && op->inputs[1]) {
        int64_t k = 0;
        linear_expr_i64_t side;
        if (const_expr_to_i64(g, op->inputs[0], &k) &&
            parse_linear_i64_expr(g, op->inputs[1], &side)) {
            linear_expr_scale(&side, k);
            *out = side;
            return true;
        }
        if (const_expr_to_i64(g, op->inputs[1], &k) &&
            parse_linear_i64_expr(g, op->inputs[0], &side)) {
            linear_expr_scale(&side, k);
            *out = side;
            return true;
        }
    }

    return false;
}

/* Detect SUM/AVG integer-linear inputs for scalar aggregate fast path.
 * Example: (v1 + 1) * 2, v1 + v2 + 1 */
static bool try_linear_sumavg_input_i64(td_graph_t* g, td_t* tbl, td_op_t* input_op,
                                        agg_linear_t* out_plan) {
    if (!g || !tbl || !input_op || !out_plan) return false;
    linear_expr_i64_t lin;
    if (!parse_linear_i64_expr(g, input_op, &lin)) return false;

    memset(out_plan, 0, sizeof(*out_plan));
    out_plan->n_terms = lin.n_terms;
    out_plan->bias_i64 = lin.bias_i64;
    for (uint8_t i = 0; i < lin.n_terms; i++) {
        td_t* col = td_table_get_col(tbl, lin.syms[i]);
        if (!col || !type_is_linear_i64_col(col->type)) return false;
        out_plan->term_ptrs[i] = td_data(col);
        out_plan->term_types[i] = col->type;
        out_plan->coeff_i64[i] = lin.coeff_i64[i];
    }
    out_plan->enabled = true;
    return true;
}

/* Detect SUM/AVG affine inputs of form (scan +/- const) and return scan vector
 * plus the additive bias so we can adjust results from (sum,count) directly. */
static bool try_affine_sumavg_input(td_graph_t* g, td_t* tbl, td_op_t* input_op,
                                    td_t** out_vec, agg_affine_t* out_affine) {
    if (!g || !tbl || !input_op || !out_vec || !out_affine) return false;
    if (input_op->opcode != OP_ADD && input_op->opcode != OP_SUB) return false;
    if (input_op->arity != 2 || !input_op->inputs[0] || !input_op->inputs[1]) return false;

    td_op_t* lhs = input_op->inputs[0];
    td_op_t* rhs = input_op->inputs[1];
    td_op_t* base_op = NULL;
    int sign = 1;
    double c_f = 0.0;
    int64_t c_i = 0;
    bool c_is_f64 = false;

    double lhs_f = 0.0, rhs_f = 0.0;
    int64_t lhs_i = 0, rhs_i = 0;
    bool lhs_is_f64 = false, rhs_is_f64 = false;
    bool lhs_const = eval_const_numeric_expr(g, lhs, &lhs_f, &lhs_i, &lhs_is_f64);
    bool rhs_const = eval_const_numeric_expr(g, rhs, &rhs_f, &rhs_i, &rhs_is_f64);

    if (input_op->opcode == OP_ADD) {
        if (lhs_const) {
            base_op = rhs;
            sign = 1;
            c_f = lhs_f;
            c_i = lhs_i;
            c_is_f64 = lhs_is_f64;
        } else if (rhs_const) {
            base_op = lhs;
            sign = 1;
            c_f = rhs_f;
            c_i = rhs_i;
            c_is_f64 = rhs_is_f64;
        }
    } else { /* OP_SUB */
        if (rhs_const) {
            base_op = lhs;
            sign = -1;
            c_f = rhs_f;
            c_i = rhs_i;
            c_is_f64 = rhs_is_f64;
        }
    }
    if (!base_op) return false;

    td_op_ext_t* base_ext = find_ext(g, base_op->id);
    if (!base_ext || base_ext->base.opcode != OP_SCAN) return false;
    td_t* base_vec = td_table_get_col(tbl, base_ext->sym);
    if (!base_vec) return false;

    int8_t bt = base_vec->type;
    if (bt == TD_F64) {
        out_affine->enabled = true;
        out_affine->bias_f64 = (double)sign * (c_is_f64 ? c_f : (double)c_i);
        out_affine->bias_i64 = (int64_t)out_affine->bias_f64;
        *out_vec = base_vec;
        return true;
    }

    if (bt == TD_I64 || bt == TD_TIMESTAMP ||
        bt == TD_I32 || bt == TD_I16 || bt == TD_U8 || bt == TD_BOOL ||
        TD_IS_SYM(bt)) {
        int64_t c = 0;
        if (c_is_f64) {
            if (!isfinite(c_f)) return false;
            double ip = 0.0;
            if (modf(c_f, &ip) != 0.0) return false;
            if (ip > (double)INT64_MAX || ip < (double)INT64_MIN) return false;
            c = (int64_t)ip;
        } else {
            c = c_i;
        }
        out_affine->enabled = true;
        out_affine->bias_i64 = sign > 0 ? c : -c;
        out_affine->bias_f64 = (double)out_affine->bias_i64;
        *out_vec = base_vec;
        return true;
    }

    return false;
}

/* ============================================================================
 * Expression Compiler: morsel-batched fused evaluation
 *
 * Compiles an expression DAG (e.g. v1 + v2 * 3) into a flat instruction
 * array. Evaluates in morsel-sized chunks (1024 elements) with scratch
 * registers — never allocates full-length intermediate vectors.
 * ============================================================================ */

#define EXPR_MAX_REGS 16
#define EXPR_MAX_INS  48
#define EXPR_MORSEL   TD_MORSEL_ELEMS

typedef struct {
    uint8_t opcode;     /* OP_ADD, OP_NEG, OP_CAST, etc. */
    uint8_t dst;        /* destination register */
    uint8_t src1;       /* source 1 register */
    uint8_t src2;       /* source 2 register (0xFF for unary) */
} expr_ins_t;

enum { REG_SCAN = 0, REG_CONST = 1, REG_SCRATCH = 2 };

typedef struct {
    uint8_t n_ins;
    uint8_t n_regs;
    uint8_t n_scratch;      /* scratch registers needed */
    uint8_t out_reg;
    int8_t  out_type;       /* TD_F64, TD_I64, or TD_BOOL */
    bool    has_parted;     /* true if any REG_SCAN refs a parted column */
    struct {
        uint8_t     kind;       /* REG_SCAN / REG_CONST / REG_SCRATCH */
        int8_t      type;       /* computational type: TD_F64 / TD_I64 / TD_BOOL */
        int8_t      col_type;   /* original column type (REG_SCAN only) */
        uint8_t     col_attrs;  /* column attrs — TD_SYM width (REG_SCAN only) */
        bool        is_parted;  /* true if this SCAN refs a parted column */
        const void* data;       /* column data pointer (REG_SCAN only) */
        td_t*       parted_col; /* parted wrapper (is_parted only) */
        double      const_f64;  /* scalar value (REG_CONST) */
        int64_t     const_i64;  /* scalar value (REG_CONST) */
    } regs[EXPR_MAX_REGS];
    expr_ins_t ins[EXPR_MAX_INS];
} td_expr_t;

/* Is this opcode an element-wise op suitable for expression compilation? */
static inline bool expr_is_elementwise(uint16_t op) {
    return (op >= OP_NEG && op <= OP_CAST) || (op >= OP_ADD && op <= OP_MAX2);
}

/* Insert CAST instruction to promote register to target type */
static uint8_t expr_ensure_type(td_expr_t* out, uint8_t src, int8_t target) {
    if (out->regs[src].type == target) return src;
    if (out->n_regs >= EXPR_MAX_REGS || out->n_ins >= EXPR_MAX_INS) return src;
    uint8_t r = out->n_regs;
    out->regs[r].kind = REG_SCRATCH;
    out->regs[r].type = target;
    out->n_regs++;
    out->n_scratch++;
    out->ins[out->n_ins++] = (expr_ins_t){
        .opcode = OP_CAST, .dst = r, .src1 = src, .src2 = 0xFF,
    };
    return r;
}

/* Compile expression DAG into flat instruction array.
 * Returns true on success. Only compiles element-wise subtrees. */
static bool expr_compile(td_graph_t* g, td_t* tbl, td_op_t* root, td_expr_t* out) {
    memset(out, 0, sizeof(*out));
    if (!root || !g || !tbl) return false;
    if (root->opcode == OP_SCAN || root->opcode == OP_CONST) return false;
    if (!expr_is_elementwise(root->opcode)) return false;

    uint32_t nc = g->node_count;
    if (nc > 4096) return false; /* guard against stack overflow from VLA */
    uint8_t node_reg[nc];
    memset(node_reg, 0xFF, nc * sizeof(uint8_t));

    /* Post-order DFS with explicit stack */
    /* Depth limit 64 — expressions deeper than 64 levels fall back to non-fused path. */
    typedef struct { td_op_t* node; uint8_t phase; } dfs_t;
    dfs_t dfs[64];
    int sp = 0;
    dfs[sp++] = (dfs_t){root, 0};

    while (sp > 0) {
        dfs_t* top = &dfs[sp - 1];
        td_op_t* node = top->node;

        if (node->id < nc && node_reg[node->id] != 0xFF) { sp--; continue; }

        if (top->phase == 0) {
            top->phase = 1;
            for (int i = node->arity - 1; i >= 0; i--) {
                td_op_t* ch = node->inputs[i];
                if (!ch) continue;
                if (ch->id < nc && node_reg[ch->id] != 0xFF) continue;
                if (sp >= 64) return false;
                dfs[sp++] = (dfs_t){ch, 0};
            }
        } else {
            sp--;
            uint8_t r = out->n_regs;
            if (r >= EXPR_MAX_REGS) return false;

            if (node->opcode == OP_SCAN) {
                td_op_ext_t* ext = find_ext(g, node->id);
                if (!ext) return false;
                td_t* col = td_table_get_col(tbl, ext->sym);
                if (!col) return false;
                if (col->type == TD_MAPCOMMON) return false;
                out->regs[r].kind = REG_SCAN;
                if (TD_IS_PARTED(col->type)) {
                    int8_t base = (int8_t)TD_PARTED_BASETYPE(col->type);
                    out->regs[r].col_type = base;
                    out->regs[r].data = NULL; /* resolved per-segment */
                    out->regs[r].is_parted = true;
                    out->regs[r].parted_col = col;
                    out->regs[r].type = (base == TD_F64) ? TD_F64 : TD_I64;
                    out->has_parted = true;
                } else {
                    out->regs[r].col_type = col->type;
                    out->regs[r].col_attrs = col->attrs;
                    out->regs[r].data = td_data(col);
                    out->regs[r].is_parted = false;
                    out->regs[r].parted_col = NULL;
                    out->regs[r].type = (col->type == TD_F64) ? TD_F64 : TD_I64;
                }
            } else if (node->opcode == OP_CONST) {
                td_op_ext_t* ext = find_ext(g, node->id);
                if (!ext || !ext->literal) return false;
                double cf; int64_t ci; bool is_f64;
                if (!atom_to_numeric(ext->literal, &cf, &ci, &is_f64)) {
                    /* Try resolving string constant to symbol intern ID —
                     * enables fused evaluation of SYM column comparisons
                     * (e.g. id2 = 'id080' compiles to integer EQ). */
                    if (ext->literal->type == TD_ATOM_STR) {
                        const char* s = td_str_ptr(ext->literal);
                        size_t slen = td_str_len(ext->literal);
                        int64_t sid = td_sym_find(s, slen);
                        if (sid < 0) return false;
                        ci = sid;
                        cf = (double)sid;
                        is_f64 = false;
                    } else {
                        return false;
                    }
                }
                out->regs[r].kind = REG_CONST;
                out->regs[r].type = is_f64 ? TD_F64 : TD_I64;
                out->regs[r].const_f64 = cf;
                out->regs[r].const_i64 = ci;
            } else if (expr_is_elementwise(node->opcode)) {
                if (!node->inputs[0]) return false;
                uint8_t s1 = node_reg[node->inputs[0]->id];
                if (s1 == 0xFF) return false;
                uint8_t s2 = 0xFF;
                if (node->arity >= 2 && node->inputs[1]) {
                    s2 = node_reg[node->inputs[1]->id];
                    if (s2 == 0xFF) return false;
                }

                int8_t t1 = out->regs[s1].type;
                int8_t t2 = (s2 != 0xFF) ? out->regs[s2].type : t1;
                uint16_t op = node->opcode;
                int8_t ot;

                /* Determine output type */
                if (op == OP_CAST)
                    ot = node->out_type;
                else if ((op >= OP_EQ && op <= OP_GE) ||
                    op == OP_AND || op == OP_OR || op == OP_NOT)
                    ot = TD_BOOL;
                else if (t1 == TD_F64 || t2 == TD_F64 || op == OP_DIV ||
                         op == OP_SQRT || op == OP_LOG || op == OP_EXP)
                    ot = TD_F64;
                else
                    ot = TD_I64;

                /* Type promotion: ensure both sources match for the operation.
                 * Skip for OP_CAST — the instruction itself IS the conversion. */
                if (op == OP_CAST) {
                    /* No promotion needed; CAST handles the conversion */
                    r = out->n_regs;
                } else if (ot == TD_F64 && s2 != 0xFF) {
                    /* Arithmetic with f64 output — promote i64 inputs to f64 */
                    s1 = expr_ensure_type(out, s1, TD_F64);
                    s2 = expr_ensure_type(out, s2, TD_F64);
                    r = out->n_regs; /* re-read after possible CAST inserts */
                    if (r >= EXPR_MAX_REGS) return false;
                } else if (ot == TD_F64 && s2 == 0xFF) {
                    /* Unary f64 — promote input */
                    s1 = expr_ensure_type(out, s1, TD_F64);
                    r = out->n_regs;
                    if (r >= EXPR_MAX_REGS) return false;
                } else if (ot == TD_BOOL && s2 != 0xFF && t1 != t2) {
                    /* Comparison with mixed types — promote both to f64 */
                    int8_t pt = (t1 == TD_F64 || t2 == TD_F64) ? TD_F64 : TD_I64;
                    s1 = expr_ensure_type(out, s1, pt);
                    s2 = expr_ensure_type(out, s2, pt);
                    r = out->n_regs;
                    if (r >= EXPR_MAX_REGS) return false;
                }

                out->regs[r].kind = REG_SCRATCH;
                out->regs[r].type = ot;
                out->n_scratch++;

                if (out->n_ins >= EXPR_MAX_INS) return false;
                out->ins[out->n_ins++] = (expr_ins_t){
                    .opcode = (uint8_t)op, .dst = r, .src1 = s1, .src2 = s2,
                };
            } else {
                return false;
            }

            out->n_regs++;
            if (node->id < nc) node_reg[node->id] = r;
        }
    }

    if (out->n_regs == 0 || out->n_ins == 0) return false;
    out->out_reg = out->n_regs - 1;
    out->out_type = out->regs[out->out_reg].type;
    return true;
}

/* ---- Morsel-batched expression evaluator ---- */

/* Load SCAN column data into i64 scratch buffer with type conversion */
static void expr_load_i64(int64_t* dst, const void* data, int8_t col_type,
                          uint8_t col_attrs, int64_t start, int64_t n) {
    switch (col_type) {
        case TD_I64: case TD_TIMESTAMP:
            memcpy(dst, (const int64_t*)data + start, (size_t)n * 8);
            break;
        case TD_SYM: {
            for (int64_t j = 0; j < n; j++)
                dst[j] = td_read_sym(data, start + j, col_type, col_attrs);
        } break;
        case TD_I32: case TD_DATE: case TD_TIME: {
            const int32_t* s = (const int32_t*)data + start;
            for (int64_t j = 0; j < n; j++) dst[j] = s[j];
        } break;
        case TD_U8: case TD_BOOL: {
            const uint8_t* s = (const uint8_t*)data + start;
            for (int64_t j = 0; j < n; j++) dst[j] = s[j];
        } break;
        case TD_I16: {
            const int16_t* s = (const int16_t*)data + start;
            for (int64_t j = 0; j < n; j++) dst[j] = s[j];
        } break;
        default: memset(dst, 0, (size_t)n * 8); break;
    }
}

/* Load SCAN column data into f64 scratch buffer with type conversion */
static void expr_load_f64(double* dst, const void* data, int8_t col_type,
                          uint8_t col_attrs, int64_t start, int64_t n) {
    switch (col_type) {
        case TD_F64:
            memcpy(dst, (const double*)data + start, (size_t)n * 8);
            break;
        case TD_I64: case TD_TIMESTAMP: {
            const int64_t* s = (const int64_t*)data + start;
            for (int64_t j = 0; j < n; j++) dst[j] = (double)s[j];
        } break;
        case TD_SYM: {
            for (int64_t j = 0; j < n; j++)
                dst[j] = (double)td_read_sym(data, start + j, col_type, col_attrs);
        } break;
        case TD_I32: case TD_DATE: case TD_TIME: {
            const int32_t* s = (const int32_t*)data + start;
            for (int64_t j = 0; j < n; j++) dst[j] = (double)s[j];
        } break;
        case TD_U8: case TD_BOOL: {
            const uint8_t* s = (const uint8_t*)data + start;
            for (int64_t j = 0; j < n; j++) dst[j] = (double)s[j];
        } break;
        case TD_I16: {
            const int16_t* s = (const int16_t*)data + start;
            for (int64_t j = 0; j < n; j++) dst[j] = (double)s[j];
        } break;
        default: memset(dst, 0, (size_t)n * 8); break;
    }
}

/* Execute a binary instruction over n elements.
 * Switch is OUTSIDE the loop so each case auto-vectorizes. */
static void expr_exec_binary(uint8_t opcode, int8_t dt, void* dp,
                              int8_t t1, const void* ap,
                              int8_t t2, const void* bp, int64_t n) {
    (void)t2;
    if (dt == TD_F64) {
        double* d = (double*)dp;
        const double* a = (const double*)ap;
        const double* b = (const double*)bp;
        switch (opcode) {
            case OP_ADD: for (int64_t j = 0; j < n; j++) d[j] = a[j] + b[j]; break;
            case OP_SUB: for (int64_t j = 0; j < n; j++) d[j] = a[j] - b[j]; break;
            case OP_MUL: for (int64_t j = 0; j < n; j++) d[j] = a[j] * b[j]; break;
            case OP_DIV: for (int64_t j = 0; j < n; j++) d[j] = b[j] != 0 ? a[j] / b[j] : 0; break;
            case OP_MOD: for (int64_t j = 0; j < n; j++) d[j] = b[j] != 0 ? fmod(a[j], b[j]) : 0; break;
            case OP_MIN2: for (int64_t j = 0; j < n; j++) d[j] = a[j] < b[j] ? a[j] : b[j]; break;
            case OP_MAX2: for (int64_t j = 0; j < n; j++) d[j] = a[j] > b[j] ? a[j] : b[j]; break;
            default: break;
        }
    } else if (dt == TD_I64) {
        int64_t* d = (int64_t*)dp;
        const int64_t* a = (const int64_t*)ap;
        const int64_t* b = (const int64_t*)bp;
        switch (opcode) {
            /* Use uint64_t casts to get defined wrapping on overflow */
            case OP_ADD: for (int64_t j = 0; j < n; j++) d[j] = (int64_t)((uint64_t)a[j] + (uint64_t)b[j]); break;
            case OP_SUB: for (int64_t j = 0; j < n; j++) d[j] = (int64_t)((uint64_t)a[j] - (uint64_t)b[j]); break;
            case OP_MUL: for (int64_t j = 0; j < n; j++) d[j] = (int64_t)((uint64_t)a[j] * (uint64_t)b[j]); break;
            case OP_DIV: for (int64_t j = 0; j < n; j++) d[j] = (b[j] != 0 && !(a[j] == INT64_MIN && b[j] == -1)) ? a[j] / b[j] : 0; break;
            case OP_MOD: for (int64_t j = 0; j < n; j++) d[j] = (b[j] != 0 && !(a[j] == INT64_MIN && b[j] == -1)) ? a[j] % b[j] : 0; break;
            case OP_MIN2: for (int64_t j = 0; j < n; j++) d[j] = a[j] < b[j] ? a[j] : b[j]; break;
            case OP_MAX2: for (int64_t j = 0; j < n; j++) d[j] = a[j] > b[j] ? a[j] : b[j]; break;
            default: break;
        }
    } else if (dt == TD_BOOL) {
        uint8_t* d = (uint8_t*)dp;
        if (t1 == TD_F64) {
            const double* a = (const double*)ap;
            const double* b = (const double*)bp;
            switch (opcode) {
                case OP_EQ: for (int64_t j = 0; j < n; j++) d[j] = a[j] == b[j]; break;
                case OP_NE: for (int64_t j = 0; j < n; j++) d[j] = a[j] != b[j]; break;
                case OP_LT: for (int64_t j = 0; j < n; j++) d[j] = a[j] < b[j]; break;
                case OP_LE: for (int64_t j = 0; j < n; j++) d[j] = a[j] <= b[j]; break;
                case OP_GT: for (int64_t j = 0; j < n; j++) d[j] = a[j] > b[j]; break;
                case OP_GE: for (int64_t j = 0; j < n; j++) d[j] = a[j] >= b[j]; break;
                default: break;
            }
        } else if (t1 == TD_I64) {
            const int64_t* a = (const int64_t*)ap;
            const int64_t* b = (const int64_t*)bp;
            switch (opcode) {
                case OP_EQ: for (int64_t j = 0; j < n; j++) d[j] = a[j] == b[j]; break;
                case OP_NE: for (int64_t j = 0; j < n; j++) d[j] = a[j] != b[j]; break;
                case OP_LT: for (int64_t j = 0; j < n; j++) d[j] = a[j] < b[j]; break;
                case OP_LE: for (int64_t j = 0; j < n; j++) d[j] = a[j] <= b[j]; break;
                case OP_GT: for (int64_t j = 0; j < n; j++) d[j] = a[j] > b[j]; break;
                case OP_GE: for (int64_t j = 0; j < n; j++) d[j] = a[j] >= b[j]; break;
                default: break;
            }
        } else { /* both bool */
            const uint8_t* a = (const uint8_t*)ap;
            const uint8_t* b = (const uint8_t*)bp;
            switch (opcode) {
                case OP_AND: for (int64_t j = 0; j < n; j++) d[j] = a[j] && b[j]; break;
                case OP_OR:  for (int64_t j = 0; j < n; j++) d[j] = a[j] || b[j]; break;
                default: break;
            }
        }
    }
}

/* Execute a unary instruction over n elements */
static void expr_exec_unary(uint8_t opcode, int8_t dt, void* dp,
                             int8_t t1, const void* ap, int64_t n) {
    if (dt == TD_F64) {
        double* d = (double*)dp;
        if (t1 == TD_F64) {
            const double* a = (const double*)ap;
            switch (opcode) {
                case OP_NEG:   for (int64_t j = 0; j < n; j++) d[j] = -a[j]; break;
                case OP_ABS:   for (int64_t j = 0; j < n; j++) d[j] = fabs(a[j]); break;
                case OP_SQRT:  for (int64_t j = 0; j < n; j++) d[j] = sqrt(a[j]); break;
                case OP_LOG:   for (int64_t j = 0; j < n; j++) d[j] = log(a[j]); break;
                case OP_EXP:   for (int64_t j = 0; j < n; j++) d[j] = exp(a[j]); break;
                case OP_CEIL:  for (int64_t j = 0; j < n; j++) d[j] = ceil(a[j]); break;
                case OP_FLOOR: for (int64_t j = 0; j < n; j++) d[j] = floor(a[j]); break;
                default: break;
            }
        } else { /* CAST i64→f64 */
            const int64_t* a = (const int64_t*)ap;
            for (int64_t j = 0; j < n; j++) d[j] = (double)a[j];
        }
    } else if (dt == TD_I64) {
        int64_t* d = (int64_t*)dp;
        if (t1 == TD_I64) {
            const int64_t* a = (const int64_t*)ap;
            switch (opcode) {
                /* Unsigned negation avoids UB on INT64_MIN */
                case OP_NEG: for (int64_t j = 0; j < n; j++) d[j] = (int64_t)(-(uint64_t)a[j]); break;
                case OP_ABS: for (int64_t j = 0; j < n; j++) d[j] = a[j] < 0 ? (int64_t)(-(uint64_t)a[j]) : a[j]; break;
                default: break;
            }
        } else { /* CAST f64→i64 — clamp to avoid out-of-range UB */
            const double* a = (const double*)ap;
            for (int64_t j = 0; j < n; j++)
                d[j] = (a[j] >= (double)INT64_MAX) ? INT64_MAX
                     : (a[j] <= (double)INT64_MIN) ? INT64_MIN
                     : (int64_t)a[j];
        }
    } else if (dt == TD_BOOL) {
        uint8_t* d = (uint8_t*)dp;
        const uint8_t* a = (const uint8_t*)ap;
        switch (opcode) {
            case OP_NOT: for (int64_t j = 0; j < n; j++) d[j] = !a[j]; break;
            default: break;
        }
    }
}

/* Evaluate compiled expression for morsel [start, end).
 * scratch: array of EXPR_MAX_REGS buffers, each EXPR_MORSEL*8 bytes.
 * Returns pointer to output data (morsel-relative indexing). */
static void* expr_eval_morsel(const td_expr_t* expr, void** scratch,
                               int64_t start, int64_t end) {
    int64_t n = end - start;
    if (n <= 0) return NULL;

    void* rptrs[EXPR_MAX_REGS];
    for (uint8_t r = 0; r < expr->n_regs; r++) {
        int8_t rt = expr->regs[r].type;
        int8_t ct = expr->regs[r].col_type;
        switch (expr->regs[r].kind) {
            case REG_SCAN: {
                /* Direct pointer if native type matches, else convert */
                uint8_t ca = expr->regs[r].col_attrs;
                if (rt == TD_F64 && ct == TD_F64) {
                    rptrs[r] = (double*)expr->regs[r].data + start;
                } else if (rt == TD_I64 && (ct == TD_I64 || ct == TD_TIMESTAMP)) {
                    rptrs[r] = (int64_t*)expr->regs[r].data + start;
                } else if (rt == TD_I64 && ct == TD_SYM &&
                           (ca & TD_SYM_W_MASK) == TD_SYM_W64) {
                    rptrs[r] = (int64_t*)expr->regs[r].data + start;
                } else {
                    rptrs[r] = scratch[r];
                    if (rt == TD_F64)
                        expr_load_f64(scratch[r], expr->regs[r].data, ct, ca, start, n);
                    else
                        expr_load_i64(scratch[r], expr->regs[r].data, ct, ca, start, n);
                }
            }
                break;
            case REG_CONST:
                rptrs[r] = scratch[r];
                if (rt == TD_F64) {
                    double v = expr->regs[r].const_f64;
                    double* d = (double*)scratch[r];
                    for (int64_t j = 0; j < n; j++) d[j] = v;
                } else {
                    int64_t v = expr->regs[r].const_i64;
                    int64_t* d = (int64_t*)scratch[r];
                    for (int64_t j = 0; j < n; j++) d[j] = v;
                }
                break;
            default: /* REG_SCRATCH */
                rptrs[r] = scratch[r];
                break;
        }
    }

    for (uint8_t i = 0; i < expr->n_ins; i++) {
        const expr_ins_t* ins = &expr->ins[i];
        int8_t dt = expr->regs[ins->dst].type;
        if (ins->src2 != 0xFF) {
            expr_exec_binary(ins->opcode, dt, rptrs[ins->dst],
                             expr->regs[ins->src1].type, rptrs[ins->src1],
                             expr->regs[ins->src2].type, rptrs[ins->src2], n);
        } else {
            expr_exec_unary(ins->opcode, dt, rptrs[ins->dst],
                            expr->regs[ins->src1].type, rptrs[ins->src1], n);
        }
    }

    return rptrs[expr->out_reg];
}

/* Context for parallel full-vector expression evaluation */
typedef struct {
    const td_expr_t* expr;
    void*  out_data;
    int8_t out_type;
} expr_full_ctx_t;

static void expr_full_fn(void* ctx, uint32_t worker_id, int64_t start, int64_t end) {
    (void)worker_id;
    expr_full_ctx_t* c = (expr_full_ctx_t*)ctx;
    const td_expr_t* expr = c->expr;
    uint8_t esz = td_elem_size(c->out_type);

    /* Per-worker scratch buffers (heap-allocated via arena, morsel-sized) */
    td_t* scratch_hdr = NULL;
    char* scratch_mem = (char*)scratch_alloc(&scratch_hdr,
                            (size_t)EXPR_MAX_REGS * EXPR_MORSEL * 8);
    if (!scratch_mem) return;
    void* scratch[EXPR_MAX_REGS];
    for (uint8_t r = 0; r < expr->n_regs; r++)
        scratch[r] = scratch_mem + (size_t)r * EXPR_MORSEL * 8;

    for (int64_t ms = start; ms < end; ms += EXPR_MORSEL) {
        int64_t me = (ms + EXPR_MORSEL < end) ? ms + EXPR_MORSEL : end;
        void* result = expr_eval_morsel(expr, scratch, ms, me);
        if (result)
            memcpy((char*)c->out_data + ms * esz, result, (size_t)(me - ms) * esz);
    }
    scratch_free(scratch_hdr);
}

/* Evaluate compiled expression over parted (segmented) columns.
 * Iterates segments as outer loop, rebinds data pointers per segment,
 * then dispatches the existing morsel evaluator per segment. Zero copy. */
static td_t* expr_eval_full_parted(const td_expr_t* expr, int64_t nrows) {
    td_t* out = td_vec_new(expr->out_type, nrows);
    if (!out || TD_IS_ERR(out)) {
        return out;
    }
    out->len = nrows;

    /* Find first parted register to get segment structure */
    td_t* ref_parted = NULL;
    for (uint8_t r = 0; r < expr->n_regs; r++) {
        if (expr->regs[r].is_parted) {
            ref_parted = expr->regs[r].parted_col;
            break;
        }
    }
    if (!ref_parted) { td_release(out); return TD_ERR_PTR(TD_ERR_NYI); }

    int64_t n_segs = ref_parted->len;
    td_t** ref_segs = (td_t**)td_data(ref_parted);
    uint8_t esz = td_elem_size(expr->out_type);
    td_pool_t* pool = td_pool_get();
    int64_t global_off = 0;

    for (int64_t s = 0; s < n_segs; s++) {
        int64_t seg_len = ref_segs[s]->len;
        if (seg_len <= 0) continue;

        /* Stack-copy expr, rebind parted registers to this segment's data */
        td_expr_t seg_expr = *expr;
        for (uint8_t r = 0; r < seg_expr.n_regs; r++) {
            if (seg_expr.regs[r].is_parted) {
                td_t** segs = (td_t**)td_data(seg_expr.regs[r].parted_col);
                seg_expr.regs[r].data = td_data(segs[s]);
            }
        }

        expr_full_ctx_t ctx = {
            .expr = &seg_expr,
            .out_data = (char*)td_data(out) + global_off * esz,
            .out_type = expr->out_type,
        };
        if (pool && seg_len >= TD_PARALLEL_THRESHOLD)
            td_pool_dispatch(pool, expr_full_fn, &ctx, seg_len);
        else
            expr_full_fn(&ctx, 0, 0, seg_len);

        global_off += seg_len;
    }
    return out;
}

/* Evaluate compiled expression into a full-length output vector.
 * Replaces exec_node() for expression subtrees — no intermediate vectors. */
static td_t* expr_eval_full(const td_expr_t* expr, int64_t nrows) {
    if (expr->has_parted)
        return expr_eval_full_parted(expr, nrows);

    td_t* out = td_vec_new(expr->out_type, nrows);
    if (!out || TD_IS_ERR(out)) return out;
    out->len = nrows;

    expr_full_ctx_t ctx = {
        .expr = expr, .out_data = td_data(out), .out_type = expr->out_type,
    };

    td_pool_t* pool = td_pool_get();
    if (pool && nrows >= TD_PARALLEL_THRESHOLD)
        td_pool_dispatch(pool, expr_full_fn, &ctx, nrows);
    else
        expr_full_fn(&ctx, 0, 0, nrows);

    return out;
}

/* ============================================================================
 * Element-wise execution
 * ============================================================================ */

static td_t* exec_elementwise_unary(td_graph_t* g, td_op_t* op, td_t* input) {
    (void)g;
    if (!input || TD_IS_ERR(input)) return input;
    int64_t len = input->len;
    int8_t in_type = input->type;
    int8_t out_type = op->out_type;

    td_t* result = td_vec_new(out_type, len);
    if (!result || TD_IS_ERR(result)) return result;
    result->len = len;

    td_morsel_t m;
    td_morsel_init(&m, input);
    int64_t out_off = 0;

    while (td_morsel_next(&m)) {
        int64_t n = m.morsel_len;
        void* dst = (char*)td_data(result) + out_off * td_elem_size(out_type);

        if (in_type == TD_F64 || in_type == TD_I64) {
            for (int64_t i = 0; i < n; i++) {
                if (in_type == TD_F64) {
                    double v = ((double*)m.morsel_ptr)[i];
                    double r;
                    switch (op->opcode) {
                        case OP_NEG:   r = -v; break;
                        case OP_ABS:   r = fabs(v); break;
                        case OP_SQRT:  r = sqrt(v); break;
                        case OP_LOG:   r = log(v); break;
                        case OP_EXP:   r = exp(v); break;
                        case OP_CEIL:  r = ceil(v); break;
                        case OP_FLOOR: r = floor(v); break;
                        default:       r = v; break;
                    }
                    if (out_type == TD_F64) ((double*)dst)[i] = r;
                    else if (out_type == TD_I64) ((int64_t*)dst)[i] = (int64_t)r;
                } else {
                    int64_t v = ((int64_t*)m.morsel_ptr)[i];
                    if (out_type == TD_I64) {
                        int64_t r;
                        switch (op->opcode) {
                            /* Unsigned negation avoids UB on INT64_MIN */
                            case OP_NEG: r = (int64_t)(-(uint64_t)v); break;
                            case OP_ABS: r = v < 0 ? (int64_t)(-(uint64_t)v) : v; break;
                            default:     r = v; break;
                        }
                        ((int64_t*)dst)[i] = r;
                    } else if (out_type == TD_F64) {
                        double r;
                        switch (op->opcode) {
                            case OP_NEG:   r = -(double)v; break;
                            case OP_SQRT:  r = sqrt((double)v); break;
                            case OP_LOG:   r = log((double)v); break;
                            case OP_EXP:   r = exp((double)v); break;
                            default:       r = (double)v; break;
                        }
                        ((double*)dst)[i] = r;
                    } else if (out_type == TD_BOOL) {
                        /* ISNULL: for non-null vecs, always false */
                        ((uint8_t*)dst)[i] = 0;
                    }
                }
            }
        } else if (in_type == TD_BOOL && op->opcode == OP_NOT) {
            for (int64_t i = 0; i < n; i++) {
                ((uint8_t*)dst)[i] = !((uint8_t*)m.morsel_ptr)[i];
            }
        }

        out_off += n;
    }

    return result;
}

/* Inner loop for binary element-wise over a range [start, end) */
static void binary_range(td_op_t* op, int8_t out_type,
                         td_t* lhs, td_t* rhs, td_t* result,
                         bool l_scalar, bool r_scalar,
                         double l_f64, double r_f64,
                         int64_t l_i64, int64_t r_i64,
                         int64_t start, int64_t end) {
    uint8_t out_esz = td_elem_size(out_type);
    void* dst = (char*)td_data(result) + start * out_esz;
    int64_t n = end - start;

    /* Pointers into source data at offset start */
    double* lp_f64 = NULL; int64_t* lp_i64 = NULL; uint8_t* lp_bool = NULL;
    double* rp_f64 = NULL; int64_t* rp_i64 = NULL; uint8_t* rp_bool = NULL;

    int32_t* lp_i32 = NULL; uint32_t* lp_u32 = NULL; int16_t* lp_i16 = NULL;
    int32_t* rp_i32 = NULL; uint32_t* rp_u32 = NULL; int16_t* rp_i16 = NULL;

    int64_t lsym_buf[n], rsym_buf[n]; /* stack VLA for narrow TD_SYM (n<=1024) */
    if (!l_scalar) {
        void* lbase = (char*)td_data(lhs) + start * td_sym_elem_size(lhs->type, lhs->attrs);
        if (lhs->type == TD_F64) lp_f64 = (double*)lbase;
        else if (lhs->type == TD_I64 || lhs->type == TD_TIMESTAMP) lp_i64 = (int64_t*)lbase;
        else if (TD_IS_SYM(lhs->type)) {
            uint8_t w = lhs->attrs & TD_SYM_W_MASK;
            if (w == TD_SYM_W64) lp_i64 = (int64_t*)lbase;
            else if (w == TD_SYM_W32) lp_u32 = (uint32_t*)lbase;
            else { for (int64_t j = 0; j < n; j++) lsym_buf[j] = td_read_sym(td_data(lhs), start+j, lhs->type, lhs->attrs); lp_i64 = lsym_buf; }
        }
        else if (lhs->type == TD_I32 || lhs->type == TD_DATE || lhs->type == TD_TIME) lp_i32 = (int32_t*)lbase;
        else if (lhs->type == TD_I16) lp_i16 = (int16_t*)lbase;
        else if (lhs->type == TD_BOOL || lhs->type == TD_U8) lp_bool = (uint8_t*)lbase;
    }
    if (!r_scalar) {
        void* rbase = (char*)td_data(rhs) + start * td_sym_elem_size(rhs->type, rhs->attrs);
        if (rhs->type == TD_F64) rp_f64 = (double*)rbase;
        else if (rhs->type == TD_I64 || rhs->type == TD_TIMESTAMP) rp_i64 = (int64_t*)rbase;
        else if (TD_IS_SYM(rhs->type)) {
            uint8_t w = rhs->attrs & TD_SYM_W_MASK;
            if (w == TD_SYM_W64) rp_i64 = (int64_t*)rbase;
            else if (w == TD_SYM_W32) rp_u32 = (uint32_t*)rbase;
            else { for (int64_t j = 0; j < n; j++) rsym_buf[j] = td_read_sym(td_data(rhs), start+j, rhs->type, rhs->attrs); rp_i64 = rsym_buf; }
        }
        else if (rhs->type == TD_I32 || rhs->type == TD_DATE || rhs->type == TD_TIME) rp_i32 = (int32_t*)rbase;
        else if (rhs->type == TD_I16) rp_i16 = (int16_t*)rbase;
        else if (rhs->type == TD_BOOL || rhs->type == TD_U8) rp_bool = (uint8_t*)rbase;
    }

    for (int64_t i = 0; i < n; i++) {
        double lv, rv;
        if (lp_f64)       lv = lp_f64[i];
        else if (lp_i64)  lv = (double)lp_i64[i];
        else if (lp_i32)  lv = (double)lp_i32[i];
        else if (lp_u32)  lv = (double)lp_u32[i];
        else if (lp_i16)  lv = (double)lp_i16[i];
        else if (lp_bool) lv = (double)lp_bool[i];
        else if (l_scalar && (lhs->type == TD_ATOM_F64 || lhs->type == -TD_F64 || lhs->type == TD_F64)) lv = l_f64;
        else              lv = (double)l_i64;

        if (rp_f64)       rv = rp_f64[i];
        else if (rp_i64)  rv = (double)rp_i64[i];
        else if (rp_i32)  rv = (double)rp_i32[i];
        else if (rp_u32)  rv = (double)rp_u32[i];
        else if (rp_i16)  rv = (double)rp_i16[i];
        else if (rp_bool) rv = (double)rp_bool[i];
        else if (r_scalar && (rhs->type == TD_ATOM_F64 || rhs->type == -TD_F64 || rhs->type == TD_F64)) rv = r_f64;
        else              rv = (double)r_i64;

        if (out_type == TD_F64) {
            double r;
            switch (op->opcode) {
                case OP_ADD: r = lv + rv; break;
                case OP_SUB: r = lv - rv; break;
                case OP_MUL: r = lv * rv; break;
                case OP_DIV: r = rv != 0.0 ? lv / rv : 0.0; break;
                case OP_MOD: r = rv != 0.0 ? fmod(lv, rv) : 0.0; break;
                case OP_MIN2: r = lv < rv ? lv : rv; break;
                case OP_MAX2: r = lv > rv ? lv : rv; break;
                default: r = 0.0; break;
            }
            ((double*)dst)[i] = r;
        } else if (out_type == TD_I64) {
            int64_t li = (int64_t)lv, ri = (int64_t)rv;
            int64_t r;
            switch (op->opcode) {
                /* Use uint64_t casts to get defined wrapping on overflow */
                case OP_ADD: r = (int64_t)((uint64_t)li + (uint64_t)ri); break;
                case OP_SUB: r = (int64_t)((uint64_t)li - (uint64_t)ri); break;
                case OP_MUL: r = (int64_t)((uint64_t)li * (uint64_t)ri); break;
                case OP_DIV: r = (ri != 0 && !(li == INT64_MIN && ri == -1)) ? li / ri : 0; break;
                case OP_MOD: r = (ri != 0 && !(li == INT64_MIN && ri == -1)) ? li % ri : 0; break;
                case OP_MIN2: r = li < ri ? li : ri; break;
                case OP_MAX2: r = li > ri ? li : ri; break;
                default: r = 0; break;
            }
            ((int64_t*)dst)[i] = r;
        } else if (out_type == TD_BOOL) {
            uint8_t r;
            switch (op->opcode) {
                case OP_EQ:  r = lv == rv; break;
                case OP_NE:  r = lv != rv; break;
                case OP_LT:  r = lv < rv; break;
                case OP_LE:  r = lv <= rv; break;
                case OP_GT:  r = lv > rv; break;
                case OP_GE:  r = lv >= rv; break;
                case OP_AND: r = (uint8_t)lv && (uint8_t)rv; break;
                case OP_OR:  r = (uint8_t)lv || (uint8_t)rv; break;
                default: r = 0; break;
            }
            ((uint8_t*)dst)[i] = r;
        }
    }
}

/* Context for parallel binary dispatch */
typedef struct {
    td_op_t* op;
    int8_t   out_type;
    td_t*    lhs;
    td_t*    rhs;
    td_t*    result;
    bool     l_scalar;
    bool     r_scalar;
    double   l_f64, r_f64;
    int64_t  l_i64, r_i64;
} par_binary_ctx_t;

static void par_binary_fn(void* ctx, uint32_t worker_id, int64_t start, int64_t end) {
    (void)worker_id;
    par_binary_ctx_t* c = (par_binary_ctx_t*)ctx;
    binary_range(c->op, c->out_type, c->lhs, c->rhs, c->result,
                 c->l_scalar, c->r_scalar,
                 c->l_f64, c->r_f64, c->l_i64, c->r_i64,
                 start, end);
}

static td_t* exec_elementwise_binary(td_graph_t* g, td_op_t* op, td_t* lhs, td_t* rhs) {
    (void)g;
    if (!lhs || TD_IS_ERR(lhs)) return lhs;
    if (!rhs || TD_IS_ERR(rhs)) return rhs;

    bool l_scalar = td_is_atom(lhs) || (lhs->type > 0 && lhs->len == 1);
    bool r_scalar = td_is_atom(rhs) || (rhs->type > 0 && rhs->len == 1);

    int64_t len = 1;
    if (!l_scalar && !r_scalar) {
        if (lhs->len != rhs->len) return TD_ERR_PTR(TD_ERR_LENGTH);
        len = lhs->len;
    } else if (l_scalar && !r_scalar) {
        len = rhs->len;
    } else if (!l_scalar && r_scalar) {
        len = lhs->len;
    }

    int8_t out_type = op->out_type;
    td_t* result = td_vec_new(out_type, len);
    if (!result || TD_IS_ERR(result)) return result;
    result->len = len;

    /* SYM vs STR comparison: resolve string constant to intern ID so we
       can compare numerically against SYM intern indices.
       td_sym_find returns -1 if string not in table → no match. */
    bool str_resolved = false;
    int64_t resolved_sym_id = 0;
    if (r_scalar && rhs->type == TD_ATOM_STR &&
        TD_IS_SYM(lhs->type)) {
        const char* s = td_str_ptr(rhs);
        size_t slen = td_str_len(rhs);
        resolved_sym_id = td_sym_find(s, slen);
        str_resolved = true;
    } else if (l_scalar && lhs->type == TD_ATOM_STR &&
               TD_IS_SYM(rhs->type)) {
        const char* s = td_str_ptr(lhs);
        size_t slen = td_str_len(lhs);
        resolved_sym_id = td_sym_find(s, slen);
        str_resolved = true;
    }

    double l_f64_val = 0, r_f64_val = 0;
    int64_t l_i64_val = 0, r_i64_val = 0;
    if (l_scalar) {
        if (str_resolved && lhs->type == TD_ATOM_STR)
            l_i64_val = resolved_sym_id;
        else if (td_is_atom(lhs)) {
            if (lhs->type == TD_ATOM_F64 || lhs->type == -TD_F64) l_f64_val = lhs->f64;
            else l_i64_val = lhs->i64;
        } else {
            int8_t t = lhs->type;
            if (t == TD_F64) l_f64_val = ((double*)td_data(lhs))[0];
            else l_i64_val = read_col_i64(td_data(lhs), 0, t, lhs->attrs);
        }
    }
    if (r_scalar) {
        if (str_resolved && rhs->type == TD_ATOM_STR)
            r_i64_val = resolved_sym_id;
        else if (td_is_atom(rhs)) {
            if (rhs->type == TD_ATOM_F64 || rhs->type == -TD_F64) r_f64_val = rhs->f64;
            else r_i64_val = rhs->i64;
        } else {
            int8_t t = rhs->type;
            if (t == TD_F64) r_f64_val = ((double*)td_data(rhs))[0];
            else r_i64_val = read_col_i64(td_data(rhs), 0, t, rhs->attrs);
        }
    }

    td_pool_t* pool = td_pool_get();
    if (pool && len >= TD_PARALLEL_THRESHOLD) {
        par_binary_ctx_t ctx = {
            .op = op, .out_type = out_type,
            .lhs = lhs, .rhs = rhs, .result = result,
            .l_scalar = l_scalar, .r_scalar = r_scalar,
            .l_f64 = l_f64_val, .r_f64 = r_f64_val,
            .l_i64 = l_i64_val, .r_i64 = r_i64_val,
        };
        td_pool_dispatch(pool, par_binary_fn, &ctx, len);
        return result;
    }

    /* Sequential fallback */
    binary_range(op, out_type, lhs, rhs, result,
                 l_scalar, r_scalar,
                 l_f64_val, r_f64_val, l_i64_val, r_i64_val,
                 0, len);
    return result;
}

/* ============================================================================
 * Reduction execution
 * ============================================================================ */

typedef struct {
    double sum_f, min_f, max_f, prod_f, first_f, last_f, sum_sq_f;
    int64_t sum_i, min_i, max_i, prod_i, first_i, last_i, sum_sq_i;
    int64_t cnt;
    bool has_first;
} reduce_acc_t;

static void reduce_acc_init(reduce_acc_t* acc) {
    acc->sum_f = 0; acc->min_f = DBL_MAX; acc->max_f = -DBL_MAX;
    acc->prod_f = 1.0; acc->first_f = 0; acc->last_f = 0; acc->sum_sq_f = 0;
    acc->sum_i = 0; acc->min_i = INT64_MAX; acc->max_i = INT64_MIN;
    acc->prod_i = 1; acc->first_i = 0; acc->last_i = 0; acc->sum_sq_i = 0;
    acc->cnt = 0; acc->has_first = false;
}

static void reduce_range(td_t* input, int64_t start, int64_t end, reduce_acc_t* acc) {
    int8_t in_type = input->type;
    void* base = td_data(input);

    for (int64_t row = start; row < end; row++) {
        if (in_type == TD_F64) {
            double v = ((double*)base)[row];
            acc->sum_f += v;
            acc->sum_sq_f += v * v;
            acc->prod_f *= v;
            if (v < acc->min_f) acc->min_f = v;
            if (v > acc->max_f) acc->max_f = v;
            if (!acc->has_first) { acc->first_f = v; acc->has_first = true; }
            acc->last_f = v;
        } else {
            int64_t v = read_col_i64(base, row, in_type, input->attrs);
            acc->sum_i += v;
            acc->sum_sq_i += v * v;
            acc->prod_i *= v;
            if (v < acc->min_i) acc->min_i = v;
            if (v > acc->max_i) acc->max_i = v;
            if (!acc->has_first) { acc->first_i = v; acc->has_first = true; }
            acc->last_i = v;
        }
        acc->cnt++;
    }
}

/* Context for parallel reduction */
typedef struct {
    td_t*         input;
    reduce_acc_t* accs;   /* one per worker */
} par_reduce_ctx_t;

static void par_reduce_fn(void* ctx, uint32_t worker_id, int64_t start, int64_t end) {
    par_reduce_ctx_t* c = (par_reduce_ctx_t*)ctx;
    reduce_range(c->input, start, end, &c->accs[worker_id]);
}

static void reduce_merge(reduce_acc_t* dst, const reduce_acc_t* src, int8_t in_type) {
    if (in_type == TD_F64) {
        dst->sum_f += src->sum_f;
        dst->sum_sq_f += src->sum_sq_f;
        dst->prod_f *= src->prod_f;
        if (src->min_f < dst->min_f) dst->min_f = src->min_f;
        if (src->max_f > dst->max_f) dst->max_f = src->max_f;
    } else {
        dst->sum_i += src->sum_i;
        dst->sum_sq_i += src->sum_sq_i;
        dst->prod_i *= src->prod_i;
        if (src->min_i < dst->min_i) dst->min_i = src->min_i;
        if (src->max_i > dst->max_i) dst->max_i = src->max_i;
    }
    dst->cnt += src->cnt;
    /* reduce_merge does not merge first/last; caller handles these separately.
     * Since workers process sequential ranges, worker 0's first is the global first,
     * and the last worker's last is the global last. */
}

static td_t* exec_reduction(td_graph_t* g, td_op_t* op, td_t* input) {
    (void)g;
    if (!input || TD_IS_ERR(input)) return input;

    int8_t in_type = input->type;
    int64_t len = input->len;

    td_pool_t* pool = td_pool_get();
    if (pool && len >= TD_PARALLEL_THRESHOLD) {
        uint32_t nw = td_pool_total_workers(pool);
        td_t* accs_hdr;
        reduce_acc_t* accs = (reduce_acc_t*)scratch_calloc(&accs_hdr, nw * sizeof(reduce_acc_t));
        if (!accs) return TD_ERR_PTR(TD_ERR_OOM);
        for (uint32_t i = 0; i < nw; i++) reduce_acc_init(&accs[i]);

        par_reduce_ctx_t ctx = { .input = input, .accs = accs };
        td_pool_dispatch(pool, par_reduce_fn, &ctx, len);

        /* Merge: worker 0 is the base, merge the rest in order */
        reduce_acc_t merged;
        reduce_acc_init(&merged);
        merged = accs[0];
        for (uint32_t i = 1; i < nw; i++) {
            if (!accs[i].has_first) continue;
            reduce_merge(&merged, &accs[i], in_type);
        }
        /* first = accs[first worker with data], last = accs[last worker with data] */
        for (uint32_t i = 0; i < nw; i++) {
            if (accs[i].has_first) {
                if (in_type == TD_F64) merged.first_f = accs[i].first_f;
                else merged.first_i = accs[i].first_i;
                break;
            }
        }
        for (int32_t i = (int32_t)nw - 1; i >= 0; i--) {
            if (accs[i].has_first) {
                if (in_type == TD_F64) merged.last_f = accs[i].last_f;
                else merged.last_i = accs[i].last_i;
                break;
            }
        }

        td_t* result;
        switch (op->opcode) {
            case OP_SUM:   result = in_type == TD_F64 ? td_f64(merged.sum_f) : td_i64(merged.sum_i); break;
            case OP_PROD:  result = in_type == TD_F64 ? td_f64(merged.prod_f) : td_i64(merged.prod_i); break;
            case OP_MIN:   result = in_type == TD_F64 ? td_f64(merged.cnt > 0 ? merged.min_f : 0.0) : td_i64(merged.cnt > 0 ? merged.min_i : 0); break;
            case OP_MAX:   result = in_type == TD_F64 ? td_f64(merged.cnt > 0 ? merged.max_f : 0.0) : td_i64(merged.cnt > 0 ? merged.max_i : 0); break;
            case OP_COUNT: result = td_i64(merged.cnt); break;
            case OP_AVG:   result = in_type == TD_F64 ? td_f64(merged.cnt > 0 ? merged.sum_f / merged.cnt : 0.0) : td_f64(merged.cnt > 0 ? (double)merged.sum_i / merged.cnt : 0.0); break;
            case OP_FIRST: result = in_type == TD_F64 ? td_f64(merged.first_f) : td_i64(merged.first_i); break;
            case OP_LAST:  result = in_type == TD_F64 ? td_f64(merged.last_f) : td_i64(merged.last_i); break;
            case OP_VAR: case OP_VAR_POP:
            case OP_STDDEV: case OP_STDDEV_POP: {
                double mean, var_pop;
                if (in_type == TD_F64) { mean = merged.sum_f / merged.cnt; var_pop = merged.sum_sq_f / merged.cnt - mean * mean; }
                else { mean = (double)merged.sum_i / merged.cnt; var_pop = (double)merged.sum_sq_i / merged.cnt - mean * mean; }
                if (var_pop < 0) var_pop = 0;
                double val;
                if (op->opcode == OP_VAR_POP) val = merged.cnt > 0 ? var_pop : NAN;
                else if (op->opcode == OP_VAR) val = merged.cnt > 1 ? var_pop * merged.cnt / (merged.cnt - 1) : NAN;
                else if (op->opcode == OP_STDDEV_POP) val = merged.cnt > 0 ? sqrt(var_pop) : NAN;
                else val = merged.cnt > 1 ? sqrt(var_pop * merged.cnt / (merged.cnt - 1)) : NAN;
                result = td_f64(val);
                break;
            }
            default:       result = TD_ERR_PTR(TD_ERR_NYI); break;
        }
        scratch_free(accs_hdr);
        return result;
    }

    reduce_acc_t acc;
    reduce_acc_init(&acc);
    reduce_range(input, 0, len, &acc);

    switch (op->opcode) {
        case OP_SUM:   return in_type == TD_F64 ? td_f64(acc.sum_f) : td_i64(acc.sum_i);
        case OP_PROD:  return in_type == TD_F64 ? td_f64(acc.prod_f) : td_i64(acc.prod_i);
        case OP_MIN:   return in_type == TD_F64 ? td_f64(acc.cnt > 0 ? acc.min_f : 0.0) : td_i64(acc.cnt > 0 ? acc.min_i : 0);
        case OP_MAX:   return in_type == TD_F64 ? td_f64(acc.cnt > 0 ? acc.max_f : 0.0) : td_i64(acc.cnt > 0 ? acc.max_i : 0);
        case OP_COUNT: return td_i64(acc.cnt);
        case OP_AVG:   return in_type == TD_F64 ? td_f64(acc.cnt > 0 ? acc.sum_f / acc.cnt : 0.0) : td_f64(acc.cnt > 0 ? (double)acc.sum_i / acc.cnt : 0.0);
        case OP_FIRST: return in_type == TD_F64 ? td_f64(acc.first_f) : td_i64(acc.first_i);
        case OP_LAST:  return in_type == TD_F64 ? td_f64(acc.last_f) : td_i64(acc.last_i);
        case OP_VAR: case OP_VAR_POP:
        case OP_STDDEV: case OP_STDDEV_POP: {
            double mean, var_pop;
            if (in_type == TD_F64) { mean = acc.sum_f / acc.cnt; var_pop = acc.sum_sq_f / acc.cnt - mean * mean; }
            else { mean = (double)acc.sum_i / acc.cnt; var_pop = (double)acc.sum_sq_i / acc.cnt - mean * mean; }
            if (var_pop < 0) var_pop = 0;
            double val;
            if (op->opcode == OP_VAR_POP) val = acc.cnt > 0 ? var_pop : NAN;
            else if (op->opcode == OP_VAR) val = acc.cnt > 1 ? var_pop * acc.cnt / (acc.cnt - 1) : NAN;
            else if (op->opcode == OP_STDDEV_POP) val = acc.cnt > 0 ? sqrt(var_pop) : NAN;
            else val = acc.cnt > 1 ? sqrt(var_pop * acc.cnt / (acc.cnt - 1)) : NAN;
            return td_f64(val);
        }
        default:       return TD_ERR_PTR(TD_ERR_NYI);
    }
}

/* ============================================================================
 * Parallel index gather — used by filter, sort, and join
 * ============================================================================ */

/* Fused multi-column gather — single pass over index array for all columns.
 * Much faster than per-column gather because: (1) indices read once not N times,
 * (2) prefetch covers all columns at once. */
#define MGATHER_MAX_COLS 16
typedef struct {
    const int64_t* idx;
    char*          srcs[MGATHER_MAX_COLS];
    char*          dsts[MGATHER_MAX_COLS];
    uint8_t        esz[MGATHER_MAX_COLS];
    int64_t        ncols;
} multi_gather_ctx_t;

static void multi_gather_fn(void* raw, uint32_t wid, int64_t start, int64_t end) {
    (void)wid;
    multi_gather_ctx_t* c = (multi_gather_ctx_t*)raw;
    const int64_t* restrict idx = c->idx;
    int64_t nc = c->ncols;

    /* Process one column at a time per batch of rows.
     * This focuses random reads on a single source array, giving the
     * hardware prefetcher only 1 stream to track (instead of ncols
     * concurrent streams, which overflows the L2 miss queue). */
#define MG_BATCH 512
#define MG_PF    16
    for (int64_t base = start; base < end; base += MG_BATCH) {
        int64_t bstart = base;
        int64_t bend = base + MG_BATCH;
        if (bend > end) bend = end;
        for (int64_t col = 0; col < nc; col++) {
            uint8_t e = c->esz[col];
            char* src = c->srcs[col];
            char* dst = c->dsts[col];
            if (e == 8) {
                const uint64_t* restrict s8 = (const uint64_t*)src;
                uint64_t* restrict d8 = (uint64_t*)dst;
                for (int64_t i = bstart; i < bend; i++) {
                    if (i + MG_PF < bend)
                        __builtin_prefetch(&s8[idx[i + MG_PF]], 0, 0);
                    d8[i] = s8[idx[i]];
                }
            } else if (e == 4) {
                const uint32_t* restrict s4 = (const uint32_t*)src;
                uint32_t* restrict d4 = (uint32_t*)dst;
                for (int64_t i = bstart; i < bend; i++) {
                    if (i + MG_PF < bend)
                        __builtin_prefetch(&s4[idx[i + MG_PF]], 0, 0);
                    d4[i] = s4[idx[i]];
                }
            } else {
                for (int64_t i = bstart; i < bend; i++) {
                    if (i + MG_PF < bend)
                        __builtin_prefetch(src + idx[i + MG_PF] * e, 0, 0);
                    memcpy(dst + i * e, src + idx[i] * e, e);
                }
            }
        }
    }
#undef MG_PF
#undef MG_BATCH
}

/* Parallel index gather — single column with prefetching */
typedef struct {
    int64_t*     idx;
    td_t*        src_col;
    td_t*        dst_col;
    uint8_t      esz;
    bool         nullable;  /* true = idx may contain -1 (LEFT JOIN nulls) */
} gather_ctx_t;

static void gather_fn(void* raw, uint32_t wid, int64_t start, int64_t end) {
    (void)wid;
    gather_ctx_t* c = (gather_ctx_t*)raw;
    char* restrict src = (char*)td_data(c->src_col);
    char* restrict dst = (char*)td_data(c->dst_col);
    uint8_t esz = c->esz;
    const int64_t* restrict idx = c->idx;
#define GATHER_PF 16

    if (c->nullable) {
        for (int64_t i = start; i < end; i++) {
            if (i + GATHER_PF < end) {
                int64_t pf = idx[i + GATHER_PF];
                if (pf >= 0) __builtin_prefetch(src + pf * esz, 0, 0);
            }
            int64_t r = idx[i];
            if (r >= 0)
                memcpy(dst + i * esz, src + r * esz, esz);
            else
                memset(dst + i * esz, 0, esz);
        }
    } else {
        for (int64_t i = start; i < end; i++) {
            if (i + GATHER_PF < end)
                __builtin_prefetch(src + idx[i + GATHER_PF] * esz, 0, 0);
            memcpy(dst + i * esz, src + idx[i] * esz, esz);
        }
    }
#undef GATHER_PF
}

/* ============================================================================
 * Filter execution
 * ============================================================================ */

/* Gather from a parted column using global row indices (sorted ascending).
 * Walks match_idx with an advancing segment cursor — O(count + n_segs). */
static void parted_gather_col(td_t* parted_col, const int64_t* match_idx,
                               int64_t count, td_t* dst_col) {
    int64_t n_segs = parted_col->len;
    td_t** segs = (td_t**)td_data(parted_col);
    int8_t base = (int8_t)TD_PARTED_BASETYPE(parted_col->type);
    uint8_t base_attrs = (base == TD_SYM && n_segs > 0 && segs[0])
                       ? segs[0]->attrs : 0;
    uint8_t esz = td_sym_elem_size(base, base_attrs);
    char* dst = (char*)td_data(dst_col);

    /* Build prefix-sum segment end table */
    int64_t seg_ends[n_segs];
    int64_t cumul = 0;
    for (int64_t i = 0; i < n_segs; i++) {
        cumul += segs[i]->len;
        seg_ends[i] = cumul;
    }

    /* Walk match_idx (sorted ascending) with advancing segment cursor */
    int64_t seg = 0;
    for (int64_t i = 0; i < count; i++) {
        int64_t row = match_idx[i];
        while (seg < n_segs - 1 && row >= seg_ends[seg]) seg++;
        int64_t seg_start = (seg > 0) ? seg_ends[seg - 1] : 0;
        int64_t local_row = row - seg_start;
        char* src = (char*)td_data(segs[seg]);
        memcpy(dst + i * esz, src + local_row * esz, esz);
    }
}

/* Filter a single vector by boolean predicate. */
static td_t* exec_filter_vec(td_t* input, td_t* pred, int64_t pass_count) {
    uint8_t esz = col_esz(input);
    td_t* result = col_vec_new(input, pass_count);
    if (!result || TD_IS_ERR(result)) return result;
    result->len = pass_count;

    td_morsel_t mi, mf;
    td_morsel_init(&mi, input);
    td_morsel_init(&mf, pred);
    int64_t out_idx = 0;

    while (td_morsel_next(&mi) && td_morsel_next(&mf)) {
        uint8_t* bits = (uint8_t*)mf.morsel_ptr;
        char* src = (char*)mi.morsel_ptr;
        char* dst = (char*)td_data(result);
        for (int64_t i = 0; i < mi.morsel_len; i++) {
            if (bits[i]) {
                memcpy(dst + out_idx * esz, src + i * esz, esz);
                out_idx++;
            }
        }
    }

    return result;
}

/* Filter a parted column by boolean predicate (sequential). */
static td_t* exec_filter_parted_vec(td_t* parted_col, td_t* pred,
                                     int64_t pass_count) {
    int8_t base = (int8_t)TD_PARTED_BASETYPE(parted_col->type);
    td_t** segs_peek = (td_t**)td_data(parted_col);
    uint8_t base_attrs = (base == TD_SYM && parted_col->len > 0 && segs_peek[0])
                       ? segs_peek[0]->attrs : 0;
    uint8_t esz = td_sym_elem_size(base, base_attrs);
    td_t* result = typed_vec_new(base, base_attrs, pass_count);
    if (!result || TD_IS_ERR(result)) return result;
    result->len = pass_count;

    td_t** segs = (td_t**)td_data(parted_col);
    int64_t n_segs = parted_col->len;
    int64_t out_idx = 0;
    int64_t pred_off = 0;
    uint8_t* pred_data = (uint8_t*)td_data(pred);

    for (int64_t s = 0; s < n_segs; s++) {
        int64_t seg_len = segs[s]->len;
        char* src = (char*)td_data(segs[s]);
        char* dst = (char*)td_data(result);
        for (int64_t i = 0; i < seg_len; i++) {
            if (pred_data[pred_off + i]) {
                memcpy(dst + out_idx * esz, src + i * esz, esz);
                out_idx++;
            }
        }
        pred_off += seg_len;
    }
    return result;
}

/* Sequential table filter fallback (small tables or alloc failure). */
static td_t* exec_filter_seq(td_t* input, td_t* pred, int64_t ncols,
                             int64_t pass_count) {
    td_t* tbl = td_table_new(ncols);
    if (!tbl || TD_IS_ERR(tbl)) return tbl;
    for (int64_t c = 0; c < ncols; c++) {
        td_t* col = td_table_get_col_idx(input, c);
        if (!col || TD_IS_ERR(col)) continue;
        int64_t name_id = td_table_col_name(input, c);
        if (col->type == TD_MAPCOMMON) {
            td_t* mc_filt = materialize_mapcommon_filter(col, pred, pass_count);
            if (!mc_filt || TD_IS_ERR(mc_filt)) { td_release(tbl); return mc_filt; }
            tbl = td_table_add_col(tbl, name_id, mc_filt);
            td_release(mc_filt);
            continue;
        }
        td_t* filtered;
        if (TD_IS_PARTED(col->type))
            filtered = exec_filter_parted_vec(col, pred, pass_count);
        else
            filtered = exec_filter_vec(col, pred, pass_count);
        if (!filtered || TD_IS_ERR(filtered)) { td_release(tbl); return filtered; }
        tbl = td_table_add_col(tbl, name_id, filtered);
        td_release(filtered);
    }
    return tbl;
}

static td_t* exec_filter(td_graph_t* g, td_op_t* op, td_t* input, td_t* pred) {
    (void)g;
    (void)op;
    if (!input || TD_IS_ERR(input)) return input;
    if (!pred || TD_IS_ERR(pred)) return pred;

    /* Count passing elements — single sequential scan over predicate */
    int64_t pass_count = 0;
    {
        td_morsel_t mp;
        td_morsel_init(&mp, pred);
        while (td_morsel_next(&mp)) {
            uint8_t* bits = (uint8_t*)mp.morsel_ptr;
            for (int64_t i = 0; i < mp.morsel_len; i++)
                if (bits[i]) pass_count++;
        }
    }

    /* Vector filter — single column, use sequential path */
    if (input->type != TD_TABLE)
        return exec_filter_vec(input, pred, pass_count);

    /* table filter: parallel gather using compact match index */
    int64_t ncols = td_table_ncols(input);
    int64_t nrows = td_table_nrows(input);

    /* Fall back to sequential for tiny inputs or degenerate tables */
    if (nrows <= TD_PARALLEL_THRESHOLD || ncols <= 0)
        return exec_filter_seq(input, pred, ncols, pass_count);

    /* VLA guard: cap at 256 columns for stack safety (256*16 = 4KB).
     * Wider tables fall back to sequential filter. */
    if (ncols > 256) return exec_filter_seq(input, pred, ncols, pass_count);

    /* Build match_idx: match_idx[j] = row of j-th matching element */
    td_t* idx_hdr = NULL;
    int64_t* match_idx = (int64_t*)scratch_alloc(&idx_hdr,
                                   (size_t)pass_count * sizeof(int64_t));
    if (!match_idx)
        return exec_filter_seq(input, pred, ncols, pass_count);

    {
        int64_t j = 0;
        td_morsel_t mp;
        td_morsel_init(&mp, pred);
        int64_t row_base = 0;
        while (td_morsel_next(&mp)) {
            uint8_t* bits = (uint8_t*)mp.morsel_ptr;
            for (int64_t i = 0; i < mp.morsel_len; i++)
                if (bits[i]) match_idx[j++] = row_base + i;
            row_base += mp.morsel_len;
        }
    }

    /* Parallel gather — same pattern as sort gather */
    td_pool_t* pool = td_pool_get();
    td_t* tbl = td_table_new(ncols);
    if (!tbl || TD_IS_ERR(tbl)) { scratch_free(idx_hdr); return tbl; }

    /* Pre-allocate output columns */
    td_t* new_cols[ncols];
    int64_t col_names[ncols];
    int64_t valid_ncols = 0;

    bool has_parted_cols = false;
    for (int64_t c = 0; c < ncols; c++) {
        td_t* col = td_table_get_col_idx(input, c);
        col_names[c] = td_table_col_name(input, c);
        if (!col || TD_IS_ERR(col)) { new_cols[c] = NULL; continue; }
        if (col->type == TD_MAPCOMMON) {
            /* Materialize MAPCOMMON through filter predicate */
            new_cols[c] = materialize_mapcommon_filter(col, pred, pass_count);
            if (new_cols[c] && !TD_IS_ERR(new_cols[c])) valid_ncols++;
            else new_cols[c] = NULL;
            continue;
        }
        int8_t out_type = TD_IS_PARTED(col->type)
                        ? (int8_t)TD_PARTED_BASETYPE(col->type)
                        : col->type;
        uint8_t out_attrs = 0;
        if (out_type == TD_SYM) {
            if (TD_IS_PARTED(col->type)) {
                td_t** sp = (td_t**)td_data(col);
                if (col->len > 0 && sp[0]) out_attrs = sp[0]->attrs;
            } else {
                out_attrs = col->attrs;
            }
        }
        if (TD_IS_PARTED(col->type)) has_parted_cols = true;
        td_t* nc = typed_vec_new(out_type, out_attrs, pass_count);
        if (!nc || TD_IS_ERR(nc)) { new_cols[c] = NULL; continue; }
        nc->len = pass_count;
        new_cols[c] = nc;
        valid_ncols++;
    }

    if (has_parted_cols) {
        /* Parted-aware gather: use parted_gather_col for parted columns,
         * sequential flat gather for non-parted columns */
        for (int64_t c = 0; c < ncols; c++) {
            td_t* col = td_table_get_col_idx(input, c);
            if (!col || !new_cols[c]) continue;
            if (col->type == TD_MAPCOMMON) continue; /* already materialized */
            if (TD_IS_PARTED(col->type)) {
                parted_gather_col(col, match_idx, pass_count, new_cols[c]);
            } else {
                uint8_t esz = col_esz(col);
                char* src = (char*)td_data(col);
                char* dst = (char*)td_data(new_cols[c]);
                for (int64_t i = 0; i < pass_count; i++)
                    memcpy(dst + i * esz, src + match_idx[i] * esz, esz);
            }
        }
    } else if (pool && valid_ncols > 0 && valid_ncols <= MGATHER_MAX_COLS) {
        /* Fused multi-column gather */
        multi_gather_ctx_t mgctx = { .idx = match_idx, .ncols = 0 };
        for (int64_t c = 0; c < ncols; c++) {
            if (!new_cols[c]) continue;
            td_t* col = td_table_get_col_idx(input, c);
            if (col && col->type == TD_MAPCOMMON) continue; /* already materialized */
            int64_t ci = mgctx.ncols;
            mgctx.srcs[ci] = (char*)td_data(col);
            mgctx.dsts[ci] = (char*)td_data(new_cols[c]);
            mgctx.esz[ci]  = col_esz(col);
            mgctx.ncols++;
        }
        td_pool_dispatch(pool, multi_gather_fn, &mgctx, pass_count);
    } else if (pool) {
        /* Per-column parallel gather */
        for (int64_t c = 0; c < ncols; c++) {
            td_t* col = td_table_get_col_idx(input, c);
            if (!col || !new_cols[c]) continue;
            gather_ctx_t gctx = {
                .idx = match_idx, .src_col = col, .dst_col = new_cols[c],
                .esz = col_esz(col), .nullable = false,
            };
            td_pool_dispatch(pool, gather_fn, &gctx, pass_count);
        }
    } else {
        /* Sequential gather with index */
        for (int64_t c = 0; c < ncols; c++) {
            td_t* col = td_table_get_col_idx(input, c);
            if (!col || !new_cols[c]) continue;
            uint8_t esz = col_esz(col);
            char* src = (char*)td_data(col);
            char* dst = (char*)td_data(new_cols[c]);
            for (int64_t i = 0; i < pass_count; i++)
                memcpy(dst + i * esz, src + match_idx[i] * esz, esz);
        }
    }

    for (int64_t c = 0; c < ncols; c++) {
        if (!new_cols[c]) continue;
        tbl = td_table_add_col(tbl, col_names[c], new_cols[c]);
        td_release(new_cols[c]);
    }

    scratch_free(idx_hdr);
    return tbl;
}

/* ============================================================================
 * exec_filter_head — filter table, keeping only the first `limit` matches
 *
 * Scans the predicate sequentially, collecting matching row indices and
 * stopping as soon as `limit` matches are found.  Only those rows are
 * gathered into the result table, avoiding full-table gather when the
 * number of matches far exceeds the limit.
 * ============================================================================ */
static td_t* exec_filter_head(td_t* input, td_t* pred, int64_t limit) {
    if (!input || TD_IS_ERR(input)) return input;
    if (!pred || TD_IS_ERR(pred)) return pred;
    if (input->type != TD_TABLE || pred->type != TD_BOOL) return input;

    int64_t ncols = td_table_ncols(input);
    int64_t nrows = td_table_nrows(input);
    if (limit <= 0 || ncols <= 0) return td_table_new(0);
    if (limit > nrows) limit = nrows;

    /* VLA guard */
    if (ncols > 256) return input;

    /* Collect up to `limit` matching row indices, stopping early */
    td_t* idx_hdr = NULL;
    int64_t* match_idx = (int64_t*)scratch_alloc(&idx_hdr,
                                    (size_t)limit * sizeof(int64_t));
    if (!match_idx) return input;

    int64_t found = 0;
    {
        td_morsel_t mp;
        td_morsel_init(&mp, pred);
        int64_t row_base = 0;
        while (td_morsel_next(&mp) && found < limit) {
            uint8_t* bits = (uint8_t*)mp.morsel_ptr;
            for (int64_t i = 0; i < mp.morsel_len && found < limit; i++)
                if (bits[i]) match_idx[found++] = row_base + i;
            row_base += mp.morsel_len;
        }
    }

    /* Build result table with gathered rows */
    td_t* tbl = td_table_new(ncols);
    if (!tbl || TD_IS_ERR(tbl)) { scratch_free(idx_hdr); return tbl; }

    for (int64_t c = 0; c < ncols; c++) {
        td_t* col = td_table_get_col_idx(input, c);
        int64_t name_id = td_table_col_name(input, c);
        if (!col) continue;
        int8_t out_type = TD_IS_PARTED(col->type)
                        ? (int8_t)TD_PARTED_BASETYPE(col->type) : col->type;
        if (out_type == TD_MAPCOMMON) continue;
        uint8_t out_attrs = 0;
        if (out_type == TD_SYM) {
            if (TD_IS_PARTED(col->type)) {
                td_t** sp = (td_t**)td_data(col);
                if (col->len > 0 && sp[0]) out_attrs = sp[0]->attrs;
            } else out_attrs = col->attrs;
        }
        uint8_t esz = td_sym_elem_size(out_type, out_attrs);
        td_t* new_col = typed_vec_new(out_type, out_attrs, found);
        if (!new_col || TD_IS_ERR(new_col)) continue;
        new_col->len = found;
        char* dst = (char*)td_data(new_col);

        if (TD_IS_PARTED(col->type)) {
            /* Parted column: build flat pointer + length arrays for lookup */
            td_t** segs = (td_t**)td_data(col);
            int64_t n_segs = col->len;
            /* Build prefix sums for segment offsets */
            int64_t seg_start = 0;
            int64_t cur_seg = 0;
            int64_t cur_seg_end = (n_segs > 0 && segs[0]) ? segs[0]->len : 0;
            for (int64_t j = 0; j < found; j++) {
                int64_t r = match_idx[j];
                while (cur_seg < n_segs - 1 && r >= cur_seg_end) {
                    seg_start = cur_seg_end;
                    cur_seg++;
                    cur_seg_end += segs[cur_seg] ? segs[cur_seg]->len : 0;
                }
                char* src = (char*)td_data(segs[cur_seg]);
                memcpy(dst + j * esz, src + (r - seg_start) * esz, esz);
            }
        } else {
            char* src = (char*)td_data(col);
            for (int64_t j = 0; j < found; j++)
                memcpy(dst + j * esz, src + match_idx[j] * esz, esz);
        }
        tbl = td_table_add_col(tbl, name_id, new_col);
        td_release(new_col);
    }

    scratch_free(idx_hdr);
    return tbl;
}

/* ============================================================================
 * sel_compact — materialize a table by applying a TD_SEL bitmap
 *
 * Used at boundary ops (sort/join/window) that need dense contiguous data.
 * Reuses the same parallel multi-column gather as exec_filter.
 * ============================================================================ */

static td_t* sel_compact(td_graph_t* g, td_t* tbl, td_t* sel) {
    (void)g;
    if (!tbl || TD_IS_ERR(tbl) || !sel || sel->type != TD_SEL)
        return tbl;

    int64_t nrows = td_table_nrows(tbl);
    td_sel_meta_t* meta = td_sel_meta(sel);
    int64_t pass_count = meta->total_pass;

    /* All-pass: nothing to compact */
    if (pass_count == nrows) { td_retain(tbl); return tbl; }

    /* None-pass: return empty table with same schema */
    if (pass_count == 0) {
        int64_t ncols = td_table_ncols(tbl);
        td_t* empty = td_table_new(ncols);
        if (!empty || TD_IS_ERR(empty)) return empty;
        for (int64_t c = 0; c < ncols; c++) {
            td_t* col = td_table_get_col_idx(tbl, c);
            if (!col) continue;
            int8_t ct = TD_IS_PARTED(col->type)
                      ? (int8_t)TD_PARTED_BASETYPE(col->type) : col->type;
            td_t* nc = td_vec_new(ct, 0);
            if (nc && !TD_IS_ERR(nc)) {
                nc->len = 0;
                empty = td_table_add_col(empty, td_table_col_name(tbl, c), nc);
                td_release(nc);
            }
        }
        return empty;
    }

    int64_t ncols = td_table_ncols(tbl);
    if (ncols <= 0) { td_retain(tbl); return tbl; }

    /* Build match_idx from bitmap */
    td_t* idx_hdr = NULL;
    int64_t* match_idx = (int64_t*)scratch_alloc(&idx_hdr,
                                       (size_t)pass_count * sizeof(int64_t));
    if (!match_idx) { td_retain(tbl); return tbl; }

    {
        const uint64_t* bits = td_sel_bits(sel);
        const uint8_t* flags = td_sel_flags(sel);
        uint32_t n_segs = meta->n_segs;
        int64_t j = 0;
        for (uint32_t seg = 0; seg < n_segs; seg++) {
            int64_t seg_start = (int64_t)seg * TD_MORSEL_ELEMS;
            int64_t seg_end = seg_start + TD_MORSEL_ELEMS;
            if (seg_end > nrows) seg_end = nrows;

            if (flags[seg] == TD_SEL_NONE) continue;
            if (flags[seg] == TD_SEL_ALL) {
                for (int64_t r = seg_start; r < seg_end; r++)
                    match_idx[j++] = r;
            } else {
                for (int64_t r = seg_start; r < seg_end; r++)
                    if (TD_SEL_BIT_TEST(bits, r)) match_idx[j++] = r;
            }
        }
    }

    /* Parallel multi-column gather (same pattern as exec_filter) */
    td_pool_t* pool = td_pool_get();
    td_t* out = td_table_new(ncols);
    if (!out || TD_IS_ERR(out)) { scratch_free(idx_hdr); return out; }

    /* VLA guard: 256 cols max for stack arrays */
    if (ncols > 256) ncols = 256;

    td_t* new_cols[ncols];
    int64_t col_names[ncols];
    int64_t valid_ncols = 0;
    bool has_parted = false;

    for (int64_t c = 0; c < ncols; c++) {
        td_t* col = td_table_get_col_idx(tbl, c);
        col_names[c] = td_table_col_name(tbl, c);
        if (!col || TD_IS_ERR(col)) { new_cols[c] = NULL; continue; }
        int8_t ct = TD_IS_PARTED(col->type)
                  ? (int8_t)TD_PARTED_BASETYPE(col->type) : col->type;
        uint8_t ca = 0;
        if (ct == TD_SYM) {
            if (TD_IS_PARTED(col->type)) {
                td_t** sp = (td_t**)td_data(col);
                if (col->len > 0 && sp[0]) ca = sp[0]->attrs;
            } else ca = col->attrs;
        }
        if (TD_IS_PARTED(col->type)) has_parted = true;
        td_t* nc = typed_vec_new(ct, ca, pass_count);
        if (!nc || TD_IS_ERR(nc)) { new_cols[c] = NULL; continue; }
        nc->len = pass_count;
        new_cols[c] = nc;
        valid_ncols++;
    }

    if (has_parted) {
        for (int64_t c = 0; c < ncols; c++) {
            td_t* col = td_table_get_col_idx(tbl, c);
            if (!col || !new_cols[c]) continue;
            if (TD_IS_PARTED(col->type)) {
                parted_gather_col(col, match_idx, pass_count, new_cols[c]);
            } else {
                uint8_t esz = col_esz(col);
                char* src = (char*)td_data(col);
                char* dst = (char*)td_data(new_cols[c]);
                for (int64_t i = 0; i < pass_count; i++)
                    memcpy(dst + i * esz, src + match_idx[i] * esz, esz);
            }
        }
    } else if (pool && valid_ncols > 0 && valid_ncols <= MGATHER_MAX_COLS) {
        multi_gather_ctx_t mgctx = { .idx = match_idx, .ncols = 0 };
        for (int64_t c = 0; c < ncols; c++) {
            if (!new_cols[c]) continue;
            td_t* col = td_table_get_col_idx(tbl, c);
            int64_t ci = mgctx.ncols;
            mgctx.srcs[ci] = (char*)td_data(col);
            mgctx.dsts[ci] = (char*)td_data(new_cols[c]);
            mgctx.esz[ci]  = col_esz(col);
            mgctx.ncols++;
        }
        td_pool_dispatch(pool, multi_gather_fn, &mgctx, pass_count);
    } else if (pool) {
        for (int64_t c = 0; c < ncols; c++) {
            td_t* col = td_table_get_col_idx(tbl, c);
            if (!col || !new_cols[c]) continue;
            gather_ctx_t gctx = {
                .idx = match_idx, .src_col = col, .dst_col = new_cols[c],
                .esz = col_esz(col), .nullable = false,
            };
            td_pool_dispatch(pool, gather_fn, &gctx, pass_count);
        }
    } else {
        for (int64_t c = 0; c < ncols; c++) {
            td_t* col = td_table_get_col_idx(tbl, c);
            if (!col || !new_cols[c]) continue;
            uint8_t esz = col_esz(col);
            char* src = (char*)td_data(col);
            char* dst = (char*)td_data(new_cols[c]);
            for (int64_t i = 0; i < pass_count; i++)
                memcpy(dst + i * esz, src + match_idx[i] * esz, esz);
        }
    }

    for (int64_t c = 0; c < ncols; c++) {
        if (!new_cols[c]) continue;
        out = td_table_add_col(out, col_names[c], new_cols[c]);
        td_release(new_cols[c]);
    }

    scratch_free(idx_hdr);
    return out;
}

/* ============================================================================
 * Sort execution (simple insertion sort)
 * ============================================================================ */

/* Forward declaration — exec_node is defined later */
static td_t* exec_node(td_graph_t* g, td_op_t* op);

/* --------------------------------------------------------------------------
 * Sort comparator: compare two row indices across all sort keys.
 * Returns negative if a < b, positive if a > b, 0 if equal.
 * -------------------------------------------------------------------------- */
/* Sort comparison context.
 * Bounds on desc[] and nulls_first[] are guaranteed by graph construction:
 * n_sort is uint8_t (max 255), and arrays are allocated to that size. */
typedef struct {
    td_t**       vecs;
    uint8_t*     desc;
    uint8_t*     nulls_first;
    uint8_t      n_sort;
} sort_cmp_ctx_t;

static int sort_cmp(const sort_cmp_ctx_t* ctx, int64_t a, int64_t b) {
    for (uint8_t k = 0; k < ctx->n_sort; k++) {
        td_t* col = ctx->vecs[k];
        if (!col) continue;
        int cmp = 0;
        int null_cmp = 0;
        int desc = ctx->desc ? ctx->desc[k] : 0;
        int nf = ctx->nulls_first ? ctx->nulls_first[k] : desc;

        if (col->type == TD_F64) {
            double va = ((double*)td_data(col))[a];
            double vb = ((double*)td_data(col))[b];
            int a_null = isnan(va);
            int b_null = isnan(vb);
            if (a_null && b_null) { cmp = 0; null_cmp = 1; }
            else if (a_null) { cmp = nf ? -1 : 1; null_cmp = 1; }
            else if (b_null) { cmp = nf ? 1 : -1; null_cmp = 1; }
            else if (va < vb) cmp = -1;
            else if (va > vb) cmp = 1;
        } else if (col->type == TD_I64 || col->type == TD_TIMESTAMP) {
            int64_t va = ((int64_t*)td_data(col))[a];
            int64_t vb = ((int64_t*)td_data(col))[b];
            if (va < vb) cmp = -1;
            else if (va > vb) cmp = 1;
        } else if (col->type == TD_I32) {
            int32_t va = ((int32_t*)td_data(col))[a];
            int32_t vb = ((int32_t*)td_data(col))[b];
            if (va < vb) cmp = -1;
            else if (va > vb) cmp = 1;
        } else if (TD_IS_SYM(col->type)) {
            int64_t va = td_read_sym(td_data(col), a, col->type, col->attrs);
            int64_t vb = td_read_sym(td_data(col), b, col->type, col->attrs);
            td_t* sa = td_sym_str(va);
            td_t* sb = td_sym_str(vb);
            if (sa && sb) cmp = td_str_cmp(sa, sb);
        } else if (col->type == TD_I16) {
            int16_t va = ((int16_t*)td_data(col))[a];
            int16_t vb = ((int16_t*)td_data(col))[b];
            if (va < vb) cmp = -1;
            else if (va > vb) cmp = 1;
        } else if (col->type == TD_BOOL || col->type == TD_U8) {
            uint8_t va = ((uint8_t*)td_data(col))[a];
            uint8_t vb = ((uint8_t*)td_data(col))[b];
            if (va < vb) cmp = -1;
            else if (va > vb) cmp = 1;
        } else if (col->type == TD_DATE || col->type == TD_TIME) {
            int32_t va = ((int32_t*)td_data(col))[a];
            int32_t vb = ((int32_t*)td_data(col))[b];
            if (va < vb) cmp = -1;
            else if (va > vb) cmp = 1;
        }

        if (desc && !null_cmp) cmp = -cmp;
        if (cmp != 0) return cmp;
    }
    return 0;
}

/* --------------------------------------------------------------------------
 * Parallel LSB radix sort (8-bit digits, 256 buckets)
 *
 * Used for single-key sorts on I64/F64/I32/SYM/TIMESTAMP columns,
 * and composite-key sorts where all keys are integer types with total
 * bit width <= 64.
 *
 * Three phases per byte:
 *   1. Parallel histogram — each task counts byte occurrences in its range
 *   2. Sequential prefix-sum — compute per-task scatter offsets
 *   3. Parallel scatter — write elements to sorted positions
 *
 * Byte-skip: after histogram, if all elements share the same byte value,
 * skip that pass entirely.  Critical for small-range integers where most
 * upper bytes are identical.
 * -------------------------------------------------------------------------- */

/* Radix pass context (shared across histogram + scatter phases) */
typedef struct {
    const uint64_t*  keys;
    const int64_t*   idx;
    uint64_t*        keys_out;
    int64_t*         idx_out;
    int64_t          n;
    uint8_t          shift;
    uint32_t         n_tasks;
    uint32_t*        hist;       /* flat [n_tasks * 256] */
    int64_t*         offsets;    /* flat [n_tasks * 256] */
} radix_pass_ctx_t;

/* Phase 1: histogram — each task counts byte values in its fixed range */
static void radix_hist_fn(void* arg, uint32_t wid, int64_t start, int64_t end) {
    (void)wid; (void)end;
    radix_pass_ctx_t* c = (radix_pass_ctx_t*)arg;
    int64_t task = start; /* dispatch_n: [task, task+1) */

    /* Zero histogram slice BEFORE early return — empty tasks must still
     * clear their slice so the prefix-sum sees zeros, not garbage. */
    uint32_t* h = c->hist + task * 256;
    memset(h, 0, 256 * sizeof(uint32_t));

    int64_t chunk = (c->n + c->n_tasks - 1) / c->n_tasks;
    int64_t lo = task * chunk;
    int64_t hi = lo + chunk;
    if (hi > c->n) hi = c->n;
    if (lo >= hi) return;

    const uint64_t* keys = c->keys;
    uint8_t shift = c->shift;
    for (int64_t i = lo; i < hi; i++)
        h[(keys[i] >> shift) & 0xFF]++;
}

/* Phase 3: scatter — each task writes its range to the correct output positions */
static void radix_scatter_fn(void* arg, uint32_t wid, int64_t start, int64_t end) {
    (void)wid; (void)end;
    radix_pass_ctx_t* c = (radix_pass_ctx_t*)arg;
    int64_t task = start;

    int64_t chunk = (c->n + c->n_tasks - 1) / c->n_tasks;
    int64_t lo = task * chunk;
    int64_t hi = lo + chunk;
    if (hi > c->n) hi = c->n;
    if (lo >= hi) return;

    int64_t* off = c->offsets + task * 256;
    const uint64_t* k_in = c->keys;
    const int64_t*  i_in = c->idx;
    uint64_t* k_out = c->keys_out;
    int64_t*  i_out = c->idx_out;
    uint8_t shift = c->shift;

    for (int64_t i = lo; i < hi; i++) {
        uint8_t byte = (k_in[i] >> shift) & 0xFF;
        int64_t pos = off[byte]++;
        k_out[pos] = k_in[i];
        i_out[pos] = i_in[i];
    }
}

/* Run radix sort on pre-encoded uint64_t keys + int64_t indices.
 * Returns pointer to the final sorted index array (either `indices` or
 * `idx_tmp`).  Caller must keep both alive until done reading indices
 * (the result may point into idx_tmp if an odd number of passes executed).
 * The corresponding itmp_hdr must not be freed until after gather completes.
 * Returns NULL on failure. */
static int64_t* radix_sort_run(td_pool_t* pool,
                                uint64_t* keys, int64_t* indices,
                                uint64_t* keys_tmp, int64_t* idx_tmp,
                                int64_t n) {
    uint32_t n_tasks = pool ? td_pool_total_workers(pool) : 1;
    if (n_tasks < 1) n_tasks = 1;

    td_t *hist_hdr = NULL, *off_hdr = NULL;
    uint32_t* hist = (uint32_t*)scratch_alloc(&hist_hdr,
                        (size_t)n_tasks * 256 * sizeof(uint32_t));
    int64_t* offsets = (int64_t*)scratch_alloc(&off_hdr,
                        (size_t)n_tasks * 256 * sizeof(int64_t));
    if (!hist || !offsets) {
        scratch_free(hist_hdr); scratch_free(off_hdr);
        return NULL;
    }

    uint64_t* src_k = keys,     *dst_k = keys_tmp;
    int64_t*  src_i = indices,   *dst_i = idx_tmp;

    for (uint8_t bp = 0; bp < 8; bp++) {
        uint8_t shift = bp * 8;

        radix_pass_ctx_t ctx = {
            .keys = src_k, .idx = src_i,
            .keys_out = dst_k, .idx_out = dst_i,
            .n = n, .shift = shift, .n_tasks = n_tasks,
            .hist = hist, .offsets = offsets,
        };

        /* Phase 1: parallel histogram */
        if (pool && n_tasks > 1)
            td_pool_dispatch_n(pool, radix_hist_fn, &ctx, n_tasks);
        else
            radix_hist_fn(&ctx, 0, 0, 1);

        /* Check uniformity via global histogram */
        bool uniform = false;
        for (int b = 0; b < 256; b++) {
            uint32_t total = 0;
            for (uint32_t t = 0; t < n_tasks; t++)
                total += hist[t * 256 + b];
            if (total == (uint32_t)n) { uniform = true; break; }
        }
        if (uniform) continue; /* all same byte — skip this pass */

        /* Phase 2: prefix sum → per-task scatter offsets */
        int64_t running = 0;
        for (int b = 0; b < 256; b++) {
            for (uint32_t t = 0; t < n_tasks; t++) {
                offsets[t * 256 + b] = running;
                running += hist[t * 256 + b];
            }
        }

        /* Phase 3: parallel scatter */
        if (pool && n_tasks > 1)
            td_pool_dispatch_n(pool, radix_scatter_fn, &ctx, n_tasks);
        else
            radix_scatter_fn(&ctx, 0, 0, 1);

        /* Swap double-buffer pointers */
        uint64_t* tk = src_k; src_k = dst_k; dst_k = tk;
        int64_t*  ti = src_i; src_i = dst_i; dst_i = ti;
    }

    scratch_free(hist_hdr);
    scratch_free(off_hdr);
    return src_i;  /* pointer to final sorted indices */
}

/* Key-encoding context for parallel encode phase */
typedef struct {
    uint64_t*       keys;      /* output */
    /* Single-key fields: */
    const void*     data;      /* raw column data */
    int8_t          type;      /* column type */
    uint8_t         col_attrs; /* TD_SYM width attrs */
    bool            desc;
    bool            nulls_first; /* for single-key F64: 1=nulls first */
    /* SYM rank mapping (NULL if not sym): */
    const uint32_t* enum_rank; /* intern_id → sort rank */
    /* Composite-key fields (n_keys > 1): */
    uint8_t         n_keys;
    td_t**          vecs;
    int64_t         mins[16];
    int64_t         ranges[16];
    uint8_t         bit_shifts[16]; /* bit offset for key k in composite */
    uint8_t         descs[16];
    const uint32_t* enum_ranks[16]; /* per-key rank mappings */
} radix_encode_ctx_t;

static void radix_encode_fn(void* arg, uint32_t wid, int64_t start, int64_t end) {
    (void)wid;
    radix_encode_ctx_t* c = (radix_encode_ctx_t*)arg;

    if (c->n_keys <= 1) {
        /* Single-key fast path */
        switch (c->type) {
        case TD_I64: case TD_TIMESTAMP: {
            const int64_t* d = (const int64_t*)c->data;
            if (c->desc) {
                for (int64_t i = start; i < end; i++)
                    c->keys[i] = ~((uint64_t)d[i] ^ ((uint64_t)1 << 63));
            } else {
                for (int64_t i = start; i < end; i++)
                    c->keys[i] = (uint64_t)d[i] ^ ((uint64_t)1 << 63);
            }
            break;
        }
        case TD_F64: {
            const double* d = (const double*)c->data;
            bool nf   = c->nulls_first;
            bool desc = c->desc;
            /* NaN override: encode NaN so it sorts first or last.
             * For ASC  NULLS FIRST → e=0            (smallest key)
             * For ASC  NULLS LAST  → e=UINT64_MAX   (largest key)
             * For DESC NULLS FIRST → e=UINT64_MAX   (~e=0, smallest)
             * For DESC NULLS LAST  → e=0            (~e=UINT64_MAX, largest)
             * Pattern: e = (nf ^ desc) ? 0 : UINT64_MAX */
            uint64_t nan_e = (nf ^ desc) ? 0 : UINT64_MAX;
            for (int64_t i = start; i < end; i++) {
                uint64_t bits;
                memcpy(&bits, &d[i], 8);
                /* NaN: exponent all-1s (0x7FF) and mantissa non-zero */
                if ((bits & 0x7FF0000000000000ULL) == 0x7FF0000000000000ULL &&
                    (bits & 0x000FFFFFFFFFFFFFULL)) {
                    c->keys[i] = desc ? ~nan_e : nan_e;
                } else {
                    uint64_t mask = -(bits >> 63) | ((uint64_t)1 << 63);
                    uint64_t e = bits ^ mask;
                    c->keys[i] = desc ? ~e : e;
                }
            }
            break;
        }
        case TD_I32: case TD_DATE: case TD_TIME: {
            const int32_t* d = (const int32_t*)c->data;
            if (c->desc) {
                for (int64_t i = start; i < end; i++)
                    c->keys[i] = ~((uint64_t)((uint32_t)d[i] ^ ((uint32_t)1 << 31)));
            } else {
                for (int64_t i = start; i < end; i++)
                    c->keys[i] = (uint64_t)((uint32_t)d[i] ^ ((uint32_t)1 << 31));
            }
            break;
        }
        case TD_SYM: {
            const uint32_t* rank = c->enum_rank;
            if (c->desc) {
                for (int64_t i = start; i < end; i++) {
                    uint32_t raw = (uint32_t)td_read_sym(c->data, i, c->type, c->col_attrs);
                    c->keys[i] = ~(uint64_t)rank[raw];
                }
            } else {
                for (int64_t i = start; i < end; i++) {
                    uint32_t raw = (uint32_t)td_read_sym(c->data, i, c->type, c->col_attrs);
                    c->keys[i] = (uint64_t)rank[raw];
                }
            }
            break;
        }
        case TD_BOOL: {
            const uint8_t* d = (const uint8_t*)c->data;
            if (c->desc) {
                for (int64_t i = start; i < end; i++)
                    c->keys[i] = ~(uint64_t)d[i];
            } else {
                for (int64_t i = start; i < end; i++)
                    c->keys[i] = (uint64_t)d[i];
            }
            break;
        }
        }
    } else {
        /* Composite-key encoding */
        for (int64_t i = start; i < end; i++) {
            uint64_t composite = 0;
            for (uint8_t k = 0; k < c->n_keys; k++) {
                td_t* col = c->vecs[k];
                int64_t val;
                if (c->enum_ranks[k]) {
                    uint32_t raw = (uint32_t)td_read_sym(td_data(col), i, col->type, col->attrs);
                    val = (int64_t)c->enum_ranks[k][raw];
                } else if (col->type == TD_I64 || col->type == TD_TIMESTAMP) {
                    val = ((const int64_t*)td_data(col))[i];
                } else if (col->type == TD_F64) {
                    uint64_t bits;
                    memcpy(&bits, &((const double*)td_data(col))[i], 8);
                    uint64_t mask = -(bits >> 63) | ((uint64_t)1 << 63);
                    val = (int64_t)(bits ^ mask);
                } else if (col->type == TD_I32 || col->type == TD_DATE || col->type == TD_TIME) {
                    val = (int64_t)((const int32_t*)td_data(col))[i];
                } else if (col->type == TD_BOOL) {
                    val = (int64_t)((const uint8_t*)td_data(col))[i];
                } else {
                    val = 0;
                }
                uint64_t part = (uint64_t)(val - c->mins[k]);
                if (c->descs[k]) part = (uint64_t)c->ranges[k] - part;
                composite |= part << c->bit_shifts[k];
            }
            c->keys[i] = composite;
        }
    }
}

/* Build SYM rank mapping: intern_id → sorted rank by string value.
 * Caller must scratch_free(*hdr_out) when done.
 * Returns pointer to rank array of size (max_id + 1), or NULL on error. */
/* Parallel max_id scan context */
typedef struct {
    const void* data;
    int8_t      type;
    uint8_t     attrs;
    uint32_t*   pw_max;  /* per-worker max */
} enum_max_ctx_t;

static void enum_max_fn(void* arg, uint32_t wid,
                         int64_t start, int64_t end) {
    enum_max_ctx_t* c = (enum_max_ctx_t*)arg;
    uint32_t local_max = c->pw_max[wid];
    for (int64_t i = start; i < end; i++) {
        uint32_t v = (uint32_t)td_read_sym(c->data, i, c->type, c->attrs);
        if (v > local_max) local_max = v;
    }
    c->pw_max[wid] = local_max;
}

static uint32_t* build_enum_rank(td_t* col, int64_t nrows, td_t** hdr_out) {
    const void* data = td_data(col);
    int8_t type = col->type;
    uint8_t attrs = col->attrs;

    /* Find max intern ID (parallel for large columns) */
    uint32_t max_id = 0;
    td_pool_t* pool = td_pool_get();
    if (pool && nrows > 100000) {
        uint32_t nw = td_pool_total_workers(pool);
        uint32_t pw_max[nw];
        memset(pw_max, 0, nw * sizeof(uint32_t));
        enum_max_ctx_t ectx = { .data = data, .type = type, .attrs = attrs, .pw_max = pw_max };
        td_pool_dispatch(pool, enum_max_fn, &ectx, nrows);
        for (uint32_t w = 0; w < nw; w++)
            if (pw_max[w] > max_id) max_id = pw_max[w];
    } else {
        for (int64_t i = 0; i < nrows; i++) {
            uint32_t v = (uint32_t)td_read_sym(data, i, type, attrs);
            if (v > max_id) max_id = v;
        }
    }

    if (max_id >= UINT32_MAX - 1) { *hdr_out = NULL; return NULL; }
    uint32_t n_ids = max_id + 1;

    /* Arena for temporaries (ids, ptrs, lens, tmp) — single reset at end */
    td_scratch_arena_t arena;
    td_scratch_arena_init(&arena);

    /* Allocate array of intern IDs to sort */
    uint32_t* ids = (uint32_t*)td_scratch_arena_push(&arena,
                        (size_t)n_ids * sizeof(uint32_t));
    if (!ids) { td_scratch_arena_reset(&arena); *hdr_out = NULL; return NULL; }
    for (uint32_t i = 0; i < n_ids; i++) ids[i] = i;

    /* Pre-cache raw string pointers and lengths for fast comparison */
    const char** ptrs = (const char**)td_scratch_arena_push(&arena,
                             (size_t)n_ids * sizeof(const char*));
    uint32_t* lens = (uint32_t*)td_scratch_arena_push(&arena,
                         (size_t)n_ids * sizeof(uint32_t));
    if (!ptrs || !lens) {
        td_scratch_arena_reset(&arena); *hdr_out = NULL; return NULL;
    }
    for (uint32_t i = 0; i < n_ids; i++) {
        td_t* s = td_sym_str((int64_t)i);
        if (s) {
            ptrs[i] = td_str_ptr(s);
            lens[i] = (uint32_t)td_str_len(s);
        } else {
            ptrs[i] = NULL;
            lens[i] = 0;
        }
    }

    /* Merge sort intern IDs by full string comparison.  For ≤100K SYM
     * values this completes in <1ms and correctly handles strings that
     * share long common prefixes (e.g. "id000000001"–"id000099999"). */
    {
        uint32_t* tmp = (uint32_t*)td_scratch_arena_push(&arena,
                             (size_t)n_ids * sizeof(uint32_t));
        if (!tmp) { td_scratch_arena_reset(&arena);
                    *hdr_out = NULL; return NULL; }

        /* Bottom-up merge sort */
        for (uint32_t width = 1; width < n_ids; width *= 2) {
            for (uint32_t i = 0; i < n_ids; i += 2 * width) {
                uint32_t lo = i;
                uint32_t mid = lo + width;
                if (mid > n_ids) mid = n_ids;
                uint32_t hi = lo + 2 * width;
                if (hi > n_ids) hi = n_ids;
                /* Merge ids[lo..mid) and ids[mid..hi) into tmp[lo..hi) */
                uint32_t a = lo, b = mid, k = lo;
                while (a < mid && b < hi) {
                    uint32_t ia = ids[a], ib = ids[b];
                    uint32_t la = lens[ia], lb = lens[ib];
                    uint32_t ml = la < lb ? la : lb;
                    int cmp = 0;
                    if (ml > 0) cmp = memcmp(ptrs[ia], ptrs[ib], ml);
                    if (cmp == 0) cmp = (la > lb) - (la < lb);
                    if (cmp <= 0) tmp[k++] = ids[a++];
                    else          tmp[k++] = ids[b++];
                }
                while (a < mid) tmp[k++] = ids[a++];
                while (b < hi)  tmp[k++] = ids[b++];
            }
            /* Swap ids and tmp */
            uint32_t* s = ids; ids = tmp; tmp = s;
        }
    }

    /* Build rank[intern_id] = sorted position (output — not arena'd) */
    td_t* rank_hdr;
    uint32_t* rank = (uint32_t*)scratch_calloc(&rank_hdr,
                        (size_t)n_ids * sizeof(uint32_t));
    if (!rank) { td_scratch_arena_reset(&arena); *hdr_out = NULL; return NULL; }

    for (uint32_t i = 0; i < n_ids; i++)
        rank[ids[i]] = i;

    td_scratch_arena_reset(&arena);  /* free all temporaries at once */
    *hdr_out = rank_hdr;
    return rank;
}

/* Insertion sort for small arrays — used as base case for merge sort */
static void sort_insertion(const sort_cmp_ctx_t* ctx, int64_t* arr, int64_t n) {
    for (int64_t i = 1; i < n; i++) {
        int64_t key = arr[i];
        int64_t j = i - 1;
        while (j >= 0 && sort_cmp(ctx, arr[j], key) > 0) {
            arr[j + 1] = arr[j];
            j--;
        }
        arr[j + 1] = key;
    }
}

/* Single-threaded merge sort (recursive, with insertion sort base case) */
static void sort_merge_recursive(const sort_cmp_ctx_t* ctx,
                                  int64_t* arr, int64_t* tmp, int64_t n) {
    if (n <= 64) {
        sort_insertion(ctx, arr, n);
        return;
    }
    int64_t mid = n / 2;
    sort_merge_recursive(ctx, arr, tmp, mid);
    sort_merge_recursive(ctx, arr + mid, tmp + mid, n - mid);

    /* Merge arr[0..mid) and arr[mid..n) into tmp, then copy back */
    int64_t i = 0, j = mid, k = 0;
    while (i < mid && j < n) {
        if (sort_cmp(ctx, arr[i], arr[j]) <= 0)
            tmp[k++] = arr[i++];
        else
            tmp[k++] = arr[j++];
    }
    while (i < mid) tmp[k++] = arr[i++];
    while (j < n) tmp[k++] = arr[j++];
    memcpy(arr, tmp, (size_t)n * sizeof(int64_t));
}

/* Parallel sort phase 1 context */
typedef struct {
    const sort_cmp_ctx_t* cmp_ctx;
    int64_t*  indices;
    int64_t*  tmp;
    int64_t   nrows;
    uint32_t  n_chunks;
} sort_phase1_ctx_t;

static void sort_phase1_fn(void* arg, uint32_t worker_id, int64_t start, int64_t end) {
    (void)worker_id;
    sort_phase1_ctx_t* ctx = (sort_phase1_ctx_t*)arg;
    for (int64_t chunk_idx = start; chunk_idx < end; chunk_idx++) {
        int64_t chunk_size = (ctx->nrows + ctx->n_chunks - 1) / ctx->n_chunks;
        int64_t lo = chunk_idx * chunk_size;
        int64_t hi = lo + chunk_size;
        if (hi > ctx->nrows) hi = ctx->nrows;
        if (lo >= hi) continue;
        sort_merge_recursive(ctx->cmp_ctx, ctx->indices + lo, ctx->tmp + lo, hi - lo);
    }
}

/* Merge two adjacent sorted runs: [lo..mid) and [mid..hi) from src into dst */
static void merge_runs(const sort_cmp_ctx_t* ctx,
                        const int64_t* src, int64_t* dst,
                        int64_t lo, int64_t mid, int64_t hi) {
    int64_t i = lo, j = mid, k = lo;
    while (i < mid && j < hi) {
        if (sort_cmp(ctx, src[i], src[j]) <= 0)
            dst[k++] = src[i++];
        else
            dst[k++] = src[j++];
    }
    while (i < mid) dst[k++] = src[i++];
    while (j < hi) dst[k++] = src[j++];
}

/* Parallel merge pass context */
typedef struct {
    const sort_cmp_ctx_t* cmp_ctx;
    const int64_t*  src;
    int64_t*        dst;
    int64_t         nrows;
    int64_t         run_size;
} sort_merge_ctx_t;

static void sort_merge_fn(void* arg, uint32_t worker_id, int64_t start, int64_t end) {
    (void)worker_id;
    sort_merge_ctx_t* ctx = (sort_merge_ctx_t*)arg;
    for (int64_t pair_idx = start; pair_idx < end; pair_idx++) {
        int64_t lo = pair_idx * 2 * ctx->run_size;
        int64_t mid = lo + ctx->run_size;
        int64_t hi = mid + ctx->run_size;
        if (mid > ctx->nrows) mid = ctx->nrows;
        if (hi > ctx->nrows) hi = ctx->nrows;
        if (lo >= ctx->nrows) continue;
        if (mid >= hi) {
            /* Only one run — copy directly */
            memcpy(ctx->dst + lo, ctx->src + lo, (size_t)(hi - lo) * sizeof(int64_t));
        } else {
            merge_runs(ctx->cmp_ctx, ctx->src, ctx->dst, lo, mid, hi);
        }
    }
}

/* --------------------------------------------------------------------------
 * Parallel multi-key min/max prescan for composite radix sort.
 * Each worker scans all n_keys columns over its row range, then the main
 * thread merges per-worker results.
 * -------------------------------------------------------------------------- */

#define MK_PRESCAN_MAX_KEYS 8  /* max sort keys for stack allocation */

typedef struct {
    td_t*     const* vecs;
    uint32_t* const* enum_ranks;
    uint8_t          n_keys;
    int64_t          nrows;
    uint32_t         n_workers;
    /* per-worker results: [n_workers][n_keys] */
    int64_t*         pw_mins;
    int64_t*         pw_maxs;
} mk_prescan_ctx_t;

static void mk_prescan_fn(void* arg, uint32_t wid,
                           int64_t start, int64_t end) {
    mk_prescan_ctx_t* c = (mk_prescan_ctx_t*)arg;
    uint8_t nk = c->n_keys;
    int64_t* my_mins = c->pw_mins + (int64_t)wid * nk;
    int64_t* my_maxs = c->pw_maxs + (int64_t)wid * nk;

    /* Initialize on first morsel, merge on subsequent */
    for (uint8_t k = 0; k < nk; k++) {
        if (my_mins[k] == INT64_MAX) {
            /* first morsel for this worker — will be set below */
        }
    }

    for (uint8_t k = 0; k < nk; k++) {
        td_t* col = c->vecs[k];
        int64_t kmin = my_mins[k], kmax = my_maxs[k];

        if (c->enum_ranks[k]) {
            const void* cdata = td_data(col);
            int8_t ctype = col->type;
            uint8_t cattrs = col->attrs;
            const uint32_t* ranks = c->enum_ranks[k];
            for (int64_t i = start; i < end; i++) {
                uint32_t raw = (uint32_t)td_read_sym(cdata, i, ctype, cattrs);
                int64_t v = (int64_t)ranks[raw];
                if (v < kmin) kmin = v;
                if (v > kmax) kmax = v;
            }
        } else if (col->type == TD_I64 || col->type == TD_TIMESTAMP) {
            const int64_t* d = (const int64_t*)td_data(col);
            for (int64_t i = start; i < end; i++) {
                if (d[i] < kmin) kmin = d[i];
                if (d[i] > kmax) kmax = d[i];
            }
        } else if (col->type == TD_F64) {
            const double* d = (const double*)td_data(col);
            for (int64_t i = start; i < end; i++) {
                uint64_t bits;
                memcpy(&bits, &d[i], 8);
                uint64_t mask = -(bits >> 63) | ((uint64_t)1 << 63);
                int64_t v = (int64_t)(bits ^ mask);
                if (v < kmin) kmin = v;
                if (v > kmax) kmax = v;
            }
        } else if (col->type == TD_I32 || col->type == TD_DATE || col->type == TD_TIME) {
            const int32_t* d = (const int32_t*)td_data(col);
            for (int64_t i = start; i < end; i++) {
                int64_t v = (int64_t)d[i];
                if (v < kmin) kmin = v;
                if (v > kmax) kmax = v;
            }
        } else if (col->type == TD_BOOL) {
            const uint8_t* d = (const uint8_t*)td_data(col);
            for (int64_t i = start; i < end; i++) {
                int64_t v = (int64_t)d[i];
                if (v < kmin) kmin = v;
                if (v > kmax) kmax = v;
            }
        }

        my_mins[k] = kmin;
        my_maxs[k] = kmax;
    }
}

/* --------------------------------------------------------------------------
 * Top-N heap selection: for ORDER BY ... LIMIT N where N is small,
 * a single-pass heap beats the 8-pass radix sort.
 * -------------------------------------------------------------------------- */

typedef struct { uint64_t key; int64_t idx; } topn_entry_t;

static inline void topn_sift_down(topn_entry_t* h, int64_t n, int64_t i) {
    for (;;) {
        int64_t largest = i, l = 2*i+1, r = 2*i+2;
        if (l < n && h[l].key > h[largest].key) largest = l;
        if (r < n && h[r].key > h[largest].key) largest = r;
        if (largest == i) return;
        topn_entry_t t = h[i]; h[i] = h[largest]; h[largest] = t;
        i = largest;
    }
}

/* --------------------------------------------------------------------------
 * Fused encode + top-N: composite-key encode and heap insert in one pass,
 * avoiding the 80MB intermediate keys array.
 * -------------------------------------------------------------------------- */

typedef struct {
    int64_t         limit;
    topn_entry_t*   heaps;   /* [n_workers][limit] */
    int64_t*        counts;
    /* Composite-key encode params (same as radix_encode_ctx_t fields): */
    uint8_t         n_keys;
    td_t**          vecs;
    int64_t         mins[16];
    int64_t         ranges[16];
    uint8_t         bit_shifts[16];
    uint8_t         descs[16];
    const uint32_t* enum_ranks[16];
} fused_topn_ctx_t;

static void fused_topn_fn(void* arg, uint32_t wid,
                           int64_t start, int64_t end) {
    fused_topn_ctx_t* c = (fused_topn_ctx_t*)arg;
    int64_t K = c->limit;
    topn_entry_t* heap = c->heaps + (int64_t)wid * K;
    int64_t cnt = c->counts[wid];
    uint8_t nk = c->n_keys;

    for (int64_t i = start; i < end; i++) {
        /* Inline composite key encode */
        uint64_t composite = 0;
        for (uint8_t k = 0; k < nk; k++) {
            td_t* col = c->vecs[k];
            int64_t val;
            if (c->enum_ranks[k]) {
                uint32_t raw = (uint32_t)td_read_sym(td_data(col), i, col->type, col->attrs);
                val = (int64_t)c->enum_ranks[k][raw];
            } else if (col->type == TD_I64 || col->type == TD_TIMESTAMP) {
                val = ((const int64_t*)td_data(col))[i];
            } else if (col->type == TD_F64) {
                uint64_t bits;
                memcpy(&bits, &((const double*)td_data(col))[i], 8);
                uint64_t mask = -(bits >> 63) | ((uint64_t)1 << 63);
                val = (int64_t)(bits ^ mask);
            } else if (col->type == TD_I32 || col->type == TD_DATE || col->type == TD_TIME) {
                val = (int64_t)((const int32_t*)td_data(col))[i];
            } else if (col->type == TD_BOOL) {
                val = (int64_t)((const uint8_t*)td_data(col))[i];
            } else {
                val = 0;
            }
            uint64_t part = (uint64_t)(val - c->mins[k]);
            if (c->descs[k]) part = (uint64_t)c->ranges[k] - part;
            composite |= part << c->bit_shifts[k];
        }

        /* Inline heap insert */
        if (cnt < K) {
            heap[cnt].key = composite;
            heap[cnt].idx = i;
            cnt++;
            if (cnt == K) {
                for (int64_t j = K/2 - 1; j >= 0; j--)
                    topn_sift_down(heap, K, j);
            }
        } else if (composite < heap[0].key) {
            heap[0].key = composite;
            heap[0].idx = i;
            topn_sift_down(heap, K, 0);
        }
    }
    c->counts[wid] = cnt;
}

typedef struct {
    const uint64_t* keys;
    int64_t         limit;
    topn_entry_t*   heaps;   /* [n_workers][limit] */
    int64_t*        counts;  /* actual count per worker */
} topn_ctx_t;

static void topn_scan_fn(void* arg, uint32_t wid, int64_t start, int64_t end) {
    topn_ctx_t* c = (topn_ctx_t*)arg;
    int64_t K = c->limit;
    topn_entry_t* heap = c->heaps + (int64_t)wid * K;
    const uint64_t* keys = c->keys;
    int64_t cnt = c->counts[wid];   /* accumulate across morsels */

    for (int64_t i = start; i < end; i++) {
        uint64_t k = keys[i];
        if (cnt < K) {
            heap[cnt].key = k;
            heap[cnt].idx = i;
            cnt++;
            if (cnt == K) {
                for (int64_t j = K/2 - 1; j >= 0; j--)
                    topn_sift_down(heap, K, j);
            }
        } else if (k < heap[0].key) {
            heap[0].key = k;
            heap[0].idx = i;
            topn_sift_down(heap, K, 0);
        }
    }
    c->counts[wid] = cnt;
}

#define TOPN_MAX 8192  /* max limit for heap-based top-N (merge VLA ≤ 128KB) */

static int64_t topn_merge_fused(fused_topn_ctx_t* ctx, uint32_t n_workers,
                                 int64_t* out, int64_t limit) {
    /* Clamp to TOPN_MAX for VLA stack safety (≤ 128KB). */
    if (limit > TOPN_MAX) limit = TOPN_MAX;
    topn_entry_t merge[limit];
    int64_t cnt = 0;
    for (uint32_t w = 0; w < n_workers; w++) {
        topn_entry_t* wh = ctx->heaps + (int64_t)w * limit;
        int64_t wc = ctx->counts[w];
        for (int64_t j = 0; j < wc; j++) {
            if (cnt < limit) {
                merge[cnt++] = wh[j];
                if (cnt == limit) {
                    for (int64_t m = limit/2 - 1; m >= 0; m--)
                        topn_sift_down(merge, limit, m);
                }
            } else if (wh[j].key < merge[0].key) {
                merge[0] = wh[j];
                topn_sift_down(merge, limit, 0);
            }
        }
    }
    if (cnt > 1) {
        for (int64_t m = cnt/2 - 1; m >= 0; m--)
            topn_sift_down(merge, cnt, m);
        for (int64_t i = cnt - 1; i > 0; i--) {
            topn_entry_t t = merge[0]; merge[0] = merge[i]; merge[i] = t;
            topn_sift_down(merge, i, 0);
        }
    }
    for (int64_t i = 0; i < cnt; i++)
        out[i] = merge[i].idx;
    return cnt;
}

/* Merge per-worker heaps → sorted indices in out[0..return_val-1]. */
static int64_t topn_merge(topn_ctx_t* ctx, uint32_t n_workers,
                           int64_t* out, int64_t limit) {
    /* Clamp to TOPN_MAX for VLA stack safety (≤ 128KB). */
    if (limit > TOPN_MAX) limit = TOPN_MAX;
    topn_entry_t merge[limit];
    int64_t cnt = 0;

    for (uint32_t w = 0; w < n_workers; w++) {
        topn_entry_t* wh = ctx->heaps + (int64_t)w * limit;
        int64_t wc = ctx->counts[w];
        for (int64_t j = 0; j < wc; j++) {
            if (cnt < limit) {
                merge[cnt++] = wh[j];
                if (cnt == limit) {
                    for (int64_t m = limit/2 - 1; m >= 0; m--)
                        topn_sift_down(merge, limit, m);
                }
            } else if (wh[j].key < merge[0].key) {
                merge[0] = wh[j];
                topn_sift_down(merge, limit, 0);
            }
        }
    }

    /* Heapsort for ascending order */
    if (cnt > 1) {
        for (int64_t m = cnt/2 - 1; m >= 0; m--)
            topn_sift_down(merge, cnt, m);
        for (int64_t i = cnt - 1; i > 0; i--) {
            topn_entry_t t = merge[0]; merge[0] = merge[i]; merge[i] = t;
            topn_sift_down(merge, i, 0);
        }
    }

    for (int64_t i = 0; i < cnt; i++)
        out[i] = merge[i].idx;
    return cnt;
}

static td_t* exec_sort(td_graph_t* g, td_op_t* op, td_t* tbl, int64_t limit) {
    if (!tbl || TD_IS_ERR(tbl)) return tbl;

    td_op_ext_t* ext = find_ext(g, op->id);
    if (!ext) return TD_ERR_PTR(TD_ERR_NYI);

    int64_t nrows = td_table_nrows(tbl);
    int64_t ncols = td_table_ncols(tbl);
    if (ncols > 4096) return TD_ERR_PTR(TD_ERR_NYI); /* stack safety */
    uint8_t n_sort = ext->sort.n_cols;

    /* Allocate index array */
    td_t* indices_hdr;
    int64_t* indices = (int64_t*)scratch_alloc(&indices_hdr, (size_t)nrows * sizeof(int64_t));
    if (!indices) return TD_ERR_PTR(TD_ERR_OOM);
    for (int64_t i = 0; i < nrows; i++) indices[i] = i;

    /* Resolve sort key vectors */
    td_t* sort_vecs[n_sort > 0 ? n_sort : 1];
    uint8_t sort_owned[n_sort > 0 ? n_sort : 1];
    memset(sort_vecs, 0, (n_sort > 0 ? n_sort : 1) * sizeof(td_t*));
    memset(sort_owned, 0, n_sort > 0 ? n_sort : 1);

    for (uint8_t k = 0; k < n_sort; k++) {
        td_op_t* key_op = ext->sort.columns[k];
        td_op_ext_t* key_ext = find_ext(g, key_op->id);
        if (key_ext && key_ext->base.opcode == OP_SCAN) {
            sort_vecs[k] = td_table_get_col(tbl, key_ext->sym);
        } else {
            td_t* saved = g->table;
            g->table = tbl;
            sort_vecs[k] = exec_node(g, key_op);
            g->table = saved;
            sort_owned[k] = 1;
        }
        if (!sort_vecs[k] || TD_IS_ERR(sort_vecs[k])) {
            td_t* err = sort_vecs[k] ? sort_vecs[k] : TD_ERR_PTR(TD_ERR_NYI);
            for (uint8_t j = 0; j < k; j++) {
                if (sort_owned[j] && sort_vecs[j] && !TD_IS_ERR(sort_vecs[j]))
                    td_release(sort_vecs[j]);
            }
            scratch_free(indices_hdr);
            return err;
        }
    }

    /* --- Radix sort fast path ------------------------------------------------
     * Try radix sort for integer/float/enum keys.  Falls back to merge sort
     * for unsupported types (SYM with arbitrary strings, mixed types, etc.). */
    bool radix_done = false;
    int64_t* sorted_idx = indices;  /* may point to itmp after radix sort */
    td_t* radix_itmp_hdr = NULL;   /* kept alive until after gather */
    td_t* enum_rank_hdrs[n_sort];
    memset(enum_rank_hdrs, 0, n_sort * sizeof(td_t*));

    if (nrows > 64) {
        /* Check if all sort keys are radix-sortable types */
        bool can_radix = true;
        for (uint8_t k = 0; k < n_sort; k++) {
            if (!sort_vecs[k]) { can_radix = false; break; }
            int8_t t = sort_vecs[k]->type;
            if (t != TD_I64 && t != TD_F64 && t != TD_I32 && t != TD_DATE &&
                t != TD_SYM &&
                t != TD_TIME && t != TD_TIMESTAMP) {
                can_radix = false; break;
            }
        }

        if (can_radix) {
            td_pool_t* pool = td_pool_get();

            /* Build SYM rank mappings (intern_id → sorted rank by string) */
            uint32_t* enum_ranks[n_sort];
            memset(enum_ranks, 0, n_sort * sizeof(uint32_t*));
            for (uint8_t k = 0; k < n_sort; k++) {
                if (TD_IS_SYM(sort_vecs[k]->type)) {
                    enum_ranks[k] = build_enum_rank(sort_vecs[k], nrows,
                                                     &enum_rank_hdrs[k]);
                    if (!enum_ranks[k]) { can_radix = false; break; }
                }
            }

            if (can_radix && n_sort == 1) {
                /* --- Single-key sort --- */
                bool use_topn = (limit > 0 && limit <= TOPN_MAX
                                 && nrows > limit * 8);

                /* Encode keys (needed by both paths) */
                td_t *keys_hdr;
                uint64_t* keys = (uint64_t*)scratch_alloc(&keys_hdr,
                                    (size_t)nrows * sizeof(uint64_t));
                if (keys) {
                    bool desc = ext->sort.desc ? ext->sort.desc[0] : 0;
                    /* Default: ASC → nulls last (nf=0), DESC → nulls first (nf=1) */
                    bool nf = ext->sort.nulls_first ? ext->sort.nulls_first[0] : desc;
                    radix_encode_ctx_t enc = {
                        .keys = keys, .data = td_data(sort_vecs[0]),
                        .type = sort_vecs[0]->type,
                        .col_attrs = sort_vecs[0]->attrs,
                        .desc = desc,
                        .nulls_first = nf,
                        .enum_rank = enum_ranks[0], .n_keys = 1,
                    };
                    if (pool)
                        td_pool_dispatch(pool, radix_encode_fn, &enc, nrows);
                    else
                        radix_encode_fn(&enc, 0, 0, nrows);

                    if (use_topn) {
                        /* Top-N heap selection (1 pass over keys) */
                        uint32_t nw = pool ? td_pool_total_workers(pool) : 1;
                        td_t* heaps_hdr;
                        topn_entry_t* heaps = (topn_entry_t*)scratch_alloc(
                            &heaps_hdr, (size_t)nw * (size_t)limit * sizeof(topn_entry_t));
                        int64_t wc[nw];
                        memset(wc, 0, (size_t)nw * sizeof(int64_t));
                        if (heaps) {
                            topn_ctx_t tctx = {
                                .keys = keys, .limit = limit,
                                .heaps = heaps, .counts = wc,
                            };
                            if (pool)
                                td_pool_dispatch(pool, topn_scan_fn, &tctx, nrows);
                            else
                                topn_scan_fn(&tctx, 0, 0, nrows);

                            topn_merge(&tctx, nw, indices, limit);
                            sorted_idx = indices;
                            radix_done = true;
                        }
                        scratch_free(heaps_hdr);
                    } else {
                        /* Full 8-pass radix sort */
                        td_t *ktmp_hdr, *itmp_hdr;
                        uint64_t* ktmp = (uint64_t*)scratch_alloc(&ktmp_hdr,
                                            (size_t)nrows * sizeof(uint64_t));
                        int64_t*  itmp = (int64_t*)scratch_alloc(&itmp_hdr,
                                            (size_t)nrows * sizeof(int64_t));
                        if (ktmp && itmp) {
                            sorted_idx = radix_sort_run(pool, keys, indices,
                                                         ktmp, itmp, nrows);
                            radix_done = (sorted_idx != NULL);
                        }
                        scratch_free(ktmp_hdr);
                        if (sorted_idx != itmp) scratch_free(itmp_hdr);
                        else radix_itmp_hdr = itmp_hdr;
                    }
                }
                scratch_free(keys_hdr);

            } else if (can_radix && n_sort > 1) {
                /* --- Multi-key composite radix sort --- */
                int64_t mins[n_sort], maxs[n_sort];
                uint8_t total_bits = 0;
                bool fits = true;

                if (n_sort <= MK_PRESCAN_MAX_KEYS && pool) {
                    uint32_t nw = td_pool_total_workers(pool);
                    size_t pw_count = (size_t)nw * n_sort;
                    int64_t pw_mins_stack[512], pw_maxs_stack[512];
                    td_t *pw_mins_hdr = NULL, *pw_maxs_hdr = NULL;
                    int64_t* pw_mins = (pw_count <= 512)
                        ? pw_mins_stack
                        : (int64_t*)scratch_alloc(&pw_mins_hdr, pw_count * sizeof(int64_t));
                    int64_t* pw_maxs = (pw_count <= 512)
                        ? pw_maxs_stack
                        : (int64_t*)scratch_alloc(&pw_maxs_hdr, pw_count * sizeof(int64_t));
                    for (size_t i = 0; i < pw_count; i++) {
                        pw_mins[i] = INT64_MAX;
                        pw_maxs[i] = INT64_MIN;
                    }
                    mk_prescan_ctx_t pctx = {
                        .vecs = sort_vecs, .enum_ranks = enum_ranks,
                        .n_keys = n_sort, .nrows = nrows, .n_workers = nw,
                        .pw_mins = pw_mins, .pw_maxs = pw_maxs,
                    };
                    td_pool_dispatch(pool, mk_prescan_fn, &pctx, nrows);

                    /* Merge per-worker results */
                    for (uint8_t k = 0; k < n_sort; k++) {
                        int64_t kmin = INT64_MAX, kmax = INT64_MIN;
                        for (uint32_t w = 0; w < nw; w++) {
                            int64_t wmin = pw_mins[w * n_sort + k];
                            int64_t wmax = pw_maxs[w * n_sort + k];
                            if (wmin < kmin) kmin = wmin;
                            if (wmax > kmax) kmax = wmax;
                        }
                        mins[k] = kmin;
                        maxs[k] = kmax;
                        uint64_t range = (uint64_t)(kmax - kmin);
                        uint8_t bits = 1;
                        while (((uint64_t)1 << bits) <= range && bits < 64)
                            bits++;
                        total_bits += bits;
                    }
                    if (pw_mins_hdr) scratch_free(pw_mins_hdr);
                    if (pw_maxs_hdr) scratch_free(pw_maxs_hdr);
                } else {
                    /* Sequential fallback (no pool or too many keys) */
                    for (uint8_t k = 0; k < n_sort; k++) {
                        td_t* col = sort_vecs[k];
                        int64_t kmin = INT64_MAX, kmax = INT64_MIN;

                        if (enum_ranks[k]) {
                            const void* cdata = td_data(col);
                            int8_t ctype = col->type;
                            uint8_t cattrs = col->attrs;
                            for (int64_t i = 0; i < nrows; i++) {
                                uint32_t raw = (uint32_t)td_read_sym(cdata, i, ctype, cattrs);
                                int64_t v = (int64_t)enum_ranks[k][raw];
                                if (v < kmin) kmin = v;
                                if (v > kmax) kmax = v;
                            }
                        } else if (col->type == TD_I64 || col->type == TD_TIMESTAMP) {
                            const int64_t* d = (const int64_t*)td_data(col);
                            for (int64_t i = 0; i < nrows; i++) {
                                if (d[i] < kmin) kmin = d[i];
                                if (d[i] > kmax) kmax = d[i];
                            }
                        } else if (col->type == TD_I32 || col->type == TD_DATE || col->type == TD_TIME) {
                            const int32_t* d = (const int32_t*)td_data(col);
                            for (int64_t i = 0; i < nrows; i++) {
                                if (d[i] < kmin) kmin = (int64_t)d[i];
                                if (d[i] > kmax) kmax = (int64_t)d[i];
                            }
                        }

                        mins[k] = kmin;
                        maxs[k] = kmax;
                        uint64_t range = (uint64_t)(kmax - kmin);
                        uint8_t bits = 1;
                        while (((uint64_t)1 << bits) <= range && bits < 64)
                            bits++;
                        total_bits += bits;
                    }
                }

                if (total_bits > 64) fits = false;

                if (fits) {
                    /* Compute bit-shift for each key: primary key in MSBs */
                    uint8_t bit_shifts[n_sort];
                    uint8_t accum = 0;
                    for (int k = n_sort - 1; k >= 0; k--) {
                        bit_shifts[k] = accum;
                        uint64_t range = (uint64_t)(maxs[k] - mins[k]);
                        uint8_t bits = 1;
                        while (((uint64_t)1 << bits) <= range && bits < 64)
                            bits++;
                        accum += bits;
                    }

                    bool use_topn = (limit > 0 && limit <= TOPN_MAX
                                     && nrows > limit * 8);

                    if (use_topn) {
                        /* Fused encode + top-N: no 80MB keys array needed */
                        uint32_t nw = pool ? td_pool_total_workers(pool) : 1;
                        td_t* heaps_hdr;
                        topn_entry_t* heaps = (topn_entry_t*)scratch_alloc(
                            &heaps_hdr, (size_t)nw * (size_t)limit * sizeof(topn_entry_t));
                        int64_t wc[nw];
                        memset(wc, 0, (size_t)nw * sizeof(int64_t));
                        if (heaps) {
                            fused_topn_ctx_t fctx = {
                                .limit = limit, .heaps = heaps, .counts = wc,
                                .n_keys = n_sort, .vecs = sort_vecs,
                            };
                            for (uint8_t k = 0; k < n_sort; k++) {
                                fctx.mins[k] = mins[k];
                                fctx.ranges[k] = maxs[k] - mins[k];
                                fctx.bit_shifts[k] = bit_shifts[k];
                                fctx.descs[k] = ext->sort.desc ? ext->sort.desc[k] : 0;
                                fctx.enum_ranks[k] = enum_ranks[k];
                            }
                            if (pool)
                                td_pool_dispatch(pool, fused_topn_fn, &fctx, nrows);
                            else
                                fused_topn_fn(&fctx, 0, 0, nrows);

                            topn_merge_fused(&fctx, nw, indices, limit);
                            sorted_idx = indices;
                            radix_done = true;
                        }
                        scratch_free(heaps_hdr);
                    } else {
                        /* Full encode + radix sort */
                        td_t *keys_hdr;
                        uint64_t* keys = (uint64_t*)scratch_alloc(&keys_hdr,
                                            (size_t)nrows * sizeof(uint64_t));
                        if (keys) {
                            radix_encode_ctx_t enc = {
                                .keys = keys, .n_keys = n_sort, .vecs = sort_vecs,
                            };
                            for (uint8_t k = 0; k < n_sort; k++) {
                                enc.mins[k] = mins[k];
                                enc.ranges[k] = maxs[k] - mins[k];
                                enc.bit_shifts[k] = bit_shifts[k];
                                enc.descs[k] = ext->sort.desc ? ext->sort.desc[k] : 0;
                                enc.enum_ranks[k] = enum_ranks[k];
                            }
                            if (pool)
                                td_pool_dispatch(pool, radix_encode_fn, &enc, nrows);
                            else
                                radix_encode_fn(&enc, 0, 0, nrows);

                            td_t *ktmp_hdr, *itmp_hdr;
                            uint64_t* ktmp = (uint64_t*)scratch_alloc(&ktmp_hdr,
                                                (size_t)nrows * sizeof(uint64_t));
                            int64_t*  itmp = (int64_t*)scratch_alloc(&itmp_hdr,
                                                (size_t)nrows * sizeof(int64_t));
                            if (ktmp && itmp) {
                                sorted_idx = radix_sort_run(pool, keys, indices,
                                                             ktmp, itmp, nrows);
                                radix_done = (sorted_idx != NULL);
                            }
                            scratch_free(ktmp_hdr);
                            if (sorted_idx != itmp) scratch_free(itmp_hdr);
                            else radix_itmp_hdr = itmp_hdr;
                        }
                        scratch_free(keys_hdr);
                    }
                }
            }
        }
    }

    /* --- Merge sort fallback ------------------------------------------------ */
    if (!radix_done) {
        sort_cmp_ctx_t cmp_ctx = {
            .vecs = sort_vecs,
            .desc = ext->sort.desc,
            .nulls_first = ext->sort.nulls_first,
            .n_sort = n_sort,
        };

        if (nrows <= 64) {
            sort_insertion(&cmp_ctx, indices, nrows);
        } else {
            td_pool_t* pool = td_pool_get();
            uint32_t n_workers = pool ? td_pool_total_workers(pool) : 1;

            td_t* tmp_hdr;
            int64_t* tmp = (int64_t*)scratch_alloc(&tmp_hdr,
                                (size_t)nrows * sizeof(int64_t));
            if (!tmp) {
                scratch_free(indices_hdr);
                return TD_ERR_PTR(TD_ERR_OOM);
            }

            uint32_t n_chunks = n_workers;
            if (pool && n_chunks > 1 && nrows > 1024) {
                sort_phase1_ctx_t p1ctx = {
                    .cmp_ctx = &cmp_ctx, .indices = indices, .tmp = tmp,
                    .nrows = nrows, .n_chunks = n_chunks,
                };
                td_pool_dispatch_n(pool, sort_phase1_fn, &p1ctx, n_chunks);
            } else {
                n_chunks = 1;
                sort_merge_recursive(&cmp_ctx, indices, tmp, nrows);
            }

            if (n_chunks > 1) {
                int64_t chunk_size = (nrows + n_chunks - 1) / n_chunks;
                int64_t run_size = chunk_size;
                int64_t* src = indices;
                int64_t* dst = tmp;

                while (run_size < nrows) {
                    int64_t n_pairs = (nrows + 2 * run_size - 1) / (2 * run_size);
                    sort_merge_ctx_t mctx = {
                        .cmp_ctx = &cmp_ctx, .src = src, .dst = dst,
                        .nrows = nrows, .run_size = run_size,
                    };
                    if (pool && n_pairs > 1)
                        td_pool_dispatch_n(pool, sort_merge_fn, &mctx,
                                            (uint32_t)n_pairs);
                    else
                        sort_merge_fn(&mctx, 0, 0, n_pairs);
                    int64_t* t = src; src = dst; dst = t;
                    run_size *= 2;
                }

                if (src != indices)
                    memcpy(indices, src, (size_t)nrows * sizeof(int64_t));
            }

            scratch_free(tmp_hdr);
        }
    }

    /* Check cancellation before expensive gather phase */
    {
        td_pool_t* cp = td_pool_get();
        if (pool_cancelled(cp)) {
            for (uint8_t k = 0; k < n_sort; k++) {
                if (sort_owned[k] && sort_vecs[k] && !TD_IS_ERR(sort_vecs[k]))
                    td_release(sort_vecs[k]);
                scratch_free(enum_rank_hdrs[k]);
            }
            scratch_free(radix_itmp_hdr);
            scratch_free(indices_hdr);
            return TD_ERR_PTR(TD_ERR_CANCEL);
        }
    }

    /* Materialize sorted result — fused multi-column gather.
     * When limit > 0, only gather the first `limit` rows (SORT+LIMIT fusion). */
    int64_t gather_rows = nrows;
    if (limit > 0 && limit < nrows) gather_rows = limit;

    td_t* result = td_table_new(ncols);
    if (!result || TD_IS_ERR(result)) {
        for (uint8_t k = 0; k < n_sort; k++) {
            if (sort_owned[k] && sort_vecs[k] && !TD_IS_ERR(sort_vecs[k]))
                td_release(sort_vecs[k]);
            scratch_free(enum_rank_hdrs[k]);
        }
        scratch_free(radix_itmp_hdr);
        scratch_free(indices_hdr);
        return result;
    }

    /* Pre-allocate all output columns, then do a single fused gather pass */
    td_pool_t* gather_pool = (gather_rows > TD_PARALLEL_THRESHOLD) ? td_pool_get() : NULL;
    td_t* new_cols[ncols];
    int64_t col_names[ncols];
    int64_t valid_ncols = 0;

    for (int64_t c = 0; c < ncols; c++) {
        td_t* col = td_table_get_col_idx(tbl, c);
        col_names[c] = td_table_col_name(tbl, c);
        if (!col) { new_cols[c] = NULL; continue; }
        td_t* nc = col_vec_new(col, gather_rows);
        if (!nc || TD_IS_ERR(nc)) { new_cols[c] = NULL; continue; }
        nc->len = gather_rows;
        new_cols[c] = nc;
        valid_ncols++;
    }

    if (gather_pool && valid_ncols > 0 && valid_ncols <= MGATHER_MAX_COLS) {
        /* Fused multi-column gather: one pass over indices for all columns */
        multi_gather_ctx_t mgctx = { .idx = sorted_idx, .ncols = 0 };
        for (int64_t c = 0; c < ncols; c++) {
            if (!new_cols[c]) continue;
            td_t* col = td_table_get_col_idx(tbl, c);
            int64_t ci = mgctx.ncols;
            mgctx.srcs[ci] = (char*)td_data(col);
            mgctx.dsts[ci] = (char*)td_data(new_cols[c]);
            mgctx.esz[ci]  = col_esz(col);
            mgctx.ncols++;
        }
        td_pool_dispatch(gather_pool, multi_gather_fn, &mgctx, gather_rows);
    } else {
        /* Fallback: per-column gather */
        for (int64_t c = 0; c < ncols; c++) {
            td_t* col = td_table_get_col_idx(tbl, c);
            if (!col || !new_cols[c]) continue;
            if (gather_pool) {
                gather_ctx_t gctx = {
                    .idx = sorted_idx, .src_col = col, .dst_col = new_cols[c],
                    .esz = col_esz(col), .nullable = false,
                };
                td_pool_dispatch(gather_pool, gather_fn, &gctx, gather_rows);
            } else {
                uint8_t esz = col_esz(col);
                char* src_p = (char*)td_data(col);
                char* dst_p = (char*)td_data(new_cols[c]);
                for (int64_t i = 0; i < gather_rows; i++)
                    memcpy(dst_p + i * esz, src_p + sorted_idx[i] * esz, esz);
            }
        }
    }

    for (int64_t c = 0; c < ncols; c++) {
        if (!new_cols[c]) continue;
        result = td_table_add_col(result, col_names[c], new_cols[c]);
        td_release(new_cols[c]);
    }

    /* Free expression-evaluated sort keys and SYM rank mappings */
    for (uint8_t k = 0; k < n_sort; k++) {
        if (sort_owned[k] && sort_vecs[k] && !TD_IS_ERR(sort_vecs[k]))
            td_release(sort_vecs[k]);
        scratch_free(enum_rank_hdrs[k]);
    }

    scratch_free(radix_itmp_hdr);
    scratch_free(indices_hdr);
    return result;
}

/* ============================================================================
 * Group-by execution — with parallel local hash tables + merge
 * ============================================================================ */

/* Hash using td_t** (used by join code) */
static uint64_t hash_row_keys(td_t** key_vecs, uint8_t n_keys, int64_t row) {
    uint64_t h = 0;
    for (uint8_t k = 0; k < n_keys; k++) {
        td_t* col = key_vecs[k];
        if (!col) continue;
        uint64_t kh;
        if (col->type == TD_F64)
            kh = td_hash_f64(((double*)td_data(col))[row]);
        else
            kh = td_hash_i64(read_col_i64(td_data(col), row, col->type, col->attrs));
        h = (k == 0) ? kh : td_hash_combine(h, kh);
    }
    return h;
}

/* Extract salt from hash (upper 16 bits) for fast mismatch rejection */
#define HT_SALT(h) ((uint8_t)((h) >> 56))

/* Flags controlling which accumulator arrays are allocated */
#define GHT_NEED_SUM   0x01
#define GHT_NEED_MIN   0x02
#define GHT_NEED_MAX   0x04
#define GHT_NEED_SUMSQ 0x08

/* ── Row-layout HT ──────────────────────────────────────────────────────
 * Keys + accumulators stored inline in both radix entries and group rows.
 * After phase1 copies data from original columns, phase2 and phase3 never
 * touch column data again — all access is sequential/local.
 * ────────────────────────────────────────────────────────────────────── */

typedef struct {
    uint16_t entry_stride;    /* bytes per radix entry: 8 + n_keys*8 + n_agg_vals*8 */
    uint16_t row_stride;      /* bytes per group row: 8 + n_keys*8 + accum_bytes */
    uint8_t  n_keys;
    uint8_t  n_aggs;
    uint8_t  n_agg_vals;      /* non-NULL agg columns (excludes COUNT) */
    uint8_t  need_flags;
    uint8_t  agg_is_f64;      /* bitmask: bit a set => agg[a] source is f64 */
    uint8_t  agg_is_first;   /* bitmask: bit a set => agg[a] is OP_FIRST */
    uint8_t  agg_is_last;    /* bitmask: bit a set => agg[a] is OP_LAST  */
    int8_t   agg_val_slot[8]; /* agg_idx -> entry/accum slot (-1 = no value) */
    /* Unified accumulator offsets: each block is n_agg_vals * 8 bytes.
     * Each 8B slot is double or int64_t based on agg_is_f64 bitmask. */
    uint16_t off_sum;         /* 0 => not allocated */
    uint16_t off_min;
    uint16_t off_max;
    uint16_t off_sumsq;       /* sum-of-squares for STDDEV/VAR */
} ght_layout_t;

static ght_layout_t ght_compute_layout(uint8_t n_keys, uint8_t n_aggs,
                                        td_t** agg_vecs, uint8_t need_flags,
                                        const uint16_t* agg_ops) {
    ght_layout_t ly;
    memset(&ly, 0, sizeof(ly));
    ly.n_keys = n_keys;
    ly.n_aggs = n_aggs;
    ly.need_flags = need_flags;

    uint8_t nv = 0;
    for (uint8_t a = 0; a < n_aggs && a < 8; a++) {
        if (agg_vecs[a]) {
            ly.agg_val_slot[a] = (int8_t)nv;
            if (agg_vecs[a]->type == TD_F64)
                ly.agg_is_f64 |= (1u << a);
            nv++;
        } else {
            ly.agg_val_slot[a] = -1;
        }
        if (agg_ops) {
            if (agg_ops[a] == OP_FIRST) ly.agg_is_first |= (1u << a);
            if (agg_ops[a] == OP_LAST)  ly.agg_is_last  |= (1u << a);
        }
    }
    ly.n_agg_vals = nv;
    ly.entry_stride = (uint16_t)(8 + (uint16_t)n_keys * 8 + (uint16_t)nv * 8);

    uint16_t off = (uint16_t)(8 + (uint16_t)n_keys * 8);
    uint16_t block = (uint16_t)nv * 8;
    if (need_flags & GHT_NEED_SUM)   { ly.off_sum   = off; off += block; }
    if (need_flags & GHT_NEED_MIN)   { ly.off_min   = off; off += block; }
    if (need_flags & GHT_NEED_MAX)   { ly.off_max   = off; off += block; }
    if (need_flags & GHT_NEED_SUMSQ) { ly.off_sumsq = off; off += block; }
    ly.row_stride = off;
    return ly;
}

/* Packed HT slots: [salt:8 | gid:24] in 4 bytes.
 * Max groups per HT = 16M (24 bits) — ample for partitioned probes.
 * 4B slots halve cache footprint vs 8B, fitting HT in L2. */
#define HT_EMPTY    UINT32_MAX
#define HT_PACK(salt, gid)  (((uint32_t)(uint8_t)(salt) << 24) | ((gid) & 0xFFFFFF))
#define HT_GID(s)   ((s) & 0xFFFFFF)
#define HT_SALT_V(s) ((uint8_t)((s) >> 24))

typedef struct {
    uint32_t*    slots;       /* packed [salt:8|gid:24], HT_EMPTY=empty */
    uint32_t     ht_cap;
    char*        rows;        /* flat row store: rows + gid * layout.row_stride */
    uint32_t     grp_count;
    uint32_t     grp_cap;
    ght_layout_t layout;
    td_t*        _h_slots;
    td_t*        _h_rows;
} group_ht_t;

static bool group_ht_init_sized(group_ht_t* ht, uint32_t cap,
                                 const ght_layout_t* ly, uint32_t init_grp_cap) {
    ht->ht_cap = cap;
    ht->layout = *ly;
    ht->slots = (uint32_t*)scratch_alloc(&ht->_h_slots, (size_t)cap * sizeof(uint32_t));
    if (!ht->slots) return false;
    memset(ht->slots, 0xFF, (size_t)cap * sizeof(uint32_t)); /* HT_EMPTY = all-1s */
    ht->grp_cap = init_grp_cap;
    ht->grp_count = 0;
    ht->rows = (char*)scratch_alloc(&ht->_h_rows,
        (size_t)init_grp_cap * ly->row_stride);
    if (!ht->rows) return false;
    return true;
}

static bool group_ht_init(group_ht_t* ht, uint32_t cap, const ght_layout_t* ly) {
    return group_ht_init_sized(ht, cap, ly, 256);
}

static void group_ht_free(group_ht_t* ht) {
    scratch_free(ht->_h_slots);
    scratch_free(ht->_h_rows);
}

static bool group_ht_grow(group_ht_t* ht) {
    uint32_t old_cap = ht->grp_cap;
    uint32_t new_cap = old_cap * 2;
    uint16_t rs = ht->layout.row_stride;
    char* new_rows = (char*)scratch_realloc(
        &ht->_h_rows, (size_t)old_cap * rs, (size_t)new_cap * rs);
    if (!new_rows) return false;
    ht->rows = new_rows;
    ht->grp_cap = new_cap;
    return true;
}

/* Hash inline int64_t keys (for rehash — no original column access) */
static inline uint64_t hash_keys_inline(const int64_t* keys, const int8_t* key_types,
                                         uint8_t n_keys) {
    uint64_t h = 0;
    for (uint8_t k = 0; k < n_keys; k++) {
        uint64_t kh;
        if (key_types[k] == TD_F64) {
            double dv;
            memcpy(&dv, &keys[k], 8);
            kh = td_hash_f64(dv);
        } else {
            kh = td_hash_i64(keys[k]);
        }
        h = (k == 0) ? kh : td_hash_combine(h, kh);
    }
    return h;
}

static void group_ht_rehash(group_ht_t* ht, const int8_t* key_types) {
    uint32_t new_cap = ht->ht_cap * 2;
    td_t* new_h = NULL;
    uint32_t* new_slots = (uint32_t*)scratch_alloc(&new_h, (size_t)new_cap * sizeof(uint32_t));
    if (!new_slots) return; /* OOM: keep old HT, it still works (just slower) */
    scratch_free(ht->_h_slots);
    ht->_h_slots = new_h;
    ht->slots = new_slots;
    memset(ht->slots, 0xFF, (size_t)new_cap * sizeof(uint32_t));
    ht->ht_cap = new_cap;
    uint32_t mask = new_cap - 1;
    uint16_t rs = ht->layout.row_stride;
    uint8_t nk = ht->layout.n_keys;
    for (uint32_t gi = 0; gi < ht->grp_count; gi++) {
        const int64_t* row_keys = (const int64_t*)(ht->rows + (size_t)gi * rs + 8);
        uint64_t h = hash_keys_inline(row_keys, key_types, nk);
        uint32_t slot = (uint32_t)(h & mask);
        while (ht->slots[slot] != HT_EMPTY)
            slot = (slot + 1) & mask;
        ht->slots[slot] = HT_PACK(HT_SALT(h), gi);
    }
}

/* Initialize accumulators for a new group from entry's inline agg values.
 * Each unified block has n_agg_vals slots of 8 bytes, typed by agg_is_f64. */
static inline void init_accum_from_entry(char* row, const char* entry,
                                          const ght_layout_t* ly) {
    uint16_t accum_start = (uint16_t)(8 + (uint16_t)ly->n_keys * 8);
    if (ly->row_stride > accum_start)
        memset(row + accum_start, 0, ly->row_stride - accum_start);

    const char* agg_data = entry + 8 + ly->n_keys * 8;
    uint8_t na = ly->n_aggs;
    uint8_t nf = ly->need_flags;

    for (uint8_t a = 0; a < na; a++) {
        int8_t s = ly->agg_val_slot[a];
        if (s < 0) continue;
        /* Copy raw 8 bytes from entry into each enabled accumulator block */
        if (nf & GHT_NEED_SUM) memcpy(row + ly->off_sum + s * 8, agg_data + s * 8, 8);
        if (nf & GHT_NEED_MIN) memcpy(row + ly->off_min + s * 8, agg_data + s * 8, 8);
        if (nf & GHT_NEED_MAX) memcpy(row + ly->off_max + s * 8, agg_data + s * 8, 8);
        if (nf & GHT_NEED_SUMSQ) {
            /* sumsq = v * v for the first entry */
            if (ly->agg_is_f64 & (1u << a)) {
                double v; memcpy(&v, agg_data + s * 8, 8);
                double sq = v * v;
                memcpy(row + ly->off_sumsq + s * 8, &sq, 8);
            } else {
                int64_t v; memcpy(&v, agg_data + s * 8, 8);
                double sq = (double)v * (double)v;
                memcpy(row + ly->off_sumsq + s * 8, &sq, 8);
            }
        }
    }
}

/* Row-layout accessors: cast through void* for strict-aliasing safety.
 * All row offsets are 8-byte aligned by construction. */
#define ROW_RD_F64(row, off, slot) (((const double*)((const void*)((row) + (off))))[(slot)])
#define ROW_RD_I64(row, off, slot) (((const int64_t*)((const void*)((row) + (off))))[(slot)])
#define ROW_WR_F64(row, off, slot) (((double*)((void*)((row) + (off))))[(slot)])
#define ROW_WR_I64(row, off, slot) (((int64_t*)((void*)((row) + (off))))[(slot)])

/* Accumulate into existing group from entry's inline agg values */
static inline void accum_from_entry(char* row, const char* entry,
                                     const ght_layout_t* ly) {
    const char* agg_data = entry + 8 + ly->n_keys * 8;
    uint8_t na = ly->n_aggs;
    uint8_t nf = ly->need_flags;

    for (uint8_t a = 0; a < na; a++) {
        int8_t s = ly->agg_val_slot[a];
        if (s < 0) continue;
        const char* val = agg_data + s * 8;

        uint8_t amask = (1u << a);
        if (ly->agg_is_f64 & amask) {
            double v;
            memcpy(&v, val, 8);
            if (nf & GHT_NEED_SUM) {
                if (ly->agg_is_first & amask) { /* keep init value */ }
                else if (ly->agg_is_last & amask) { memcpy(row + ly->off_sum + s * 8, val, 8); }
                else { ROW_WR_F64(row, ly->off_sum, s) += v; }
            }
            if (nf & GHT_NEED_MIN) { double* p = &ROW_WR_F64(row, ly->off_min, s); if (v < *p) *p = v; }
            if (nf & GHT_NEED_MAX) { double* p = &ROW_WR_F64(row, ly->off_max, s); if (v > *p) *p = v; }
            if (nf & GHT_NEED_SUMSQ) { ROW_WR_F64(row, ly->off_sumsq, s) += v * v; }
        } else {
            int64_t v;
            memcpy(&v, val, 8);
            if (nf & GHT_NEED_SUM) {
                if (ly->agg_is_first & amask) { /* keep init value */ }
                else if (ly->agg_is_last & amask) { memcpy(row + ly->off_sum + s * 8, val, 8); }
                else { ROW_WR_I64(row, ly->off_sum, s) += v; }
            }
            if (nf & GHT_NEED_MIN) { int64_t* p = &ROW_WR_I64(row, ly->off_min, s); if (v < *p) *p = v; }
            if (nf & GHT_NEED_MAX) { int64_t* p = &ROW_WR_I64(row, ly->off_max, s); if (v > *p) *p = v; }
            if (nf & GHT_NEED_SUMSQ) { ROW_WR_F64(row, ly->off_sumsq, s) += (double)v * (double)v; }
        }
    }
}

/* Probe + accumulate a single fat entry into the HT. Returns updated mask. */
static inline uint32_t group_probe_entry(group_ht_t* ht,
    const char* entry, const int8_t* key_types, uint32_t mask) {
    const ght_layout_t* ly = &ht->layout;
    uint64_t hash = *(const uint64_t*)entry;
    const char* ekeys = entry + 8;
    uint8_t salt = HT_SALT(hash);
    uint32_t slot = (uint32_t)(hash & mask);
    uint16_t key_bytes = ly->n_keys * 8;

    for (;;) {
        uint32_t sv = ht->slots[slot];
        if (sv == HT_EMPTY) {
            /* New group */
            if (ht->grp_count >= ht->grp_cap) {
                if (!group_ht_grow(ht)) return mask; /* OOM: stop adding groups */
            }
            uint32_t gid = ht->grp_count++;
            char* row = ht->rows + (size_t)gid * ly->row_stride;
            *(int64_t*)row = 1;   /* count = 1 */
            memcpy(row + 8, ekeys, key_bytes);
            init_accum_from_entry(row, entry, ly);
            ht->slots[slot] = HT_PACK(salt, gid);
            if (ht->grp_count * 2 > ht->ht_cap) {
                group_ht_rehash(ht, key_types);
                mask = ht->ht_cap - 1;
            }
            return mask;
        }
        if (HT_SALT_V(sv) == salt) {
            uint32_t gid = HT_GID(sv);
            char* row = ht->rows + (size_t)gid * ly->row_stride;
            if (memcmp(row + 8, ekeys, key_bytes) == 0) {
                (*(int64_t*)row)++;   /* count++ */
                accum_from_entry(row, entry, ly);
                return mask;
            }
        }
        slot = (slot + 1) & mask;
    }
}

/* Process rows [start, end) from original columns into a local hash table.
 * Converts each row to a fat entry on the stack, then probes. */
#define GROUP_PREFETCH_BATCH 16

static void group_rows_range(group_ht_t* ht, void** key_data, int8_t* key_types,
                              uint8_t* key_attrs, td_t** agg_vecs,
                              int64_t start, int64_t end) {
    const ght_layout_t* ly = &ht->layout;
    uint8_t nk = ly->n_keys;
    uint8_t na = ly->n_aggs;
    uint32_t mask = ht->ht_cap - 1;
    /* Stack buffer for one entry (max: 8 + 8*8 + 8*8 = 136 bytes) */
    char ebuf[8 + 8 * 8 + 8 * 8];

    for (int64_t row = start; row < end; row++) {
        uint64_t h = 0;
        int64_t* ek = (int64_t*)(ebuf + 8);
        for (uint8_t k = 0; k < nk; k++) {
            int8_t t = key_types[k];
            int64_t kv;
            if (t == TD_F64)
                memcpy(&kv, &((double*)key_data[k])[row], 8);
            else
                kv = read_col_i64(key_data[k], row, t, key_attrs[k]);
            ek[k] = kv;
            uint64_t kh = (t == TD_F64) ? td_hash_f64(((double*)key_data[k])[row])
                                        : td_hash_i64(kv);
            h = (k == 0) ? kh : td_hash_combine(h, kh);
        }
        *(uint64_t*)ebuf = h;

        int64_t* ev = (int64_t*)(ebuf + 8 + nk * 8);
        uint8_t vi = 0;
        for (uint8_t a = 0; a < na; a++) {
            td_t* ac = agg_vecs[a];
            if (!ac) continue;
            if (ac->type == TD_F64)
                memcpy(&ev[vi], &((double*)td_data(ac))[row], 8);
            else
                ev[vi] = read_col_i64(td_data(ac), row, ac->type, ac->attrs);
            vi++;
        }

        mask = group_probe_entry(ht, ebuf, key_types, mask);
    }
}

/* ============================================================================
 * Radix-partitioned parallel group-by
 *
 * Phase 1 (parallel): Each worker reads keys+agg values from original columns,
 *         packs into fat entries (hash, keys, agg_vals), scatters into
 *         thread-local per-partition buffers.
 * Phase 2 (parallel): Each partition is aggregated independently using
 *         inline data — no original column access needed.
 * Phase 3: Build result columns from inline group rows.
 * ============================================================================ */

#define RADIX_BITS  8
#define RADIX_P     (1u << RADIX_BITS)   /* 256 partitions */
#define RADIX_MASK  (RADIX_P - 1)
#define RADIX_PART(h) (((uint32_t)((h) >> 16)) & RADIX_MASK)

/* Per-worker, per-partition buffer of fat entries */
typedef struct {
    char*    data;           /* flat buffer: data[i * entry_stride] */
    uint32_t count;
    uint32_t cap;
    bool     oom;            /* set on realloc failure */
    td_t*    _hdr;
} radix_buf_t;

static inline void radix_buf_push(radix_buf_t* buf, uint16_t entry_stride,
                                   uint64_t hash, const int64_t* keys, uint8_t n_keys,
                                   const int64_t* agg_vals, uint8_t n_agg_vals) {
    if (__builtin_expect(buf->count >= buf->cap, 0)) {
        uint32_t old_cap = buf->cap;
        uint32_t new_cap = old_cap * 2;
        char* new_data = (char*)scratch_realloc(
            &buf->_hdr, (size_t)old_cap * entry_stride,
            (size_t)new_cap * entry_stride);
        if (!new_data) { buf->oom = true; return; }
        buf->data = new_data;
        buf->cap = new_cap;
    }
    char* dst = buf->data + (size_t)buf->count * entry_stride;
    *(uint64_t*)dst = hash;
    memcpy(dst + 8, keys, (size_t)n_keys * 8);
    if (n_agg_vals)
        memcpy(dst + 8 + (size_t)n_keys * 8, agg_vals, (size_t)n_agg_vals * 8);
    buf->count++;
}

typedef struct {
    void**       key_data;
    int8_t*      key_types;
    uint8_t*     key_attrs;
    td_t**       agg_vecs;
    uint32_t     n_workers;
    radix_buf_t* bufs;        /* [n_workers * RADIX_P] */
    ght_layout_t layout;
    const uint64_t* mask;
    const uint8_t*  sel_flags; /* per-segment TD_SEL_NONE/ALL/MIX (NULL=all pass) */
} radix_phase1_ctx_t;

static void radix_phase1_fn(void* ctx, uint32_t worker_id, int64_t start, int64_t end) {
    radix_phase1_ctx_t* c = (radix_phase1_ctx_t*)ctx;
    const ght_layout_t* ly = &c->layout;
    radix_buf_t* my_bufs = &c->bufs[(size_t)worker_id * RADIX_P];
    uint8_t nk = ly->n_keys;
    uint8_t na = ly->n_aggs;
    uint8_t nv = ly->n_agg_vals;
    uint16_t estride = ly->entry_stride;
    const uint64_t* mask = c->mask;
    const uint8_t* sel_flags = c->sel_flags;

    int64_t keys[8];
    int64_t agg_vals[8];

    for (int64_t row = start; row < end; ) {
        /* Segment-level skip for TD_SEL_NONE */
        if (sel_flags) {
            uint32_t seg = (uint32_t)(row / TD_MORSEL_ELEMS);
            int64_t seg_end = (int64_t)(seg + 1) * TD_MORSEL_ELEMS;
            if (seg_end > end) seg_end = end;
            if (sel_flags[seg] == TD_SEL_NONE) { row = seg_end; continue; }
        }

        if (TD_UNLIKELY(mask && !TD_SEL_BIT_TEST(mask, row))) { row++; continue; }
        uint64_t h = 0;
        for (uint8_t k = 0; k < nk; k++) {
            int8_t t = c->key_types[k];
            int64_t kv;
            if (t == TD_F64)
                memcpy(&kv, &((double*)c->key_data[k])[row], 8);
            else
                kv = read_col_i64(c->key_data[k], row, t, c->key_attrs[k]);
            keys[k] = kv;
            uint64_t kh = (t == TD_F64) ? td_hash_f64(((double*)c->key_data[k])[row])
                                        : td_hash_i64(kv);
            h = (k == 0) ? kh : td_hash_combine(h, kh);
        }

        uint8_t vi = 0;
        for (uint8_t a = 0; a < na; a++) {
            td_t* ac = c->agg_vecs[a];
            if (!ac) continue;
            if (ac->type == TD_F64)
                memcpy(&agg_vals[vi], &((double*)td_data(ac))[row], 8);
            else
                agg_vals[vi] = read_col_i64(td_data(ac), row, ac->type, ac->attrs);
            vi++;
        }

        uint32_t part = RADIX_PART(h);
        radix_buf_push(&my_bufs[part], estride, h, keys, nk, agg_vals, nv);
        row++;
    }
}

/* Process pre-partitioned fat entries into an HT with prefetch batching.
 * Two-phase prefetch: (1) prefetch HT slots, (2) prefetch group rows. */
static void group_rows_indirect(group_ht_t* ht, const int8_t* key_types,
                                 const char* entries, uint32_t n_entries,
                                 uint16_t entry_stride) {
    uint32_t mask = ht->ht_cap - 1;
    /* Stride-ahead prefetch: prefetch HT slot for entry i+D while processing i.
     * D=8 covers ~200ns L2/L3 latency at ~25ns per probe iteration. */
    enum { PF_DIST = 8 };
    /* Prime the prefetch pipeline */
    uint32_t pf_end = (n_entries < PF_DIST) ? n_entries : PF_DIST;
    for (uint32_t j = 0; j < pf_end; j++) {
        uint64_t h = *(const uint64_t*)(entries + (size_t)j * entry_stride);
        __builtin_prefetch(&ht->slots[(uint32_t)(h & mask)], 0, 1);
    }
    for (uint32_t i = 0; i < n_entries; i++) {
        /* Prefetch PF_DIST entries ahead */
        if (i + PF_DIST < n_entries) {
            uint64_t h = *(const uint64_t*)(entries + (size_t)(i + PF_DIST) * entry_stride);
            __builtin_prefetch(&ht->slots[(uint32_t)(h & mask)], 0, 1);
        }
        const char* e = entries + (size_t)i * entry_stride;
        mask = group_probe_entry(ht, e, key_types, mask);
    }
}

/* Phase 3: build result columns from inline group rows */
typedef struct {
    int8_t  out_type;
    bool    src_f64;
    uint16_t agg_op;
    bool    affine;
    double  bias_f64;
    int64_t bias_i64;
    void*   dst;
} agg_out_t;

typedef struct {
    group_ht_t*   part_hts;
    uint32_t*     part_offsets;
    char**        key_dsts;
    int8_t*       key_types;
    uint8_t*      key_attrs;
    uint8_t*      key_esizes;
    uint8_t       n_keys;
    agg_out_t*    agg_outs;
    uint8_t       n_aggs;
} radix_phase3_ctx_t;

static void radix_phase3_fn(void* ctx, uint32_t worker_id, int64_t start, int64_t end) {
    (void)worker_id;
    radix_phase3_ctx_t* c = (radix_phase3_ctx_t*)ctx;
    uint8_t nk = c->n_keys;
    uint8_t na = c->n_aggs;

    for (int64_t p = start; p < end; p++) {
        group_ht_t* ph = &c->part_hts[p];
        uint32_t gc = ph->grp_count;
        if (gc == 0) continue;
        uint32_t off = c->part_offsets[p];
        const ght_layout_t* ly = &ph->layout;
        uint16_t rs = ly->row_stride;

        /* Single pass over group rows: read each row once, scatter keys + aggs.
         * Reduces memory traffic from nk+na passes over group data to 1 pass. */
        for (uint32_t gi = 0; gi < gc; gi++) {
            const char* row = ph->rows + (size_t)gi * rs;
            const int64_t* rkeys = (const int64_t*)(const void*)(row + 8);
            int64_t cnt = *(const int64_t*)(const void*)row;
            uint32_t di = off + gi;

            /* Scatter keys to result columns */
            for (uint8_t k = 0; k < nk; k++) {
                int64_t kv = rkeys[k];
                int8_t kt = c->key_types[k];
                char* dst = c->key_dsts[k];
                uint8_t esz = c->key_esizes[k];
                size_t doff = (size_t)di * esz;
                if (kt == TD_F64)
                    memcpy(dst + doff, &kv, 8);
                else
                    write_col_i64(dst, di, kv, kt, c->key_attrs[k]);
            }

            /* Scatter agg results to result columns */
            for (uint8_t a = 0; a < na; a++) {
                agg_out_t* ao = &c->agg_outs[a];
                uint16_t op = ao->agg_op;
                bool sf = ao->src_f64;
                int8_t s = ly->agg_val_slot[a];
                if (ao->out_type == TD_F64) {
                    double v;
                    switch (op) {
                        case OP_SUM:
                            v = sf ? ROW_RD_F64(row, ly->off_sum, s)
                                   : (double)ROW_RD_I64(row, ly->off_sum, s);
                            if (ao->affine) v += ao->bias_f64 * cnt;
                            break;
                        case OP_AVG:
                            v = sf ? ROW_RD_F64(row, ly->off_sum, s) / cnt
                                   : (double)ROW_RD_I64(row, ly->off_sum, s) / cnt;
                            if (ao->affine) v += ao->bias_f64;
                            break;
                        case OP_MIN:
                            v = sf ? ROW_RD_F64(row, ly->off_min, s)
                                   : (double)ROW_RD_I64(row, ly->off_min, s);
                            break;
                        case OP_MAX:
                            v = sf ? ROW_RD_F64(row, ly->off_max, s)
                                   : (double)ROW_RD_I64(row, ly->off_max, s);
                            break;
                        case OP_FIRST: case OP_LAST:
                            v = sf ? ROW_RD_F64(row, ly->off_sum, s)
                                   : (double)ROW_RD_I64(row, ly->off_sum, s);
                            break;
                        case OP_VAR: case OP_VAR_POP:
                        case OP_STDDEV: case OP_STDDEV_POP: {
                            double sum_val = sf ? ROW_RD_F64(row, ly->off_sum, s)
                                                : (double)ROW_RD_I64(row, ly->off_sum, s);
                            double sq_val = ly->off_sumsq ? ROW_RD_F64(row, ly->off_sumsq, s) : 0.0;
                            double mean = cnt > 0 ? sum_val / cnt : 0.0;
                            double var_pop = cnt > 0 ? sq_val / cnt - mean * mean : 0.0;
                            if (var_pop < 0) var_pop = 0;
                            if (op == OP_VAR_POP) v = cnt > 0 ? var_pop : NAN;
                            else if (op == OP_VAR) v = cnt > 1 ? var_pop * cnt / (cnt - 1) : NAN;
                            else if (op == OP_STDDEV_POP) v = cnt > 0 ? sqrt(var_pop) : NAN;
                            else v = cnt > 1 ? sqrt(var_pop * cnt / (cnt - 1)) : NAN;
                            break;
                        }
                        default: v = 0.0; break;
                    }
                    ((double*)(void*)ao->dst)[di] = v;
                } else {
                    int64_t v;
                    switch (op) {
                        case OP_SUM:
                            v = ROW_RD_I64(row, ly->off_sum, s);
                            if (ao->affine) v += ao->bias_i64 * cnt;
                            break;
                        case OP_COUNT: v = cnt; break;
                        case OP_MIN:   v = ROW_RD_I64(row, ly->off_min, s); break;
                        case OP_MAX:   v = ROW_RD_I64(row, ly->off_max, s); break;
                        case OP_FIRST: case OP_LAST: v = ROW_RD_I64(row, ly->off_sum, s); break;
                        default:       v = 0; break;
                    }
                    ((int64_t*)(void*)ao->dst)[di] = v;
                }
            }
        }
    }
}

/* Phase 2: aggregate each partition independently using inline data */
typedef struct {
    int8_t*      key_types;
    uint8_t      n_keys;
    uint32_t     n_workers;
    radix_buf_t* bufs;
    group_ht_t*  part_hts;
    ght_layout_t layout;
} radix_phase2_ctx_t;

static void radix_phase2_fn(void* ctx, uint32_t worker_id, int64_t start, int64_t end) {
    (void)worker_id;
    radix_phase2_ctx_t* c = (radix_phase2_ctx_t*)ctx;
    uint16_t estride = c->layout.entry_stride;

    for (int64_t p = start; p < end; p++) {
        uint32_t total = 0;
        for (uint32_t w = 0; w < c->n_workers; w++)
            total += c->bufs[(size_t)w * RADIX_P + p].count;
        if (total == 0) continue;

        uint32_t part_ht_cap = 256;
        {
            uint64_t target = (uint64_t)total * 2;
            if (target < 256) target = 256;
            while (part_ht_cap < target) part_ht_cap *= 2;
        }
        /* Pre-size group store to avoid grows. Use next_pow2(total) as upper
         * bound on groups. Over-allocation is bounded: worst case total >> groups,
         * but total * row_stride is already committed via HT capacity anyway. */
        uint32_t init_grp = 256;
        while (init_grp < total) init_grp *= 2;
        if (!group_ht_init_sized(&c->part_hts[p], part_ht_cap, &c->layout, init_grp))
            continue;

        for (uint32_t w = 0; w < c->n_workers; w++) {
            radix_buf_t* buf = &c->bufs[(size_t)w * RADIX_P + p];
            if (buf->count == 0) continue;
            group_rows_indirect(&c->part_hts[p], c->key_types,
                                buf->data, buf->count, estride);
        }
    }
}

/* ============================================================================
 * Parallel direct-array accumulation for low-cardinality single integer key
 * ============================================================================ */

/* Parallel min/max scan for direct-array key range detection */
typedef struct {
    const void* key_data;
    int8_t      key_type;
    uint8_t     key_attrs;
    int64_t*    per_worker_min;  /* [n_workers] */
    int64_t*    per_worker_max;  /* [n_workers] */
    uint32_t    n_workers;
    const uint64_t* mask;
    const uint8_t*  sel_flags;   /* per-segment TD_SEL_NONE/ALL/MIX (NULL=all pass) */
} minmax_ctx_t;

static void minmax_scan_fn(void* ctx, uint32_t worker_id, int64_t start, int64_t end) {
    minmax_ctx_t* c = (minmax_ctx_t*)ctx;
    uint32_t wid = worker_id % c->n_workers;
    const uint64_t* mask = c->mask;
    const uint8_t* sel_flags = c->sel_flags;
    int64_t kmin = INT64_MAX, kmax = INT64_MIN;
    int8_t t = c->key_type;

    #define MINMAX_SEG_LOOP(TYPE, CAST) \
        do { \
            const TYPE* kd = (const TYPE*)c->key_data; \
            for (int64_t r = start; r < end; ) { \
                if (sel_flags) { \
                    uint32_t seg = (uint32_t)(r / TD_MORSEL_ELEMS); \
                    int64_t seg_end = (int64_t)(seg + 1) * TD_MORSEL_ELEMS; \
                    if (seg_end > end) seg_end = end; \
                    if (sel_flags[seg] == TD_SEL_NONE) { r = seg_end; continue; } \
                    bool need_bit = (sel_flags[seg] == TD_SEL_MIX); \
                    for (; r < seg_end; r++) { \
                        if (need_bit && !TD_SEL_BIT_TEST(mask, r)) continue; \
                        int64_t v = (int64_t)CAST kd[r]; \
                        if (v < kmin) kmin = v; \
                        if (v > kmax) kmax = v; \
                    } \
                } else if (mask) { \
                    if (!TD_SEL_BIT_TEST(mask, r)) { r++; continue; } \
                    int64_t v = (int64_t)CAST kd[r]; \
                    if (v < kmin) kmin = v; \
                    if (v > kmax) kmax = v; \
                    r++; \
                } else { \
                    int64_t v = (int64_t)CAST kd[r]; \
                    if (v < kmin) kmin = v; \
                    if (v > kmax) kmax = v; \
                    r++; \
                } \
            } \
        } while (0)

    if (t == TD_I64 || t == TD_TIMESTAMP)
        MINMAX_SEG_LOOP(int64_t, );
    else if (TD_IS_SYM(t)) {
        uint8_t w = c->key_attrs & TD_SYM_W_MASK;
        if (w == TD_SYM_W64) MINMAX_SEG_LOOP(int64_t, );
        else if (w == TD_SYM_W32) MINMAX_SEG_LOOP(uint32_t, );
        else if (w == TD_SYM_W16) MINMAX_SEG_LOOP(uint16_t, );
        else MINMAX_SEG_LOOP(uint8_t, );
    }
    else if (t == TD_BOOL || t == TD_U8)
        MINMAX_SEG_LOOP(uint8_t, );
    else if (t == TD_I16)
        MINMAX_SEG_LOOP(int16_t, );
    else /* TD_I32, TD_DATE, TD_TIME */
        MINMAX_SEG_LOOP(int32_t, );

    #undef MINMAX_SEG_LOOP

    /* Merge with existing per-worker values (a worker may process multiple morsels) */
    if (kmin < c->per_worker_min[wid]) c->per_worker_min[wid] = kmin;
    if (kmax > c->per_worker_max[wid]) c->per_worker_max[wid] = kmax;
}

typedef union { double f; int64_t i; } da_val_t;

typedef struct {
    da_val_t* sum;       /* SUM/AVG/FIRST/LAST [n_slots * n_aggs] */
    da_val_t* min_val;   /* MIN [n_slots * n_aggs] */
    da_val_t* max_val;   /* MAX [n_slots * n_aggs] */
    double*   sumsq_f64; /* sum-of-squares for STDDEV/VAR */
    int64_t*  count;     /* group counts [n_slots] */
    /* Arena headers */
    td_t* _h_sum;
    td_t* _h_min;
    td_t* _h_max;
    td_t* _h_sumsq;
    td_t* _h_count;
} da_accum_t;

static inline void da_accum_free(da_accum_t* a) {
    scratch_free(a->_h_sum);
    scratch_free(a->_h_min);
    scratch_free(a->_h_max);
    scratch_free(a->_h_sumsq);
    scratch_free(a->_h_count);
}

/* Unified agg result emitter — used by both DA and HT paths.
 * Arrays indexed by [gi * n_aggs + a], counts by [gi]. */
static void emit_agg_columns(td_t** result, td_graph_t* g, const td_op_ext_t* ext,
                              td_t* const* agg_vecs, uint32_t grp_count,
                              uint8_t n_aggs,
                              const double*  sum_f64,  const int64_t* sum_i64,
                              const double*  min_f64,  const double*  max_f64,
                              const int64_t* min_i64,  const int64_t* max_i64,
                              const int64_t* counts,
                              const agg_affine_t* affine,
                              const double*  sumsq_f64) {
    for (uint8_t a = 0; a < n_aggs; a++) {
        uint16_t agg_op = ext->agg_ops[a];
        td_t* agg_col = agg_vecs[a];
        bool is_f64 = agg_col && agg_col->type == TD_F64;
        int8_t out_type;
        switch (agg_op) {
            case OP_AVG:
            case OP_STDDEV: case OP_STDDEV_POP:
            case OP_VAR: case OP_VAR_POP:
                out_type = TD_F64; break;
            case OP_COUNT: out_type = TD_I64; break;
            case OP_SUM: case OP_PROD:
                out_type = is_f64 ? TD_F64 : TD_I64; break;
            default:
                out_type = agg_col ? agg_col->type : TD_I64; break;
        }
        td_t* new_col = td_vec_new(out_type, (int64_t)grp_count);
        if (!new_col || TD_IS_ERR(new_col)) continue;
        new_col->len = (int64_t)grp_count;
        for (uint32_t gi = 0; gi < grp_count; gi++) {
            size_t idx = (size_t)gi * n_aggs + a;
            if (out_type == TD_F64) {
                double v;
                switch (agg_op) {
                    case OP_SUM:
                        v = is_f64 ? sum_f64[idx] : (double)sum_i64[idx];
                        if (affine && affine[a].enabled)
                            v += affine[a].bias_f64 * counts[gi];
                        break;
                    case OP_AVG:
                        v = is_f64 ? sum_f64[idx] / counts[gi] : (double)sum_i64[idx] / counts[gi];
                        if (affine && affine[a].enabled)
                            v += affine[a].bias_f64;
                        break;
                    case OP_MIN: v = is_f64 ? min_f64[idx] : (double)min_i64[idx]; break;
                    case OP_MAX: v = is_f64 ? max_f64[idx] : (double)max_i64[idx]; break;
                    case OP_FIRST: case OP_LAST:
                        v = is_f64 ? sum_f64[idx] : (double)sum_i64[idx]; break;
                    case OP_VAR: case OP_VAR_POP:
                    case OP_STDDEV: case OP_STDDEV_POP: {
                        int64_t cnt = counts[gi];
                        double sum_val = is_f64 ? sum_f64[idx] : (double)sum_i64[idx];
                        double sq_val = sumsq_f64 ? sumsq_f64[idx] : 0.0;
                        double mean = cnt > 0 ? sum_val / cnt : 0.0;
                        double var_pop = cnt > 0 ? sq_val / cnt - mean * mean : 0.0;
                        if (var_pop < 0) var_pop = 0;
                        if (agg_op == OP_VAR_POP) v = cnt > 0 ? var_pop : NAN;
                        else if (agg_op == OP_VAR) v = cnt > 1 ? var_pop * cnt / (cnt - 1) : NAN;
                        else if (agg_op == OP_STDDEV_POP) v = cnt > 0 ? sqrt(var_pop) : NAN;
                        else v = cnt > 1 ? sqrt(var_pop * cnt / (cnt - 1)) : NAN;
                        break;
                    }
                    default:     v = 0.0; break;
                }
                ((double*)td_data(new_col))[gi] = v;
            } else {
                int64_t v;
                switch (agg_op) {
                    case OP_SUM:
                        v = sum_i64[idx];
                        if (affine && affine[a].enabled)
                            v += affine[a].bias_i64 * counts[gi];
                        break;
                    case OP_COUNT: v = counts[gi]; break;
                    case OP_MIN:   v = min_i64[idx]; break;
                    case OP_MAX:   v = max_i64[idx]; break;
                    case OP_FIRST: case OP_LAST: v = sum_i64[idx]; break;
                    default:       v = 0; break;
                }
                ((int64_t*)td_data(new_col))[gi] = v;
            }
        }
        /* Generate unique column name: base_name + agg suffix (e.g. "v1_sum") */
        td_op_ext_t* agg_ext = find_ext(g, ext->agg_ins[a]->id);
        int64_t name_id;
        if (agg_ext && agg_ext->base.opcode == OP_SCAN) {
            td_t* name_atom = td_sym_str(agg_ext->sym);
            const char* base = name_atom ? td_str_ptr(name_atom) : NULL;
            size_t blen = base ? td_str_len(name_atom) : 0;
            const char* sfx = "";
            size_t slen = 0;
            switch (agg_op) {
                case OP_SUM:   sfx = "_sum";   slen = 4; break;
                case OP_COUNT: sfx = "_count"; slen = 6; break;
                case OP_AVG:   sfx = "_mean";  slen = 5; break;
                case OP_MIN:   sfx = "_min";   slen = 4; break;
                case OP_MAX:   sfx = "_max";   slen = 4; break;
                case OP_FIRST: sfx = "_first"; slen = 6; break;
                case OP_LAST:  sfx = "_last";  slen = 5; break;
                case OP_STDDEV:     sfx = "_stddev";     slen = 7; break;
                case OP_STDDEV_POP: sfx = "_stddev_pop"; slen = 11; break;
                case OP_VAR:        sfx = "_var";        slen = 4; break;
                case OP_VAR_POP:    sfx = "_var_pop";    slen = 8; break;
            }
            char buf[256];
            if (base && blen + slen < sizeof(buf)) {
                memcpy(buf, base, blen);
                memcpy(buf + blen, sfx, slen);
                name_id = td_sym_intern(buf, blen + slen);
            } else {
                name_id = agg_ext->sym;
            }
        } else {
            /* Expression agg input — synthetic name like "_e0_sum" */
            char nbuf[32];
            int np = 0;
            nbuf[np++] = '_'; nbuf[np++] = 'e';
            /* Multi-digit agg index */
            { uint8_t v = a; char dig[3]; int nd = 0;
              do { dig[nd++] = (char)('0' + v % 10); v /= 10; } while (v);
              while (nd--) nbuf[np++] = dig[nd]; }
            const char* nsfx = "";
            size_t nslen = 0;
            switch (agg_op) {
                case OP_SUM:   nsfx = "_sum";   nslen = 4; break;
                case OP_COUNT: nsfx = "_count"; nslen = 6; break;
                case OP_AVG:   nsfx = "_mean";  nslen = 5; break;
                case OP_MIN:   nsfx = "_min";   nslen = 4; break;
                case OP_MAX:   nsfx = "_max";   nslen = 4; break;
                case OP_FIRST: nsfx = "_first"; nslen = 6; break;
                case OP_LAST:  nsfx = "_last";  nslen = 5; break;
                case OP_STDDEV:     nsfx = "_stddev";     nslen = 7; break;
                case OP_STDDEV_POP: nsfx = "_stddev_pop"; nslen = 11; break;
                case OP_VAR:        nsfx = "_var";        nslen = 4; break;
                case OP_VAR_POP:    nsfx = "_var_pop";    nslen = 8; break;
            }
            memcpy(nbuf + np, nsfx, nslen);
            name_id = td_sym_intern(nbuf, (size_t)np + nslen);
        }
        *result = td_table_add_col(*result, name_id, new_col);
        td_release(new_col);
    }
}

/* Bitmask for which accumulator arrays are actually needed */
#define DA_NEED_SUM   0x01  /* da_val_t sum array */
#define DA_NEED_MIN   0x02  /* da_val_t min_val array */
#define DA_NEED_MAX   0x04  /* da_val_t max_val array */
#define DA_NEED_COUNT 0x08  /* count array */
#define DA_NEED_SUMSQ 0x10  /* sumsq_f64 array (for STDDEV/VAR) */

typedef struct {
    da_accum_t*    accums;
    uint32_t       n_accums;     /* number of accumulator sets (may < pool workers) */
    void**         key_ptrs;     /* key data pointers [n_keys] */
    int8_t*        key_types;    /* key type codes [n_keys] */
    uint8_t*       key_attrs;    /* key attrs for TD_SYM width [n_keys] */
    uint8_t*       key_esz;      /* pre-computed per-key elem size [n_keys] */
    int64_t*       key_mins;     /* per-key minimum [n_keys] */
    int64_t*       key_strides;  /* per-key stride [n_keys] */
    uint8_t        n_keys;
    void**         agg_ptrs;
    int8_t*        agg_types;
    uint16_t*      agg_ops;      /* per-agg operation code */
    uint8_t        n_aggs;
    uint8_t        need_flags;   /* DA_NEED_* bitmask */
    uint32_t       agg_f64_mask; /* bitmask: bit a set if agg[a] is TD_F64 */
    bool           all_sum;      /* true when all ops are SUM/AVG/COUNT (no MIN/MAX/FIRST/LAST) */
    uint32_t       n_slots;
    const uint64_t* mask;
    const uint8_t*  sel_flags;   /* per-segment TD_SEL_NONE/ALL/MIX (NULL=all pass) */
} da_ctx_t;

/* Composite GID from multi-key.  Arithmetic overflow is prevented in practice
 * by the DA budget check (DA_PER_WORKER_MAX) which limits total_slots to 262K. */
static inline int32_t da_composite_gid(da_ctx_t* c, int64_t r) {
    int32_t gid = 0;
    for (uint8_t k = 0; k < c->n_keys; k++) {
        int64_t val = read_by_esz(c->key_ptrs[k], r, c->key_esz[k]);
        gid += (int32_t)((val - c->key_mins[k]) * c->key_strides[k]);
    }
    return gid;
}

/* Typed composite GID: eliminates per-element switch when all keys share width */
#define DEFINE_DA_COMPOSITE_GID_TYPED(SUFFIX, KTYPE) \
static inline int32_t da_composite_gid_##SUFFIX(da_ctx_t* c, int64_t r) { \
    int32_t gid = 0; \
    for (uint8_t k = 0; k < c->n_keys; k++) { \
        int64_t val = (int64_t)((const KTYPE*)c->key_ptrs[k])[r]; \
        gid += (int32_t)((val - c->key_mins[k]) * c->key_strides[k]); \
    } \
    return gid; \
}
DEFINE_DA_COMPOSITE_GID_TYPED(u8,  uint8_t)
DEFINE_DA_COMPOSITE_GID_TYPED(u16, uint16_t)
DEFINE_DA_COMPOSITE_GID_TYPED(u32, uint32_t)
DEFINE_DA_COMPOSITE_GID_TYPED(i64, int64_t)
#undef DEFINE_DA_COMPOSITE_GID_TYPED

static inline void da_read_val(const void* ptr, int8_t type, uint8_t attrs,
                               int64_t r, double* out_f64, int64_t* out_i64) {
    if (type == TD_F64) {
        *out_f64 = ((const double*)ptr)[r];
        *out_i64 = (int64_t)*out_f64;
    } else {
        *out_i64 = read_col_i64(ptr, r, type, attrs);
        *out_f64 = (double)*out_i64;
    }
}

/* Materialize a scalar (atom or len-1 vector) into a full-length vector so
 * group-aggregation loops can read row-wise without out-of-bounds access. */
static td_t* materialize_broadcast_input(td_t* src, int64_t nrows) {
    if (!src || TD_IS_ERR(src) || nrows < 0) return NULL;

    int8_t out_type = td_is_atom(src) ? (int8_t)-src->type : src->type;
    if (out_type <= 0 || out_type >= TD_TYPE_COUNT) return NULL;

    td_t* out = td_vec_new(out_type, nrows);
    if (!out || TD_IS_ERR(out)) return out;
    out->len = nrows;
    if (nrows == 0) return out;

    if (!td_is_atom(src)) {
        uint8_t esz = col_esz(src);
        const char* s = (const char*)td_data(src);
        char* d = (char*)td_data(out);
        for (int64_t i = 0; i < nrows; i++)
            memcpy(d + (size_t)i * esz, s, esz);
        return out;
    }

    switch (src->type) {
        case TD_ATOM_F64: {
            double v = src->f64;
            for (int64_t i = 0; i < nrows; i++) ((double*)td_data(out))[i] = v;
            return out;
        }
        case TD_ATOM_I64:
        case TD_ATOM_SYM:
        case TD_ATOM_TIMESTAMP: {
            int64_t v = src->i64;
            for (int64_t i = 0; i < nrows; i++) ((int64_t*)td_data(out))[i] = v;
            return out;
        }
        case TD_ATOM_DATE:
        case TD_ATOM_TIME: {
            int32_t v = (int32_t)src->i64;
            for (int64_t i = 0; i < nrows; i++) ((int32_t*)td_data(out))[i] = v;
            return out;
        }
        case TD_ATOM_I32: {
            int32_t v = src->i32;
            for (int64_t i = 0; i < nrows; i++) ((int32_t*)td_data(out))[i] = v;
            return out;
        }
        case TD_ATOM_I16: {
            int16_t v = src->i16;
            for (int64_t i = 0; i < nrows; i++) ((int16_t*)td_data(out))[i] = v;
            return out;
        }
        case TD_ATOM_U8:
        case TD_ATOM_BOOL: {
            uint8_t v = src->u8;
            for (int64_t i = 0; i < nrows; i++) ((uint8_t*)td_data(out))[i] = v;
            return out;
        }
        default:
            td_release(out);
            return NULL;
    }
}

/* ---- Scalar aggregate (n_keys==0): one flat scan, no GID, no hash ---- */
typedef struct {
    void**         agg_ptrs;
    int8_t*        agg_types;
    uint16_t*      agg_ops;
    agg_linear_t*  agg_linear;
    uint8_t        n_aggs;
    uint8_t        need_flags;
    const uint64_t* mask;
    const uint8_t*  sel_flags;   /* per-segment TD_SEL_NONE/ALL/MIX (NULL=all pass) */
    /* per-worker accumulators (1 slot each) */
    da_accum_t*    accums;
    uint32_t       n_accums;
} scalar_ctx_t;

static inline int64_t scalar_i64_at(const void* ptr, int8_t type, int64_t r) {
    return read_col_i64(ptr, r, type, 0);  /* attrs=0: agg columns are numeric, never SYM */
}

/* Tight SIMD-friendly loop for single SUM/AVG on i64 (no mask).
 * Note: int64 sum can overflow; caller responsibility to use appropriate types. */
static void scalar_sum_i64_fn(void* ctx, uint32_t worker_id, int64_t start, int64_t end) {
    scalar_ctx_t* c = (scalar_ctx_t*)ctx;
    da_accum_t* acc = &c->accums[worker_id];
    const int64_t* restrict data = (const int64_t*)c->agg_ptrs[0];
    int64_t sum = 0;
    for (int64_t r = start; r < end; r++)
        sum += data[r];
    acc->sum[0].i += sum;
    acc->count[0] += end - start;
}

/* Tight SIMD-friendly loop for single SUM/AVG on f64 (no mask) */
static void scalar_sum_f64_fn(void* ctx, uint32_t worker_id, int64_t start, int64_t end) {
    scalar_ctx_t* c = (scalar_ctx_t*)ctx;
    da_accum_t* acc = &c->accums[worker_id];
    const double* restrict data = (const double*)c->agg_ptrs[0];
    double sum = 0.0;
    for (int64_t r = start; r < end; r++)
        sum += data[r];
    acc->sum[0].f += sum;
    acc->count[0] += end - start;
}

/* Tight loop for single SUM/AVG on integer linear expression (no mask). */
static void scalar_sum_linear_i64_fn(void* ctx, uint32_t worker_id, int64_t start, int64_t end) {
    scalar_ctx_t* c = (scalar_ctx_t*)ctx;
    da_accum_t* acc = &c->accums[worker_id];
    const agg_linear_t* lin = &c->agg_linear[0];
    int64_t n = end - start;

    int64_t sum = lin->bias_i64 * n;
    for (uint8_t t = 0; t < lin->n_terms; t++) {
        int64_t coeff = lin->coeff_i64[t];
        if (coeff == 0) continue;
        const void* ptr = lin->term_ptrs[t];
        int8_t type = lin->term_types[t];
        int64_t term_sum = 0;
        for (int64_t r = start; r < end; r++)
            term_sum += scalar_i64_at(ptr, type, r);
        sum += coeff * term_sum;
    }

    acc->sum[0].i += sum;
    acc->count[0] += n;
}

/* Generic scalar accumulation: handles all ops, all types, mask */
/* Inner scalar accumulation for a single row */
static inline void scalar_accum_row(scalar_ctx_t* c, da_accum_t* acc, int64_t r) {
    uint8_t n_aggs = c->n_aggs;
    acc->count[0]++;
    for (uint8_t a = 0; a < n_aggs; a++) {
        double fv; int64_t iv;
        if (c->agg_linear && c->agg_linear[a].enabled) {
            const agg_linear_t* lin = &c->agg_linear[a];
            iv = lin->bias_i64;
            for (uint8_t t = 0; t < lin->n_terms; t++) {
                iv += lin->coeff_i64[t] *
                      scalar_i64_at(lin->term_ptrs[t], lin->term_types[t], r);
            }
            fv = (double)iv;
        } else {
            if (!c->agg_ptrs[a]) continue;
            da_read_val(c->agg_ptrs[a], c->agg_types[a], 0, r, &fv, &iv);
        }
        uint16_t op = c->agg_ops[a];
        bool is_f = (c->agg_types[a] == TD_F64);
        if (op == OP_SUM || op == OP_AVG || op == OP_STDDEV || op == OP_STDDEV_POP || op == OP_VAR || op == OP_VAR_POP) {
            if (is_f) acc->sum[a].f += fv;
            else acc->sum[a].i += iv;
            if (acc->sumsq_f64) acc->sumsq_f64[a] += fv * fv;
        } else if (op == OP_FIRST) {
            if (acc->count[0] == 1) {
                if (is_f) acc->sum[a].f = fv; else acc->sum[a].i = iv;
            }
        } else if (op == OP_LAST) {
            if (is_f) acc->sum[a].f = fv; else acc->sum[a].i = iv;
        } else if (op == OP_MIN) {
            if (is_f) { if (fv < acc->min_val[a].f) acc->min_val[a].f = fv; }
            else      { if (iv < acc->min_val[a].i) acc->min_val[a].i = iv; }
        } else if (op == OP_MAX) {
            if (is_f) { if (fv > acc->max_val[a].f) acc->max_val[a].f = fv; }
            else      { if (iv > acc->max_val[a].i) acc->max_val[a].i = iv; }
        }
    }
}

static void scalar_accum_fn(void* ctx, uint32_t worker_id, int64_t start, int64_t end) {
    scalar_ctx_t* c = (scalar_ctx_t*)ctx;
    da_accum_t* acc = &c->accums[worker_id];
    const uint64_t* mask = c->mask;
    const uint8_t* sel_flags = c->sel_flags;

    for (int64_t r = start; r < end; ) {
        /* Segment-level skip */
        if (sel_flags) {
            uint32_t seg = (uint32_t)(r / TD_MORSEL_ELEMS);
            int64_t seg_end = (int64_t)(seg + 1) * TD_MORSEL_ELEMS;
            if (seg_end > end) seg_end = end;
            if (sel_flags[seg] == TD_SEL_NONE) { r = seg_end; continue; }
            bool need_bit = (sel_flags[seg] == TD_SEL_MIX);

            for (; r < seg_end; r++) {
                if (need_bit && !TD_SEL_BIT_TEST(mask, r)) continue;
                scalar_accum_row(c, acc, r);
            }
            continue;
        }

        if (TD_UNLIKELY(mask && !TD_SEL_BIT_TEST(mask, r))) { r++; continue; }
        scalar_accum_row(c, acc, r);
        r++;
    }
}

/* Inner DA accumulation for a single row — shared by single-key and multi-key paths.
 * Fast path for SUM/AVG-only queries: eliminates op-code dispatch and da_read_val
 * dual-write overhead.  The branch on c->all_sum is perfectly predicted (invariant
 * across all rows). */
static inline void da_accum_row(da_ctx_t* c, da_accum_t* acc, int32_t gid, int64_t r) {
    uint8_t n_aggs = c->n_aggs;
    acc->count[gid]++;
    size_t base = (size_t)gid * n_aggs;

    if (TD_LIKELY(c->all_sum)) {
        /* SUM/AVG/COUNT fast path — no op-code dispatch, typed read only.
         * COUNT-only queries have acc->sum==NULL; count[gid]++ above suffices. */
        if (!acc->sum) return;
        uint32_t f64m = c->agg_f64_mask;
        for (uint8_t a = 0; a < n_aggs; a++) {
            if (!c->agg_ptrs[a]) continue;
            size_t idx = base + a;
            if (f64m & (1u << a))
                acc->sum[idx].f += ((const double*)c->agg_ptrs[a])[r];
            else
                acc->sum[idx].i += read_col_i64(c->agg_ptrs[a], r,
                                                c->agg_types[a], 0);
        }
        return;
    }

    for (uint8_t a = 0; a < n_aggs; a++) {
        if (!c->agg_ptrs[a]) continue;
        size_t idx = base + a;
        double fv; int64_t iv;
        da_read_val(c->agg_ptrs[a], c->agg_types[a], 0, r, &fv, &iv);
        uint16_t op = c->agg_ops[a];
        if (op == OP_SUM || op == OP_AVG || op == OP_STDDEV || op == OP_STDDEV_POP || op == OP_VAR || op == OP_VAR_POP) {
            if (c->agg_types[a] == TD_F64) acc->sum[idx].f += fv;
            else acc->sum[idx].i = (int64_t)((uint64_t)acc->sum[idx].i + (uint64_t)iv);
            if (acc->sumsq_f64) acc->sumsq_f64[idx] += fv * fv;
        } else if (op == OP_FIRST) {
            if (acc->count[gid] == 1) {
                if (c->agg_types[a] == TD_F64) acc->sum[idx].f = fv;
                else acc->sum[idx].i = iv;
            }
        } else if (op == OP_LAST) {
            if (c->agg_types[a] == TD_F64) acc->sum[idx].f = fv;
            else acc->sum[idx].i = iv;
        } else if (op == OP_MIN) {
            if (c->agg_types[a] == TD_F64) {
                if (fv < acc->min_val[idx].f) acc->min_val[idx].f = fv;
            } else {
                if (iv < acc->min_val[idx].i) acc->min_val[idx].i = iv;
            }
        } else if (op == OP_MAX) {
            if (c->agg_types[a] == TD_F64) {
                if (fv > acc->max_val[idx].f) acc->max_val[idx].f = fv;
            } else {
                if (iv > acc->max_val[idx].i) acc->max_val[idx].i = iv;
            }
        }
    }
}

static void da_accum_fn(void* ctx, uint32_t worker_id, int64_t start, int64_t end) {
    da_ctx_t* c = (da_ctx_t*)ctx;
    da_accum_t* acc = &c->accums[worker_id];
    uint8_t n_aggs = c->n_aggs;
    uint8_t n_keys = c->n_keys;
    const uint64_t* mask = c->mask;
    const uint8_t* sel_flags = c->sel_flags;

    /* Fast path: single key — avoid composite GID loop overhead.
     * Templated by key element size: the entire loop is stamped out per width
     * so the compiler generates direct movzbl/movzwl/movl/movq — zero dispatch. */
    #define DA_PF_DIST 8
    #define DA_SINGLE_KEY_LOOP(KTYPE, KCAST) \
    do { \
        const KTYPE* kp = (const KTYPE*)c->key_ptrs[0]; \
        int64_t kmin = c->key_mins[0]; \
        bool da_pf = c->n_slots >= 4096; \
        for (int64_t r = start; r < end; ) { \
            if (sel_flags) { \
                uint32_t seg = (uint32_t)(r / TD_MORSEL_ELEMS); \
                int64_t seg_end = (int64_t)(seg + 1) * TD_MORSEL_ELEMS; \
                if (seg_end > end) seg_end = end; \
                if (sel_flags[seg] == TD_SEL_NONE) { r = seg_end; continue; } \
                bool need_bit = (sel_flags[seg] == TD_SEL_MIX); \
                for (; r < seg_end; r++) { \
                    if (need_bit && !TD_SEL_BIT_TEST(mask, r)) continue; \
                    if (da_pf && TD_LIKELY(r + DA_PF_DIST < end)) { \
                        int64_t pfk = (int64_t)KCAST kp[r + DA_PF_DIST]; \
                        __builtin_prefetch(&acc->count[(int32_t)(pfk - kmin)], 1, 1); \
                        if (acc->sum) __builtin_prefetch( \
                            &acc->sum[(size_t)(int32_t)(pfk - kmin) * n_aggs], 1, 1); \
                    } \
                    int64_t kv = (int64_t)KCAST kp[r]; \
                    da_accum_row(c, acc, (int32_t)(kv - kmin), r); \
                } \
                continue; \
            } \
            if (TD_UNLIKELY(mask && !TD_SEL_BIT_TEST(mask, r))) { r++; continue; } \
            if (da_pf && TD_LIKELY(r + DA_PF_DIST < end)) { \
                int64_t pfk = (int64_t)KCAST kp[r + DA_PF_DIST]; \
                __builtin_prefetch(&acc->count[(int32_t)(pfk - kmin)], 1, 1); \
                if (acc->sum) __builtin_prefetch( \
                    &acc->sum[(size_t)(int32_t)(pfk - kmin) * n_aggs], 1, 1); \
            } \
            int64_t kv = (int64_t)KCAST kp[r]; \
            da_accum_row(c, acc, (int32_t)(kv - kmin), r); \
            r++; \
        } \
    } while (0)

    if (n_keys == 1) {
        switch (c->key_esz[0]) {
        case 1: DA_SINGLE_KEY_LOOP(uint8_t, ); break;
        case 2: DA_SINGLE_KEY_LOOP(uint16_t, ); break;
        case 4: DA_SINGLE_KEY_LOOP(uint32_t, (int64_t)); break;
        default: DA_SINGLE_KEY_LOOP(int64_t, ); break;
        }
        #undef DA_SINGLE_KEY_LOOP
        return;
    }

    /* Multi-key composite GID — typed inner loop eliminates read_by_esz switch.
     * When all keys share the same element size, use da_composite_gid_XX(). */
    #define DA_MULTI_KEY_LOOP(GID_FN) \
    do { \
        bool _da_pf = c->n_slots >= 4096; \
        for (int64_t r = start; r < end; ) { \
            if (sel_flags) { \
                uint32_t seg = (uint32_t)(r / TD_MORSEL_ELEMS); \
                int64_t seg_end = (int64_t)(seg + 1) * TD_MORSEL_ELEMS; \
                if (seg_end > end) seg_end = end; \
                if (sel_flags[seg] == TD_SEL_NONE) { r = seg_end; continue; } \
                bool need_bit = (sel_flags[seg] == TD_SEL_MIX); \
                for (; r < seg_end; r++) { \
                    if (need_bit && !TD_SEL_BIT_TEST(mask, r)) continue; \
                    if (_da_pf && TD_LIKELY(r + DA_PF_DIST < end)) { \
                        int32_t pf_gid = GID_FN(r + DA_PF_DIST); \
                        __builtin_prefetch(&acc->count[pf_gid], 1, 1); \
                        if (acc->sum) __builtin_prefetch(&acc->sum[(size_t)pf_gid * n_aggs], 1, 1); \
                    } \
                    da_accum_row(c, acc, GID_FN(r), r); \
                } \
                continue; \
            } \
            if (TD_UNLIKELY(mask && !TD_SEL_BIT_TEST(mask, r))) { r++; continue; } \
            if (_da_pf && TD_LIKELY(r + DA_PF_DIST < end)) { \
                int32_t pf_gid = GID_FN(r + DA_PF_DIST); \
                __builtin_prefetch(&acc->count[pf_gid], 1, 1); \
                if (acc->sum) __builtin_prefetch(&acc->sum[(size_t)pf_gid * n_aggs], 1, 1); \
            } \
            da_accum_row(c, acc, GID_FN(r), r); \
            r++; \
        } \
    } while (0)

    /* Check if all keys share the same element size */
    bool uniform_esz = true;
    for (uint8_t k = 1; k < n_keys; k++)
        if (c->key_esz[k] != c->key_esz[0]) { uniform_esz = false; break; }

    if (uniform_esz) {
        switch (c->key_esz[0]) {
        case 1:
#define GID_FN(R) da_composite_gid_u8(c, (R))
            DA_MULTI_KEY_LOOP(GID_FN);
#undef GID_FN
            break;
        case 2:
#define GID_FN(R) da_composite_gid_u16(c, (R))
            DA_MULTI_KEY_LOOP(GID_FN);
#undef GID_FN
            break;
        case 4:
#define GID_FN(R) da_composite_gid_u32(c, (R))
            DA_MULTI_KEY_LOOP(GID_FN);
#undef GID_FN
            break;
        default:
#define GID_FN(R) da_composite_gid_i64(c, (R))
            DA_MULTI_KEY_LOOP(GID_FN);
#undef GID_FN
            break;
        }
    } else {
#define GID_FN(R) da_composite_gid(c, (R))
        DA_MULTI_KEY_LOOP(GID_FN);
#undef GID_FN
    }
    #undef DA_MULTI_KEY_LOOP
    #undef DA_PF_DIST
}

/* Parallel DA merge: merge per-worker accumulators into accums[0] by
 * dispatching disjoint slot ranges across pool workers. */
typedef struct {
    da_accum_t* accums;
    uint32_t    n_src_workers; /* number of source workers to merge (1..n) */
    uint8_t     need_flags;
    uint8_t     n_aggs;
    const int8_t* agg_types;  /* per-agg value type (for typed merge) */
    const uint16_t* agg_ops;  /* per-agg opcode (for FIRST/LAST merge) */
} da_merge_ctx_t;

static void da_merge_fn(void* ctx, uint32_t wid, int64_t start, int64_t end) {
    (void)wid;
    da_merge_ctx_t* c = (da_merge_ctx_t*)ctx;
    da_accum_t* merged = &c->accums[0];
    uint8_t n_aggs = c->n_aggs;
    const int8_t* agg_types = c->agg_types;
    for (uint32_t w = 1; w < c->n_src_workers; w++) {
        da_accum_t* wa = &c->accums[w];
        for (int64_t s = start; s < end; s++) {
            size_t base = (size_t)s * n_aggs;
            if (c->need_flags & DA_NEED_SUMSQ) {
                for (uint8_t a = 0; a < n_aggs; a++)
                    merged->sumsq_f64[base + a] += wa->sumsq_f64[base + a];
            }
            if (c->need_flags & DA_NEED_SUM) {
                for (uint8_t a = 0; a < n_aggs; a++) {
                    size_t idx = base + a;
                    uint16_t aop = c->agg_ops ? c->agg_ops[a] : OP_SUM;
                    if (aop == OP_FIRST) {
                        /* Keep worker 0 value; take from w only if merged has no data */
                        if (merged->count[s] == 0 && wa->count[s] > 0)
                            merged->sum[idx] = wa->sum[idx];
                    } else if (aop == OP_LAST) {
                        /* Overwrite with last worker that has data */
                        if (wa->count[s] > 0)
                            merged->sum[idx] = wa->sum[idx];
                    } else if (agg_types[a] == TD_F64)
                        merged->sum[idx].f += wa->sum[idx].f;
                    else
                        merged->sum[idx].i += wa->sum[idx].i;
                }
            }
            if (c->need_flags & DA_NEED_MIN) {
                for (uint8_t a = 0; a < n_aggs; a++) {
                    size_t idx = base + a;
                    if (agg_types[a] == TD_F64) {
                        if (wa->min_val[idx].f < merged->min_val[idx].f)
                            merged->min_val[idx].f = wa->min_val[idx].f;
                    } else {
                        if (wa->min_val[idx].i < merged->min_val[idx].i)
                            merged->min_val[idx].i = wa->min_val[idx].i;
                    }
                }
            }
            if (c->need_flags & DA_NEED_MAX) {
                for (uint8_t a = 0; a < n_aggs; a++) {
                    size_t idx = base + a;
                    if (agg_types[a] == TD_F64) {
                        if (wa->max_val[idx].f > merged->max_val[idx].f)
                            merged->max_val[idx].f = wa->max_val[idx].f;
                    } else {
                        if (wa->max_val[idx].i > merged->max_val[idx].i)
                            merged->max_val[idx].i = wa->max_val[idx].i;
                    }
                }
            }
            merged->count[s] += wa->count[s];
        }
    }
}

/* ============================================================================
 * Partition-aware group-by: detect parted columns, concatenate segments into
 * a flat table, then run standard exec_group once.
 * ============================================================================ */
static td_t* exec_group(td_graph_t* g, td_op_t* op, td_t* tbl,
                        int64_t group_limit); /* forward decl */

/* Forward declaration — defined below exec_group */
static td_t* exec_group_per_partition(td_t* parted_tbl, td_op_ext_t* ext,
                                       int32_t n_parts, const int64_t* key_syms,
                                       const int64_t* agg_syms, int has_avg,
                                       int has_stddev, int64_t group_limit);

/* --------------------------------------------------------------------------
 * exec_group_parted — dispatch per-partition or concat-fallback
 * -------------------------------------------------------------------------- */
static td_t* exec_group_parted(td_graph_t* g, td_op_t* op, td_t* parted_tbl,
                               int64_t group_limit) {
    int64_t ncols = td_table_ncols(parted_tbl);
    if (ncols <= 0) return TD_ERR_PTR(TD_ERR_NYI);

    td_op_ext_t* ext = find_ext(g, op->id);
    if (!ext) return TD_ERR_PTR(TD_ERR_NYI);

    uint8_t n_keys = ext->n_keys;
    uint8_t n_aggs = ext->n_aggs;

    /* Find partition count and total rows from first parted column */
    int32_t n_parts = 0;
    int64_t total_rows = 0;
    for (int64_t c = 0; c < ncols; c++) {
        td_t* col = td_table_get_col_idx(parted_tbl, c);
        if (col && TD_IS_PARTED(col->type)) {
            n_parts = (int32_t)col->len;
            total_rows = td_parted_nrows(col);
            break;
        }
    }
    if (n_parts <= 0 || total_rows <= 0) return TD_ERR_PTR(TD_ERR_NYI);

    /* Check eligibility for per-partition exec + merge:
     * - All keys and agg inputs must be simple SCANs
     * - Supported agg ops: SUM, COUNT, MIN, MAX, AVG, FIRST, LAST,
     *   STDDEV, STDDEV_POP, VAR, VAR_POP */
    int can_partition = 1;
    int has_avg = 0;
    int has_stddev = 0;
    int64_t key_syms[8];
    for (uint8_t k = 0; k < n_keys && can_partition; k++) {
        td_op_ext_t* ke = find_ext(g, ext->keys[k]->id);
        if (!ke || ke->base.opcode != OP_SCAN) { can_partition = 0; break; }
        key_syms[k] = ke->sym;
    }
    int64_t agg_syms[8];
    for (uint8_t a = 0; a < n_aggs && can_partition; a++) {
        uint16_t aop = ext->agg_ops[a];
        if (aop != OP_SUM && aop != OP_COUNT && aop != OP_MIN &&
            aop != OP_MAX && aop != OP_AVG && aop != OP_FIRST &&
            aop != OP_LAST && aop != OP_STDDEV && aop != OP_STDDEV_POP &&
            aop != OP_VAR && aop != OP_VAR_POP) { can_partition = 0; break; }
        if (aop == OP_AVG) has_avg = 1;
        if (aop == OP_STDDEV || aop == OP_STDDEV_POP ||
            aop == OP_VAR || aop == OP_VAR_POP) has_stddev = 1;
        td_op_ext_t* ae = find_ext(g, ext->agg_ins[a]->id);
        if (!ae || ae->base.opcode != OP_SCAN) { can_partition = 0; break; }
        agg_syms[a] = ae->sym;
    }

    /* Cardinality gate: estimate groups from first partition.
     * Per-partition only wins when #groups << partition_size. */
    if (can_partition) {
        int64_t rows_per_part = total_rows / n_parts;
        int64_t est_groups = 1;
        for (uint8_t k = 0; k < n_keys; k++) {
            td_t* pcol = td_table_get_col(parted_tbl, key_syms[k]);
            if (!pcol) { est_groups = rows_per_part; break; }
            /* MAPCOMMON key: constant per partition — excluded from
             * per-partition sub-GROUP-BY, contributes 0 to cardinality. */
            if (pcol->type == TD_MAPCOMMON) { continue; }
            if (!TD_IS_PARTED(pcol->type)) { est_groups = rows_per_part; break; }
            td_t* seg0 = ((td_t**)td_data(pcol))[0];
            if (!seg0 || seg0->len <= 0) { est_groups = rows_per_part; break; }
            int8_t bt = TD_PARTED_BASETYPE(pcol->type);
            int64_t card;
            if (TD_IS_SYM(bt)) {
                uint32_t sym_n = td_sym_count();
                if (sym_n == 0 || sym_n > 4194304) { est_groups = rows_per_part; break; }
                size_t bwords = ((size_t)sym_n + 63) / 64;
                td_t* bits_hdr = NULL;
                uint64_t* bits = (uint64_t*)scratch_calloc(&bits_hdr, bwords * 8);
                if (!bits) { est_groups = rows_per_part; break; }
                for (int64_t r = 0; r < seg0->len; r++) {
                    uint32_t id = (uint32_t)td_read_sym(td_data(seg0), r, seg0->type, seg0->attrs);
                    bits[id / 64] |= 1ULL << (id % 64);
                }
                card = 0;
                for (size_t i = 0; i < bwords; i++)
                    card += __builtin_popcountll(bits[i]);
                scratch_free(bits_hdr);
            } else if (bt == TD_I64) {
                const int64_t* v = (const int64_t*)td_data(seg0);
                int64_t lo = v[0], hi = v[0];
                for (int64_t r = 1; r < seg0->len; r++) {
                    if (v[r] < lo) lo = v[r];
                    if (v[r] > hi) hi = v[r];
                }
                card = hi - lo + 1;
            } else if (bt == TD_I32) {
                const int32_t* v = (const int32_t*)td_data(seg0);
                int32_t lo = v[0], hi = v[0];
                for (int64_t r = 1; r < seg0->len; r++) {
                    if (v[r] < lo) lo = v[r];
                    if (v[r] > hi) hi = v[r];
                }
                card = (int64_t)(hi - lo + 1);
            } else {
                card = seg0->len;
            }
            est_groups *= card;
            if (est_groups > rows_per_part) { est_groups = rows_per_part; break; }
        }
        /* Block per-partition when cardinality is high AND the concat
         * fallback would fit in memory (< 4 GB estimated).  When concat is
         * too large, per-partition with batched merge is the only option. */
        int64_t concat_bytes = total_rows * 8LL * (int64_t)(n_keys + n_aggs);
        if (est_groups * 100 > rows_per_part &&
            concat_bytes < 4LL * 1024 * 1024 * 1024)
            can_partition = 0;
    }

    /* Try per-partition path (separate noinline function to avoid I-cache pressure) */
    if (can_partition) {
        td_t* result = exec_group_per_partition(parted_tbl, ext, n_parts,
                                                 key_syms, agg_syms, has_avg,
                                                 has_stddev, group_limit);
        if (result) return result;
        /* NULL = per-partition failed, fall through to concat */
    }

    /* ---- Concat fallback ---- */
    /* ---- Concat-only-needed-columns fallback ----
     * Used when query has AVG or expression keys/aggs.
     * Only concatenates the columns actually referenced by the GROUP BY. */
    {
        /* Collect needed column sym IDs (keys + agg inputs) */
        int64_t needed[16];
        int n_needed = 0;
        for (uint8_t k = 0; k < n_keys; k++) {
            td_op_ext_t* ke = find_ext(g, ext->keys[k]->id);
            if (ke && ke->base.opcode == OP_SCAN) {
                int dup = 0;
                for (int i = 0; i < n_needed; i++)
                    if (needed[i] == ke->sym) { dup = 1; break; }
                if (!dup) needed[n_needed++] = ke->sym;
            }
        }
        for (uint8_t a = 0; a < n_aggs; a++) {
            td_op_ext_t* ae = find_ext(g, ext->agg_ins[a]->id);
            if (ae && ae->base.opcode == OP_SCAN) {
                int dup = 0;
                for (int i = 0; i < n_needed; i++)
                    if (needed[i] == ae->sym) { dup = 1; break; }
                if (!dup) needed[n_needed++] = ae->sym;
            } else {
                /* Expression agg input — need all columns for evaluation.
                 * Fall back to copying everything. */
                n_needed = 0;
                break;
            }
        }

        /* Build flat table with only needed columns (or all if n_needed==0) */
        td_t* flat_tbl = td_table_new(n_needed > 0 ? (int64_t)n_needed : ncols);
        if (!flat_tbl || TD_IS_ERR(flat_tbl)) return flat_tbl;

        int64_t cols_to_iter = n_needed > 0 ? (int64_t)n_needed : ncols;
        for (int64_t ci = 0; ci < cols_to_iter; ci++) {
            td_t* col;
            int64_t name_id;
            if (n_needed > 0) {
                col = td_table_get_col(parted_tbl, needed[ci]);
                name_id = needed[ci];
            } else {
                col = td_table_get_col_idx(parted_tbl, ci);
                name_id = td_table_col_name(parted_tbl, ci);
            }
            if (!col) continue;
            if (col->type == TD_MAPCOMMON) {
                td_t* mc_flat = materialize_mapcommon(col);
                if (mc_flat && !TD_IS_ERR(mc_flat)) {
                    flat_tbl = td_table_add_col(flat_tbl, name_id, mc_flat);
                    td_release(mc_flat);
                }
                continue;
            }

            if (!TD_IS_PARTED(col->type)) {
                td_retain(col);
                flat_tbl = td_table_add_col(flat_tbl, name_id, col);
                td_release(col);
                continue;
            }

            int8_t base_type = (int8_t)TD_PARTED_BASETYPE(col->type);
            td_t** segs = (td_t**)td_data(col);
            uint8_t base_attrs = (base_type == TD_SYM && col->len > 0 && segs[0])
                               ? segs[0]->attrs : 0;
            td_t* flat = typed_vec_new(base_type, base_attrs, total_rows);
            if (!flat || TD_IS_ERR(flat)) {
                td_release(flat_tbl);
                return TD_ERR_PTR(TD_ERR_OOM);
            }
            flat->len = total_rows;

            size_t elem_size = (size_t)td_sym_elem_size(base_type, base_attrs);
            int64_t offset = 0;
            for (int32_t p = 0; p < n_parts; p++) {
                td_t* seg = segs[p];
                if (!seg || seg->len <= 0) continue;
                memcpy((char*)td_data(flat) + (size_t)offset * elem_size,
                       td_data(seg), (size_t)seg->len * elem_size);
                offset += seg->len;
            }

            flat_tbl = td_table_add_col(flat_tbl, name_id, flat);
            td_release(flat);
        }

        td_t* saved = g->table;
        g->table = flat_tbl;
        td_t* result = exec_group(g, op, flat_tbl, 0);
        g->table = saved;
        td_release(flat_tbl);
        return result;
    }
}

static td_t* exec_group(td_graph_t* g, td_op_t* op, td_t* tbl,
                        int64_t group_limit) {
    if (!tbl || TD_IS_ERR(tbl)) return tbl;

    /* Parted dispatch: detect parted input columns */
    {
        int64_t nc = td_table_ncols(tbl);
        for (int64_t c = 0; c < nc; c++) {
            td_t* col = td_table_get_col_idx(tbl, c);
            if (col && (TD_IS_PARTED(col->type) || col->type == TD_MAPCOMMON)) {
                return exec_group_parted(g, op, tbl, group_limit);
            }
        }
    }

    td_op_ext_t* ext = find_ext(g, op->id);
    if (!ext) return TD_ERR_PTR(TD_ERR_NYI);

    int64_t nrows = td_table_nrows(tbl);
    uint8_t n_keys = ext->n_keys;
    uint8_t n_aggs = ext->n_aggs;

    /* Factorized shortcut: if input is a factorized expand result with
     * (_src, _count) columns, and GROUP BY _src with COUNT/SUM(_count),
     * return the pre-aggregated table directly without re-scanning. */
    if (n_keys == 1 && n_aggs > 0 && nrows > 0) {
        int64_t cnt_sym = td_sym_intern("_count", 6);
        td_t* cnt_col = td_table_get_col(tbl, cnt_sym);
        if (cnt_col && cnt_col->type == TD_I64) {
            td_op_ext_t* key_ext = find_ext(g, ext->keys[0]->id);
            int64_t src_sym = td_sym_intern("_src", 4);
            if (key_ext && key_ext->base.opcode == OP_SCAN &&
                key_ext->sym == src_sym) {
                /* Verify all aggs are compatible with factorized data:
                 * COUNT(*) → use _count directly
                 * SUM(_count) → use _count directly */
                bool all_compat = true;
                for (uint8_t a = 0; a < n_aggs; a++) {
                    uint16_t aop = ext->agg_ops[a];
                    td_op_ext_t* agg_ext = find_ext(g, ext->agg_ins[a]->id);
                    if (aop == OP_COUNT) continue;
                    if (aop == OP_SUM && agg_ext &&
                        agg_ext->base.opcode == OP_SCAN &&
                        agg_ext->sym == cnt_sym) continue;
                    all_compat = false;
                    break;
                }
                if (all_compat) {
                    /* The factorized table already has one row per group.
                     * Build result with _src key + agg columns from _count. */
                    td_t* src_col = td_table_get_col(tbl, src_sym);
                    if (src_col) {
                        int64_t out_nkeys = 1;
                        int64_t out_ncols = out_nkeys + n_aggs;
                        td_t* result = td_table_new((int64_t)out_ncols);
                        if (!result || TD_IS_ERR(result))
                            return TD_ERR_PTR(TD_ERR_OOM);
                        td_retain(src_col);
                        result = td_table_add_col(result, src_sym, src_col);
                        td_release(src_col);
                        for (uint8_t a = 0; a < n_aggs; a++) {
                            td_retain(cnt_col);
                            int64_t agg_name = td_sym_intern("_agg", 4);
                            if (n_aggs > 1) {
                                char buf[16];
                                int n = snprintf(buf, sizeof(buf), "_agg%d", a);
                                agg_name = td_sym_intern(buf, (size_t)n);
                            }
                            result = td_table_add_col(result, agg_name, cnt_col);
                            td_release(cnt_col);
                        }
                        return result;
                    }
                }
            }
        }
    }

    /* Extract selection bitmap for pushdown (skip filtered rows in scan loops) */
    const uint64_t* mask = NULL;
    const uint8_t* sel_flags = NULL;
    if (g->selection && g->selection->type == TD_SEL
        && g->selection->len == nrows) {
        mask = td_sel_bits(g->selection);
        sel_flags = td_sel_flags(g->selection);
    }

    if (n_keys > 8 || n_aggs > 8) return TD_ERR_PTR(TD_ERR_NYI);

    /* Resolve key columns (VLA — n_keys ≤ 8) */
    td_t* key_vecs[n_keys];
    memset(key_vecs, 0, n_keys * sizeof(td_t*));

    uint8_t key_owned[n_keys]; /* 1 = we allocated via exec_node, must free */
    memset(key_owned, 0, n_keys * sizeof(uint8_t));
    for (uint8_t k = 0; k < n_keys; k++) {
        td_op_t* key_op = ext->keys[k];
        td_op_ext_t* key_ext = find_ext(g, key_op->id);
        if (key_ext && key_ext->base.opcode == OP_SCAN) {
            key_vecs[k] = td_table_get_col(tbl, key_ext->sym);
        } else {
            /* Expression key (CASE WHEN etc) — evaluate against current tbl */
            td_t* saved_table = g->table;
            g->table = tbl;
            td_t* vec = exec_node(g, key_op);
            g->table = saved_table;
            if (vec && !TD_IS_ERR(vec)) {
                key_vecs[k] = vec;
                key_owned[k] = 1;
            }
        }
    }

    /* Resolve agg input columns (VLA — n_aggs ≤ 8) */
    td_t* agg_vecs[n_aggs];
    uint8_t agg_owned[n_aggs]; /* 1 = we allocated via exec_node, must free */
    agg_affine_t agg_affine[n_aggs];
    agg_linear_t agg_linear[n_aggs];
    memset(agg_vecs, 0, n_aggs * sizeof(td_t*));
    memset(agg_owned, 0, n_aggs * sizeof(uint8_t));
    memset(agg_affine, 0, n_aggs * sizeof(agg_affine_t));
    memset(agg_linear, 0, n_aggs * sizeof(agg_linear_t));

    for (uint8_t a = 0; a < n_aggs; a++) {
        td_op_t* agg_input_op = ext->agg_ins[a];
        td_op_ext_t* agg_ext = find_ext(g, agg_input_op->id);

        /* SUM/AVG(scan +/- const): aggregate base scan and apply bias at emit. */
        uint16_t agg_kind = ext->agg_ops[a];
        if ((agg_kind == OP_SUM || agg_kind == OP_AVG) &&
            try_affine_sumavg_input(g, tbl, agg_input_op, &agg_vecs[a], &agg_affine[a])) {
            continue;
        }

        /* SUM/AVG(integer-linear expr): scalar path can aggregate directly
         * without materializing the expression vector. */
        if (n_keys == 0 && nrows > 0 &&
            (agg_kind == OP_SUM || agg_kind == OP_AVG) &&
            try_linear_sumavg_input_i64(g, tbl, agg_input_op, &agg_linear[a])) {
            continue;
        }

        if (agg_ext && agg_ext->base.opcode == OP_SCAN) {
            agg_vecs[a] = td_table_get_col(tbl, agg_ext->sym);
        } else if (agg_ext && agg_ext->base.opcode == OP_CONST && agg_ext->literal) {
            agg_vecs[a] = agg_ext->literal;
        } else {
            /* Expression node (ADD/MUL etc) — try compiled expression first */
            td_expr_t agg_expr;
            if (expr_compile(g, tbl, agg_input_op, &agg_expr)) {
                td_t* vec = expr_eval_full(&agg_expr, nrows);
                if (vec && !TD_IS_ERR(vec)) {
                    agg_vecs[a] = vec;
                    agg_owned[a] = 1;
                    continue;
                }
            }
            /* Fallback: full recursive evaluation */
            td_t* saved_table = g->table;
            g->table = tbl;
            td_t* vec = exec_node(g, agg_input_op);
            g->table = saved_table;
            if (vec && !TD_IS_ERR(vec)) {
                agg_vecs[a] = vec;
                agg_owned[a] = 1;
            }
        }
    }

    /* Normalize scalar agg inputs to full-length vectors.
     * Constants and scalar sub-expressions (len=1) must be broadcast to nrows
     * before row-wise aggregation loops. */
    for (uint8_t a = 0; a < n_aggs; a++) {
        if (!agg_vecs[a] || TD_IS_ERR(agg_vecs[a])) continue;
        if (ext->agg_ops[a] == OP_COUNT) continue; /* value is ignored for COUNT */

        bool needs_broadcast = td_is_atom(agg_vecs[a]) ||
                               (agg_vecs[a]->type > 0 && agg_vecs[a]->len == 1 && nrows > 1);
        if (!needs_broadcast) continue;

        td_t* bcast = materialize_broadcast_input(agg_vecs[a], nrows);
        if (!bcast || TD_IS_ERR(bcast)) {
            for (uint8_t i = 0; i < n_aggs; i++) {
                if (agg_owned[i] && agg_vecs[i]) td_release(agg_vecs[i]);
            }
            for (uint8_t k = 0; k < n_keys; k++) {
                if (key_owned[k] && key_vecs[k]) td_release(key_vecs[k]);
            }
            return bcast && TD_IS_ERR(bcast) ? bcast : TD_ERR_PTR(TD_ERR_OOM);
        }

        if (agg_owned[a]) td_release(agg_vecs[a]);
        agg_vecs[a] = bcast;
        agg_owned[a] = 1;
    }

    /* Pre-compute key metadata (VLA — n_keys ≤ 8) */
    void* key_data[n_keys];
    int8_t key_types[n_keys];
    uint8_t key_attrs[n_keys];
    for (uint8_t k = 0; k < n_keys; k++) {
        if (key_vecs[k]) {
            key_data[k]  = td_data(key_vecs[k]);
            key_types[k] = key_vecs[k]->type;
            key_attrs[k] = key_vecs[k]->attrs;
        } else {
            key_data[k]  = NULL;
            key_types[k] = 0;
            key_attrs[k] = 0;
        }
    }

    /* ---- Scalar aggregate fast path (n_keys == 0): flat vector scan ---- */
    if (n_keys == 0 && nrows > 0) {
        uint8_t need_flags = DA_NEED_COUNT;
        for (uint8_t a = 0; a < n_aggs; a++) {
            uint16_t aop = ext->agg_ops[a];
            if (aop == OP_SUM || aop == OP_AVG || aop == OP_FIRST || aop == OP_LAST)
                need_flags |= DA_NEED_SUM;
            else if (aop == OP_STDDEV || aop == OP_STDDEV_POP || aop == OP_VAR || aop == OP_VAR_POP)
                { need_flags |= DA_NEED_SUM; need_flags |= DA_NEED_SUMSQ; }
            else if (aop == OP_MIN) need_flags |= DA_NEED_MIN;
            else if (aop == OP_MAX) need_flags |= DA_NEED_MAX;
        }

        void* agg_ptrs[n_aggs];
        int8_t agg_types[n_aggs];
        for (uint8_t a = 0; a < n_aggs; a++) {
            if (agg_vecs[a]) {
                agg_ptrs[a]  = td_data(agg_vecs[a]);
                agg_types[a] = agg_vecs[a]->type;
            } else {
                agg_ptrs[a]  = NULL;
                agg_types[a] = 0;
            }
        }

        td_pool_t* sc_pool = td_pool_get();
        uint32_t sc_n = (sc_pool && nrows >= TD_PARALLEL_THRESHOLD)
                        ? td_pool_total_workers(sc_pool) : 1;

        td_t* sc_hdr;
        da_accum_t* sc_acc = (da_accum_t*)scratch_calloc(&sc_hdr,
            sc_n * sizeof(da_accum_t));
        if (!sc_acc) goto da_path;

        /* Allocate 1-slot accumulators per worker (n_aggs entries) */
        bool alloc_ok = true;
        for (uint32_t w = 0; w < sc_n; w++) {
            if (need_flags & DA_NEED_SUM) {
                sc_acc[w].sum = (da_val_t*)scratch_calloc(&sc_acc[w]._h_sum,
                    n_aggs * sizeof(da_val_t));
                if (!sc_acc[w].sum) { alloc_ok = false; break; }
            }
            if (need_flags & DA_NEED_MIN) {
                sc_acc[w].min_val = (da_val_t*)scratch_alloc(&sc_acc[w]._h_min,
                    n_aggs * sizeof(da_val_t));
                if (!sc_acc[w].min_val) { alloc_ok = false; break; }
                for (uint8_t a = 0; a < n_aggs; a++) {
                    if (agg_types[a] == TD_F64) sc_acc[w].min_val[a].f = DBL_MAX;
                    else sc_acc[w].min_val[a].i = INT64_MAX;
                }
            }
            if (need_flags & DA_NEED_MAX) {
                sc_acc[w].max_val = (da_val_t*)scratch_alloc(&sc_acc[w]._h_max,
                    n_aggs * sizeof(da_val_t));
                if (!sc_acc[w].max_val) { alloc_ok = false; break; }
                for (uint8_t a = 0; a < n_aggs; a++) {
                    if (agg_types[a] == TD_F64) sc_acc[w].max_val[a].f = -DBL_MAX;
                    else sc_acc[w].max_val[a].i = INT64_MIN;
                }
            }
            if (need_flags & DA_NEED_SUMSQ) {
                sc_acc[w].sumsq_f64 = (double*)scratch_calloc(&sc_acc[w]._h_sumsq,
                    n_aggs * sizeof(double));
                if (!sc_acc[w].sumsq_f64) { alloc_ok = false; break; }
            }
            sc_acc[w].count = (int64_t*)scratch_calloc(&sc_acc[w]._h_count,
                1 * sizeof(int64_t));
            if (!sc_acc[w].count) { alloc_ok = false; break; }
        }
        if (!alloc_ok) {
            for (uint32_t w = 0; w < sc_n; w++) da_accum_free(&sc_acc[w]);
            scratch_free(sc_hdr);
            goto da_path;
        }

        scalar_ctx_t sc_ctx = {
            .agg_ptrs   = agg_ptrs,
            .agg_types  = agg_types,
            .agg_ops    = ext->agg_ops,
            .agg_linear = agg_linear,
            .n_aggs     = n_aggs,
            .need_flags = need_flags,
            .mask       = mask,
            .sel_flags  = sel_flags,
            .accums     = sc_acc,
            .n_accums   = sc_n,
        };

        /* Pick specialized tight loop when possible, else generic */
        typedef void (*scalar_fn_t)(void*, uint32_t, int64_t, int64_t);
        scalar_fn_t sc_fn = scalar_accum_fn;
        if (n_aggs == 1 && !mask && agg_ptrs[0] != NULL) {
            uint16_t op0 = ext->agg_ops[0];
            int8_t   t0  = agg_types[0];
            if ((op0 == OP_SUM || op0 == OP_AVG) &&
                (t0 == TD_I64 || t0 == TD_SYM || t0 == TD_TIMESTAMP))
                sc_fn = scalar_sum_i64_fn;
            else if ((op0 == OP_SUM || op0 == OP_AVG) && t0 == TD_F64)
                sc_fn = scalar_sum_f64_fn;
        } else if (n_aggs == 1 && !mask && agg_linear[0].enabled) {
            uint16_t op0 = ext->agg_ops[0];
            if (op0 == OP_SUM || op0 == OP_AVG)
                sc_fn = scalar_sum_linear_i64_fn;
        }

        if (sc_n > 1)
            td_pool_dispatch(sc_pool, sc_fn, &sc_ctx, nrows);
        else
            sc_fn(&sc_ctx, 0, 0, nrows);

        /* Merge per-worker accumulators into sc_acc[0] */
        da_accum_t* m = &sc_acc[0];
        for (uint32_t w = 1; w < sc_n; w++) {
            da_accum_t* wa = &sc_acc[w];
            if (need_flags & DA_NEED_SUM) {
                for (uint8_t a = 0; a < n_aggs; a++) {
                    uint16_t merge_op = ext->agg_ops[a];
                    if (merge_op == OP_FIRST) {
                        if (m->count[0] == 0 && wa->count[0] > 0)
                            m->sum[a] = wa->sum[a];
                    } else if (merge_op == OP_LAST) {
                        if (wa->count[0] > 0)
                            m->sum[a] = wa->sum[a];
                    } else {
                        if (agg_types[a] == TD_F64)
                            m->sum[a].f += wa->sum[a].f;
                        else
                            m->sum[a].i += wa->sum[a].i;
                    }
                }
            }
            if (need_flags & DA_NEED_SUMSQ) {
                for (uint8_t a = 0; a < n_aggs; a++)
                    m->sumsq_f64[a] += wa->sumsq_f64[a];
            }
            if (need_flags & DA_NEED_MIN) {
                for (uint8_t a = 0; a < n_aggs; a++) {
                    if (agg_types[a] == TD_F64) {
                        if (wa->min_val[a].f < m->min_val[a].f)
                            m->min_val[a].f = wa->min_val[a].f;
                    } else {
                        if (wa->min_val[a].i < m->min_val[a].i)
                            m->min_val[a].i = wa->min_val[a].i;
                    }
                }
            }
            if (need_flags & DA_NEED_MAX) {
                for (uint8_t a = 0; a < n_aggs; a++) {
                    if (agg_types[a] == TD_F64) {
                        if (wa->max_val[a].f > m->max_val[a].f)
                            m->max_val[a].f = wa->max_val[a].f;
                    } else {
                        if (wa->max_val[a].i > m->max_val[a].i)
                            m->max_val[a].i = wa->max_val[a].i;
                    }
                }
            }
            m->count[0] += wa->count[0];
        }
        for (uint32_t w = 1; w < sc_n; w++) da_accum_free(&sc_acc[w]);

        /* Emit 1-row result with no key columns */
        td_t* result = td_table_new(n_aggs);
        if (!result || TD_IS_ERR(result)) {
            da_accum_free(&sc_acc[0]); scratch_free(sc_hdr);
            return result ? result : TD_ERR_PTR(TD_ERR_OOM);
        }

        emit_agg_columns(&result, g, ext, agg_vecs, 1, n_aggs,
                         (double*)m->sum, (int64_t*)m->sum,
                         (double*)m->min_val, (double*)m->max_val,
                         (int64_t*)m->min_val, (int64_t*)m->max_val,
                         m->count, agg_affine, m->sumsq_f64);

        da_accum_free(&sc_acc[0]); scratch_free(sc_hdr);
        for (uint8_t a = 0; a < n_aggs; a++)
            if (agg_owned[a] && agg_vecs[a]) td_release(agg_vecs[a]);
        for (uint8_t k = 0; k < n_keys; k++)
            if (key_owned[k] && key_vecs[k]) td_release(key_vecs[k]);
        return result;
    }

da_path:;
    /* ---- Direct-array fast path for low-cardinality integer keys ---- */
    /* Supports multi-key via composite index: product of ranges <= MAX */
    #define DA_MAX_COMPOSITE_SLOTS 262144  /* 256K slots max */
    #define DA_MEM_BUDGET      (256ULL << 20)  /* 256 MB total across all workers */
    #define DA_PER_WORKER_MAX  (6ULL << 20)    /* 6 MB per-worker max */
    {
        bool da_eligible = (nrows > 0 && n_keys > 0 && n_keys <= 8);
        for (uint8_t k = 0; k < n_keys && da_eligible; k++) {
            if (!key_data[k]) { da_eligible = false; break; }
            int8_t t = key_types[k];
            if (t != TD_I64 && t != TD_SYM && t != TD_I32
                && t != TD_TIMESTAMP && t != TD_DATE && t != TD_TIME
                && t != TD_BOOL && t != TD_U8 && t != TD_I16) {
                da_eligible = false;
            }
        }

        int64_t da_key_min[8], da_key_range[8], da_key_stride[8];
        uint64_t total_slots = 1;
        bool da_fits = false;


        if (da_eligible) {
            da_fits = true;
            td_pool_t* mm_pool = td_pool_get();
            uint32_t mm_n = (mm_pool && nrows >= TD_PARALLEL_THRESHOLD)
                            ? td_pool_total_workers(mm_pool) : 1;
            /* VLA bounded by worker count — max ~2KB per key even on 256-core systems. */
            int64_t mm_mins[mm_n], mm_maxs[mm_n];
            for (uint8_t k = 0; k < n_keys && da_fits; k++) {
                int64_t kmin, kmax;
                for (uint32_t w = 0; w < mm_n; w++) {
                    mm_mins[w] = INT64_MAX;
                    mm_maxs[w] = INT64_MIN;
                }
                minmax_ctx_t mm_ctx = {
                    .key_data       = key_data[k],
                    .key_type       = key_types[k],
                    .key_attrs      = key_attrs[k],
                    .per_worker_min = mm_mins,
                    .per_worker_max = mm_maxs,
                    .n_workers      = mm_n,
                    .mask           = mask,
                    .sel_flags      = sel_flags,
                };
                if (mm_n > 1) {
                    td_pool_dispatch(mm_pool, minmax_scan_fn, &mm_ctx, nrows);
                } else {
                    minmax_scan_fn(&mm_ctx, 0, 0, nrows);
                }
                kmin = INT64_MAX; kmax = INT64_MIN;
                for (uint32_t w = 0; w < mm_n; w++) {
                    if (mm_mins[w] < kmin) kmin = mm_mins[w];
                    if (mm_maxs[w] > kmax) kmax = mm_maxs[w];
                }
                da_key_min[k]   = kmin;
                da_key_range[k] = kmax - kmin + 1;
                if (da_key_range[k] <= 0) { da_fits = false; break; }
                total_slots *= (uint64_t)da_key_range[k];
                if (total_slots > DA_MAX_COMPOSITE_SLOTS) da_fits = false;
            }
        }

        if (da_fits) {
            /* Compute which accumulator arrays we actually need */
            uint8_t need_flags = DA_NEED_COUNT; /* always need count */
            for (uint8_t a = 0; a < n_aggs; a++) {
                uint16_t aop = ext->agg_ops[a];
                if (aop == OP_SUM || aop == OP_AVG || aop == OP_FIRST || aop == OP_LAST) need_flags |= DA_NEED_SUM;
                else if (aop == OP_STDDEV || aop == OP_STDDEV_POP || aop == OP_VAR || aop == OP_VAR_POP)
                    { need_flags |= DA_NEED_SUM; need_flags |= DA_NEED_SUMSQ; }
                else if (aop == OP_MIN) need_flags |= DA_NEED_MIN;
                else if (aop == OP_MAX) need_flags |= DA_NEED_MAX;
            }

            /* Compute per-worker memory budget.  Actual allocation is 1 union
             * array per type, but MIN/MAX use conditional random writes that
             * perform worse than radix-partitioned HT at high group counts.
             * Weight MIN/MAX at 2x to keep those queries on the HT path. */
            uint32_t arrays_per_agg = 0;
            if (need_flags & DA_NEED_SUM) arrays_per_agg += 1;
            if (need_flags & DA_NEED_MIN) arrays_per_agg += 2; /* 2x: DA MIN slow at high cardinality */
            if (need_flags & DA_NEED_MAX) arrays_per_agg += 2; /* 2x: DA MAX slow at high cardinality */
            if (need_flags & DA_NEED_SUMSQ) arrays_per_agg += 1;
            uint64_t per_worker = total_slots * (arrays_per_agg * n_aggs + 1u) * 8u;
            if (per_worker > DA_PER_WORKER_MAX)
                da_fits = false;
        }

        if (da_fits) {
            /* Recompute need_flags (da_fits may have changed scope) */
            uint8_t need_flags = DA_NEED_COUNT;
            bool all_sum = true;
            for (uint8_t a = 0; a < n_aggs; a++) {
                uint16_t aop = ext->agg_ops[a];
                if (aop == OP_SUM || aop == OP_AVG || aop == OP_FIRST || aop == OP_LAST) need_flags |= DA_NEED_SUM;
                else if (aop == OP_STDDEV || aop == OP_STDDEV_POP || aop == OP_VAR || aop == OP_VAR_POP)
                    { need_flags |= DA_NEED_SUM; need_flags |= DA_NEED_SUMSQ; }
                else if (aop == OP_MIN) need_flags |= DA_NEED_MIN;
                else if (aop == OP_MAX) need_flags |= DA_NEED_MAX;
                if (aop != OP_SUM && aop != OP_AVG && aop != OP_COUNT)
                    all_sum = false;
            }

            /* Compute strides: stride[k] = product of ranges[k+1..n_keys-1]
             * Guard against overflow: if any product exceeds INT64_MAX,
             * fall through to HT path. */
            bool stride_overflow = false;
            for (uint8_t k = 0; k < n_keys; k++) {
                int64_t s = 1;
                for (uint8_t j = k + 1; j < n_keys; j++) {
                    if (da_key_range[j] != 0 && s > INT64_MAX / da_key_range[j]) {
                        stride_overflow = true; break;
                    }
                    s *= da_key_range[j];
                }
                if (stride_overflow) break;
                da_key_stride[k] = s;
            }
            if (stride_overflow) da_fits = false;

            uint32_t n_slots = (uint32_t)total_slots;
            size_t total = (size_t)n_slots * n_aggs;

            void* agg_ptrs[n_aggs];
            int8_t agg_types[n_aggs];
            uint32_t agg_f64_mask = 0;
            for (uint8_t a = 0; a < n_aggs; a++) {
                if (agg_vecs[a]) {
                    agg_ptrs[a]  = td_data(agg_vecs[a]);
                    agg_types[a] = agg_vecs[a]->type;
                    if (agg_vecs[a]->type == TD_F64)
                        agg_f64_mask |= (1u << a);
                } else {
                    agg_ptrs[a]  = NULL;
                    agg_types[a] = 0;
                }
            }

            td_pool_t* da_pool = td_pool_get();
            uint32_t da_n_workers = (da_pool && nrows >= TD_PARALLEL_THRESHOLD)
                                    ? td_pool_total_workers(da_pool) : 1;

            /* Check memory budget — need one accumulator set per worker.
             * Weight MIN/MAX at 2x in budget (same as eligibility check) to
             * keep MIN/MAX-heavy queries on the faster radix-HT path. */
            uint32_t arrays_per_agg = 0;
            if (need_flags & DA_NEED_SUM) arrays_per_agg += 1;
            if (need_flags & DA_NEED_MIN) arrays_per_agg += 2;
            if (need_flags & DA_NEED_MAX) arrays_per_agg += 2;
            if (need_flags & DA_NEED_SUMSQ) arrays_per_agg += 1;
            uint64_t per_worker_bytes = (uint64_t)n_slots * (arrays_per_agg * n_aggs + 1u) * 8u;
            if ((uint64_t)da_n_workers * per_worker_bytes > DA_MEM_BUDGET)
                da_n_workers = 1;

            td_t* accums_hdr;
            da_accum_t* accums = (da_accum_t*)scratch_calloc(&accums_hdr,
                da_n_workers * sizeof(da_accum_t));
            if (!accums) goto ht_path;

            bool alloc_ok = true;
            for (uint32_t w = 0; w < da_n_workers; w++) {
                if (need_flags & DA_NEED_SUM) {
                    accums[w].sum = (da_val_t*)scratch_calloc(&accums[w]._h_sum,
                        total * sizeof(da_val_t));
                    if (!accums[w].sum) { alloc_ok = false; break; }
                }
                if (need_flags & DA_NEED_SUMSQ) {
                    accums[w].sumsq_f64 = (double*)scratch_calloc(&accums[w]._h_sumsq,
                        total * sizeof(double));
                    if (!accums[w].sumsq_f64) { alloc_ok = false; break; }
                }
                if (need_flags & DA_NEED_MIN) {
                    accums[w].min_val = (da_val_t*)scratch_alloc(&accums[w]._h_min,
                        total * sizeof(da_val_t));
                    if (!accums[w].min_val) { alloc_ok = false; break; }
                    for (size_t i = 0; i < total; i++) {
                        uint8_t a = (uint8_t)(i % n_aggs);
                        if (agg_types[a] == TD_F64) accums[w].min_val[i].f = DBL_MAX;
                        else accums[w].min_val[i].i = INT64_MAX;
                    }
                }
                if (need_flags & DA_NEED_MAX) {
                    accums[w].max_val = (da_val_t*)scratch_alloc(&accums[w]._h_max,
                        total * sizeof(da_val_t));
                    if (!accums[w].max_val) { alloc_ok = false; break; }
                    for (size_t i = 0; i < total; i++) {
                        uint8_t a = (uint8_t)(i % n_aggs);
                        if (agg_types[a] == TD_F64) accums[w].max_val[i].f = -DBL_MAX;
                        else accums[w].max_val[i].i = INT64_MIN;
                    }
                }
                accums[w].count = (int64_t*)scratch_calloc(&accums[w]._h_count,
                    n_slots * sizeof(int64_t));
                if (!accums[w].count) { alloc_ok = false; break; }
            }
            if (!alloc_ok) {
                for (uint32_t w = 0; w < da_n_workers; w++)
                    da_accum_free(&accums[w]);
                scratch_free(accums_hdr);
                goto ht_path;
            }


            /* Pre-compute per-key element sizes for fast DA reads */
            uint8_t da_key_esz[n_keys];
            for (uint8_t k = 0; k < n_keys; k++)
                da_key_esz[k] = td_sym_elem_size(key_types[k], key_attrs[k]);

            da_ctx_t da_ctx = {
                .accums      = accums,
                .n_accums    = da_n_workers,
                .key_ptrs    = key_data,
                .key_types   = key_types,
                .key_attrs   = key_attrs,
                .key_esz     = da_key_esz,
                .key_mins    = da_key_min,
                .key_strides = da_key_stride,
                .n_keys      = n_keys,
                .agg_ptrs    = agg_ptrs,
                .agg_types   = agg_types,
                .agg_ops     = ext->agg_ops,
                .n_aggs      = n_aggs,
                .need_flags  = need_flags,
                .agg_f64_mask = agg_f64_mask,
                .all_sum     = all_sum,
                .n_slots     = n_slots,
                .mask        = mask,
                .sel_flags   = sel_flags,
            };

            if (da_n_workers > 1)
                td_pool_dispatch(da_pool, da_accum_fn, &da_ctx, nrows);
            else
                da_accum_fn(&da_ctx, 0, 0, nrows);

            /* Merge target is always accums[0] */
            da_accum_t* merged = &accums[0];

            /* Check if any agg is FIRST/LAST (needs ordered per-worker merge) */
            bool has_first_last = false;
            for (uint8_t a = 0; a < n_aggs; a++) {
                uint16_t aop = ext->agg_ops[a];
                if (aop == OP_FIRST || aop == OP_LAST) { has_first_last = true; break; }
            }

            /* Merge per-worker accumulators into accums[0].
             * FIRST/LAST require worker-order-dependent merge (sequential).
             * All other ops are commutative — dispatch over disjoint slot
             * ranges for parallel merge. */
            if (has_first_last) {
                for (uint32_t w = 1; w < da_n_workers; w++) {
                    da_accum_t* wa = &accums[w];
                    if (need_flags & DA_NEED_SUMSQ) {
                        for (size_t i = 0; i < total; i++)
                            merged->sumsq_f64[i] += wa->sumsq_f64[i];
                    }
                    if (need_flags & DA_NEED_SUM) {
                        for (uint32_t s = 0; s < n_slots; s++) {
                            size_t base = (size_t)s * n_aggs;
                            for (uint8_t a = 0; a < n_aggs; a++) {
                                size_t idx = base + a;
                                uint16_t aop = ext->agg_ops[a];
                                if (aop == OP_SUM || aop == OP_AVG || aop == OP_STDDEV || aop == OP_STDDEV_POP || aop == OP_VAR || aop == OP_VAR_POP) {
                                    if (agg_types[a] == TD_F64) merged->sum[idx].f += wa->sum[idx].f;
                                    else merged->sum[idx].i += wa->sum[idx].i;
                                } else if (aop == OP_FIRST) {
                                    if (merged->count[s] == 0 && wa->count[s] > 0)
                                        merged->sum[idx] = wa->sum[idx];
                                } else if (aop == OP_LAST) {
                                    if (wa->count[s] > 0)
                                        merged->sum[idx] = wa->sum[idx];
                                }
                            }
                        }
                    }
                    if (need_flags & DA_NEED_MIN) {
                        for (size_t i = 0; i < total; i++) {
                            uint8_t a = (uint8_t)(i % n_aggs);
                            if (agg_types[a] == TD_F64) {
                                if (wa->min_val[i].f < merged->min_val[i].f)
                                    merged->min_val[i].f = wa->min_val[i].f;
                            } else {
                                if (wa->min_val[i].i < merged->min_val[i].i)
                                    merged->min_val[i].i = wa->min_val[i].i;
                            }
                        }
                    }
                    if (need_flags & DA_NEED_MAX) {
                        for (size_t i = 0; i < total; i++) {
                            uint8_t a = (uint8_t)(i % n_aggs);
                            if (agg_types[a] == TD_F64) {
                                if (wa->max_val[i].f > merged->max_val[i].f)
                                    merged->max_val[i].f = wa->max_val[i].f;
                            } else {
                                if (wa->max_val[i].i > merged->max_val[i].i)
                                    merged->max_val[i].i = wa->max_val[i].i;
                            }
                        }
                    }
                    for (uint32_t s = 0; s < n_slots; s++)
                        merged->count[s] += wa->count[s];
                }
            } else if (da_n_workers > 1 && n_slots >= 1024 && da_pool) {
                /* Parallel merge: dispatch over disjoint slot ranges */
                da_merge_ctx_t merge_ctx = {
                    .accums        = accums,
                    .n_src_workers = da_n_workers,
                    .need_flags    = need_flags,
                    .n_aggs        = n_aggs,
                    .agg_types     = agg_types,
                    .agg_ops       = ext->agg_ops,
                };
                td_pool_dispatch(da_pool, da_merge_fn, &merge_ctx, (int64_t)n_slots);
            } else {
                /* Sequential merge for small slot counts */
                for (uint32_t w = 1; w < da_n_workers; w++) {
                    da_accum_t* wa = &accums[w];
                    if (need_flags & DA_NEED_SUMSQ) {
                        for (size_t i = 0; i < total; i++)
                            merged->sumsq_f64[i] += wa->sumsq_f64[i];
                    }
                    if (need_flags & DA_NEED_SUM) {
                        for (uint32_t s = 0; s < n_slots; s++) {
                            size_t base = (size_t)s * n_aggs;
                            for (uint8_t a = 0; a < n_aggs; a++) {
                                size_t idx = base + a;
                                uint16_t aop = ext->agg_ops[a];
                                if (aop == OP_FIRST) {
                                    if (merged->count[s] == 0 && wa->count[s] > 0)
                                        merged->sum[idx] = wa->sum[idx];
                                } else if (aop == OP_LAST) {
                                    if (wa->count[s] > 0)
                                        merged->sum[idx] = wa->sum[idx];
                                } else if (agg_types[a] == TD_F64)
                                    merged->sum[idx].f += wa->sum[idx].f;
                                else
                                    merged->sum[idx].i += wa->sum[idx].i;
                            }
                        }
                    }
                    if (need_flags & DA_NEED_MIN) {
                        for (size_t i = 0; i < total; i++) {
                            uint8_t a = (uint8_t)(i % n_aggs);
                            if (agg_types[a] == TD_F64) {
                                if (wa->min_val[i].f < merged->min_val[i].f)
                                    merged->min_val[i].f = wa->min_val[i].f;
                            } else {
                                if (wa->min_val[i].i < merged->min_val[i].i)
                                    merged->min_val[i].i = wa->min_val[i].i;
                            }
                        }
                    }
                    if (need_flags & DA_NEED_MAX) {
                        for (size_t i = 0; i < total; i++) {
                            uint8_t a = (uint8_t)(i % n_aggs);
                            if (agg_types[a] == TD_F64) {
                                if (wa->max_val[i].f > merged->max_val[i].f)
                                    merged->max_val[i].f = wa->max_val[i].f;
                            } else {
                                if (wa->max_val[i].i > merged->max_val[i].i)
                                    merged->max_val[i].i = wa->max_val[i].i;
                            }
                        }
                    }
                    for (uint32_t s = 0; s < n_slots; s++)
                        merged->count[s] += wa->count[s];
                }
            }



            for (uint32_t w = 1; w < da_n_workers; w++)
                da_accum_free(&accums[w]);

            da_val_t* da_sum      = merged->sum;      /* may be NULL if !DA_NEED_SUM */
            da_val_t* da_min_val  = merged->min_val;  /* may be NULL if !DA_NEED_MIN */
            da_val_t* da_max_val  = merged->max_val;  /* may be NULL if !DA_NEED_MAX */
            double*   da_sumsq   = merged->sumsq_f64; /* may be NULL if !DA_NEED_SUMSQ */
            int64_t*  da_count   = merged->count;

            uint32_t grp_count = 0;
            for (uint32_t s = 0; s < n_slots; s++)
                if (da_count[s] > 0) grp_count++;

            int64_t total_cols = n_keys + n_aggs;
            td_t* result = td_table_new(total_cols);
            if (!result || TD_IS_ERR(result)) {
                da_accum_free(&accums[0]); scratch_free(accums_hdr);
                return result ? result : TD_ERR_PTR(TD_ERR_OOM);
            }

            /* Key columns — decompose composite slot back to per-key values */
            for (uint8_t k = 0; k < n_keys; k++) {
                td_t* src_col = key_vecs[k];
                if (!src_col) continue;
                td_t* key_col = col_vec_new(src_col, (int64_t)grp_count);
                if (!key_col || TD_IS_ERR(key_col)) continue;
                key_col->len = (int64_t)grp_count;
                uint32_t gi = 0;
                for (uint32_t s = 0; s < n_slots; s++) {
                    if (da_count[s] == 0) continue;
                    int64_t offset = ((int64_t)s / da_key_stride[k]) % da_key_range[k];
                    int64_t key_val = da_key_min[k] + offset;
                    write_col_i64(td_data(key_col), gi, key_val, src_col->type, key_col->attrs);
                    gi++;
                }
                td_op_ext_t* key_ext = find_ext(g, ext->keys[k]->id);
                int64_t name_id = key_ext ? key_ext->sym : (int64_t)k;
                result = td_table_add_col(result, name_id, key_col);
                td_release(key_col);
            }

            /* Agg columns — compact sparse DA arrays into dense, then emit */
            size_t dense_total = (size_t)grp_count * n_aggs;
            td_t *_h_dsum = NULL, *_h_dmin = NULL, *_h_dmax = NULL;
            td_t *_h_dsq = NULL, *_h_dcnt = NULL;
            da_val_t* dense_sum     = da_sum     ? (da_val_t*)scratch_alloc(&_h_dsum, dense_total * sizeof(da_val_t)) : NULL;
            da_val_t* dense_min_val = da_min_val ? (da_val_t*)scratch_alloc(&_h_dmin, dense_total * sizeof(da_val_t)) : NULL;
            da_val_t* dense_max_val = da_max_val ? (da_val_t*)scratch_alloc(&_h_dmax, dense_total * sizeof(da_val_t)) : NULL;
            double*   dense_sumsq   = da_sumsq   ? (double*)scratch_alloc(&_h_dsq, dense_total * sizeof(double)) : NULL;
            int64_t*  dense_counts  = (int64_t*)scratch_alloc(&_h_dcnt, grp_count * sizeof(int64_t));

            uint32_t gi = 0;
            for (uint32_t s = 0; s < n_slots; s++) {
                if (da_count[s] == 0) continue;
                dense_counts[gi] = da_count[s];
                for (uint8_t a = 0; a < n_aggs; a++) {
                    size_t si = (size_t)s * n_aggs + a;
                    size_t di = (size_t)gi * n_aggs + a;
                    if (dense_sum)     dense_sum[di]     = da_sum[si];
                    if (dense_min_val) dense_min_val[di] = da_min_val[si];
                    if (dense_max_val) dense_max_val[di] = da_max_val[si];
                    if (dense_sumsq)   dense_sumsq[di]   = da_sumsq[si];
                }
                gi++;
            }

            emit_agg_columns(&result, g, ext, agg_vecs, grp_count, n_aggs,
                             (double*)dense_sum, (int64_t*)dense_sum,
                             (double*)dense_min_val, (double*)dense_max_val,
                             (int64_t*)dense_min_val, (int64_t*)dense_max_val,
                             dense_counts, agg_affine, dense_sumsq);

            scratch_free(_h_dsum); scratch_free(_h_dmin);
            scratch_free(_h_dmax);
            scratch_free(_h_dsq); scratch_free(_h_dcnt);

            da_accum_free(&accums[0]); scratch_free(accums_hdr);
            for (uint8_t a = 0; a < n_aggs; a++)
                if (agg_owned[a] && agg_vecs[a]) td_release(agg_vecs[a]);
            for (uint8_t k = 0; k < n_keys; k++)
                if (key_owned[k] && key_vecs[k]) td_release(key_vecs[k]);
            return result;
        }
    }

ht_path:;
    /* Compute which accumulator arrays the HT needs based on agg ops.
     * COUNT only reads group row's count field — no accumulator needed. */
    uint8_t ght_need = 0;
    for (uint8_t a = 0; a < n_aggs; a++) {
        uint16_t aop = ext->agg_ops[a];
        if (aop == OP_SUM || aop == OP_AVG || aop == OP_FIRST || aop == OP_LAST)
            ght_need |= GHT_NEED_SUM;
        if (aop == OP_STDDEV || aop == OP_STDDEV_POP || aop == OP_VAR || aop == OP_VAR_POP)
            { ght_need |= GHT_NEED_SUM; ght_need |= GHT_NEED_SUMSQ; }
        if (aop == OP_MIN) ght_need |= GHT_NEED_MIN;
        if (aop == OP_MAX) ght_need |= GHT_NEED_MAX;
    }

    /* Compute row-layout: keys + agg values inline */
    ght_layout_t ght_layout = ght_compute_layout(n_keys, n_aggs, agg_vecs, ght_need, ext->agg_ops);

    /* Right-sized hash table: start small, rehash on load > 0.5 */
    uint32_t ht_cap = 256;
    {
        uint64_t target = (uint64_t)nrows < 65536 ? (uint64_t)nrows : 65536;
        if (target < 256) target = 256;
        while (ht_cap < target) ht_cap *= 2;
    }

    /* Parallel path: radix-partitioned group-by */
    td_pool_t* pool = td_pool_get();
    uint32_t n_total = pool ? td_pool_total_workers(pool) : 1;

    group_ht_t single_ht;
    group_ht_t* final_ht = NULL;
    td_t* result = NULL;

    td_t* radix_bufs_hdr = NULL;
    radix_buf_t* radix_bufs = NULL;
    td_t* part_hts_hdr = NULL;
    group_ht_t*  part_hts   = NULL;

    if (pool && nrows >= TD_PARALLEL_THRESHOLD && n_total > 1) {
        size_t n_bufs = (size_t)n_total * RADIX_P;
        radix_bufs = (radix_buf_t*)scratch_calloc(&radix_bufs_hdr,
            n_bufs * sizeof(radix_buf_t));
        if (!radix_bufs) goto sequential_fallback;

        /* Pre-size each buffer: 1.5x expected, capped so total ≤ 2 GB.
         * Buffers grow on demand via radix_buf_push doubling. */
        uint32_t buf_init = (uint32_t)((uint64_t)nrows / (RADIX_P * n_total));
        if (buf_init < 64) buf_init = 64;
        buf_init = buf_init + buf_init / 2;  /* 1.5x headroom */
        uint16_t estride = ght_layout.entry_stride;
        {
            /* Cap: total pre-alloc ≤ 2 GB */
            size_t total_pre = (size_t)n_bufs * buf_init * estride;
            if (total_pre > (size_t)2 << 30) {
                buf_init = (uint32_t)(((size_t)2 << 30) / ((size_t)n_bufs * estride));
                if (buf_init < 64) buf_init = 64;
            }
        }
        for (size_t i = 0; i < n_bufs; i++) {
            radix_bufs[i].data = (char*)scratch_alloc(
                &radix_bufs[i]._hdr, (size_t)buf_init * estride);
            radix_bufs[i].count = 0;
            radix_bufs[i].cap = buf_init;
        }

        /* Phase 1: parallel hash + copy keys/agg values into fat entries */
        radix_phase1_ctx_t p1ctx = {
            .key_data  = key_data,
            .key_types = key_types,
            .key_attrs = key_attrs,
            .agg_vecs  = agg_vecs,
            .n_workers = n_total,
            .bufs      = radix_bufs,
            .layout    = ght_layout,
            .mask      = mask,
            .sel_flags = sel_flags,
        };
        td_pool_dispatch(pool, radix_phase1_fn, &p1ctx, nrows);
        CHECK_CANCEL_GOTO(pool, cleanup);

        /* Check for OOM during phase 1 radix buffer growth */
        {
            bool phase1_oom = false;
            for (size_t i = 0; i < n_bufs; i++) {
                if (radix_bufs[i].oom) { phase1_oom = true; break; }
            }
            if (phase1_oom) {
                for (size_t i = 0; i < n_bufs; i++) scratch_free(radix_bufs[i]._hdr);
                scratch_free(radix_bufs_hdr);
                radix_bufs = NULL;
                goto sequential_fallback;
            }
        }

        /* Phase 2: parallel per-partition aggregation (no column access) */
        part_hts = (group_ht_t*)scratch_calloc(&part_hts_hdr,
            RADIX_P * sizeof(group_ht_t));
        if (!part_hts) {
            for (size_t i = 0; i < n_bufs; i++) scratch_free(radix_bufs[i]._hdr);
            scratch_free(radix_bufs_hdr);
            radix_bufs = NULL;
            goto sequential_fallback;
        }

        radix_phase2_ctx_t p2ctx = {
            .key_types   = key_types,
            .n_keys      = n_keys,
            .n_workers   = n_total,
            .bufs        = radix_bufs,
            .part_hts    = part_hts,
            .layout      = ght_layout,
        };
        td_pool_dispatch_n(pool, radix_phase2_fn, &p2ctx, RADIX_P);
        CHECK_CANCEL_GOTO(pool, cleanup);

        /* Prefix offsets */
        uint32_t part_offsets[RADIX_P + 1];
        part_offsets[0] = 0;
        for (uint32_t p = 0; p < RADIX_P; p++)
            part_offsets[p + 1] = part_offsets[p] + part_hts[p].grp_count;
        uint32_t total_grps = part_offsets[RADIX_P];

        /* Build result directly from partition HTs */
        int64_t total_cols = n_keys + n_aggs;
        result = td_table_new(total_cols);
        if (!result || TD_IS_ERR(result)) goto cleanup;

        /* Pre-allocate key columns */
        td_t* key_cols[n_keys];
        char* key_dsts[n_keys];
        int8_t key_out_types[n_keys];
        uint8_t key_esizes[n_keys];
        for (uint8_t k = 0; k < n_keys; k++) {
            td_t* src_col = key_vecs[k];
            key_cols[k] = NULL;
            key_dsts[k] = NULL;
            key_out_types[k] = 0;
            key_esizes[k] = 0;
            if (!src_col) continue;
            uint8_t esz = td_sym_elem_size(src_col->type, src_col->attrs);
            td_t* new_col;
            if (src_col->type == TD_SYM)
                new_col = td_sym_vec_new(src_col->attrs & TD_SYM_W_MASK, (int64_t)total_grps);
            else
                new_col = td_vec_new(src_col->type, (int64_t)total_grps);
            if (!new_col || TD_IS_ERR(new_col)) continue;
            new_col->len = (int64_t)total_grps;
            key_cols[k] = new_col;
            key_dsts[k] = (char*)td_data(new_col);
            key_out_types[k] = src_col->type;
            key_esizes[k] = esz;
        }

        /* Pre-allocate agg result vectors */
        agg_out_t agg_outs[n_aggs];
        td_t* agg_cols[n_aggs];
        for (uint8_t a = 0; a < n_aggs; a++) {
            uint16_t agg_op = ext->agg_ops[a];
            td_t* agg_col = agg_vecs[a];
            bool is_f64 = agg_col && agg_col->type == TD_F64;
            int8_t out_type;
            switch (agg_op) {
                case OP_AVG:
                case OP_STDDEV: case OP_STDDEV_POP:
                case OP_VAR: case OP_VAR_POP:
                    out_type = TD_F64; break;
                case OP_COUNT: out_type = TD_I64; break;
                case OP_SUM: case OP_PROD:
                    out_type = is_f64 ? TD_F64 : TD_I64; break;
                default:
                    out_type = agg_col ? agg_col->type : TD_I64; break;
            }
            td_t* new_col = td_vec_new(out_type, (int64_t)total_grps);
            if (!new_col || TD_IS_ERR(new_col)) { agg_cols[a] = NULL; continue; }
            new_col->len = (int64_t)total_grps;
            agg_cols[a] = new_col;
            agg_outs[a] = (agg_out_t){
                .out_type = out_type, .src_f64 = is_f64,
                .agg_op = agg_op,
                .affine = agg_affine[a].enabled,
                .bias_f64 = agg_affine[a].bias_f64,
                .bias_i64 = agg_affine[a].bias_i64,
                .dst = td_data(new_col),
            };
        }

        /* Phase 3: parallel key gather + agg result building from inline rows */
        {
            radix_phase3_ctx_t p3ctx = {
                .part_hts     = part_hts,
                .part_offsets = part_offsets,
                .key_dsts     = key_dsts,
                .key_types    = key_out_types,
                .key_attrs    = key_attrs,
                .key_esizes   = key_esizes,
                .n_keys       = n_keys,
                .agg_outs     = agg_outs,
                .n_aggs       = n_aggs,
            };
            td_pool_dispatch_n(pool, radix_phase3_fn, &p3ctx, RADIX_P);
        }

        /* Add key columns to result */
        for (uint8_t k = 0; k < n_keys; k++) {
            if (!key_cols[k]) continue;
            td_op_ext_t* key_ext = find_ext(g, ext->keys[k]->id);
            int64_t name_id = key_ext ? key_ext->sym : k;
            result = td_table_add_col(result, name_id, key_cols[k]);
            td_release(key_cols[k]);
        }

        /* Add agg columns to result */
        for (uint8_t a = 0; a < n_aggs; a++) {
            if (!agg_cols[a]) continue;
            uint16_t agg_op = ext->agg_ops[a];
            td_op_ext_t* agg_ext = find_ext(g, ext->agg_ins[a]->id);
            int64_t name_id;
            if (agg_ext && agg_ext->base.opcode == OP_SCAN) {
                td_t* name_atom = td_sym_str(agg_ext->sym);
                const char* base = name_atom ? td_str_ptr(name_atom) : NULL;
                size_t blen = base ? td_str_len(name_atom) : 0;
                const char* sfx = "";
                size_t slen = 0;
                switch (agg_op) {
                    case OP_SUM:   sfx = "_sum";   slen = 4; break;
                    case OP_COUNT: sfx = "_count"; slen = 6; break;
                    case OP_AVG:   sfx = "_mean";  slen = 5; break;
                    case OP_MIN:   sfx = "_min";   slen = 4; break;
                    case OP_MAX:   sfx = "_max";   slen = 4; break;
                    case OP_FIRST: sfx = "_first"; slen = 6; break;
                    case OP_LAST:  sfx = "_last";  slen = 5; break;
                    case OP_STDDEV:     sfx = "_stddev";     slen = 7; break;
                    case OP_STDDEV_POP: sfx = "_stddev_pop"; slen = 11; break;
                    case OP_VAR:        sfx = "_var";        slen = 4; break;
                    case OP_VAR_POP:    sfx = "_var_pop";    slen = 8; break;
                }
                char buf[256];
                td_t* name_dyn_hdr = NULL;
                char* nbp = buf;
                size_t nbc = sizeof(buf);
                if (base && blen + slen >= sizeof(buf)) {
                    nbp = (char*)scratch_alloc(&name_dyn_hdr, blen + slen + 1);
                    if (nbp) nbc = blen + slen + 1;
                    else { nbp = buf; nbc = sizeof(buf); }
                }
                if (base && blen + slen < nbc) {
                    memcpy(nbp, base, blen);
                    memcpy(nbp + blen, sfx, slen);
                    name_id = td_sym_intern(nbp, blen + slen);
                } else {
                    name_id = agg_ext->sym;
                }
                scratch_free(name_dyn_hdr);
            } else {
                name_id = (int64_t)(n_keys + a);
            }
            result = td_table_add_col(result, name_id, agg_cols[a]);
            td_release(agg_cols[a]);
        }

        goto cleanup;
    }

sequential_fallback:;
    /* Sequential path using row-layout HT */
    if (!group_ht_init(&single_ht, ht_cap, &ght_layout))
        return TD_ERR_PTR(TD_ERR_OOM);
    group_rows_range(&single_ht, key_data, key_types, key_attrs, agg_vecs, 0, nrows);

    final_ht = &single_ht;

    /* Build result from sequential HT (inline row layout) */
    {
    uint32_t grp_count = final_ht->grp_count;
    const ght_layout_t* ly = &final_ht->layout;
    int64_t total_cols = n_keys + n_aggs;
    result = td_table_new(total_cols);
    if (!result || TD_IS_ERR(result)) goto cleanup;

    /* Key columns: read from inline group rows, narrow to original type */
    for (uint8_t k = 0; k < n_keys; k++) {
        td_t* src_col = key_vecs[k];
        if (!src_col) continue;
        uint8_t esz = col_esz(src_col);
        int8_t kt = src_col->type;

        td_t* new_col = col_vec_new(src_col, (int64_t)grp_count);
        if (!new_col || TD_IS_ERR(new_col)) continue;
        new_col->len = (int64_t)grp_count;

        for (uint32_t gi = 0; gi < grp_count; gi++) {
            const char* row = final_ht->rows + (size_t)gi * ly->row_stride;
            int64_t kv = ((const int64_t*)(row + 8))[k];
            if (kt == TD_F64) {
                char* dst = (char*)td_data(new_col) + (size_t)gi * esz;
                memcpy(dst, &kv, 8);
            } else
                write_col_i64(td_data(new_col), gi, kv, kt, new_col->attrs);
        }

        td_op_ext_t* key_ext = find_ext(g, ext->keys[k]->id);
        int64_t name_id = key_ext ? key_ext->sym : k;
        result = td_table_add_col(result, name_id, new_col);
        td_release(new_col);
    }

    /* Agg columns from inline accumulators */
    for (uint8_t a = 0; a < n_aggs; a++) {
        uint16_t agg_op = ext->agg_ops[a];
        td_t* agg_col = agg_vecs[a];
        bool is_f64 = agg_col && agg_col->type == TD_F64;
        int8_t out_type;
        switch (agg_op) {
            case OP_AVG:
            case OP_STDDEV: case OP_STDDEV_POP:
            case OP_VAR: case OP_VAR_POP:
                out_type = TD_F64; break;
            case OP_COUNT: out_type = TD_I64; break;
            case OP_SUM: case OP_PROD:
                out_type = is_f64 ? TD_F64 : TD_I64; break;
            default:
                out_type = agg_col ? agg_col->type : TD_I64; break;
        }
        td_t* new_col = td_vec_new(out_type, (int64_t)grp_count);
        if (!new_col || TD_IS_ERR(new_col)) continue;
        new_col->len = (int64_t)grp_count;

        int8_t s = ly->agg_val_slot[a]; /* unified accum slot */
        for (uint32_t gi = 0; gi < grp_count; gi++) {
            const char* row = final_ht->rows + (size_t)gi * ly->row_stride;
            int64_t cnt = *(const int64_t*)(const void*)row;
            if (out_type == TD_F64) {
                double v;
                switch (agg_op) {
                    case OP_SUM:
                        v = is_f64 ? ROW_RD_F64(row, ly->off_sum, s)
                                   : (double)ROW_RD_I64(row, ly->off_sum, s);
                        if (agg_affine[a].enabled) v += agg_affine[a].bias_f64 * cnt;
                        break;
                    case OP_AVG:
                        v = is_f64 ? ROW_RD_F64(row, ly->off_sum, s) / cnt
                                   : (double)ROW_RD_I64(row, ly->off_sum, s) / cnt;
                        if (agg_affine[a].enabled) v += agg_affine[a].bias_f64;
                        break;
                    case OP_MIN:
                        v = is_f64 ? ROW_RD_F64(row, ly->off_min, s)
                                   : (double)ROW_RD_I64(row, ly->off_min, s);
                        break;
                    case OP_MAX:
                        v = is_f64 ? ROW_RD_F64(row, ly->off_max, s)
                                   : (double)ROW_RD_I64(row, ly->off_max, s);
                        break;
                    case OP_FIRST: case OP_LAST:
                        v = is_f64 ? ROW_RD_F64(row, ly->off_sum, s)
                                   : (double)ROW_RD_I64(row, ly->off_sum, s);
                        break;
                    case OP_VAR: case OP_VAR_POP:
                    case OP_STDDEV: case OP_STDDEV_POP: {
                        double sum_val = is_f64 ? ROW_RD_F64(row, ly->off_sum, s)
                                                : (double)ROW_RD_I64(row, ly->off_sum, s);
                        double sq_val = ly->off_sumsq ? ROW_RD_F64(row, ly->off_sumsq, s) : 0.0;
                        double mean = cnt > 0 ? sum_val / cnt : 0.0;
                        double var_pop = cnt > 0 ? sq_val / cnt - mean * mean : 0.0;
                        if (var_pop < 0) var_pop = 0;
                        if (agg_op == OP_VAR_POP) v = cnt > 0 ? var_pop : NAN;
                        else if (agg_op == OP_VAR) v = cnt > 1 ? var_pop * cnt / (cnt - 1) : NAN;
                        else if (agg_op == OP_STDDEV_POP) v = cnt > 0 ? sqrt(var_pop) : NAN;
                        else v = cnt > 1 ? sqrt(var_pop * cnt / (cnt - 1)) : NAN;
                        break;
                    }
                    default: v = 0.0; break;
                }
                ((double*)td_data(new_col))[gi] = v;
            } else {
                int64_t v;
                switch (agg_op) {
                    case OP_SUM:
                        v = ROW_RD_I64(row, ly->off_sum, s);
                        if (agg_affine[a].enabled) v += agg_affine[a].bias_i64 * cnt;
                        break;
                    case OP_COUNT: v = cnt; break;
                    case OP_MIN:   v = ROW_RD_I64(row, ly->off_min, s); break;
                    case OP_MAX:   v = ROW_RD_I64(row, ly->off_max, s); break;
                    case OP_FIRST: case OP_LAST: v = ROW_RD_I64(row, ly->off_sum, s); break;
                    default:       v = 0; break;
                }
                ((int64_t*)td_data(new_col))[gi] = v;
            }
        }

        /* Generate unique column name */
        td_op_ext_t* agg_ext = find_ext(g, ext->agg_ins[a]->id);
        int64_t name_id;
        if (agg_ext && agg_ext->base.opcode == OP_SCAN) {
            td_t* name_atom = td_sym_str(agg_ext->sym);
            const char* base = name_atom ? td_str_ptr(name_atom) : NULL;
            size_t blen = base ? td_str_len(name_atom) : 0;
            const char* sfx = "";
            size_t slen = 0;
            switch (agg_op) {
                case OP_SUM:   sfx = "_sum";   slen = 4; break;
                case OP_COUNT: sfx = "_count"; slen = 6; break;
                case OP_AVG:   sfx = "_mean";  slen = 5; break;
                case OP_MIN:   sfx = "_min";   slen = 4; break;
                case OP_MAX:   sfx = "_max";   slen = 4; break;
                case OP_FIRST: sfx = "_first"; slen = 6; break;
                case OP_LAST:  sfx = "_last";  slen = 5; break;
                case OP_STDDEV:     sfx = "_stddev";     slen = 7; break;
                case OP_STDDEV_POP: sfx = "_stddev_pop"; slen = 11; break;
                case OP_VAR:        sfx = "_var";        slen = 4; break;
                case OP_VAR_POP:    sfx = "_var_pop";    slen = 8; break;
            }
            char buf[256];
            if (base && blen + slen < sizeof(buf)) {
                memcpy(buf, base, blen);
                memcpy(buf + blen, sfx, slen);
                name_id = td_sym_intern(buf, blen + slen);
            } else {
                name_id = agg_ext->sym;
            }
        } else {
            /* Expression agg input — synthetic name like "_e0_sum" */
            char nbuf[32];
            int np = 0;
            nbuf[np++] = '_'; nbuf[np++] = 'e';
            /* Multi-digit agg index */
            { uint8_t v = a; char dig[3]; int nd = 0;
              do { dig[nd++] = (char)('0' + v % 10); v /= 10; } while (v);
              while (nd--) nbuf[np++] = dig[nd]; }
            const char* nsfx = "";
            size_t nslen = 0;
            switch (agg_op) {
                case OP_SUM:   nsfx = "_sum";   nslen = 4; break;
                case OP_COUNT: nsfx = "_count"; nslen = 6; break;
                case OP_AVG:   nsfx = "_mean";  nslen = 5; break;
                case OP_MIN:   nsfx = "_min";   nslen = 4; break;
                case OP_MAX:   nsfx = "_max";   nslen = 4; break;
                case OP_FIRST: nsfx = "_first"; nslen = 6; break;
                case OP_LAST:  nsfx = "_last";  nslen = 5; break;
                case OP_STDDEV:     nsfx = "_stddev";     nslen = 7; break;
                case OP_STDDEV_POP: nsfx = "_stddev_pop"; nslen = 11; break;
                case OP_VAR:        nsfx = "_var";        nslen = 4; break;
                case OP_VAR_POP:    nsfx = "_var_pop";    nslen = 8; break;
            }
            memcpy(nbuf + np, nsfx, nslen);
            name_id = td_sym_intern(nbuf, (size_t)np + nslen);
        }
        result = td_table_add_col(result, name_id, new_col);
        td_release(new_col);
    }
    }

cleanup:
    if (final_ht == &single_ht) {
        group_ht_free(&single_ht);
    }
    if (radix_bufs) {
        size_t n_bufs = (size_t)n_total * RADIX_P;
        for (size_t i = 0; i < n_bufs; i++) scratch_free(radix_bufs[i]._hdr);
        scratch_free(radix_bufs_hdr);
    }
    if (part_hts) {
        for (uint32_t p = 0; p < RADIX_P; p++) {
            if (part_hts[p].rows) group_ht_free(&part_hts[p]);
        }
        scratch_free(part_hts_hdr);
    }
    for (uint8_t a = 0; a < n_aggs; a++)
        if (agg_owned[a] && agg_vecs[a]) td_release(agg_vecs[a]);
    for (uint8_t k = 0; k < n_keys; k++)
        if (key_owned[k] && key_vecs[k]) td_release(key_vecs[k]);

    return result;
}

/* --------------------------------------------------------------------------
 * exec_group_per_partition — per-partition GROUP BY with merge
 *
 * Runs exec_group on each partition independently (zero-copy mmap segments),
 * then merges the small partial results via a second exec_group pass.
 *
 * Merge ops: SUM→SUM, COUNT→SUM, MIN→MIN, MAX→MAX, FIRST→FIRST, LAST→LAST.
 * AVG: decomposed into SUM+COUNT per partition, merged, then divided.
 * STDDEV/VAR: decomposed into SUM(x)+SUM(x²)+COUNT(x) per partition,
 *   merged with SUM, then final variance/stddev computed from merged totals.
 *
 * Returns NULL if any step fails (caller falls through to concat path).
 * -------------------------------------------------------------------------- */
static td_t* __attribute__((noinline))
exec_group_per_partition(td_t* parted_tbl, td_op_ext_t* ext,
                         int32_t n_parts, const int64_t* key_syms,
                         const int64_t* agg_syms, int has_avg,
                         int has_stddev, int64_t group_limit) {

    uint8_t n_keys = ext->n_keys;
    uint8_t n_aggs = ext->n_aggs;

    /* Guard: fixed-size arrays below cap at 24 agg ops.
     * Each AVG adds 1 extra (COUNT), each STDDEV/VAR adds 2 (SUM_SQ + COUNT).
     * n_aggs + n_avg + 2*n_std must stay within 24. */
    if (n_aggs > 8 || n_keys > 8) return NULL;

    /* Identify MAPCOMMON vs PARTED keys.  MAPCOMMON keys are constant
     * within a partition, so they are excluded from per-partition GROUP BY
     * and reconstructed after concat. */
    uint8_t  n_mc_keys = 0;
    int64_t  mc_sym_ids[8];
    uint8_t  n_part_keys = 0;
    int64_t  pk_syms[8];       /* non-MAPCOMMON key sym IDs */

    for (uint8_t k = 0; k < n_keys; k++) {
        td_t* pcol = td_table_get_col(parted_tbl, key_syms[k]);
        if (pcol && pcol->type == TD_MAPCOMMON) {
            mc_sym_ids[n_mc_keys++] = key_syms[k];
        } else {
            pk_syms[n_part_keys++] = key_syms[k];
        }
    }

    /* LIMIT pushdown: when all GROUP BY keys are MAPCOMMON (n_part_keys==0),
     * each partition produces exactly 1 group.  Limit the partition loop. */
    if (group_limit > 0 && n_part_keys == 0 && group_limit < n_parts)
        n_parts = (int32_t)group_limit;

    /* Decomposition: AVG(x) → SUM(x) + COUNT(x).
     * STDDEV/VAR(x) → SUM(x) + SUM(x²) + COUNT(x).
     * Build per-partition agg_ops with decomposed ops, then merge ops. */
    uint16_t part_ops[24];   /* per-partition agg ops */
    uint16_t merge_ops[24];  /* merge agg ops */
    uint8_t  avg_idx[8];     /* which original agg slots are AVG */
    uint8_t  std_idx[8];     /* which original agg slots are STDDEV/VAR */
    uint16_t std_orig_op[8]; /* original op for each std slot */
    uint8_t  n_avg = 0;
    uint8_t  n_std = 0;
    uint8_t  part_n_aggs = n_aggs;
    /* stddev_needs_sq[a]: index into part_ops for the SUM(x²) slot */
    uint8_t  std_sq_slot[8];
    uint8_t  std_cnt_slot[8];

    for (uint8_t a = 0; a < n_aggs; a++) {
        uint16_t aop = ext->agg_ops[a];
        if (aop == OP_AVG) {
            part_ops[a] = OP_SUM;     /* partition: compute SUM */
            avg_idx[n_avg++] = a;
        } else if (aop == OP_STDDEV || aop == OP_STDDEV_POP ||
                   aop == OP_VAR || aop == OP_VAR_POP) {
            part_ops[a] = OP_SUM;     /* partition: compute SUM(x) */
            std_orig_op[n_std] = aop;
            std_idx[n_std++] = a;
        } else {
            part_ops[a] = aop;
        }
    }
    /* Guard: total decomposed slots must fit */
    if (n_aggs + n_avg + 2 * n_std > 24) return NULL;

    /* Append SUM(x²) for each STDDEV/VAR slot */
    for (uint8_t i = 0; i < n_std; i++) {
        std_sq_slot[i] = part_n_aggs;
        part_ops[part_n_aggs++] = OP_SUM;  /* SUM(x²) */
    }
    /* Append COUNT for each AVG column */
    for (uint8_t i = 0; i < n_avg; i++)
        part_ops[part_n_aggs++] = OP_COUNT;
    /* Append COUNT for each STDDEV/VAR column */
    for (uint8_t i = 0; i < n_std; i++) {
        std_cnt_slot[i] = part_n_aggs;
        part_ops[part_n_aggs++] = OP_COUNT;
    }

    /* Merge ops: SUM→SUM, COUNT→SUM, MIN→MIN, MAX→MAX,
     * FIRST→FIRST, LAST→LAST, all appended slots → SUM */
    for (uint8_t a = 0; a < part_n_aggs; a++) {
        merge_ops[a] = part_ops[a];
        if (merge_ops[a] == OP_COUNT) merge_ops[a] = OP_SUM;
    }

    /* Agg input syms for the decomposed ops.
     * AVG's COUNT uses same input column as the AVG itself.
     * STDDEV's SUM(x²) and COUNT use same input column as the STDDEV. */
    int64_t part_agg_syms[24];
    /* Flag: slot needs x*x graph node (for SUM(x²)) */
    int part_needs_sq[24];
    memset(part_needs_sq, 0, sizeof(part_needs_sq));

    for (uint8_t a = 0; a < n_aggs; a++)
        part_agg_syms[a] = agg_syms[a];
    /* SUM(x²) slots for STDDEV/VAR */
    for (uint8_t i = 0; i < n_std; i++) {
        part_agg_syms[std_sq_slot[i]] = agg_syms[std_idx[i]];
        part_needs_sq[std_sq_slot[i]] = 1;
    }
    /* COUNT slots for AVG */
    for (uint8_t i = 0; i < n_avg; i++)
        part_agg_syms[n_aggs + n_std + i] = agg_syms[avg_idx[i]];
    /* COUNT slots for STDDEV/VAR */
    for (uint8_t i = 0; i < n_std; i++)
        part_agg_syms[std_cnt_slot[i]] = agg_syms[std_idx[i]];

    /* ---- Batched incremental merge ----
     * Process partitions in batches of MERGE_BATCH.  After each batch:
     *   Phase 1: exec_group each partition in batch → batch_partials[]
     *   Phase 2: concat (running + batch_partials + MAPCOMMON) → merge_tbl
     *   Phase 3: merge GROUP BY → new running
     * Bounds peak memory to O(MERGE_BATCH × groups_per_partition). */
#define MERGE_BATCH 8

    /* Capture agg column name IDs from first partition result */
    int64_t agg_name_ids[24];
    int agg_names_captured = 0;

    td_t* running = NULL;
    td_t* merge_tbl = NULL;      /* last merge table (for column name fixup) */

    for (int32_t batch_start = 0; batch_start < n_parts;
         batch_start += MERGE_BATCH) {

        int32_t batch_end = batch_start + MERGE_BATCH;
        if (batch_end > n_parts) batch_end = n_parts;
        int32_t batch_n = batch_end - batch_start;

        /* Phase 1: exec_group each partition in this batch */
        td_t* bp[MERGE_BATCH];
        memset(bp, 0, sizeof(bp));

        for (int32_t bi = 0; bi < batch_n; bi++) {
            int32_t p = batch_start + bi;

            /* Collect unique agg input sym IDs (avoid duplicate columns) */
            int64_t unique_agg[24];
            int n_unique_agg = 0;
            for (uint8_t a = 0; a < part_n_aggs; a++) {
                int dup = 0;
                for (int j = 0; j < n_unique_agg; j++)
                    if (unique_agg[j] == part_agg_syms[a]) { dup = 1; break; }
                if (!dup) {
                    for (uint8_t k = 0; k < n_keys; k++)
                        if (key_syms[k] == part_agg_syms[a]) { dup = 1; break; }
                    if (!dup) unique_agg[n_unique_agg++] = part_agg_syms[a];
                }
            }

            td_t* sub = td_table_new((int64_t)(n_part_keys + n_unique_agg));
            if (!sub || TD_IS_ERR(sub)) goto batch_fail;

            for (uint8_t k = 0; k < n_part_keys; k++) {
                td_t* pcol = td_table_get_col(parted_tbl, pk_syms[k]);
                if (!pcol || !TD_IS_PARTED(pcol->type)) {
                    td_release(sub); goto batch_fail;
                }
                td_t* seg = ((td_t**)td_data(pcol))[p];
                if (!seg) { td_release(sub); goto batch_fail; }
                td_retain(seg);
                sub = td_table_add_col(sub, pk_syms[k], seg);
                td_release(seg);
            }
            for (int j = 0; j < n_unique_agg; j++) {
                td_t* pcol = td_table_get_col(parted_tbl, unique_agg[j]);
                if (!pcol || !TD_IS_PARTED(pcol->type)) {
                    td_release(sub); goto batch_fail;
                }
                td_t* seg = ((td_t**)td_data(pcol))[p];
                if (!seg) { td_release(sub); goto batch_fail; }
                td_retain(seg);
                sub = td_table_add_col(sub, unique_agg[j], seg);
                td_release(seg);
            }

            td_graph_t* pg = td_graph_new(sub);
            if (!pg) { td_release(sub); goto batch_fail; }

            td_op_t* pkeys[8];
            for (uint8_t k = 0; k < n_part_keys; k++) {
                td_t* sym_atom = td_sym_str(pk_syms[k]);
                pkeys[k] = td_scan(pg, td_str_ptr(sym_atom));
            }
            td_op_t* pagg_ins[24];
            for (uint8_t a = 0; a < part_n_aggs; a++) {
                td_t* sym_atom = td_sym_str(part_agg_syms[a]);
                pagg_ins[a] = td_scan(pg, td_str_ptr(sym_atom));
            }
            for (uint8_t j = 0; j < n_std; j++) {
                uint8_t sq = std_sq_slot[j];
                td_op_t* x = pagg_ins[sq];
                pagg_ins[sq] = td_mul(pg, x, x);
            }

            td_op_t* proot = td_group(pg, pkeys, n_part_keys,
                                       part_ops, pagg_ins, part_n_aggs);
            proot = td_optimize(pg, proot);
            bp[bi] = td_execute(pg, proot);
            td_graph_free(pg);
            td_release(sub);

            if (!bp[bi] || TD_IS_ERR(bp[bi])) goto batch_fail;

            /* Capture agg column name IDs once (all partials share names) */
            if (!agg_names_captured) {
                for (uint8_t a = 0; a < part_n_aggs; a++)
                    agg_name_ids[a] = td_table_col_name(
                        bp[bi], (int64_t)n_part_keys + a);
                agg_names_captured = 1;
            }
        }

        /* Phase 2: concat (running + batch_partials + MAPCOMMON) */
        int64_t mrows = running ? td_table_nrows(running) : 0;
        for (int32_t i = 0; i < batch_n; i++)
            mrows += td_table_nrows(bp[i]);

        if (merge_tbl) { td_release(merge_tbl); merge_tbl = NULL; }
        merge_tbl = td_table_new((int64_t)(n_keys + part_n_aggs));
        if (!merge_tbl || TD_IS_ERR(merge_tbl)) {
            merge_tbl = NULL; goto batch_fail;
        }

        /* Key columns */
        for (uint8_t k = 0; k < n_keys; k++) {
            int is_mc = 0;
            for (uint8_t m = 0; m < n_mc_keys; m++)
                if (mc_sym_ids[m] == key_syms[k]) { is_mc = 1; break; }

            /* Type reference for column allocation */
            td_t* tref = NULL;
            if (running) {
                tref = td_table_get_col(running, key_syms[k]);
            } else if (is_mc) {
                td_t* mc_col = td_table_get_col(parted_tbl, key_syms[k]);
                tref = ((td_t**)td_data(mc_col))[0];
            } else {
                tref = td_table_get_col(bp[0], key_syms[k]);
            }
            if (!tref) goto batch_fail;

            size_t esz = (size_t)col_esz(tref);
            td_t* flat = col_vec_new(tref, mrows);
            if (!flat || TD_IS_ERR(flat)) goto batch_fail;
            flat->len = mrows;
            char* out = (char*)td_data(flat);
            int64_t off = 0;

            /* Copy from running result */
            if (running) {
                td_t* rc = td_table_get_col(running, key_syms[k]);
                if (rc && rc->len > 0) {
                    memcpy(out, td_data(rc), (size_t)rc->len * esz);
                    off = rc->len;
                }
            }

            /* Copy from batch partials */
            for (int32_t i = 0; i < batch_n; i++) {
                int64_t pnrows = td_table_nrows(bp[i]);
                if (is_mc) {
                    /* MAPCOMMON: replicate this partition's key value */
                    int32_t p = batch_start + i;
                    td_t* mc_col = td_table_get_col(parted_tbl, key_syms[k]);
                    td_t* mc_kv = ((td_t**)td_data(mc_col))[0];
                    const char* kdata = (const char*)td_data(mc_kv);
                    for (int64_t r = 0; r < pnrows; r++)
                        memcpy(out + (size_t)(off + r) * esz,
                               kdata + (size_t)p * esz, esz);
                    off += pnrows;
                } else {
                    td_t* pc = td_table_get_col(bp[i], key_syms[k]);
                    if (pc && pc->len > 0) {
                        memcpy(out + (size_t)off * esz,
                               td_data(pc), (size_t)pc->len * esz);
                        off += pc->len;
                    }
                }
            }

            merge_tbl = td_table_add_col(merge_tbl, key_syms[k], flat);
            td_release(flat);
        }

        /* Agg columns */
        for (uint8_t a = 0; a < part_n_aggs; a++) {
            td_t* tref = running
                ? td_table_get_col_idx(running, (int64_t)n_keys + a)
                : td_table_get_col_idx(bp[0], (int64_t)n_part_keys + a);
            if (!tref) goto batch_fail;

            size_t esz = (size_t)col_esz(tref);
            td_t* flat = col_vec_new(tref, mrows);
            if (!flat || TD_IS_ERR(flat)) goto batch_fail;
            flat->len = mrows;
            char* out = (char*)td_data(flat);
            int64_t off = 0;

            if (running) {
                td_t* rc = td_table_get_col_idx(running, (int64_t)n_keys + a);
                if (rc && rc->len > 0) {
                    memcpy(out, td_data(rc), (size_t)rc->len * esz);
                    off = rc->len;
                }
            }

            for (int32_t i = 0; i < batch_n; i++) {
                td_t* pc = td_table_get_col_idx(bp[i],
                                                 (int64_t)n_part_keys + a);
                if (pc && pc->len > 0) {
                    memcpy(out + (size_t)off * esz,
                           td_data(pc), (size_t)pc->len * esz);
                    off += pc->len;
                }
            }

            merge_tbl = td_table_add_col(merge_tbl, agg_name_ids[a], flat);
            td_release(flat);
        }

        /* Free batch partials */
        for (int32_t i = 0; i < batch_n; i++) {
            td_release(bp[i]);
            bp[i] = NULL;
        }

        /* Phase 3: merge GROUP BY */
        td_graph_t* mg = td_graph_new(merge_tbl);
        if (!mg) goto batch_fail;

        td_op_t* mkeys[8];
        for (uint8_t k = 0; k < n_keys; k++) {
            td_t* sym_atom = td_sym_str(key_syms[k]);
            mkeys[k] = td_scan(mg, td_str_ptr(sym_atom));
        }

        td_op_t* magg_ins[24];
        for (uint8_t a = 0; a < part_n_aggs; a++) {
            td_t* agg_name = td_sym_str(agg_name_ids[a]);
            magg_ins[a] = td_scan(mg, td_str_ptr(agg_name));
        }

        td_op_t* mroot = td_group(mg, mkeys, n_keys,
                                   merge_ops, magg_ins, part_n_aggs);
        mroot = td_optimize(mg, mroot);
        td_t* new_running = td_execute(mg, mroot);
        td_graph_free(mg);

        if (running) td_release(running);
        running = new_running;

        if (!running || TD_IS_ERR(running)) {
            td_release(merge_tbl);
            return NULL;
        }

        /* Rename running's agg columns back to the original partial names.
         * Without this, each merge adds an extra suffix (e.g. v1_sum → v1_sum_sum). */
        for (uint8_t a = 0; a < part_n_aggs; a++)
            td_table_set_col_name(running, (int64_t)n_keys + a, agg_name_ids[a]);

        continue;

batch_fail:
        for (int32_t i = 0; i < batch_n; i++)
            if (bp[i]) td_release(bp[i]);
        if (running) td_release(running);
        if (merge_tbl) td_release(merge_tbl);
        return NULL;
    }

    td_t* result = running;

    if (!result || TD_IS_ERR(result)) {
        if (merge_tbl) td_release(merge_tbl);
        return NULL;
    }

    int64_t rncols = td_table_ncols(result);

    /* AVG/STDDEV post-processing: build trimmed table (n_keys + n_aggs cols),
     * computing final AVG = SUM/COUNT and STDDEV/VAR from SUM, SUM_SQ, COUNT. */
    if (has_avg || has_stddev) {
        td_t* trimmed = td_table_new((int64_t)(n_keys + n_aggs));
        if (!trimmed || TD_IS_ERR(trimmed)) {
            td_release(result);
            if (merge_tbl) td_release(merge_tbl);
            return NULL;
        }

        for (int64_t c = 0; c < (int64_t)(n_keys + n_aggs) && c < rncols; c++) {
            int64_t nm = td_table_col_name(result, c);

            /* Check if this agg column is an AVG or STDDEV/VAR slot */
            int is_avg_slot = 0, is_std_slot = 0;
            uint8_t avg_i = 0, std_i = 0;
            if (c >= n_keys) {
                uint8_t a = (uint8_t)(c - n_keys);
                for (uint8_t j = 0; j < n_avg; j++) {
                    if (avg_idx[j] == a) { is_avg_slot = 1; avg_i = j; break; }
                }
                for (uint8_t j = 0; j < n_std; j++) {
                    if (std_idx[j] == a) { is_std_slot = 1; std_i = j; break; }
                }
            }

            if (is_avg_slot) {
                /* AVG = SUM(x) / COUNT(x) */
                int64_t sum_ci = c;
                /* AVG COUNT slots: after n_aggs + n_std SUM_SQ slots */
                int64_t cnt_ci = (int64_t)n_keys + n_aggs + n_std + avg_i;
                td_t* sum_col = td_table_get_col_idx(result, sum_ci);
                td_t* cnt_col = (cnt_ci < rncols) ? td_table_get_col_idx(result, cnt_ci) : NULL;
                if (!sum_col || !cnt_col) {
                    if (sum_col) {
                        td_retain(sum_col);
                        trimmed = td_table_add_col(trimmed, nm, sum_col);
                        td_release(sum_col);
                    }
                    continue;
                }

                int64_t nrows = sum_col->len;
                td_t* avg_col = td_vec_new(TD_F64, nrows);
                if (!avg_col || TD_IS_ERR(avg_col)) {
                    td_release(trimmed); td_release(result);
                    if (merge_tbl) td_release(merge_tbl);
                    return NULL;
                }
                avg_col->len = nrows;

                double* out = (double*)td_data(avg_col);
                if (sum_col->type == TD_F64) {
                    const double* sv = (const double*)td_data(sum_col);
                    const int64_t* cv = (const int64_t*)td_data(cnt_col);
                    for (int64_t r = 0; r < nrows; r++)
                        out[r] = cv[r] > 0 ? sv[r] / (double)cv[r] : 0.0;
                } else {
                    const int64_t* sv = (const int64_t*)td_data(sum_col);
                    const int64_t* cv = (const int64_t*)td_data(cnt_col);
                    for (int64_t r = 0; r < nrows; r++)
                        out[r] = cv[r] > 0 ? (double)sv[r] / (double)cv[r] : 0.0;
                }
                trimmed = td_table_add_col(trimmed, nm, avg_col);
                td_release(avg_col);
            } else if (is_std_slot) {
                /* STDDEV/VAR from merged SUM(x), SUM(x²), COUNT(x):
                 * var_pop = SUM_SQ/N - (SUM/N)²
                 * var_samp = var_pop * N/(N-1)
                 * stddev_pop = sqrt(var_pop), stddev_samp = sqrt(var_samp) */
                int64_t sum_ci = c;
                int64_t sq_ci  = (int64_t)n_keys + std_sq_slot[std_i];
                int64_t cnt_ci = (int64_t)n_keys + std_cnt_slot[std_i];
                td_t* sum_col = td_table_get_col_idx(result, sum_ci);
                td_t* sq_col  = (sq_ci < rncols) ? td_table_get_col_idx(result, sq_ci) : NULL;
                td_t* cnt_col = (cnt_ci < rncols) ? td_table_get_col_idx(result, cnt_ci) : NULL;
                if (!sum_col || !sq_col || !cnt_col) {
                    if (sum_col) {
                        td_retain(sum_col);
                        trimmed = td_table_add_col(trimmed, nm, sum_col);
                        td_release(sum_col);
                    }
                    continue;
                }

                int64_t nrows = sum_col->len;
                td_t* out_col = td_vec_new(TD_F64, nrows);
                if (!out_col || TD_IS_ERR(out_col)) {
                    td_release(trimmed); td_release(result);
                    if (merge_tbl) td_release(merge_tbl);
                    return NULL;
                }
                out_col->len = nrows;
                double* out = (double*)td_data(out_col);

                uint16_t orig_op = std_orig_op[std_i];
                /* SUM(x) is always F64 after merge (SUM produces F64 for F64 input,
                 * I64 for integer input; SUM(x²) via td_mul always produces F64). */
                const double* sq = (const double*)td_data(sq_col);
                const int64_t* cv = (const int64_t*)td_data(cnt_col);
                if (sum_col->type == TD_F64) {
                    const double* sv = (const double*)td_data(sum_col);
                    for (int64_t r = 0; r < nrows; r++) {
                        double n = (double)cv[r];
                        if (n <= 0) { out[r] = NAN; continue; }
                        double mean = sv[r] / n;
                        double var_pop = sq[r] / n - mean * mean;
                        if (var_pop < 0) var_pop = 0;
                        if (orig_op == OP_VAR_POP)         out[r] = var_pop;
                        else if (orig_op == OP_VAR)         out[r] = n > 1 ? var_pop * n / (n - 1) : NAN;
                        else if (orig_op == OP_STDDEV_POP)  out[r] = sqrt(var_pop);
                        else /* OP_STDDEV */                out[r] = n > 1 ? sqrt(var_pop * n / (n - 1)) : NAN;
                    }
                } else {
                    const int64_t* sv = (const int64_t*)td_data(sum_col);
                    for (int64_t r = 0; r < nrows; r++) {
                        double n = (double)cv[r];
                        if (n <= 0) { out[r] = NAN; continue; }
                        double mean = (double)sv[r] / n;
                        double var_pop = sq[r] / n - mean * mean;
                        if (var_pop < 0) var_pop = 0;
                        if (orig_op == OP_VAR_POP)         out[r] = var_pop;
                        else if (orig_op == OP_VAR)         out[r] = n > 1 ? var_pop * n / (n - 1) : NAN;
                        else if (orig_op == OP_STDDEV_POP)  out[r] = sqrt(var_pop);
                        else /* OP_STDDEV */                out[r] = n > 1 ? sqrt(var_pop * n / (n - 1)) : NAN;
                    }
                }
                trimmed = td_table_add_col(trimmed, nm, out_col);
                td_release(out_col);
            } else {
                td_t* col = td_table_get_col_idx(result, c);
                if (col) {
                    td_retain(col);
                    trimmed = td_table_add_col(trimmed, nm, col);
                    td_release(col);
                }
            }
        }
        td_release(result);
        result = trimmed;
        rncols = td_table_ncols(result);
    }

    /* Agg column names already fixed by td_table_set_col_name inside batch loop.
     * Apply final name fixup for the user-facing n_aggs columns (trim decomposed extras). */
    for (uint8_t a = 0; a < n_aggs && (int64_t)(n_keys + a) < rncols; a++)
        td_table_set_col_name(result, (int64_t)n_keys + a, agg_name_ids[a]);

    if (merge_tbl) td_release(merge_tbl);
    return result;
}

/* ============================================================================
 * Join execution (parallel hash join)
 *
 * Three-phase pipeline:
 *   Phase 1 (sequential): Build chained hash table on right side
 *   Phase 2 (parallel):   Two-pass probe — count matches, prefix-sum, fill
 *   Phase 3 (parallel):   Column gather — assemble result columns
 * ============================================================================ */

/* Key equality helper — shared by count + fill phases */
static inline bool join_keys_eq(td_t* const* l_vecs, td_t* const* r_vecs, uint8_t n_keys,
                                 int64_t l, int64_t r) {
    for (uint8_t k = 0; k < n_keys; k++) {
        td_t* lc = l_vecs[k];
        td_t* rc = r_vecs[k];
        if (!lc || !rc) return false;
        if (lc->type == TD_F64) {
            if (((double*)td_data(lc))[l] != ((double*)td_data(rc))[r]) return false;
        } else {
            if (read_col_i64(td_data(lc), l, lc->type, lc->attrs) !=
                read_col_i64(td_data(rc), r, rc->type, rc->attrs)) return false;
        }
    }
    return true;
}

/* ── Parallel join HT build ─────────────────────────────────────────────
 * Workers hash right-side rows in parallel and insert into the shared
 * chain-linked hash table using atomic CAS on ht_heads[slot].
 * ht_next[r] is per-row (no contention). Load factor ~0.3 → negligible
 * CAS contention.
 * ──────────────────────────────────────────────────────────────────── */

/* ht_heads is accessed atomically from multiple workers during join build.
 * Using _Atomic(uint32_t)* for C11-compliant atomic access. */
#define JHT_EMPTY UINT32_MAX  /* sentinel for empty HT slot/chain end */

typedef struct {
    _Atomic(uint32_t)* ht_heads;  /* shared, protected by atomic CAS */
    uint32_t* ht_next;            /* per-row, no contention */
    uint32_t ht_mask;       /* ht_cap - 1 */
    td_t**   r_key_vecs;
    uint8_t  n_keys;
    /* ASP-Join: semijoin filter from factorized left side (NULL if N/A) */
    uint64_t* asp_bits;
    int64_t   asp_key_max;
} join_build_ctx_t;

static void join_build_fn(void* raw, uint32_t wid, int64_t start, int64_t end) {
    (void)wid;
    join_build_ctx_t* c = (join_build_ctx_t*)raw;
    _Atomic(uint32_t)* heads = c->ht_heads;
    uint32_t* restrict next  = c->ht_next;
    uint32_t mask  = c->ht_mask;

    /* ASP-Join: precompute pointer for right-side build filtering */
    uint64_t* asp_bits = c->asp_bits;
    int64_t asp_max = c->asp_key_max;
    int64_t* rk0 = (asp_bits && c->n_keys == 1) ? (int64_t*)td_data(c->r_key_vecs[0]) : NULL;

    for (int64_t r = start; r < end; r++) {
        /* ASP-Join skip: if right key not in left-side bitmap, skip insert */
        if (rk0 && rk0[r] >= 0 && rk0[r] <= asp_max &&
            !TD_SEL_BIT_TEST(asp_bits, rk0[r])) {
            next[(uint32_t)r] = JHT_EMPTY;  /* mark as unused */
            continue;
        }
        if (r + 8 < end) {
            uint64_t pf_h = hash_row_keys(c->r_key_vecs, c->n_keys, r + 8);
            __builtin_prefetch(&heads[(uint32_t)(pf_h & mask)], 1, 1);
        }
        uint64_t h = hash_row_keys(c->r_key_vecs, c->n_keys, r);
        uint32_t slot = (uint32_t)(h & mask);
        uint32_t row32 = (uint32_t)r;
        uint32_t old = atomic_load_explicit(&heads[slot], memory_order_relaxed);
        do {
            next[row32] = old;
        } while (!atomic_compare_exchange_weak_explicit(&heads[slot], &old, row32,
                    memory_order_release, memory_order_relaxed));
    }
}

#define JOIN_MORSEL 8192

typedef struct {
    _Atomic(uint32_t)* ht_heads;
    uint32_t*    ht_next;
    uint32_t     ht_cap;
    td_t**       l_key_vecs;
    td_t**       r_key_vecs;
    uint8_t      n_keys;
    uint8_t      join_type;
    int64_t      left_rows;
    /* Per-morsel counts/offsets (allocated by main thread) */
    int64_t*     morsel_counts;
    int64_t*     morsel_offsets;
    /* Shared output arrays (phase 2 fill) */
    int64_t*     l_idx;
    int64_t*     r_idx;
    /* FULL OUTER: track which right rows were matched (NULL if not full) */
    _Atomic(uint8_t)* matched_right;
    /* S-Join: semijoin filter bitmap (NULL if not applicable) */
    uint64_t*    sjoin_bits;
    int64_t      sjoin_key_max;
} join_probe_ctx_t;

/* Phase 2a: count matches per morsel */
static void join_count_fn(void* raw, uint32_t wid, int64_t task_start, int64_t task_end) {
    (void)wid; (void)task_end;
    join_probe_ctx_t* c = (join_probe_ctx_t*)raw;
    uint32_t tid = (uint32_t)task_start;
    int64_t row_start = (int64_t)tid * JOIN_MORSEL;
    int64_t row_end = row_start + JOIN_MORSEL;
    if (row_end > c->left_rows) row_end = c->left_rows;

    /* S-Join: precompute pointer for fast semijoin check */
    uint64_t* sjbits = c->sjoin_bits;
    int64_t sjmax = c->sjoin_key_max;
    int64_t* lk0 = (sjbits && c->n_keys == 1) ? (int64_t*)td_data(c->l_key_vecs[0]) : NULL;

    int64_t count = 0;
    uint32_t ht_mask = c->ht_cap - 1;
    for (int64_t l = row_start; l < row_end; l++) {
        /* S-Join skip: if left key not in right-side bitmap, skip probe */
        if (lk0 && lk0[l] >= 0 && lk0[l] <= sjmax &&
            !TD_SEL_BIT_TEST(sjbits, lk0[l])) {
            if (c->join_type >= 1) count++;  /* LEFT/FULL: emit unmatched */
            continue;
        }

        if (l + 8 < row_end) {
            uint64_t pf_h = hash_row_keys(c->l_key_vecs, c->n_keys, l + 8);
            __builtin_prefetch(&c->ht_heads[(uint32_t)(pf_h & ht_mask)], 0, 1);
        }
        uint64_t h = hash_row_keys(c->l_key_vecs, c->n_keys, l);
        uint32_t slot = (uint32_t)(h & ht_mask);
        bool matched = false;
        for (uint32_t r = c->ht_heads[slot]; r != JHT_EMPTY; r = c->ht_next[r]) {
            if (join_keys_eq(c->l_key_vecs, c->r_key_vecs, c->n_keys, l, (int64_t)r)) {
                count++;
                matched = true;
            }
        }
        if (!matched && c->join_type >= 1) count++;
    }
    c->morsel_counts[tid] = count;
}

/* Phase 2b: fill match pairs using pre-computed offsets */
static void join_fill_fn(void* raw, uint32_t wid, int64_t task_start, int64_t task_end) {
    (void)wid; (void)task_end;
    join_probe_ctx_t* c = (join_probe_ctx_t*)raw;
    uint32_t tid = (uint32_t)task_start;
    int64_t row_start = (int64_t)tid * JOIN_MORSEL;
    int64_t row_end = row_start + JOIN_MORSEL;
    if (row_end > c->left_rows) row_end = c->left_rows;

    int64_t off = c->morsel_offsets[tid];
    int64_t* restrict li = c->l_idx;
    int64_t* restrict ri = c->r_idx;

    /* S-Join: precompute pointer for fast semijoin check */
    uint64_t* sjbits = c->sjoin_bits;
    int64_t sjmax = c->sjoin_key_max;
    int64_t* lk0 = (sjbits && c->n_keys == 1) ? (int64_t*)td_data(c->l_key_vecs[0]) : NULL;

    uint32_t ht_mask = c->ht_cap - 1;
    for (int64_t l = row_start; l < row_end; l++) {
        /* S-Join skip: if left key not in right-side bitmap, skip probe */
        if (lk0 && lk0[l] >= 0 && lk0[l] <= sjmax &&
            !TD_SEL_BIT_TEST(sjbits, lk0[l])) {
            if (c->join_type >= 1) {
                li[off] = l;
                ri[off] = -1;
                off++;
            }
            continue;
        }

        if (l + 8 < row_end) {
            uint64_t pf_h = hash_row_keys(c->l_key_vecs, c->n_keys, l + 8);
            __builtin_prefetch(&c->ht_heads[(uint32_t)(pf_h & ht_mask)], 0, 1);
        }
        uint64_t h = hash_row_keys(c->l_key_vecs, c->n_keys, l);
        uint32_t slot = (uint32_t)(h & ht_mask);
        bool matched = false;
        for (uint32_t r = c->ht_heads[slot]; r != JHT_EMPTY; r = c->ht_next[r]) {
            if (join_keys_eq(c->l_key_vecs, c->r_key_vecs, c->n_keys, l, (int64_t)r)) {
                li[off] = l;
                ri[off] = (int64_t)r;
                off++;
                matched = true;
                /* Monotonic 0→1 store from multiple workers. */
                if (c->matched_right) atomic_store_explicit(&c->matched_right[r], 1, memory_order_relaxed);
            }
        }
        if (!matched && c->join_type >= 1) {
            li[off] = l;
            ri[off] = -1;
            off++;
        }
    }
}

static td_t* exec_join(td_graph_t* g, td_op_t* op, td_t* left_table, td_t* right_table) {
    if (!left_table || TD_IS_ERR(left_table)) return left_table;
    if (!right_table || TD_IS_ERR(right_table)) return right_table;

    td_op_ext_t* ext = find_ext(g, op->id);
    if (!ext) return TD_ERR_PTR(TD_ERR_NYI);

    int64_t left_rows = td_table_nrows(left_table);
    int64_t right_rows = td_table_nrows(right_table);
    /* Guard: uint32_t row indices in HT chains cannot represent >4B rows */
    if (right_rows > (int64_t)(UINT32_MAX - 1))
        return TD_ERR_PTR(TD_ERR_NYI);
    uint8_t n_keys = ext->join.n_join_keys;
    uint8_t join_type = ext->join.join_type;

    td_t* l_key_vecs[n_keys];
    td_t* r_key_vecs[n_keys];
    memset(l_key_vecs, 0, n_keys * sizeof(td_t*));
    memset(r_key_vecs, 0, n_keys * sizeof(td_t*));

    for (uint8_t k = 0; k < n_keys; k++) {
        td_op_ext_t* lk = find_ext(g, ext->join.left_keys[k]->id);
        td_op_ext_t* rk = find_ext(g, ext->join.right_keys[k]->id);
        if (lk && lk->base.opcode == OP_SCAN)
            l_key_vecs[k] = td_table_get_col(left_table, lk->sym);
        if (rk && rk->base.opcode == OP_SCAN)
            r_key_vecs[k] = td_table_get_col(right_table, rk->sym);
        if (rk && rk->base.opcode == OP_CONST && rk->literal)
            r_key_vecs[k] = rk->literal;
    }

    /* Phase 1: Build hash table on right side (parallel with atomic CAS) */
    td_pool_t* pool = td_pool_get();

    uint64_t ht_cap64 = 256;
    uint64_t target = (uint64_t)right_rows * 2;
    while (ht_cap64 < target) ht_cap64 *= 2;
    if (ht_cap64 > UINT32_MAX) ht_cap64 = (uint64_t)1 << 31;
    uint32_t ht_cap = (uint32_t)ht_cap64;

    td_t* result = NULL;
    td_t* counts_hdr = NULL;
    td_t* l_idx_hdr = NULL;
    td_t* r_idx_hdr = NULL;
    td_t* matched_right_hdr = NULL;
    td_t* sjoin_sel = NULL;
    td_t* asp_sel = NULL;
    td_t* ht_next_hdr;
    td_t* ht_heads_hdr;
    uint32_t* ht_next = (uint32_t*)scratch_alloc(&ht_next_hdr, (size_t)right_rows * sizeof(uint32_t));
    // cppcheck-suppress internalAstError
    // Valid C11/C17 _Atomic(T)* declaration; cppcheck parser may mis-handle this syntax.
    _Atomic(uint32_t)* ht_heads = (_Atomic(uint32_t)*)scratch_alloc(&ht_heads_hdr, ht_cap * sizeof(uint32_t));
    if (!ht_next || !ht_heads) {
        scratch_free(ht_next_hdr); scratch_free(ht_heads_hdr);
        return TD_ERR_PTR(TD_ERR_OOM);
    }
    memset(ht_heads, 0xFF, ht_cap * sizeof(uint32_t));  /* JHT_EMPTY = 0xFFFFFFFF */

    /* Phase 0.5: ASP-Join — extract semijoin filter from factorized left side.
     * When the left input comes from a factorized expand (_count column present),
     * build a TD_SEL bitmap of left-side key values to skip right-side rows
     * during hash-build whose keys can't match any left-side row. */
    uint64_t* asp_bits = NULL;
    int64_t asp_key_max = 0;
    if (n_keys == 1 && join_type == 0 && l_key_vecs[0] &&
        l_key_vecs[0]->type == TD_I64 && right_rows > left_rows * 2) {
        int64_t cnt_sym = td_sym_intern("_count", 6);
        td_t* cnt_col = td_table_get_col(left_table, cnt_sym);
        if (cnt_col) {  /* left is factorized */
            int64_t* lk = (int64_t*)td_data(l_key_vecs[0]);
            int64_t lk_max = 0;
            for (int64_t i = 0; i < left_rows; i++)
                if (lk[i] > lk_max) lk_max = lk[i];

            if (lk_max < (int64_t)1 << 24) {
                asp_sel = td_sel_new(lk_max + 1);
                if (asp_sel && !TD_IS_ERR(asp_sel)) {
                    asp_bits = td_sel_bits(asp_sel);
                    asp_key_max = lk_max;
                    for (int64_t i = 0; i < left_rows; i++) {
                        int64_t k = lk[i];
                        if (k >= 0 && k <= lk_max)
                            TD_SEL_BIT_SET(asp_bits, k);
                    }
                }
            }
        }
    }

    {
        join_build_ctx_t bctx = {
            .ht_heads   = ht_heads,
            .ht_next    = ht_next,
            .ht_mask    = ht_cap - 1,
            .r_key_vecs = r_key_vecs,
            .n_keys     = n_keys,
            .asp_bits   = asp_bits,
            .asp_key_max = asp_key_max,
        };
        if (pool && right_rows > TD_PARALLEL_THRESHOLD)
            td_pool_dispatch(pool, join_build_fn, &bctx, right_rows);
        else
            join_build_fn(&bctx, 0, 0, right_rows);
    }
    CHECK_CANCEL_GOTO(pool, join_cleanup);

    /* Phase 1.5: S-Join semijoin filter extraction.
     * Build a TD_SEL bitmap of all distinct right-side key values that
     * appear in the hash table. This can be used to skip left-side rows
     * whose key cannot match any right-side row.
     *
     * Applied when: single I64 key, inner join, left side is large enough
     * to benefit from filtering (> 2x right side). */
    if (n_keys == 1 && join_type == 0 && l_key_vecs[0] && r_key_vecs[0] &&
        l_key_vecs[0]->type == TD_I64 && r_key_vecs[0]->type == TD_I64 &&
        left_rows > right_rows * 2) {
        /* Determine key range to size the bitmap */
        int64_t* rk = (int64_t*)td_data(r_key_vecs[0]);
        int64_t key_max = 0;
        for (int64_t i = 0; i < right_rows; i++)
            if (rk[i] > key_max) key_max = rk[i];

        if (key_max < (int64_t)1 << 24) {  /* only for reasonably bounded keys */
            sjoin_sel = td_sel_new(key_max + 1);
            if (sjoin_sel && !TD_IS_ERR(sjoin_sel)) {
                uint64_t* bits = td_sel_bits(sjoin_sel);
                for (int64_t i = 0; i < right_rows; i++) {
                    int64_t k = rk[i];
                    if (k >= 0 && k <= key_max)
                        TD_SEL_BIT_SET(bits, k);
                }
            }
        }
    }

    /* Phase 2: Parallel probe (two-pass: count → prefix-sum → fill) */
    uint32_t n_tasks = (uint32_t)((left_rows + JOIN_MORSEL - 1) / JOIN_MORSEL);
    if (n_tasks == 0) n_tasks = 1;

    int64_t* morsel_counts = (int64_t*)scratch_calloc(&counts_hdr,
                              (size_t)(n_tasks + 1) * sizeof(int64_t));
    if (!morsel_counts) {
        scratch_free(ht_next_hdr); scratch_free(ht_heads_hdr);
        return TD_ERR_PTR(TD_ERR_OOM);
    }

    /* For FULL OUTER JOIN, allocate matched_right tracker */
    _Atomic(uint8_t)* matched_right = NULL;
    if (join_type == 2 && right_rows > 0) {
        matched_right = (_Atomic(uint8_t)*)scratch_calloc(&matched_right_hdr,
                                                           (size_t)right_rows);
        if (!matched_right) goto join_cleanup;
    }

    /* Prepare S-Join fields for probe context */
    uint64_t* sjoin_bits = NULL;
    int64_t sjoin_key_max = 0;
    if (sjoin_sel && !TD_IS_ERR(sjoin_sel)) {
        sjoin_bits = td_sel_bits(sjoin_sel);
        sjoin_key_max = sjoin_sel->len - 1;
    }

    join_probe_ctx_t probe_ctx = {
        .ht_heads    = ht_heads,
        .ht_next     = ht_next,
        .ht_cap      = ht_cap,
        .l_key_vecs  = l_key_vecs,
        .r_key_vecs  = r_key_vecs,
        .n_keys      = n_keys,
        .join_type   = join_type,
        .left_rows   = left_rows,
        .morsel_counts = morsel_counts,
        .matched_right = matched_right,
        .sjoin_bits  = sjoin_bits,
        .sjoin_key_max = sjoin_key_max,
    };

    /* 2a: Count matches per morsel */
    if (pool && n_tasks > 1)
        td_pool_dispatch_n(pool, join_count_fn, &probe_ctx, n_tasks);
    else
        for (uint32_t t = 0; t < n_tasks; t++)
            join_count_fn(&probe_ctx, 0, t, t + 1);

    /* Prefix sum → morsel_offsets (reuse counts array as offsets) */
    int64_t pair_count = 0;
    for (uint32_t t = 0; t < n_tasks; t++) {
        int64_t cnt = morsel_counts[t];
        morsel_counts[t] = pair_count;
        pair_count += cnt;
    }

    /* Allocate output pair arrays */
    int64_t* l_idx = NULL;
    int64_t* r_idx = NULL;

    if (pair_count > 0) {
        l_idx = (int64_t*)scratch_alloc(&l_idx_hdr, (size_t)pair_count * sizeof(int64_t));
        r_idx = (int64_t*)scratch_alloc(&r_idx_hdr, (size_t)pair_count * sizeof(int64_t));
        if (!l_idx || !r_idx) goto join_cleanup;
    }

    /* 2b: Fill match pairs */
    probe_ctx.morsel_offsets = morsel_counts;  /* now holds prefix sums */
    probe_ctx.l_idx = l_idx;
    probe_ctx.r_idx = r_idx;

    if (pair_count > 0) {
        if (pool && n_tasks > 1)
            td_pool_dispatch_n(pool, join_fill_fn, &probe_ctx, n_tasks);
        else
            for (uint32_t t = 0; t < n_tasks; t++)
                join_fill_fn(&probe_ctx, 0, t, t + 1);
    }

    CHECK_CANCEL_GOTO(pool, join_cleanup);

    /* FULL OUTER: append unmatched right rows (l_idx=-1, r_idx=r) */
    if (join_type == 2 && matched_right) {
        int64_t unmatched_right = 0;
        for (int64_t r = 0; r < right_rows; r++)
            if (!matched_right[r]) unmatched_right++;

        if (unmatched_right > 0) {
            int64_t total = pair_count + unmatched_right;
            td_t* new_l_hdr;
            td_t* new_r_hdr;
            int64_t* new_l = (int64_t*)scratch_alloc(&new_l_hdr,
                                (size_t)total * sizeof(int64_t));
            int64_t* new_r = (int64_t*)scratch_alloc(&new_r_hdr,
                                (size_t)total * sizeof(int64_t));
            if (!new_l || !new_r) {
                scratch_free(new_l_hdr); scratch_free(new_r_hdr);
                goto join_cleanup;
            }
            if (pair_count > 0) {
                memcpy(new_l, l_idx, (size_t)pair_count * sizeof(int64_t));
                memcpy(new_r, r_idx, (size_t)pair_count * sizeof(int64_t));
            }
            scratch_free(l_idx_hdr);
            scratch_free(r_idx_hdr);
            int64_t off = pair_count;
            for (int64_t r = 0; r < right_rows; r++) {
                if (!matched_right[r]) {
                    new_l[off] = -1;
                    new_r[off] = r;
                    off++;
                }
            }
            l_idx = new_l;  r_idx = new_r;
            l_idx_hdr = new_l_hdr;  r_idx_hdr = new_r_hdr;
            pair_count = total;
        }
    }

    /* Phase 3: Build result table with parallel column gather.
     * Use multi_gather for batched column access when possible (non-nullable
     * indices), falling back to per-column gather for nullable RIGHT columns. */
    int64_t left_ncols = td_table_ncols(left_table);
    int64_t right_ncols = td_table_ncols(right_table);
    result = td_table_new(left_ncols + right_ncols);
    if (!result || TD_IS_ERR(result)) goto join_cleanup;

    /* Count non-key right columns for multi_gather sizing */
    int64_t right_out_ncols __attribute__((unused)) = 0;
    for (int64_t c = 0; c < right_ncols; c++) {
        int64_t name_id = td_table_col_name(right_table, c);
        bool is_key = false;
        for (uint8_t k = 0; k < n_keys; k++) {
            td_op_ext_t* rk = find_ext(g, ext->join.right_keys[k]->id);
            if (rk && rk->base.opcode == OP_SCAN && rk->sym == name_id) {
                is_key = true; break;
            }
        }
        if (!is_key) right_out_ncols++;
    }

    /* Allocate all output columns upfront for batched gather */
    td_t* l_out_cols[MGATHER_MAX_COLS];
    int64_t l_out_names[MGATHER_MAX_COLS];
    int64_t l_out_count = 0;
    for (int64_t c = 0; c < left_ncols && l_out_count < MGATHER_MAX_COLS; c++) {
        td_t* col = td_table_get_col_idx(left_table, c);
        if (!col) continue;
        td_t* new_col = col_vec_new(col, pair_count);
        if (!new_col || TD_IS_ERR(new_col)) continue;
        new_col->len = pair_count;
        l_out_cols[l_out_count] = new_col;
        l_out_names[l_out_count] = td_table_col_name(left_table, c);
        l_out_count++;
    }

    td_t* r_out_cols[MGATHER_MAX_COLS];
    td_t* r_src_cols[MGATHER_MAX_COLS];
    int64_t r_out_names[MGATHER_MAX_COLS];
    int64_t r_out_count = 0;
    for (int64_t c = 0; c < right_ncols; c++) {
        td_t* col = td_table_get_col_idx(right_table, c);
        int64_t name_id = td_table_col_name(right_table, c);
        if (!col) continue;
        bool is_key = false;
        for (uint8_t k = 0; k < n_keys; k++) {
            td_op_ext_t* rk = find_ext(g, ext->join.right_keys[k]->id);
            if (rk && rk->base.opcode == OP_SCAN && rk->sym == name_id) {
                is_key = true; break;
            }
        }
        if (is_key) continue;
        if (r_out_count >= MGATHER_MAX_COLS) continue;
        td_t* new_col = col_vec_new(col, pair_count);
        if (!new_col || TD_IS_ERR(new_col)) continue;
        new_col->len = pair_count;
        r_out_cols[r_out_count] = new_col;
        r_src_cols[r_out_count] = col;
        r_out_names[r_out_count] = name_id;
        r_out_count++;
    }

    if (pair_count > 0) {
        /* Left columns: multi_gather (non-nullable for INNER/LEFT) */
        bool l_nullable = (join_type == 2);  /* only FULL OUTER */
        if (!l_nullable && l_out_count > 1 && l_out_count <= MGATHER_MAX_COLS) {
            multi_gather_ctx_t mgctx = { .idx = l_idx, .ncols = l_out_count };
            for (int64_t i = 0; i < l_out_count; i++) {
                mgctx.srcs[i] = (char*)td_data(td_table_get_col_idx(left_table,
                    /* find col with name l_out_names[i] */
                    i < left_ncols ? i : 0));
                mgctx.dsts[i] = (char*)td_data(l_out_cols[i]);
                mgctx.esz[i] = col_esz(l_out_cols[i]);
            }
            /* Rebuild srcs correctly by matching original column order */
            int64_t si = 0;
            for (int64_t c = 0; c < left_ncols && si < l_out_count; c++) {
                td_t* col = td_table_get_col_idx(left_table, c);
                if (!col) continue;
                mgctx.srcs[si] = (char*)td_data(col);
                mgctx.esz[si] = col_esz(col);
                si++;
            }
            if (pool && pair_count > TD_PARALLEL_THRESHOLD)
                td_pool_dispatch(pool, multi_gather_fn, &mgctx, pair_count);
            else
                multi_gather_fn(&mgctx, 0, 0, pair_count);
        } else {
            /* Fall back to per-column gather for nullable or single column */
            int64_t si = 0;
            for (int64_t c = 0; c < left_ncols && si < l_out_count; c++) {
                td_t* col = td_table_get_col_idx(left_table, c);
                if (!col) continue;
                gather_ctx_t gctx = {
                    .idx = l_idx, .src_col = col, .dst_col = l_out_cols[si],
                    .esz = col_esz(col), .nullable = l_nullable,
                };
                if (pool && pair_count > TD_PARALLEL_THRESHOLD)
                    td_pool_dispatch(pool, gather_fn, &gctx, pair_count);
                else
                    gather_fn(&gctx, 0, 0, pair_count);
                si++;
            }
        }

        /* Right columns: per-column gather (nullable for LEFT/FULL OUTER) */
        bool r_nullable = (join_type >= 1);
        if (!r_nullable && r_out_count > 1 && r_out_count <= MGATHER_MAX_COLS) {
            multi_gather_ctx_t mgctx = { .idx = r_idx, .ncols = r_out_count };
            for (int64_t i = 0; i < r_out_count; i++) {
                mgctx.srcs[i] = (char*)td_data(r_src_cols[i]);
                mgctx.dsts[i] = (char*)td_data(r_out_cols[i]);
                mgctx.esz[i] = col_esz(r_out_cols[i]);
            }
            if (pool && pair_count > TD_PARALLEL_THRESHOLD)
                td_pool_dispatch(pool, multi_gather_fn, &mgctx, pair_count);
            else
                multi_gather_fn(&mgctx, 0, 0, pair_count);
        } else {
            for (int64_t i = 0; i < r_out_count; i++) {
                gather_ctx_t gctx = {
                    .idx = r_idx, .src_col = r_src_cols[i], .dst_col = r_out_cols[i],
                    .esz = col_esz(r_src_cols[i]), .nullable = r_nullable,
                };
                if (pool && pair_count > TD_PARALLEL_THRESHOLD)
                    td_pool_dispatch(pool, gather_fn, &gctx, pair_count);
                else
                    gather_fn(&gctx, 0, 0, pair_count);
            }
        }
    }

    /* Add columns to result */
    for (int64_t i = 0; i < l_out_count; i++) {
        result = td_table_add_col(result, l_out_names[i], l_out_cols[i]);
        td_release(l_out_cols[i]);
    }
    for (int64_t i = 0; i < r_out_count; i++) {
        result = td_table_add_col(result, r_out_names[i], r_out_cols[i]);
        td_release(r_out_cols[i]);
    }

join_cleanup:
    scratch_free(ht_next_hdr);
    scratch_free(ht_heads_hdr);
    scratch_free(l_idx_hdr);
    scratch_free(r_idx_hdr);
    scratch_free(counts_hdr);
    scratch_free(matched_right_hdr);
    if (sjoin_sel) td_release(sjoin_sel);
    if (asp_sel) td_release(asp_sel);

    return result;
}

/* ============================================================================
 * OP_IF: ternary select  result[i] = cond[i] ? then[i] : else[i]
 * ============================================================================ */

static td_t* exec_if(td_graph_t* g, td_op_t* op) {
    /* cond = inputs[0], then = inputs[1], else_id stored in ext->literal */
    td_t* cond_v = exec_node(g, op->inputs[0]);
    td_t* then_v = exec_node(g, op->inputs[1]);

    td_op_ext_t* ext = find_ext(g, op->id);
    uint32_t else_id = (uint32_t)(uintptr_t)ext->literal;
    td_t* else_v = exec_node(g, &g->nodes[else_id]);

    if (!cond_v || TD_IS_ERR(cond_v)) {
        if (then_v && !TD_IS_ERR(then_v)) td_release(then_v);
        if (else_v && !TD_IS_ERR(else_v)) td_release(else_v);
        return cond_v;
    }
    if (!then_v || TD_IS_ERR(then_v)) {
        td_release(cond_v);
        if (else_v && !TD_IS_ERR(else_v)) td_release(else_v);
        return then_v;
    }
    if (!else_v || TD_IS_ERR(else_v)) {
        td_release(cond_v); td_release(then_v);
        return else_v;
    }

    int64_t len = cond_v->len;
    bool then_scalar = td_is_atom(then_v);
    bool else_scalar = td_is_atom(else_v);
    if (then_scalar && !else_scalar) len = else_v->len;
    if (!then_scalar) len = then_v->len;

    int8_t out_type = op->out_type;
    td_t* result = td_vec_new(out_type, len);
    if (!result || TD_IS_ERR(result)) {
        td_release(cond_v); td_release(then_v); td_release(else_v);
        return result;
    }
    result->len = len;

    uint8_t* cond_p = (uint8_t*)td_data(cond_v);

    if (out_type == TD_F64) {
        double t_scalar = then_scalar ? then_v->f64 : 0;
        double e_scalar = else_scalar ? else_v->f64 : 0;
        double* t_arr = then_scalar ? NULL : (double*)td_data(then_v);
        double* e_arr = else_scalar ? NULL : (double*)td_data(else_v);
        double* dst = (double*)td_data(result);
        for (int64_t i = 0; i < len; i++)
            dst[i] = cond_p[i] ? (t_arr ? t_arr[i] : t_scalar)
                               : (e_arr ? e_arr[i] : e_scalar);
    } else if (out_type == TD_I64) {
        int64_t t_scalar = then_scalar ? then_v->i64 : 0;
        int64_t e_scalar = else_scalar ? else_v->i64 : 0;
        int64_t* t_arr = then_scalar ? NULL : (int64_t*)td_data(then_v);
        int64_t* e_arr = else_scalar ? NULL : (int64_t*)td_data(else_v);
        int64_t* dst = (int64_t*)td_data(result);
        for (int64_t i = 0; i < len; i++)
            dst[i] = cond_p[i] ? (t_arr ? t_arr[i] : t_scalar)
                               : (e_arr ? e_arr[i] : e_scalar);
    } else if (out_type == TD_I32) {
        int32_t t_scalar = then_scalar ? then_v->i32 : 0;
        int32_t e_scalar = else_scalar ? else_v->i32 : 0;
        int32_t* t_arr = then_scalar ? NULL : (int32_t*)td_data(then_v);
        int32_t* e_arr = else_scalar ? NULL : (int32_t*)td_data(else_v);
        int32_t* dst = (int32_t*)td_data(result);
        for (int64_t i = 0; i < len; i++)
            dst[i] = cond_p[i] ? (t_arr ? t_arr[i] : t_scalar)
                               : (e_arr ? e_arr[i] : e_scalar);
    } else if (out_type == TD_SYM) {
        /* SYM columns may have narrow widths (W8/W16/W32) — use td_read_sym.
         * Scalars may be string atoms that need interning. Output is always W64. */
        int64_t t_scalar = 0, e_scalar = 0;
        if (then_scalar) {
            if (then_v->type == TD_ATOM_STR) {
                t_scalar = td_sym_intern(td_str_ptr(then_v), td_str_len(then_v));
            } else {
                t_scalar = then_v->i64;
            }
        }
        if (else_scalar) {
            if (else_v->type == TD_ATOM_STR) {
                e_scalar = td_sym_intern(td_str_ptr(else_v), td_str_len(else_v));
            } else {
                e_scalar = else_v->i64;
            }
        }
        int64_t* dst = (int64_t*)td_data(result);
        for (int64_t i = 0; i < len; i++) {
            int64_t tv = then_scalar ? t_scalar
                : td_read_sym(td_data(then_v), i, then_v->type, then_v->attrs);
            int64_t ev = else_scalar ? e_scalar
                : td_read_sym(td_data(else_v), i, else_v->type, else_v->attrs);
            dst[i] = cond_p[i] ? tv : ev;
        }
    } else if (out_type == TD_BOOL || out_type == TD_U8) {
        uint8_t t_scalar = then_scalar ? then_v->b8 : 0;
        uint8_t e_scalar = else_scalar ? else_v->b8 : 0;
        uint8_t* t_arr = then_scalar ? NULL : (uint8_t*)td_data(then_v);
        uint8_t* e_arr = else_scalar ? NULL : (uint8_t*)td_data(else_v);
        uint8_t* dst = (uint8_t*)td_data(result);
        for (int64_t i = 0; i < len; i++)
            dst[i] = cond_p[i] ? (t_arr ? t_arr[i] : t_scalar)
                               : (e_arr ? e_arr[i] : e_scalar);
    } else if (out_type == TD_TIMESTAMP || out_type == TD_TIME || out_type == TD_DATE) {
        /* TIMESTAMP is 8B like I64; DATE and TIME are 4B like I32 */
        if (out_type == TD_TIMESTAMP) {
            int64_t t_scalar2 = then_scalar ? then_v->i64 : 0;
            int64_t e_scalar2 = else_scalar ? else_v->i64 : 0;
            int64_t* t_arr = then_scalar ? NULL : (int64_t*)td_data(then_v);
            int64_t* e_arr = else_scalar ? NULL : (int64_t*)td_data(else_v);
            int64_t* dst = (int64_t*)td_data(result);
            for (int64_t i = 0; i < len; i++)
                dst[i] = cond_p[i] ? (t_arr ? t_arr[i] : t_scalar2)
                                   : (e_arr ? e_arr[i] : e_scalar2);
        } else {
            int32_t t_scalar2 = then_scalar ? then_v->i32 : 0;
            int32_t e_scalar2 = else_scalar ? else_v->i32 : 0;
            int32_t* t_arr = then_scalar ? NULL : (int32_t*)td_data(then_v);
            int32_t* e_arr = else_scalar ? NULL : (int32_t*)td_data(else_v);
            int32_t* dst = (int32_t*)td_data(result);
            for (int64_t i = 0; i < len; i++)
                dst[i] = cond_p[i] ? (t_arr ? t_arr[i] : t_scalar2)
                                   : (e_arr ? e_arr[i] : e_scalar2);
        }
    } else if (out_type == TD_I16) {
        int16_t t_scalar = then_scalar ? (int16_t)then_v->i32 : 0;
        int16_t e_scalar = else_scalar ? (int16_t)else_v->i32 : 0;
        int16_t* t_arr = then_scalar ? NULL : (int16_t*)td_data(then_v);
        int16_t* e_arr = else_scalar ? NULL : (int16_t*)td_data(else_v);
        int16_t* dst = (int16_t*)td_data(result);
        for (int64_t i = 0; i < len; i++)
            dst[i] = cond_p[i] ? (t_arr ? t_arr[i] : t_scalar)
                               : (e_arr ? e_arr[i] : e_scalar);
    }

    td_release(cond_v); td_release(then_v); td_release(else_v);
    return result;
}

/* ============================================================================
 * OP_LIKE: SQL LIKE pattern matching on SYM columns
 * ============================================================================ */

/* Simple SQL LIKE matcher: % = any (including empty), _ = single char.
 * Pattern is re-interpreted per row; could be optimized with precompilation
 * (e.g., compile once to NFA/DFA) for large datasets. */
static bool like_match(const char* str, size_t slen, const char* pat, size_t plen) {
    size_t si = 0, pi = 0;
    size_t star_p = (size_t)-1, star_s = 0;
    while (si < slen) {
        if (pi < plen && (pat[pi] == str[si] || pat[pi] == '_')) {
            si++; pi++;
        } else if (pi < plen && pat[pi] == '%') {
            star_p = pi; star_s = si;
            pi++;
        } else if (star_p != (size_t)-1) {
            pi = star_p + 1;
            star_s++;
            si = star_s;
        } else {
            return false;
        }
    }
    while (pi < plen && pat[pi] == '%') pi++;
    return pi == plen;
}

static td_t* exec_like(td_graph_t* g, td_op_t* op) {
    td_t* input = exec_node(g, op->inputs[0]);
    td_t* pat_v = exec_node(g, op->inputs[1]);
    if (!input || TD_IS_ERR(input)) { if (pat_v && !TD_IS_ERR(pat_v)) td_release(pat_v); return input; }
    if (!pat_v || TD_IS_ERR(pat_v)) { td_release(input); return pat_v; }

    /* Get pattern string */
    const char* pat_str = td_str_ptr(pat_v);
    size_t pat_len = td_str_len(pat_v);

    int64_t len = input->len;
    td_t* result = td_vec_new(TD_BOOL, len);
    if (!result || TD_IS_ERR(result)) {
        td_release(input); td_release(pat_v);
        return result;
    }
    result->len = len;
    uint8_t* dst = (uint8_t*)td_data(result);

    int8_t in_type = input->type;
    if (TD_IS_SYM(in_type)) {
        const void* base = td_data(input);
        for (int64_t i = 0; i < len; i++) {
            int64_t sym_id = td_read_sym(base, i, in_type, input->attrs);
            td_t* s = td_sym_str(sym_id);
            if (!s) { dst[i] = 0; continue; }
            const char* sp = td_str_ptr(s);
            size_t sl = td_str_len(s);
            dst[i] = like_match(sp, sl, pat_str, pat_len) ? 1 : 0;
        }
    } else {
        memset(dst, 0, (size_t)len);
    }

    td_release(input); td_release(pat_v);
    return result;
}

/* Case-insensitive LIKE: compare characters via tolower(). */
static bool ilike_match(const char* str, size_t slen, const char* pat, size_t plen) {
    size_t si = 0, pi = 0;
    size_t star_p = (size_t)-1, star_s = 0;
    while (si < slen) {
        if (pi < plen && pat[pi] != '%') {
            unsigned char sc = (unsigned char)str[si];
            unsigned char pc = (unsigned char)pat[pi];
            if (pc == '_' || (sc >= 'A' && sc <= 'Z' ? sc + 32 : sc) ==
                             (pc >= 'A' && pc <= 'Z' ? pc + 32 : pc)) {
                si++; pi++;
            } else if (star_p != (size_t)-1) {
                pi = star_p + 1; star_s++; si = star_s;
            } else {
                return false;
            }
        } else if (pi < plen && pat[pi] == '%') {
            star_p = pi; star_s = si; pi++;
        } else if (star_p != (size_t)-1) {
            pi = star_p + 1; star_s++; si = star_s;
        } else {
            return false;
        }
    }
    while (pi < plen && pat[pi] == '%') pi++;
    return pi == plen;
}

static td_t* exec_ilike(td_graph_t* g, td_op_t* op) {
    td_t* input = exec_node(g, op->inputs[0]);
    td_t* pat_v = exec_node(g, op->inputs[1]);
    if (!input || TD_IS_ERR(input)) { if (pat_v && !TD_IS_ERR(pat_v)) td_release(pat_v); return input; }
    if (!pat_v || TD_IS_ERR(pat_v)) { td_release(input); return pat_v; }

    const char* pat_str = td_str_ptr(pat_v);
    size_t pat_len = td_str_len(pat_v);

    int64_t len = input->len;
    td_t* result = td_vec_new(TD_BOOL, len);
    if (!result || TD_IS_ERR(result)) {
        td_release(input); td_release(pat_v);
        return result;
    }
    result->len = len;
    uint8_t* dst = (uint8_t*)td_data(result);

    int8_t in_type = input->type;
    if (TD_IS_SYM(in_type)) {
        const void* base = td_data(input);
        for (int64_t i = 0; i < len; i++) {
            int64_t sym_id = td_read_sym(base, i, in_type, input->attrs);
            td_t* s = td_sym_str(sym_id);
            if (!s) { dst[i] = 0; continue; }
            dst[i] = ilike_match(td_str_ptr(s), td_str_len(s), pat_str, pat_len) ? 1 : 0;
        }
    } else {
        memset(dst, 0, (size_t)len);
    }

    td_release(input); td_release(pat_v);
    return result;
}

/* ============================================================================
 * String functions: UPPER, LOWER, TRIM, STRLEN, SUBSTR, REPLACE, CONCAT
 *
 * These functions call td_sym_intern() per output row, which is
 * O(n * sym_table_lookup) per string op.  Acceptable for current workloads;
 * could be optimized with batch interning if profiling shows a bottleneck.
 * ============================================================================ */

/* Helper: resolve sym/enum element to string */
static inline void sym_elem(const td_t* input, int64_t i,
                            const char** out_str, size_t* out_len) {
    int64_t sym_id = td_read_sym(td_data((td_t*)input), i, input->type, input->attrs);
    td_t* atom = td_sym_str(sym_id);
    if (!atom) { *out_str = ""; *out_len = 0; return; }
    *out_str = td_str_ptr(atom);
    *out_len = td_str_len(atom);
}

/* UPPER / LOWER / TRIM — unary SYM → SYM */
static td_t* exec_string_unary(td_graph_t* g, td_op_t* op) {
    td_t* input = exec_node(g, op->inputs[0]);
    if (!input || TD_IS_ERR(input)) return input;

    int64_t len = input->len;
    td_t* result = td_vec_new(TD_SYM, len);
    if (!result || TD_IS_ERR(result)) { td_release(input); return result; }
    result->len = len;
    int64_t* dst = (int64_t*)td_data(result);

    uint16_t opc = op->opcode;
    for (int64_t i = 0; i < len; i++) {
        const char* sp; size_t sl;
        sym_elem(input, i, &sp, &sl);
        char sbuf[8192];
        char* buf = sbuf;
        td_t* dyn_hdr = NULL;
        if (sl >= sizeof(sbuf)) {
            buf = (char*)scratch_alloc(&dyn_hdr, sl + 1);
            if (!buf) { dst[i] = td_sym_intern("", 0); continue; }
        }
        size_t out_len = sl;
        if (opc == OP_UPPER) {
            for (size_t j = 0; j < out_len; j++) buf[j] = (char)toupper((unsigned char)sp[j]);
        } else if (opc == OP_LOWER) {
            for (size_t j = 0; j < out_len; j++) buf[j] = (char)tolower((unsigned char)sp[j]);
        } else { /* OP_TRIM */
            size_t start = 0, end = sl;
            while (start < sl && isspace((unsigned char)sp[start])) start++;
            while (end > start && isspace((unsigned char)sp[end - 1])) end--;
            out_len = end - start;
            memcpy(buf, sp + start, out_len);
        }
        buf[out_len] = '\0';
        dst[i] = td_sym_intern(buf, out_len);
        scratch_free(dyn_hdr);
    }
    td_release(input);
    return result;
}

/* LENGTH — SYM → I64 */
static td_t* exec_strlen(td_graph_t* g, td_op_t* op) {
    td_t* input = exec_node(g, op->inputs[0]);
    if (!input || TD_IS_ERR(input)) return input;

    int64_t len = input->len;
    td_t* result = td_vec_new(TD_I64, len);
    if (!result || TD_IS_ERR(result)) { td_release(input); return result; }
    result->len = len;
    int64_t* dst = (int64_t*)td_data(result);

    for (int64_t i = 0; i < len; i++) {
        const char* sp; size_t sl;
        sym_elem(input, i, &sp, &sl);
        dst[i] = (int64_t)sl;
    }
    td_release(input);
    return result;
}

/* SUBSTR(str, start, len) — 1-based start */
static td_t* exec_substr(td_graph_t* g, td_op_t* op) {
    td_t* input = exec_node(g, op->inputs[0]);
    td_t* start_v = exec_node(g, op->inputs[1]);
    if (!input || TD_IS_ERR(input)) { if (start_v && !TD_IS_ERR(start_v)) td_release(start_v); return input; }
    if (!start_v || TD_IS_ERR(start_v)) { td_release(input); return start_v; }

    /* Get len arg from ext node's literal field */
    td_op_ext_t* ext = find_ext(g, op->id);
    uint32_t len_id = (uint32_t)(uintptr_t)ext->literal;
    td_t* len_v = exec_node(g, &g->nodes[len_id]);
    if (!len_v || TD_IS_ERR(len_v)) { td_release(input); td_release(start_v); return len_v; }

    int64_t nrows = input->len;
    td_t* result = td_vec_new(TD_SYM, nrows);
    if (!result || TD_IS_ERR(result)) { td_release(input); td_release(start_v); td_release(len_v); return result; }
    result->len = nrows;
    int64_t* dst = (int64_t*)td_data(result);

    /* start_v and len_v may be atom scalars or vectors.
     * Handle TD_I32 vectors correctly (read as int32_t, not int64_t). */
    int64_t s_scalar = 0, l_scalar = 0;
    const int64_t* s_data = NULL;
    const int64_t* l_data = NULL;
    const int32_t* s_data_i32 = NULL;
    const int32_t* l_data_i32 = NULL;
    if (start_v->type == TD_ATOM_I64) s_scalar = start_v->i64;
    else if (start_v->type == TD_ATOM_F64) s_scalar = (int64_t)start_v->f64;
    else if (start_v->len == 1) {
        if (start_v->type == TD_F64)
            s_scalar = (int64_t)((double*)td_data(start_v))[0];
        else if (start_v->type == TD_I32)
            s_scalar = (int64_t)((int32_t*)td_data(start_v))[0];
        else
            s_scalar = ((int64_t*)td_data(start_v))[0];
    }
    else if (start_v->type == TD_I32) s_data_i32 = (const int32_t*)td_data(start_v);
    else s_data = (const int64_t*)td_data(start_v);
    if (len_v->type == TD_ATOM_I64) l_scalar = len_v->i64;
    else if (len_v->type == TD_ATOM_F64) l_scalar = (int64_t)len_v->f64;
    else if (len_v->len == 1) {
        if (len_v->type == TD_F64)
            l_scalar = (int64_t)((double*)td_data(len_v))[0];
        else if (len_v->type == TD_I32)
            l_scalar = (int64_t)((int32_t*)td_data(len_v))[0];
        else
            l_scalar = ((int64_t*)td_data(len_v))[0];
    }
    else if (len_v->type == TD_I32) l_data_i32 = (const int32_t*)td_data(len_v);
    else l_data = (const int64_t*)td_data(len_v);

    for (int64_t i = 0; i < nrows; i++) {
        const char* sp; size_t sl;
        sym_elem(input, i, &sp, &sl);
        int64_t st = (s_data ? s_data[i] : s_data_i32 ? (int64_t)s_data_i32[i] : s_scalar) - 1; /* 1-based → 0-based */
        int64_t ln = l_data ? l_data[i] : l_data_i32 ? (int64_t)l_data_i32[i] : l_scalar;
        if (st < 0) st = 0;
        if ((size_t)st >= sl) { dst[i] = td_sym_intern("", 0); continue; }
        if (ln < 0 || ln > (int64_t)(sl - (size_t)st)) ln = (int64_t)sl - st;
        dst[i] = td_sym_intern(sp + st, (size_t)ln);
    }
    td_release(input); td_release(start_v); td_release(len_v);
    return result;
}

/* REPLACE(str, from, to) */
static td_t* exec_replace(td_graph_t* g, td_op_t* op) {
    td_t* input = exec_node(g, op->inputs[0]);
    td_t* from_v = exec_node(g, op->inputs[1]);
    if (!input || TD_IS_ERR(input)) { if (from_v && !TD_IS_ERR(from_v)) td_release(from_v); return input; }
    if (!from_v || TD_IS_ERR(from_v)) { td_release(input); return from_v; }

    td_op_ext_t* ext = find_ext(g, op->id);
    uint32_t to_id = (uint32_t)(uintptr_t)ext->literal;
    td_t* to_v = exec_node(g, &g->nodes[to_id]);
    if (!to_v || TD_IS_ERR(to_v)) { td_release(input); td_release(from_v); return to_v; }

    /* from_v and to_v should be string constants (SYM atoms) */
    const char* from_str = td_str_ptr(from_v);
    size_t from_len = td_str_len(from_v);
    const char* to_str = td_str_ptr(to_v);
    size_t to_len = td_str_len(to_v);

    int64_t nrows = input->len;
    td_t* result = td_vec_new(TD_SYM, nrows);
    if (!result || TD_IS_ERR(result)) { td_release(input); td_release(from_v); td_release(to_v); return result; }
    result->len = nrows;
    int64_t* dst = (int64_t*)td_data(result);

    for (int64_t i = 0; i < nrows; i++) {
        const char* sp; size_t sl;
        sym_elem(input, i, &sp, &sl);
        /* Simple find-and-replace-all */
        /* Worst case: every char is a match, each replaced by to_len bytes.
         * Guard against size_t overflow when to_len >> from_len. */
        size_t n_matches = (from_len > 0) ? sl / from_len : 0;
        size_t worst;
        if (from_len > 0 && to_len > 0 && n_matches > SIZE_MAX / to_len) {
            worst = SIZE_MAX; /* overflow → cap at max; scratch_alloc will OOM */
        } else {
            worst = (from_len > 0)
                ? n_matches * to_len + (sl % from_len) + 1
                : sl + 1;
        }
        char sbuf[8192];
        char* buf = sbuf;
        td_t* dyn_hdr = NULL;
        if (worst > sizeof(sbuf)) {
            buf = (char*)scratch_alloc(&dyn_hdr, worst);
            if (!buf) { dst[i] = td_sym_intern("", 0); continue; }
        }
        size_t buf_cap = dyn_hdr ? worst : sizeof(sbuf);
        size_t bi = 0;
        for (size_t j = 0; j < sl; ) {
            if (from_len > 0 && j + from_len <= sl && memcmp(sp + j, from_str, from_len) == 0) {
                if (bi + to_len < buf_cap) { memcpy(buf + bi, to_str, to_len); bi += to_len; }
                j += from_len;
            } else {
                if (bi < buf_cap - 1) buf[bi++] = sp[j];
                j++;
            }
        }
        buf[bi] = '\0';
        dst[i] = td_sym_intern(buf, bi);
        scratch_free(dyn_hdr);
    }
    td_release(input); td_release(from_v); td_release(to_v);
    return result;
}

/* CONCAT(a, b, ...) */
static td_t* exec_concat(td_graph_t* g, td_op_t* op) {
    td_op_ext_t* ext = find_ext(g, op->id);
    if (!ext) return TD_ERR_PTR(TD_ERR_NYI);
    int64_t raw_nargs = ext->sym;
    if (raw_nargs < 2 || raw_nargs > 255) return TD_ERR_PTR(TD_ERR_DOMAIN);
    int n_args = (int)raw_nargs;

    /* Evaluate all inputs */
    td_t* args_stack[16];
    td_t** args = args_stack;
    td_t* args_hdr = NULL;
    if (n_args > 16) {
        args = (td_t**)scratch_calloc(&args_hdr, (size_t)n_args * sizeof(td_t*));
        if (!args) return TD_ERR_PTR(TD_ERR_OOM);
    }

    args[0] = exec_node(g, op->inputs[0]);
    args[1] = exec_node(g, op->inputs[1]);
    uint32_t* trail = (uint32_t*)((char*)(ext + 1));
    for (int i = 2; i < n_args; i++) {
        args[i] = exec_node(g, &g->nodes[trail[i - 2]]);
    }
    /* Error check */
    for (int i = 0; i < n_args; i++) {
        if (!args[i] || TD_IS_ERR(args[i])) {
            td_t* err = args[i];
            for (int j = 0; j < n_args; j++) {
                if (j != i && args[j] && !TD_IS_ERR(args[j])) td_release(args[j]);
            }
            scratch_free(args_hdr);
            return err;
        }
    }

    /* Derive nrows from first vector arg (scalar args have byte-length in len) */
    int64_t nrows = 1;
    for (int a = 0; a < n_args; a++) {
        int8_t at = args[a]->type;
        if (TD_IS_SYM(at)) { nrows = args[a]->len; break; }
        if (!td_is_atom(args[a])) { nrows = args[a]->len; break; }
    }
    td_t* result = td_vec_new(TD_SYM, nrows);
    if (!result || TD_IS_ERR(result)) {
        for (int i = 0; i < n_args; i++) td_release(args[i]);
        scratch_free(args_hdr);
        return result;
    }
    result->len = nrows;
    int64_t* dst = (int64_t*)td_data(result);

    for (int64_t r = 0; r < nrows; r++) {
        /* Pre-scan to compute total concat length for this row */
        size_t total = 0;
        for (int a = 0; a < n_args; a++) {
            int8_t t = args[a]->type;
            if (TD_IS_SYM(t)) {
                const char* sp; size_t sl;
                sym_elem(args[a], r, &sp, &sl);
                total += sl;
            } else if (t == TD_ATOM_STR) {
                size_t sl = td_str_len(args[a]);
                total += sl;
            }
        }
        char sbuf[8192];
        char* buf = sbuf;
        td_t* dyn_hdr = NULL;
        size_t buf_cap = sizeof(sbuf);
        if (total >= sizeof(sbuf)) {
            buf = (char*)scratch_alloc(&dyn_hdr, total + 1);
            if (!buf) { buf = sbuf; buf_cap = sizeof(sbuf); }
            else buf_cap = total + 1;
        }
        size_t bi = 0;
        for (int a = 0; a < n_args; a++) {
            int8_t t = args[a]->type;
            if (TD_IS_SYM(t)) {
                const char* sp; size_t sl;
                sym_elem(args[a], r, &sp, &sl);
                if (bi + sl < buf_cap) { memcpy(buf + bi, sp, sl); bi += sl; }
            } else if (t == TD_ATOM_STR) {
                const char* sp = td_str_ptr(args[a]);
                size_t sl = td_str_len(args[a]);
                if (sp && bi + sl < buf_cap) { memcpy(buf + bi, sp, sl); bi += sl; }
            }
        }
        buf[bi] = '\0';
        dst[r] = td_sym_intern(buf, bi);
        scratch_free(dyn_hdr);
    }
    for (int i = 0; i < n_args; i++) td_release(args[i]);
    scratch_free(args_hdr);
    return result;
}

/* ============================================================================
 * EXTRACT — date/time component extraction from temporal columns
 *
 * Input:  TD_TIMESTAMP (i64 µs since 2000-01-01), TD_DATE (i32 days since
 *         2000-01-01), or TD_TIME (i32 ms since midnight).
 * Output: i64 vector of extracted field values.
 *
 * Uses Howard Hinnant's civil_from_days algorithm (public domain) for
 * Gregorian calendar decomposition.
 * ============================================================================ */

static td_t* exec_extract(td_graph_t* g, td_op_t* op) {
    td_t* input = exec_node(g, op->inputs[0]);
    if (!input || TD_IS_ERR(input)) return input;

    td_op_ext_t* ext = find_ext(g, op->id);
    if (!ext) { td_release(input); return TD_ERR_PTR(TD_ERR_NYI); }

    int64_t field = ext->sym;
    int64_t len = input->len;
    int8_t in_type = input->type;

    td_t* result = td_vec_new(TD_I64, len);
    if (!result || TD_IS_ERR(result)) { td_release(input); return result; }
    result->len = len;

    int64_t* out = (int64_t*)td_data(result);

    #undef  USEC_PER_SEC
    #define USEC_PER_SEC  1000000LL
    #define USEC_PER_MIN  (60LL  * USEC_PER_SEC)
    #define USEC_PER_HOUR (3600LL * USEC_PER_SEC)
    #define USEC_PER_DAY  (86400LL * USEC_PER_SEC)

    td_morsel_t m;
    td_morsel_init(&m, input);
    int64_t off = 0;

    while (td_morsel_next(&m)) {
        int64_t n = m.morsel_len;

        for (int64_t i = 0; i < n; i++) {
            int64_t us;
            if (in_type == TD_DATE) {
                /* int32 days since 2000-01-01 → microseconds */
                int32_t d = ((const int32_t*)m.morsel_ptr)[i];
                us = (int64_t)d * USEC_PER_DAY;
            } else if (in_type == TD_TIME) {
                /* int32 milliseconds since midnight → microseconds */
                int32_t ms = ((const int32_t*)m.morsel_ptr)[i];
                us = (int64_t)ms * 1000LL;
            } else {
                /* TD_TIMESTAMP / TD_I64: already microseconds */
                us = ((const int64_t*)m.morsel_ptr)[i];
            }

            if (field == TD_EXTRACT_EPOCH) {
                out[off + i] = us;
            } else if (field == TD_EXTRACT_HOUR) {
                int64_t day_us = us % USEC_PER_DAY;
                if (day_us < 0) day_us += USEC_PER_DAY;
                out[off + i] = day_us / USEC_PER_HOUR;
            } else if (field == TD_EXTRACT_MINUTE) {
                int64_t day_us = us % USEC_PER_DAY;
                if (day_us < 0) day_us += USEC_PER_DAY;
                out[off + i] = (day_us % USEC_PER_HOUR) / USEC_PER_MIN;
            } else if (field == TD_EXTRACT_SECOND) {
                int64_t day_us = us % USEC_PER_DAY;
                if (day_us < 0) day_us += USEC_PER_DAY;
                out[off + i] = (day_us % USEC_PER_MIN) / USEC_PER_SEC;
            } else {
                /* Calendar fields: YEAR, MONTH, DAY, DOW, DOY */
                /* Floor-divide microseconds to get day count */
                int64_t days_since_2000 = us / USEC_PER_DAY;
                if (us < 0 && us % USEC_PER_DAY != 0) days_since_2000--;

                /* Hinnant civil_from_days: shift to 0000-03-01 era-based epoch */
                int64_t z = days_since_2000 + 10957 + 719468;
                int64_t era = (z >= 0 ? z : z - 146096) / 146097;
                uint64_t doe = (uint64_t)(z - era * 146097);
                uint64_t yoe = (doe - doe/1460 + doe/36524 - doe/146096) / 365;
                int64_t y = (int64_t)yoe + era * 400;
                uint64_t doy_mar = doe - (365*yoe + yoe/4 - yoe/100);
                uint64_t mp = (5*doy_mar + 2) / 153;
                uint64_t d = doy_mar - (153*mp + 2) / 5 + 1;
                uint64_t mo = mp < 10 ? mp + 3 : mp - 9;
                y += (mo <= 2);

                if (field == TD_EXTRACT_YEAR) {
                    out[off + i] = y;
                } else if (field == TD_EXTRACT_MONTH) {
                    out[off + i] = (int64_t)mo;
                } else if (field == TD_EXTRACT_DAY) {
                    out[off + i] = (int64_t)d;
                } else if (field == TD_EXTRACT_DOW) {
                    /* ISO day of week: Mon=1 .. Sun=7
                     * 2000-01-01 was Saturday (ISO 6).
                     * Formula: ((days%7)+7+5)%7 + 1 */
                    out[off + i] = ((days_since_2000 % 7) + 7 + 5) % 7 + 1;
                } else if (field == TD_EXTRACT_DOY) {
                    /* Day of year [1..366], January-based */
                    static const int dbm[13] = {
                        0, 0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334
                    };
                    if (mo < 1 || mo > 12) { out[off + i] = 0; continue; }
                    int leap = (y % 4 == 0 && (y % 100 != 0 || y % 400 == 0));
                    int64_t doy_jan = dbm[mo] + (int64_t)d;
                    if (mo > 2 && leap) doy_jan++;
                    out[off + i] = doy_jan;
                } else {
                    out[off + i] = 0;
                }
            }
        }
        off += n;
    }

    #undef USEC_PER_SEC
    #undef USEC_PER_MIN
    #undef USEC_PER_HOUR
    #undef USEC_PER_DAY

    td_release(input);
    return result;
}

/* ============================================================================
 * DATE_TRUNC — truncate temporal value to specified precision
 *
 * Input:  TD_TIMESTAMP (i64 µs since 2000-01-01), TD_DATE (i32 days since
 *         2000-01-01), or TD_TIME (i32 ms since midnight).
 * Output: TD_TIMESTAMP (i64 µs) — always returns microseconds since 2000-01-01.
 * Sub-day: modular arithmetic. Month/year: calendar decompose + recompose.
 * ============================================================================ */

/* Convert (year, month, day) to days since 2000-01-01 using the inverse of
 * Hinnant's civil_from_days. */
static int64_t days_from_civil(int64_t y, int64_t m, int64_t d) {
    y -= (m <= 2);
    int64_t era = (y >= 0 ? y : y - 399) / 400;
    uint64_t yoe = (uint64_t)(y - era * 400);
    uint64_t doy = (153 * (m > 2 ? (uint64_t)m - 3 : (uint64_t)m + 9) + 2) / 5 + (uint64_t)d - 1;
    uint64_t doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    return era * 146097 + (int64_t)doe - 719468 - 10957;
}

static td_t* exec_date_trunc(td_graph_t* g, td_op_t* op) {
    td_t* input = exec_node(g, op->inputs[0]);
    if (!input || TD_IS_ERR(input)) return input;

    td_op_ext_t* ext = find_ext(g, op->id);
    if (!ext) { td_release(input); return TD_ERR_PTR(TD_ERR_NYI); }

    int64_t field = ext->sym;
    int64_t len = input->len;
    int8_t in_type = input->type;

    td_t* result = td_vec_new(TD_TIMESTAMP, len);
    if (!result || TD_IS_ERR(result)) { td_release(input); return result; }
    result->len = len;

    int64_t* out = (int64_t*)td_data(result);

    #define DT_USEC_PER_SEC  1000000LL
    #define DT_USEC_PER_MIN  (60LL  * DT_USEC_PER_SEC)
    #define DT_USEC_PER_HOUR (3600LL * DT_USEC_PER_SEC)
    #define DT_USEC_PER_DAY  (86400LL * DT_USEC_PER_SEC)

    td_morsel_t m;
    td_morsel_init(&m, input);
    int64_t off = 0;

    while (td_morsel_next(&m)) {
        int64_t n = m.morsel_len;

        for (int64_t i = 0; i < n; i++) {
            int64_t us;
            if (in_type == TD_DATE) {
                int32_t d = ((const int32_t*)m.morsel_ptr)[i];
                us = (int64_t)d * DT_USEC_PER_DAY;
            } else if (in_type == TD_TIME) {
                int32_t ms = ((const int32_t*)m.morsel_ptr)[i];
                us = (int64_t)ms * 1000LL;
            } else {
                us = ((const int64_t*)m.morsel_ptr)[i];
            }

            switch (field) {
                case TD_EXTRACT_SECOND: {
                    /* Truncate to second boundary */
                    int64_t r = us % DT_USEC_PER_SEC;
                    out[off + i] = us - r - (r < 0 ? DT_USEC_PER_SEC : 0);
                    break;
                }
                case TD_EXTRACT_MINUTE: {
                    int64_t r = us % DT_USEC_PER_MIN;
                    out[off + i] = us - r - (r < 0 ? DT_USEC_PER_MIN : 0);
                    break;
                }
                case TD_EXTRACT_HOUR: {
                    int64_t r = us % DT_USEC_PER_HOUR;
                    out[off + i] = us - r - (r < 0 ? DT_USEC_PER_HOUR : 0);
                    break;
                }
                case TD_EXTRACT_DAY: {
                    int64_t r = us % DT_USEC_PER_DAY;
                    out[off + i] = us - r - (r < 0 ? DT_USEC_PER_DAY : 0);
                    break;
                }
                case TD_EXTRACT_MONTH: {
                    /* Decompose to y/m/d, set d=1, recompose */
                    int64_t days2k = us / DT_USEC_PER_DAY;
                    if (us < 0 && us % DT_USEC_PER_DAY != 0) days2k--;
                    int64_t z = days2k + 10957 + 719468;
                    int64_t era = (z >= 0 ? z : z - 146096) / 146097;
                    uint64_t doe = (uint64_t)(z - era * 146097);
                    uint64_t yoe = (doe - doe/1460 + doe/36524 - doe/146096) / 365;
                    int64_t y = (int64_t)yoe + era * 400;
                    uint64_t doy_mar = doe - (365*yoe + yoe/4 - yoe/100);
                    uint64_t mp = (5*doy_mar + 2) / 153;
                    uint64_t mo = mp < 10 ? mp + 3 : mp - 9;
                    y += (mo <= 2);
                    out[off + i] = days_from_civil(y, (int64_t)mo, 1) * DT_USEC_PER_DAY;
                    break;
                }
                case TD_EXTRACT_YEAR: {
                    /* Decompose to y/m/d, set m=1 d=1, recompose */
                    int64_t days2k = us / DT_USEC_PER_DAY;
                    if (us < 0 && us % DT_USEC_PER_DAY != 0) days2k--;
                    int64_t z = days2k + 10957 + 719468;
                    int64_t era = (z >= 0 ? z : z - 146096) / 146097;
                    uint64_t doe = (uint64_t)(z - era * 146097);
                    uint64_t yoe = (doe - doe/1460 + doe/36524 - doe/146096) / 365;
                    int64_t y = (int64_t)yoe + era * 400;
                    uint64_t doy_mar = doe - (365*yoe + yoe/4 - yoe/100);
                    uint64_t mp = (5*doy_mar + 2) / 153;
                    uint64_t mo = mp < 10 ? mp + 3 : mp - 9;
                    y += (mo <= 2);
                    out[off + i] = days_from_civil(y, 1, 1) * DT_USEC_PER_DAY;
                    break;
                }
                default:
                    out[off + i] = us;
                    break;
            }
        }
        off += n;
    }

    #undef DT_USEC_PER_SEC
    #undef DT_USEC_PER_MIN
    #undef DT_USEC_PER_HOUR
    #undef DT_USEC_PER_DAY

    td_release(input);
    return result;
}

/* ============================================================================
 * Window function execution
 * ============================================================================ */

/* Compare rows ra and rb on the given key columns. Returns true if any differ. */
static inline bool win_keys_differ(td_t* const* vecs, uint8_t n_keys,
                                    int64_t ra, int64_t rb) {
    for (uint8_t k = 0; k < n_keys; k++) {
        td_t* col = vecs[k];
        if (!col) continue;
        switch (col->type) {
        case TD_I64: case TD_TIMESTAMP:
            if (((const int64_t*)td_data(col))[ra] !=
                ((const int64_t*)td_data(col))[rb]) return true;
            break;
        case TD_F64: {
            double a = ((const double*)td_data(col))[ra];
            double b = ((const double*)td_data(col))[rb];
            if (a != b) return true;
            break;
        }
        case TD_I32: case TD_DATE: case TD_TIME:
            if (((const int32_t*)td_data(col))[ra] !=
                ((const int32_t*)td_data(col))[rb]) return true;
            break;
        case TD_SYM:
            if (td_read_sym(td_data(col), ra, col->type, col->attrs) !=
                td_read_sym(td_data(col), rb, col->type, col->attrs)) return true;
            break;
        case TD_I16:
            if (((const int16_t*)td_data(col))[ra] !=
                ((const int16_t*)td_data(col))[rb]) return true;
            break;
        case TD_BOOL: case TD_U8:
            if (((const uint8_t*)td_data(col))[ra] !=
                ((const uint8_t*)td_data(col))[rb]) return true;
            break;
        default: break;
        }
    }
    return false;
}

static inline double win_read_f64(td_t* col, int64_t row) {
    switch (col->type) {
    case TD_F64: return ((const double*)td_data(col))[row];
    case TD_I64: case TD_TIMESTAMP:
        return (double)((const int64_t*)td_data(col))[row];
    case TD_I32: case TD_DATE: case TD_TIME:
        return (double)((const int32_t*)td_data(col))[row];
    case TD_SYM:
        return (double)td_read_sym(td_data(col), row, col->type, col->attrs);
    case TD_I16: return (double)((const int16_t*)td_data(col))[row];
    case TD_BOOL: case TD_U8: return (double)((const uint8_t*)td_data(col))[row];
    default: return 0.0;
    }
}

static inline int64_t win_read_i64(td_t* col, int64_t row) {
    switch (col->type) {
    case TD_I64: case TD_TIMESTAMP:
        return ((const int64_t*)td_data(col))[row];
    case TD_I32: case TD_DATE: case TD_TIME:
        return (int64_t)((const int32_t*)td_data(col))[row];
    case TD_SYM:
        return td_read_sym(td_data(col), row, col->type, col->attrs);
    case TD_F64: return (int64_t)((const double*)td_data(col))[row];
    case TD_I16: return (int64_t)((const int16_t*)td_data(col))[row];
    case TD_BOOL: case TD_U8: return (int64_t)((const uint8_t*)td_data(col))[row];
    default: return 0;
    }
}

/* Resolve a graph op node to a column vector from tbl */
static td_t* win_resolve_vec(td_graph_t* g, td_op_t* key_op, td_t* tbl,
                              uint8_t* owned) {
    td_op_ext_t* key_ext = find_ext(g, key_op->id);
    if (key_ext && key_ext->base.opcode == OP_SCAN) {
        *owned = 0;
        return td_table_get_col(tbl, key_ext->sym);
    }
    *owned = 1;
    td_t* saved = g->table;
    g->table = tbl;
    td_t* v = exec_node(g, key_op);
    g->table = saved;
    return v;
}

/* Compute window functions for one partition [ps, pe) in sorted_idx */
static void win_compute_partition(
    td_t* const* order_vecs, uint8_t n_order,
    td_t* const* func_vecs, const uint8_t* func_kinds, const int64_t* func_params,
    uint8_t n_funcs,
    uint8_t frame_start, uint8_t frame_end,
    const int64_t* sorted_idx, int64_t ps, int64_t pe,
    td_t* const* result_vecs, const bool* is_f64)
{
    if (ps >= pe) return; /* empty partition — nothing to compute */
    int64_t part_len = pe - ps;

    for (uint8_t f = 0; f < n_funcs; f++) {
        uint8_t kind = func_kinds[f];
        td_t* fvec = func_vecs[f];
        td_t* rvec = result_vecs[f];
        bool whole = (frame_start == TD_BOUND_UNBOUNDED_PRECEDING &&
                      frame_end == TD_BOUND_UNBOUNDED_FOLLOWING);

        switch (kind) {
        case TD_WIN_ROW_NUMBER: {
            int64_t* out = (int64_t*)td_data(rvec);
            for (int64_t i = ps; i < pe; i++)
                out[sorted_idx[i]] = i - ps + 1;
            break;
        }
        case TD_WIN_RANK: {
            int64_t* out = (int64_t*)td_data(rvec);
            int64_t rank = 1;
            out[sorted_idx[ps]] = 1;
            for (int64_t i = ps + 1; i < pe; i++) {
                if (n_order > 0 && win_keys_differ(order_vecs, n_order,
                        sorted_idx[i-1], sorted_idx[i]))
                    rank = i - ps + 1;
                out[sorted_idx[i]] = rank;
            }
            break;
        }
        case TD_WIN_DENSE_RANK: {
            int64_t* out = (int64_t*)td_data(rvec);
            int64_t rank = 1;
            out[sorted_idx[ps]] = 1;
            for (int64_t i = ps + 1; i < pe; i++) {
                if (n_order > 0 && win_keys_differ(order_vecs, n_order,
                        sorted_idx[i-1], sorted_idx[i]))
                    rank++;
                out[sorted_idx[i]] = rank;
            }
            break;
        }
        case TD_WIN_NTILE: {
            int64_t n = func_params[f];
            if (n <= 0) n = 1;
            int64_t* out = (int64_t*)td_data(rvec);
            for (int64_t i = ps; i < pe; i++)
                out[sorted_idx[i]] = ((i - ps) * n) / part_len + 1;
            break;
        }
        case TD_WIN_COUNT: {
            int64_t* out = (int64_t*)td_data(rvec);
            if (whole) {
                for (int64_t i = ps; i < pe; i++)
                    out[sorted_idx[i]] = part_len;
            } else {
                for (int64_t i = ps; i < pe; i++)
                    out[sorted_idx[i]] = i - ps + 1;
            }
            break;
        }
        case TD_WIN_SUM: {
            if (!fvec) break;
            if (is_f64[f]) {
                double* out = (double*)td_data(rvec);
                if (whole) {
                    double t = 0.0;
                    for (int64_t i = ps; i < pe; i++)
                        t += win_read_f64(fvec, sorted_idx[i]);
                    for (int64_t i = ps; i < pe; i++)
                        out[sorted_idx[i]] = t;
                } else {
                    double acc = 0.0;
                    for (int64_t i = ps; i < pe; i++) {
                        acc += win_read_f64(fvec, sorted_idx[i]);
                        out[sorted_idx[i]] = acc;
                    }
                }
            } else {
                int64_t* out = (int64_t*)td_data(rvec);
                if (whole) {
                    int64_t t = 0;
                    for (int64_t i = ps; i < pe; i++)
                        t += win_read_i64(fvec, sorted_idx[i]);
                    for (int64_t i = ps; i < pe; i++)
                        out[sorted_idx[i]] = t;
                } else {
                    int64_t acc = 0;
                    for (int64_t i = ps; i < pe; i++) {
                        acc += win_read_i64(fvec, sorted_idx[i]);
                        out[sorted_idx[i]] = acc;
                    }
                }
            }
            break;
        }
        case TD_WIN_AVG: {
            if (!fvec) break;
            double* out = (double*)td_data(rvec);
            if (whole) {
                double t = 0.0;
                for (int64_t i = ps; i < pe; i++)
                    t += win_read_f64(fvec, sorted_idx[i]);
                double avg = t / (double)part_len;
                for (int64_t i = ps; i < pe; i++)
                    out[sorted_idx[i]] = avg;
            } else {
                double acc = 0.0;
                for (int64_t i = ps; i < pe; i++) {
                    acc += win_read_f64(fvec, sorted_idx[i]);
                    out[sorted_idx[i]] = acc / (double)(i - ps + 1);
                }
            }
            break;
        }
        case TD_WIN_MIN: {
            if (!fvec) break;
            if (is_f64[f]) {
                double* out = (double*)td_data(rvec);
                if (whole) {
                    double mn = DBL_MAX;
                    for (int64_t i = ps; i < pe; i++) {
                        double v = win_read_f64(fvec, sorted_idx[i]);
                        if (v < mn) mn = v;
                    }
                    for (int64_t i = ps; i < pe; i++)
                        out[sorted_idx[i]] = mn;
                } else {
                    double mn = DBL_MAX;
                    for (int64_t i = ps; i < pe; i++) {
                        double v = win_read_f64(fvec, sorted_idx[i]);
                        if (v < mn) mn = v;
                        out[sorted_idx[i]] = mn;
                    }
                }
            } else {
                int64_t* out = (int64_t*)td_data(rvec);
                if (whole) {
                    int64_t mn = INT64_MAX;
                    for (int64_t i = ps; i < pe; i++) {
                        int64_t v = win_read_i64(fvec, sorted_idx[i]);
                        if (v < mn) mn = v;
                    }
                    for (int64_t i = ps; i < pe; i++)
                        out[sorted_idx[i]] = mn;
                } else {
                    int64_t mn = INT64_MAX;
                    for (int64_t i = ps; i < pe; i++) {
                        int64_t v = win_read_i64(fvec, sorted_idx[i]);
                        if (v < mn) mn = v;
                        out[sorted_idx[i]] = mn;
                    }
                }
            }
            break;
        }
        case TD_WIN_MAX: {
            if (!fvec) break;
            if (is_f64[f]) {
                double* out = (double*)td_data(rvec);
                if (whole) {
                    double mx = -DBL_MAX;
                    for (int64_t i = ps; i < pe; i++) {
                        double v = win_read_f64(fvec, sorted_idx[i]);
                        if (v > mx) mx = v;
                    }
                    for (int64_t i = ps; i < pe; i++)
                        out[sorted_idx[i]] = mx;
                } else {
                    double mx = -DBL_MAX;
                    for (int64_t i = ps; i < pe; i++) {
                        double v = win_read_f64(fvec, sorted_idx[i]);
                        if (v > mx) mx = v;
                        out[sorted_idx[i]] = mx;
                    }
                }
            } else {
                int64_t* out = (int64_t*)td_data(rvec);
                if (whole) {
                    int64_t mx = INT64_MIN;
                    for (int64_t i = ps; i < pe; i++) {
                        int64_t v = win_read_i64(fvec, sorted_idx[i]);
                        if (v > mx) mx = v;
                    }
                    for (int64_t i = ps; i < pe; i++)
                        out[sorted_idx[i]] = mx;
                } else {
                    int64_t mx = INT64_MIN;
                    for (int64_t i = ps; i < pe; i++) {
                        int64_t v = win_read_i64(fvec, sorted_idx[i]);
                        if (v > mx) mx = v;
                        out[sorted_idx[i]] = mx;
                    }
                }
            }
            break;
        }
        case TD_WIN_LAG: {
            if (!fvec) break;
            int64_t offset = func_params[f];
            if (offset <= 0) offset = 1;
            if (is_f64[f]) {
                double* out = (double*)td_data(rvec);
                for (int64_t i = ps; i < pe; i++) {
                    int64_t src = i - offset;
                    out[sorted_idx[i]] = (src >= ps)
                        ? win_read_f64(fvec, sorted_idx[src]) : NAN;
                }
            } else {
                int64_t* out = (int64_t*)td_data(rvec);
                for (int64_t i = ps; i < pe; i++) {
                    int64_t src = i - offset;
                    out[sorted_idx[i]] = (src >= ps)
                        ? win_read_i64(fvec, sorted_idx[src]) : 0;
                }
            }
            break;
        }
        case TD_WIN_LEAD: {
            if (!fvec) break;
            int64_t offset = func_params[f];
            if (offset <= 0) offset = 1;
            if (is_f64[f]) {
                double* out = (double*)td_data(rvec);
                for (int64_t i = ps; i < pe; i++) {
                    int64_t src = i + offset;
                    out[sorted_idx[i]] = (src < pe)
                        ? win_read_f64(fvec, sorted_idx[src]) : NAN;
                }
            } else {
                int64_t* out = (int64_t*)td_data(rvec);
                for (int64_t i = ps; i < pe; i++) {
                    int64_t src = i + offset;
                    out[sorted_idx[i]] = (src < pe)
                        ? win_read_i64(fvec, sorted_idx[src]) : 0;
                }
            }
            break;
        }
        case TD_WIN_FIRST_VALUE: {
            if (!fvec) break;
            if (is_f64[f]) {
                double* out = (double*)td_data(rvec);
                double first = win_read_f64(fvec, sorted_idx[ps]);
                for (int64_t i = ps; i < pe; i++)
                    out[sorted_idx[i]] = first;
            } else {
                int64_t* out = (int64_t*)td_data(rvec);
                int64_t first = win_read_i64(fvec, sorted_idx[ps]);
                for (int64_t i = ps; i < pe; i++)
                    out[sorted_idx[i]] = first;
            }
            break;
        }
        case TD_WIN_LAST_VALUE: {
            if (!fvec) break;
            if (is_f64[f]) {
                double* out = (double*)td_data(rvec);
                if (whole) {
                    double last = win_read_f64(fvec, sorted_idx[pe - 1]);
                    for (int64_t i = ps; i < pe; i++)
                        out[sorted_idx[i]] = last;
                } else {
                    for (int64_t i = ps; i < pe; i++)
                        out[sorted_idx[i]] = win_read_f64(fvec, sorted_idx[i]);
                }
            } else {
                int64_t* out = (int64_t*)td_data(rvec);
                if (whole) {
                    int64_t last = win_read_i64(fvec, sorted_idx[pe - 1]);
                    for (int64_t i = ps; i < pe; i++)
                        out[sorted_idx[i]] = last;
                } else {
                    for (int64_t i = ps; i < pe; i++)
                        out[sorted_idx[i]] = win_read_i64(fvec, sorted_idx[i]);
                }
            }
            break;
        }
        case TD_WIN_NTH_VALUE: {
            if (!fvec) break;
            int64_t nth = func_params[f];
            if (nth < 1) nth = 1;
            if (is_f64[f]) {
                double* out = (double*)td_data(rvec);
                double val = (nth <= part_len)
                    ? win_read_f64(fvec, sorted_idx[ps + nth - 1]) : NAN;
                for (int64_t i = ps; i < pe; i++)
                    out[sorted_idx[i]] = val;
            } else {
                int64_t* out = (int64_t*)td_data(rvec);
                int64_t val = (nth <= part_len)
                    ? win_read_i64(fvec, sorted_idx[ps + nth - 1]) : 0;
                for (int64_t i = ps; i < pe; i++)
                    out[sorted_idx[i]] = val;
            }
            break;
        }
        } /* switch */
    } /* for each func */
}

/* Parallel per-partition window compute context */
typedef struct {
    td_t** order_vecs;
    uint8_t n_order;
    td_t** func_vecs;
    uint8_t* func_kinds;
    int64_t* func_params;
    uint8_t n_funcs;
    uint8_t frame_start;
    uint8_t frame_end;
    int64_t* sorted_idx;
    int64_t* part_offsets;
    td_t** result_vecs;
    bool* is_f64;
} win_par_ctx_t;

static void win_par_fn(void* arg, uint32_t worker_id,
                       int64_t start, int64_t end) {
    (void)worker_id;
    win_par_ctx_t* ctx = (win_par_ctx_t*)arg;
    for (int64_t p = start; p < end; p++) {
        win_compute_partition(
            ctx->order_vecs, ctx->n_order,
            ctx->func_vecs, ctx->func_kinds, ctx->func_params,
            ctx->n_funcs, ctx->frame_start, ctx->frame_end,
            ctx->sorted_idx, ctx->part_offsets[p], ctx->part_offsets[p + 1],
            ctx->result_vecs, ctx->is_f64);
    }
}

/* Parallel gather of partition key values into contiguous array.
 * Eliminates random-access reads during Phase 2 boundary detection. */
typedef struct {
    const int64_t* sorted_idx;
    uint64_t*      pkey_sorted;
    td_t**         sort_vecs;
    uint8_t        n_part;
} pkey_gather_ctx_t;

static void pkey_gather_fn(void* arg, uint32_t wid,
                            int64_t start, int64_t end) {
    (void)wid;
    pkey_gather_ctx_t* ctx = (pkey_gather_ctx_t*)arg;
    const int64_t* sidx = ctx->sorted_idx;
    uint64_t* out = ctx->pkey_sorted;

    if (ctx->n_part == 1) {
        td_t* pk = ctx->sort_vecs[0];
        const void* pkd = td_data(pk);
        if (TD_IS_SYM(pk->type)) {
            for (int64_t i = start; i < end; i++)
                out[i] = (uint64_t)td_read_sym(pkd, sidx[i], pk->type, pk->attrs);
        } else if (pk->type == TD_I32 || pk->type == TD_DATE || pk->type == TD_TIME) {
            const int32_t* src = (const int32_t*)pkd;
            for (int64_t i = start; i < end; i++)
                out[i] = (uint64_t)((uint32_t)(src[sidx[i]] - INT32_MIN));
        } else {
            const uint64_t* src = (const uint64_t*)pkd;
            for (int64_t i = start; i < end; i++)
                out[i] = src[sidx[i]];
        }
    } else {
        for (int64_t i = start; i < end; i++) {
            int64_t r = sidx[i];
            uint64_t key = 0;
            for (uint8_t k = 0; k < ctx->n_part; k++) {
                td_t* col = ctx->sort_vecs[k];
                const void* d = td_data(col);
                if (TD_IS_SYM(col->type))
                    key = (key << 32) | (uint32_t)td_read_sym(d, r, col->type, col->attrs);
                else if (col->type == TD_I32 || col->type == TD_DATE || col->type == TD_TIME)
                    key = (key << 32) | (uint32_t)(((const int32_t*)d)[r] - INT32_MIN);
                else {
                    key = (key << 32) | (uint32_t)((const uint64_t*)d)[r];
                }
            }
            out[i] = key;
        }
    }
}

static td_t* exec_window(td_graph_t* g, td_op_t* op, td_t* tbl) {
    if (!tbl || TD_IS_ERR(tbl)) return tbl;

    td_op_ext_t* ext = find_ext(g, op->id);
    if (!ext) return TD_ERR_PTR(TD_ERR_NYI);

    int64_t nrows = td_table_nrows(tbl);
    int64_t ncols = td_table_ncols(tbl);
    uint8_t n_part  = ext->window.n_part_keys;
    uint8_t n_order = ext->window.n_order_keys;
    uint8_t n_funcs = ext->window.n_funcs;
    /* Guard against uint8_t overflow on n_part + n_order */
    if ((uint16_t)n_part + n_order > 255)
        return TD_ERR_PTR(TD_ERR_NYI);
    uint8_t n_sort  = n_part + n_order;

    if (nrows == 0 || n_funcs == 0) {
        td_retain(tbl);
        return tbl;
    }

    /* --- Phase 0: Resolve key and func_input vectors --- */
    /* VLAs below are bounded by uint8_t limits (max 255 each),
     * so max ~10KB on stack; bounded by uint8_t limits. */
    td_t* sort_vecs[n_sort > 0 ? n_sort : 1];
    uint8_t sort_owned[n_sort > 0 ? n_sort : 1];
    uint8_t sort_descs[n_sort > 0 ? n_sort : 1];
    memset(sort_owned, 0, sizeof(sort_owned));
    memset(sort_descs, 0, sizeof(sort_descs));

    for (uint8_t k = 0; k < n_part; k++) {
        sort_vecs[k] = win_resolve_vec(g, ext->window.part_keys[k], tbl,
                                        &sort_owned[k]);
        sort_descs[k] = 0;  /* partition keys always ASC */
        if (!sort_vecs[k] || TD_IS_ERR(sort_vecs[k])) {
            td_t* err = sort_vecs[k] ? sort_vecs[k] : TD_ERR_PTR(TD_ERR_NYI);
            for (uint8_t j = 0; j < k; j++)
                if (sort_owned[j] && sort_vecs[j] && !TD_IS_ERR(sort_vecs[j]))
                    td_release(sort_vecs[j]);
            return err;
        }
    }
    for (uint8_t k = 0; k < n_order; k++) {
        sort_vecs[n_part + k] = win_resolve_vec(g, ext->window.order_keys[k],
                                                 tbl, &sort_owned[n_part + k]);
        sort_descs[n_part + k] = ext->window.order_descs[k];
        if (!sort_vecs[n_part + k] || TD_IS_ERR(sort_vecs[n_part + k])) {
            td_t* err = sort_vecs[n_part + k] ? sort_vecs[n_part + k]
                                               : TD_ERR_PTR(TD_ERR_NYI);
            for (uint8_t j = 0; j < n_part + k; j++)
                if (sort_owned[j] && sort_vecs[j] && !TD_IS_ERR(sort_vecs[j]))
                    td_release(sort_vecs[j]);
            return err;
        }
    }

    td_t* func_vecs[n_funcs];
    uint8_t func_owned[n_funcs];
    td_t* result_vecs[n_funcs];
    bool is_f64[n_funcs];
    memset(func_owned, 0, sizeof(func_owned));
    memset(result_vecs, 0, sizeof(result_vecs));
    for (uint8_t f = 0; f < n_funcs; f++) {
        td_op_t* fi = ext->window.func_inputs[f];
        if (fi) {
            func_vecs[f] = win_resolve_vec(g, fi, tbl, &func_owned[f]);
            if (!func_vecs[f] || TD_IS_ERR(func_vecs[f])) {
                td_t* err = func_vecs[f] ? func_vecs[f] : TD_ERR_PTR(TD_ERR_NYI);
                for (uint8_t j = 0; j < f; j++)
                    if (func_owned[j] && func_vecs[j] && !TD_IS_ERR(func_vecs[j]))
                        td_release(func_vecs[j]);
                for (uint8_t j = 0; j < n_sort; j++)
                    if (sort_owned[j] && sort_vecs[j] && !TD_IS_ERR(sort_vecs[j]))
                        td_release(sort_vecs[j]);
                return err;
            }
        } else {
            func_vecs[f] = NULL;
        }
    }

    /* --- Phase 1: Sort by (partition_keys ++ order_keys) --- */
    td_t* radix_itmp_hdr = NULL;
    td_t* win_enum_rank_hdrs[n_sort > 0 ? n_sort : 1];
    memset(win_enum_rank_hdrs, 0, sizeof(win_enum_rank_hdrs));

    td_t* indices_hdr = NULL;
    int64_t* indices = (int64_t*)scratch_alloc(&indices_hdr,
                                (size_t)nrows * sizeof(int64_t));
    if (!indices) goto oom;
    for (int64_t i = 0; i < nrows; i++) indices[i] = i;

    int64_t* sorted_idx = indices;

    if (n_sort > 0 && nrows <= 64) {
        sort_cmp_ctx_t cmp_ctx = {
            .vecs = sort_vecs, .desc = sort_descs,
            .nulls_first = NULL, .n_sort = n_sort,
        };
        sort_insertion(&cmp_ctx, indices, nrows);
    } else if (n_sort > 0) {
        /* --- Radix sort fast path --- */
        bool can_radix = true;
        for (uint8_t k = 0; k < n_sort; k++) {
            if (!sort_vecs[k]) { can_radix = false; break; }
            int8_t t = sort_vecs[k]->type;
            if (t != TD_I64 && t != TD_F64 && t != TD_I32 && t != TD_DATE &&
                t != TD_SYM &&
                t != TD_TIME && t != TD_TIMESTAMP) {
                can_radix = false; break;
            }
        }
        bool radix_done = false;

        if (can_radix) {
            td_pool_t* pool = td_pool_get();

            /* Build SYM rank mappings */
            uint32_t* enum_ranks[n_sort];
            memset(enum_ranks, 0, n_sort * sizeof(uint32_t*));
            for (uint8_t k = 0; k < n_sort; k++) {
                if (TD_IS_SYM(sort_vecs[k]->type)) {
                    enum_ranks[k] = build_enum_rank(sort_vecs[k], nrows,
                                                     &win_enum_rank_hdrs[k]);
                    if (!enum_ranks[k]) { can_radix = false; break; }
                }
            }

            if (can_radix && n_sort == 1) {
                /* Single-key radix sort */
                td_t *keys_hdr;
                uint64_t* keys = (uint64_t*)scratch_alloc(&keys_hdr,
                                    (size_t)nrows * sizeof(uint64_t));
                if (keys) {
                    radix_encode_ctx_t enc = {
                        .keys = keys, .data = td_data(sort_vecs[0]),
                        .type = sort_vecs[0]->type,
                        .col_attrs = sort_vecs[0]->attrs,
                        .desc = sort_descs[0],
                        .nulls_first = sort_descs[0], /* default: NULLS FIRST for DESC */
                        .enum_rank = enum_ranks[0], .n_keys = 1,
                    };
                    if (pool)
                        td_pool_dispatch(pool, radix_encode_fn, &enc, nrows);
                    else
                        radix_encode_fn(&enc, 0, 0, nrows);

                    td_t *ktmp_hdr, *itmp_hdr;
                    uint64_t* ktmp = (uint64_t*)scratch_alloc(&ktmp_hdr,
                                        (size_t)nrows * sizeof(uint64_t));
                    int64_t*  itmp = (int64_t*)scratch_alloc(&itmp_hdr,
                                        (size_t)nrows * sizeof(int64_t));
                    if (ktmp && itmp) {
                        sorted_idx = radix_sort_run(pool, keys, indices,
                                                     ktmp, itmp, nrows);
                        radix_done = (sorted_idx != NULL);
                    }
                    scratch_free(ktmp_hdr);
                    if (sorted_idx != itmp) scratch_free(itmp_hdr);
                    else radix_itmp_hdr = itmp_hdr;
                }
                scratch_free(keys_hdr);
            } else if (can_radix && n_sort > 1) {
                /* Multi-key composite radix sort */
                td_pool_t* pool2 = pool;
                int64_t mins[n_sort], maxs[n_sort];
                uint8_t total_bits = 0;
                bool fits = true;

                if (n_sort <= MK_PRESCAN_MAX_KEYS && pool2) {
                    uint32_t nw = td_pool_total_workers(pool2);
                    size_t pw_count = (size_t)nw * n_sort;
                    int64_t pw_mins_stack[512], pw_maxs_stack[512];
                    td_t *pw_mins_hdr = NULL, *pw_maxs_hdr = NULL;
                    int64_t* pw_mins = (pw_count <= 512)
                        ? pw_mins_stack
                        : (int64_t*)scratch_alloc(&pw_mins_hdr, pw_count * sizeof(int64_t));
                    int64_t* pw_maxs = (pw_count <= 512)
                        ? pw_maxs_stack
                        : (int64_t*)scratch_alloc(&pw_maxs_hdr, pw_count * sizeof(int64_t));
                    for (size_t i = 0; i < pw_count; i++) {
                        pw_mins[i] = INT64_MAX;
                        pw_maxs[i] = INT64_MIN;
                    }
                    mk_prescan_ctx_t pctx = {
                        .vecs = sort_vecs, .enum_ranks = enum_ranks,
                        .n_keys = n_sort, .nrows = nrows, .n_workers = nw,
                        .pw_mins = pw_mins, .pw_maxs = pw_maxs,
                    };
                    td_pool_dispatch(pool2, mk_prescan_fn, &pctx, nrows);

                    for (uint8_t k = 0; k < n_sort; k++) {
                        int64_t kmin = INT64_MAX, kmax = INT64_MIN;
                        for (uint32_t w = 0; w < nw; w++) {
                            int64_t wmin = pw_mins[w * n_sort + k];
                            int64_t wmax = pw_maxs[w * n_sort + k];
                            if (wmin < kmin) kmin = wmin;
                            if (wmax > kmax) kmax = wmax;
                        }
                        mins[k] = kmin;
                        maxs[k] = kmax;
                        uint64_t range = (uint64_t)(kmax - kmin);
                        uint8_t bits = 1;
                        while (((uint64_t)1 << bits) <= range && bits < 64)
                            bits++;
                        total_bits += bits;
                    }
                    if (pw_mins_hdr) scratch_free(pw_mins_hdr);
                    if (pw_maxs_hdr) scratch_free(pw_maxs_hdr);
                } else {
                    for (uint8_t k = 0; k < n_sort; k++) {
                        td_t* col = sort_vecs[k];
                        int64_t kmin = INT64_MAX, kmax = INT64_MIN;
                        if (enum_ranks[k]) {
                            const void* cdata = td_data(col);
                            int8_t ctype = col->type;
                            uint8_t cattrs = col->attrs;
                            for (int64_t i = 0; i < nrows; i++) {
                                uint32_t raw = (uint32_t)td_read_sym(cdata, i, ctype, cattrs);
                                int64_t v = (int64_t)enum_ranks[k][raw];
                                if (v < kmin) kmin = v;
                                if (v > kmax) kmax = v;
                            }
                        } else if (col->type == TD_I64 || col->type == TD_TIMESTAMP) {
                            const int64_t* d = (const int64_t*)td_data(col);
                            for (int64_t i = 0; i < nrows; i++) {
                                if (d[i] < kmin) kmin = d[i];
                                if (d[i] > kmax) kmax = d[i];
                            }
                        } else if (col->type == TD_I32 || col->type == TD_DATE || col->type == TD_TIME) {
                            const int32_t* d = (const int32_t*)td_data(col);
                            for (int64_t i = 0; i < nrows; i++) {
                                if (d[i] < kmin) kmin = (int64_t)d[i];
                                if (d[i] > kmax) kmax = (int64_t)d[i];
                            }
                        }
                        mins[k] = kmin;
                        maxs[k] = kmax;
                        uint64_t range = (uint64_t)(kmax - kmin);
                        uint8_t bits = 1;
                        while (((uint64_t)1 << bits) <= range && bits < 64)
                            bits++;
                        total_bits += bits;
                    }
                }

                if (total_bits > 64) fits = false;

                if (fits) {
                    uint8_t bit_shifts[n_sort];
                    uint8_t accum = 0;
                    for (int k = n_sort - 1; k >= 0; k--) {
                        bit_shifts[k] = accum;
                        uint64_t range = (uint64_t)(maxs[k] - mins[k]);
                        uint8_t bits = 1;
                        while (((uint64_t)1 << bits) <= range && bits < 64)
                            bits++;
                        accum += bits;
                    }

                    td_t *keys_hdr;
                    uint64_t* keys = (uint64_t*)scratch_alloc(&keys_hdr,
                                        (size_t)nrows * sizeof(uint64_t));
                    if (keys) {
                        radix_encode_ctx_t enc = {
                            .keys = keys, .n_keys = n_sort, .vecs = sort_vecs,
                        };
                        for (uint8_t k = 0; k < n_sort; k++) {
                            enc.mins[k] = mins[k];
                            enc.ranges[k] = maxs[k] - mins[k];
                            enc.bit_shifts[k] = bit_shifts[k];
                            enc.descs[k] = sort_descs[k];
                            enc.enum_ranks[k] = enum_ranks[k];
                        }
                        if (pool2)
                            td_pool_dispatch(pool2, radix_encode_fn, &enc, nrows);
                        else
                            radix_encode_fn(&enc, 0, 0, nrows);

                        td_t *ktmp_hdr, *itmp_hdr;
                        uint64_t* ktmp = (uint64_t*)scratch_alloc(&ktmp_hdr,
                                            (size_t)nrows * sizeof(uint64_t));
                        int64_t*  itmp = (int64_t*)scratch_alloc(&itmp_hdr,
                                            (size_t)nrows * sizeof(int64_t));
                        if (ktmp && itmp) {
                            sorted_idx = radix_sort_run(pool2, keys, indices,
                                                         ktmp, itmp, nrows);
                            radix_done = (sorted_idx != NULL);
                        }
                        scratch_free(ktmp_hdr);
                        if (sorted_idx != itmp) scratch_free(itmp_hdr);
                        else radix_itmp_hdr = itmp_hdr;
                    }
                    scratch_free(keys_hdr);
                }
            }
        }

        /* --- Merge sort fallback --- */
        if (!radix_done) {
            sort_cmp_ctx_t cmp_ctx = {
                .vecs = sort_vecs, .desc = sort_descs,
                .nulls_first = NULL, .n_sort = n_sort,
            };
            td_t* tmp_hdr;
            int64_t* tmp = (int64_t*)scratch_alloc(&tmp_hdr,
                                (size_t)nrows * sizeof(int64_t));
            if (!tmp) { scratch_free(indices_hdr); indices_hdr = NULL; goto oom; }

            td_pool_t* pool = td_pool_get();
            uint32_t nw = pool ? td_pool_total_workers(pool) : 1;
            if (pool && nw > 1 && nrows > 1024) {
                sort_phase1_ctx_t p1ctx = {
                    .cmp_ctx = &cmp_ctx, .indices = indices, .tmp = tmp,
                    .nrows = nrows, .n_chunks = nw,
                };
                td_pool_dispatch_n(pool, sort_phase1_fn, &p1ctx, nw);

                int64_t chunk_size = (nrows + nw - 1) / nw;
                int64_t run_size = chunk_size;
                int64_t* src = indices;
                int64_t* dst = tmp;
                while (run_size < nrows) {
                    int64_t n_pairs = (nrows + 2 * run_size - 1) / (2 * run_size);
                    sort_merge_ctx_t mctx = {
                        .cmp_ctx = &cmp_ctx, .src = src, .dst = dst,
                        .nrows = nrows, .run_size = run_size,
                    };
                    if (n_pairs > 1)
                        td_pool_dispatch_n(pool, sort_merge_fn, &mctx,
                                            (uint32_t)n_pairs);
                    else
                        sort_merge_fn(&mctx, 0, 0, n_pairs);
                    int64_t* t = src; src = dst; dst = t;
                    run_size *= 2;
                }
                if (src != indices)
                    memcpy(indices, src, (size_t)nrows * sizeof(int64_t));
            } else {
                sort_merge_recursive(&cmp_ctx, indices, tmp, nrows);
            }
            scratch_free(tmp_hdr);
            sorted_idx = indices;
        }
    }

    /* --- Phase 2: Find partition boundaries --- */
    /* Overallocate part_offsets to worst case (single-pass, no counting pass) */
    td_t* poff_hdr = NULL;
    int64_t* part_offsets = (int64_t*)scratch_alloc(&poff_hdr,
                                (size_t)(nrows + 1) * sizeof(int64_t));
    if (!part_offsets) { scratch_free(indices_hdr); goto oom; }

    part_offsets[0] = 0;
    int64_t n_parts = 0;

    if (n_part > 0) {
        /* Check if we can pack partition keys into uint64 for fast gather.
         * Multi-key packing shifts each key by 32 bits, so any key requiring
         * >32 bits in a multi-key scenario would be truncated.  Force fallback
         * when any 64-bit key appears alongside other keys. */
        uint8_t pk_bits = 0;
        bool can_pack = true;
        bool has_64bit_key = false;
        for (uint8_t k = 0; k < n_part; k++) {
            int8_t t = sort_vecs[k]->type;
            if (TD_IS_SYM(t) || t == TD_I32 || t == TD_DATE || t == TD_TIME) pk_bits += 32;
            else if (t == TD_I64 || t == TD_SYM || t == TD_TIMESTAMP ||
                     t == TD_F64) { pk_bits += 64; has_64bit_key = true; }
            else { can_pack = false; break; }
            if (pk_bits > 64) { can_pack = false; break; }
        }
        /* If multi-key with any 64-bit type, the <<32 packing truncates.
         * Force sequential fallback for correctness. */
        if (can_pack && n_part > 1 && has_64bit_key) can_pack = false;

        td_t* pkey_hdr = NULL;
        uint64_t* pkey_sorted = can_pack ?
            (uint64_t*)scratch_alloc(&pkey_hdr, (size_t)nrows * sizeof(uint64_t))
            : NULL;

        if (pkey_sorted) {
            /* Parallel gather partition keys into contiguous array */
            pkey_gather_ctx_t gctx = {
                .sorted_idx = sorted_idx, .pkey_sorted = pkey_sorted,
                .sort_vecs = sort_vecs, .n_part = n_part,
            };
            td_pool_t* gpool = td_pool_get();
            if (gpool)
                td_pool_dispatch(gpool, pkey_gather_fn, &gctx, nrows);
            else
                pkey_gather_fn(&gctx, 0, 0, nrows);

            /* Sequential scan on contiguous data (no random access) */
            for (int64_t i = 1; i < nrows; i++)
                if (pkey_sorted[i] != pkey_sorted[i - 1])
                    part_offsets[++n_parts] = i;

            scratch_free(pkey_hdr);
        } else {
            /* Fallback: single-pass random-access comparison */
            for (int64_t i = 1; i < nrows; i++)
                if (win_keys_differ(sort_vecs, n_part,
                                    sorted_idx[i - 1], sorted_idx[i]))
                    part_offsets[++n_parts] = i;
        }
        part_offsets[++n_parts] = nrows;
    } else {
        /* No partition keys: entire table is one partition.
         * Minor memory waste (part_offsets sized for nrows+1) but no
         * correctness issue — only indices 0 and 1 are used. */
        part_offsets[1] = nrows;
        n_parts = 1;
    }

    /* Check cancellation before expensive per-partition compute */
    {
        td_pool_t* cpool = td_pool_get();
        if (pool_cancelled(cpool)) {
            scratch_free(poff_hdr);
            scratch_free(indices_hdr);
            if (radix_itmp_hdr) scratch_free(radix_itmp_hdr);
            for (uint8_t k = 0; k < n_sort; k++)
                if (win_enum_rank_hdrs[k]) scratch_free(win_enum_rank_hdrs[k]);
            for (uint8_t k = 0; k < n_sort; k++)
                if (sort_owned[k] && sort_vecs[k] && !TD_IS_ERR(sort_vecs[k]))
                    td_release(sort_vecs[k]);
            for (uint8_t f = 0; f < n_funcs; f++)
                if (func_owned[f] && func_vecs[f] && !TD_IS_ERR(func_vecs[f]))
                    td_release(func_vecs[f]);
            return TD_ERR_PTR(TD_ERR_CANCEL);
        }
    }

    /* --- Phase 3: Allocate result vectors and compute per-partition --- */
    for (uint8_t f = 0; f < n_funcs; f++) {
        uint8_t kind = ext->window.func_kinds[f];
        td_t* fvec = func_vecs[f];

        bool out_f64 = false;
        if (kind == TD_WIN_AVG) {
            out_f64 = true;
        } else if (kind == TD_WIN_SUM || kind == TD_WIN_MIN ||
                   kind == TD_WIN_MAX || kind == TD_WIN_LAG ||
                   kind == TD_WIN_LEAD || kind == TD_WIN_FIRST_VALUE ||
                   kind == TD_WIN_LAST_VALUE || kind == TD_WIN_NTH_VALUE) {
            out_f64 = fvec && fvec->type == TD_F64;
        }

        is_f64[f] = out_f64;
        result_vecs[f] = td_vec_new(out_f64 ? TD_F64 : TD_I64, nrows);
        if (!result_vecs[f] || TD_IS_ERR(result_vecs[f])) {
            for (uint8_t j = 0; j < f; j++) td_release(result_vecs[j]);
            scratch_free(poff_hdr);
            scratch_free(indices_hdr);
            goto oom;
        }
        result_vecs[f]->len = nrows;
        memset(td_data(result_vecs[f]), 0, (size_t)nrows * 8);
    }

    /* Order key vectors start at sort_vecs[n_part] */
    td_t** order_vecs = n_order > 0 ? &sort_vecs[n_part] : NULL;

    {
        td_pool_t* p3pool = td_pool_get();
        if (p3pool && n_parts > 1) {
            win_par_ctx_t pctx = {
                .order_vecs = order_vecs, .n_order = n_order,
                .func_vecs = func_vecs, .func_kinds = ext->window.func_kinds,
                .func_params = ext->window.func_params, .n_funcs = n_funcs,
                .frame_start = ext->window.frame_start,
                .frame_end = ext->window.frame_end,
                .sorted_idx = sorted_idx, .part_offsets = part_offsets,
                .result_vecs = result_vecs, .is_f64 = is_f64,
            };
            td_pool_dispatch_n(p3pool, win_par_fn, &pctx, (uint32_t)n_parts);
        } else {
            for (int64_t p = 0; p < n_parts; p++) {
                win_compute_partition(
                    order_vecs, n_order,
                    func_vecs, ext->window.func_kinds, ext->window.func_params,
                    n_funcs, ext->window.frame_start, ext->window.frame_end,
                    sorted_idx, part_offsets[p], part_offsets[p + 1],
                    result_vecs, is_f64);
            }
        }
    }

    /* --- Phase 4: Build result table --- */
    td_t* result = td_table_new(ncols + n_funcs);
    if (!result || TD_IS_ERR(result)) {
        for (uint8_t f = 0; f < n_funcs; f++) td_release(result_vecs[f]);
        scratch_free(poff_hdr);
        scratch_free(indices_hdr);
        goto oom;
    }

    /* Pass-through original columns */
    for (int64_t c = 0; c < ncols; c++) {
        td_t* col = td_table_get_col_idx(tbl, c);
        if (!col) continue;
        int64_t name_id = td_table_col_name(tbl, c);
        td_retain(col);
        result = td_table_add_col(result, name_id, col);
        td_release(col);
    }

    /* Add window result columns with auto-generated names */
    for (uint8_t f = 0; f < n_funcs; f++) {
        char buf[16] = "_w";
        int pos = 2;
        if (f >= 100) buf[pos++] = '0' + (f / 100);
        if (f >= 10)  buf[pos++] = '0' + ((f / 10) % 10);
        buf[pos++] = '0' + (f % 10);
        buf[pos] = '\0';
        int64_t name_id = td_sym_intern(buf, (size_t)pos);
        result = td_table_add_col(result, name_id, result_vecs[f]);
        td_release(result_vecs[f]);
    }

    scratch_free(poff_hdr);
    if (radix_itmp_hdr) scratch_free(radix_itmp_hdr);
    scratch_free(indices_hdr);
    for (uint8_t k = 0; k < n_sort; k++)
        if (win_enum_rank_hdrs[k]) scratch_free(win_enum_rank_hdrs[k]);

    /* Free owned key/func vectors */
    for (uint8_t k = 0; k < n_sort; k++)
        if (sort_owned[k] && sort_vecs[k] && !TD_IS_ERR(sort_vecs[k]))
            td_release(sort_vecs[k]);
    for (uint8_t f = 0; f < n_funcs; f++)
        if (func_owned[f] && func_vecs[f] && !TD_IS_ERR(func_vecs[f]))
            td_release(func_vecs[f]);

    return result;

oom:
    if (radix_itmp_hdr) scratch_free(radix_itmp_hdr);
    for (uint8_t k = 0; k < n_sort; k++)
        if (win_enum_rank_hdrs[k]) scratch_free(win_enum_rank_hdrs[k]);
    for (uint8_t k = 0; k < n_sort; k++)
        if (sort_owned[k] && sort_vecs[k] && !TD_IS_ERR(sort_vecs[k]))
            td_release(sort_vecs[k]);
    for (uint8_t f = 0; f < n_funcs; f++) {
        if (func_owned[f] && func_vecs[f] && !TD_IS_ERR(func_vecs[f]))
            td_release(func_vecs[f]);
        if (result_vecs[f] && !TD_IS_ERR(result_vecs[f]))
            td_release(result_vecs[f]);
    }
    return TD_ERR_PTR(TD_ERR_OOM);
}

/* ============================================================================
 * Graph execution functions
 * ============================================================================ */

/* exec_expand_factorized: emit factorized output for expand+group fusion.
 * Returns a table with _src (unique sources) and _count (degree per source).
 * This avoids materializing the full (src, dst) cross-product. */
static td_t* exec_expand_factorized(td_rel_t* rel, uint8_t direction, td_t* src_vec) {
    int64_t n_src = src_vec->len;
    int64_t* src_data = (int64_t*)td_data(src_vec);

    /* Compute degrees for each source node */
    td_t* out_src = td_vec_new(TD_I64, n_src > 0 ? n_src : 1);
    td_t* out_cnt = td_vec_new(TD_I64, n_src > 0 ? n_src : 1);
    if (!out_src || TD_IS_ERR(out_src) || !out_cnt || TD_IS_ERR(out_cnt)) {
        if (out_src && !TD_IS_ERR(out_src)) td_release(out_src);
        if (out_cnt && !TD_IS_ERR(out_cnt)) td_release(out_cnt);
        return TD_ERR_PTR(TD_ERR_OOM);
    }

    int64_t* sd = (int64_t*)td_data(out_src);
    int64_t* cd = (int64_t*)td_data(out_cnt);
    int64_t out_len = 0;

    for (int64_t i = 0; i < n_src; i++) {
        int64_t node = src_data[i];
        int64_t deg = 0;
        if (direction == 0 || direction == 2) {
            if (node >= 0 && node < rel->fwd.n_nodes)
                deg += td_csr_degree(&rel->fwd, node);
        }
        if (direction == 1 || direction == 2) {
            if (node >= 0 && node < rel->rev.n_nodes)
                deg += td_csr_degree(&rel->rev, node);
        }
        if (deg > 0) {
            sd[out_len] = node;
            cd[out_len] = deg;
            out_len++;
        }
    }
    out_src->len = out_len;
    out_cnt->len = out_len;

    int64_t src_sym = td_sym_intern("_src", 4);
    int64_t cnt_sym = td_sym_intern("_count", 6);
    td_t* result = td_table_new(2);
    if (!result || TD_IS_ERR(result)) {
        td_release(out_src); td_release(out_cnt);
        return TD_ERR_PTR(TD_ERR_OOM);
    }
    td_t* tmp = td_table_add_col(result, src_sym, out_src);
    if (!tmp || TD_IS_ERR(tmp)) { td_release(out_src); td_release(out_cnt); td_release(result); return TD_ERR_PTR(TD_ERR_OOM); }
    result = tmp;
    tmp = td_table_add_col(result, cnt_sym, out_cnt);
    if (!tmp || TD_IS_ERR(tmp)) { td_release(out_src); td_release(out_cnt); td_release(result); return TD_ERR_PTR(TD_ERR_OOM); }
    result = tmp;
    td_release(out_src); td_release(out_cnt);
    return result;
}

/* exec_expand: 1-hop CSR neighbor expansion.
 * Count-then-fill pattern (same as exec_join). */
static td_t* exec_expand(td_graph_t* g, td_op_t* op, td_t* src_vec) {
    td_op_ext_t* ext = find_ext(g, op->id);
    if (!ext) return TD_ERR_PTR(TD_ERR_NYI);

    td_rel_t* rel = (td_rel_t*)ext->graph.rel;
    if (!rel) return TD_ERR_PTR(TD_ERR_SCHEMA);

    /* Factorized mode: emit pre-aggregated degree counts */
    if (ext->graph.factorized)
        return exec_expand_factorized(rel, ext->graph.direction, src_vec);

    uint8_t direction = ext->graph.direction;
    int64_t n_src = src_vec->len;
    int64_t* src_data = (int64_t*)td_data(src_vec);

    /* SIP runtime: check for source-side selection bitmap stored on the
     * expand ext node (set by optimizer sip_pass or manually for testing).
     *
     * If sip_sel is not pre-built but the optimizer left a filter hint in
     * pad[2..3], build a source-side bitmap by marking all source nodes
     * that have degree > 0 in the active CSR direction. */
    uint64_t* src_sel_bits = NULL;
    int64_t src_sel_len = 0;
    td_t* sip_sel = (td_t*)ext->graph.sip_sel;
    if (!sip_sel) {
        uint8_t filter_hint = ext->base.pad[2];
        if (filter_hint > 0 && n_src > 64) {
            /* Build SIP bitmap: mark source nodes with degree > 0.
             * For direction==2 (both), check both fwd and rev CSRs. */
            int64_t nn = rel->fwd.n_nodes;
            if (rel->rev.n_nodes > nn) nn = rel->rev.n_nodes;
            td_t* built_sel = td_sel_new(nn);
            if (built_sel && !TD_IS_ERR(built_sel)) {
                uint64_t* bits = td_sel_bits(built_sel);
                if (direction == 0 || direction == 2) {
                    for (int64_t nd = 0; nd < rel->fwd.n_nodes; nd++)
                        if (td_csr_degree(&rel->fwd, nd) > 0)
                            TD_SEL_BIT_SET(bits, nd);
                }
                if (direction == 1 || direction == 2) {
                    for (int64_t nd = 0; nd < rel->rev.n_nodes; nd++)
                        if (td_csr_degree(&rel->rev, nd) > 0)
                            TD_SEL_BIT_SET(bits, nd);
                }
                ext->graph.sip_sel = built_sel;
                sip_sel = built_sel;
            }
        }
    }
    if (sip_sel && !TD_IS_ERR(sip_sel) && sip_sel->type == TD_SEL) {
        src_sel_bits = td_sel_bits(sip_sel);
        src_sel_len = sip_sel->len;
    }

    /* Helper to expand one CSR direction */
    #define EXPAND_DIR(csr_ptr) do { \
        td_csr_t* csr = (csr_ptr); \
        /* Phase 1: count total output pairs */ \
        int64_t total = 0; \
        for (int64_t i = 0; i < n_src; i++) { \
            int64_t node = src_data[i]; \
            /* SIP skip: if source node not in selection, skip */ \
            if (src_sel_bits && node >= 0 && node < src_sel_len \
                && !TD_SEL_BIT_TEST(src_sel_bits, node)) continue; \
            if (node >= 0 && node < csr->n_nodes) \
                total += td_csr_degree(csr, node); \
        } \
        /* Phase 2: fill */ \
        td_t* d_src = td_vec_new(TD_I64, total > 0 ? total : 1); \
        td_t* d_dst = td_vec_new(TD_I64, total > 0 ? total : 1); \
        if (!d_src || TD_IS_ERR(d_src) || !d_dst || TD_IS_ERR(d_dst)) { \
            if (d_src && !TD_IS_ERR(d_src)) td_release(d_src); \
            if (d_dst && !TD_IS_ERR(d_dst)) td_release(d_dst); \
            return TD_ERR_PTR(TD_ERR_OOM); \
        } \
        d_src->len = total; d_dst->len = total; \
        int64_t* sd = (int64_t*)td_data(d_src); \
        int64_t* dd = (int64_t*)td_data(d_dst); \
        int64_t pos = 0; \
        for (int64_t i = 0; i < n_src; i++) { \
            int64_t node = src_data[i]; \
            if (node < 0 || node >= csr->n_nodes) continue; \
            /* SIP skip: must match count phase */ \
            if (src_sel_bits && node < src_sel_len \
                && !TD_SEL_BIT_TEST(src_sel_bits, node)) continue; \
            int64_t cnt; \
            int64_t* nbrs = td_csr_neighbors(csr, node, &cnt); \
            for (int64_t j = 0; j < cnt; j++) { \
                sd[pos] = node; \
                dd[pos] = nbrs[j]; \
                pos++; \
            } \
        } \
        /* Build result table */ \
        int64_t src_sym = td_sym_intern("_src", 4); \
        int64_t dst_sym = td_sym_intern("_dst", 4); \
        td_t* result = td_table_new(2); \
        if (!result || TD_IS_ERR(result)) { \
            td_release(d_src); td_release(d_dst); \
            return TD_ERR_PTR(TD_ERR_OOM); \
        } \
        td_t* _tmp = td_table_add_col(result, src_sym, d_src); \
        if (!_tmp || TD_IS_ERR(_tmp)) { td_release(d_src); td_release(d_dst); td_release(result); return TD_ERR_PTR(TD_ERR_OOM); } \
        result = _tmp; \
        _tmp = td_table_add_col(result, dst_sym, d_dst); \
        if (!_tmp || TD_IS_ERR(_tmp)) { td_release(d_src); td_release(d_dst); td_release(result); return TD_ERR_PTR(TD_ERR_OOM); } \
        result = _tmp; \
        td_release(d_src); td_release(d_dst); \
        return result; \
    } while (0)

    if (direction == 0) {
        EXPAND_DIR(&rel->fwd);
    } else if (direction == 1) {
        EXPAND_DIR(&rel->rev);
    } else {
        /* direction == 2: both — expand fwd, then rev, concat */
        td_csr_t* fwd = &rel->fwd;
        td_csr_t* rev = &rel->rev;

        /* Count forward */
        int64_t fwd_total = 0;
        for (int64_t i = 0; i < n_src; i++) {
            int64_t node = src_data[i];
            if (src_sel_bits && node >= 0 && node < src_sel_len
                && !TD_SEL_BIT_TEST(src_sel_bits, node)) continue;
            if (node >= 0 && node < fwd->n_nodes)
                fwd_total += td_csr_degree(fwd, node);
        }
        /* Count reverse */
        int64_t rev_total = 0;
        for (int64_t i = 0; i < n_src; i++) {
            int64_t node = src_data[i];
            if (src_sel_bits && node >= 0 && node < src_sel_len
                && !TD_SEL_BIT_TEST(src_sel_bits, node)) continue;
            if (node >= 0 && node < rev->n_nodes)
                rev_total += td_csr_degree(rev, node);
        }

        int64_t total = fwd_total + rev_total;
        td_t* d_src = td_vec_new(TD_I64, total > 0 ? total : 1);
        td_t* d_dst = td_vec_new(TD_I64, total > 0 ? total : 1);
        if (!d_src || TD_IS_ERR(d_src) || !d_dst || TD_IS_ERR(d_dst)) {
            if (d_src && !TD_IS_ERR(d_src)) td_release(d_src);
            if (d_dst && !TD_IS_ERR(d_dst)) td_release(d_dst);
            return TD_ERR_PTR(TD_ERR_OOM);
        }
        d_src->len = total; d_dst->len = total;
        int64_t* sd = (int64_t*)td_data(d_src);
        int64_t* dd = (int64_t*)td_data(d_dst);
        int64_t pos = 0;

        /* Fill forward */
        for (int64_t i = 0; i < n_src; i++) {
            int64_t node = src_data[i];
            if (node < 0 || node >= fwd->n_nodes) continue;
            if (src_sel_bits && node < src_sel_len
                && !TD_SEL_BIT_TEST(src_sel_bits, node)) continue;
            int64_t cnt;
            int64_t* nbrs = td_csr_neighbors(fwd, node, &cnt);
            for (int64_t j = 0; j < cnt; j++) {
                sd[pos] = node; dd[pos] = nbrs[j]; pos++;
            }
        }
        /* Fill reverse */
        for (int64_t i = 0; i < n_src; i++) {
            int64_t node = src_data[i];
            if (node < 0 || node >= rev->n_nodes) continue;
            if (src_sel_bits && node < src_sel_len
                && !TD_SEL_BIT_TEST(src_sel_bits, node)) continue;
            int64_t cnt;
            int64_t* nbrs = td_csr_neighbors(rev, node, &cnt);
            for (int64_t j = 0; j < cnt; j++) {
                sd[pos] = node; dd[pos] = nbrs[j]; pos++;
            }
        }

        int64_t src_sym = td_sym_intern("_src", 4);
        int64_t dst_sym = td_sym_intern("_dst", 4);
        td_t* result = td_table_new(2);
        if (!result || TD_IS_ERR(result)) {
            td_release(d_src); td_release(d_dst);
            return TD_ERR_PTR(TD_ERR_OOM);
        }
        td_t* tmp = td_table_add_col(result, src_sym, d_src);
        if (!tmp || TD_IS_ERR(tmp)) { td_release(d_src); td_release(d_dst); td_release(result); return TD_ERR_PTR(TD_ERR_OOM); }
        result = tmp;
        tmp = td_table_add_col(result, dst_sym, d_dst);
        if (!tmp || TD_IS_ERR(tmp)) { td_release(d_src); td_release(d_dst); td_release(result); return TD_ERR_PTR(TD_ERR_OOM); }
        result = tmp;
        td_release(d_src); td_release(d_dst);
        return result;
    }
    #undef EXPAND_DIR
}

/* exec_var_expand: iterative BFS with depth limit and cycle detection */
static td_t* exec_var_expand(td_graph_t* g, td_op_t* op, td_t* start_vec) {
    td_op_ext_t* ext = find_ext(g, op->id);
    if (!ext) return TD_ERR_PTR(TD_ERR_NYI);

    td_rel_t* rel = (td_rel_t*)ext->graph.rel;
    if (!rel) return TD_ERR_PTR(TD_ERR_SCHEMA);

    uint8_t direction = ext->graph.direction;
    uint8_t min_depth = ext->graph.min_depth;
    uint8_t max_depth = ext->graph.max_depth;
    td_csr_t* csr_fwd = &rel->fwd;
    td_csr_t* csr_rev = &rel->rev;
    /* For direction==2 (both), use fwd for n_nodes bound but expand both */
    td_csr_t* csr = (direction == 1) ? csr_rev : csr_fwd;

    int64_t n_start = start_vec->len;
    int64_t* start_data = (int64_t*)td_data(start_vec);

    /* Pre-allocate output buffers (grow as needed) */
    int64_t out_cap = 1024;
    td_t *start_hdr, *end_hdr, *depth_hdr;
    int64_t* out_start = (int64_t*)scratch_alloc(&start_hdr, (size_t)out_cap * sizeof(int64_t));
    int64_t* out_end   = (int64_t*)scratch_alloc(&end_hdr,   (size_t)out_cap * sizeof(int64_t));
    int64_t* out_depth = (int64_t*)scratch_alloc(&depth_hdr, (size_t)out_cap * sizeof(int64_t));
    if (!out_start || !out_end || !out_depth) {
        scratch_free(start_hdr); scratch_free(end_hdr); scratch_free(depth_hdr);
        return TD_ERR_PTR(TD_ERR_OOM);
    }
    int64_t out_count = 0;

    /* For direction==2, use the larger n_nodes bound */
    int64_t bfs_n_nodes = csr->n_nodes;
    if (direction == 2 && csr_rev->n_nodes > bfs_n_nodes)
        bfs_n_nodes = csr_rev->n_nodes;

    /* BFS per start node */
    for (int64_t s = 0; s < n_start; s++) {
        int64_t start_node = start_data[s];
        if (start_node < 0 || start_node >= bfs_n_nodes) continue;

        /* Visited bitmap via TD_SEL */
        td_t* visited_sel = td_sel_new(bfs_n_nodes);
        if (!visited_sel || TD_IS_ERR(visited_sel)) continue;
        uint64_t* visited = td_sel_bits(visited_sel);
        TD_SEL_BIT_SET(visited, start_node);

        /* Frontier */
        td_t* front_hdr;
        int64_t front_cap = 256;
        int64_t* frontier = (int64_t*)scratch_alloc(&front_hdr, (size_t)front_cap * sizeof(int64_t));
        if (!frontier) { td_release(visited_sel); continue; }
        frontier[0] = start_node;
        int64_t front_len = 1;

        for (uint8_t depth = 1; depth <= max_depth && front_len > 0; depth++) {
            td_t* next_hdr;
            int64_t next_cap = (front_len > INT64_MAX / 4) ? INT64_MAX : front_len * 4;
            if (next_cap < 64) next_cap = 64;
            int64_t* next_front = (int64_t*)scratch_alloc(&next_hdr, (size_t)next_cap * sizeof(int64_t));
            if (!next_front) { scratch_free(front_hdr); td_release(visited_sel); goto cleanup; }
            int64_t next_len = 0;

            for (int64_t f = 0; f < front_len; f++) {
                int64_t node = frontier[f];
                /* Expand neighbors from active CSR(s).
                 * For direction==2 (both), expand fwd then rev. */
                int n_csrs = (direction == 2) ? 2 : 1;
                td_csr_t* csrs[2] = { csr, csr_rev };
                for (int ci = 0; ci < n_csrs; ci++) {
                    td_csr_t* cur_csr = csrs[ci];
                    if (node < 0 || node >= cur_csr->n_nodes) continue;
                int64_t cnt;
                int64_t* nbrs = td_csr_neighbors(cur_csr, node, &cnt);
                for (int64_t j = 0; j < cnt; j++) {
                    int64_t nbr = nbrs[j];
                    if (nbr < 0 || nbr >= bfs_n_nodes) continue;
                    if (TD_SEL_BIT_TEST(visited, nbr)) continue;
                    TD_SEL_BIT_SET(visited, nbr);

                    /* Grow next_front if needed */
                    if (next_len >= next_cap) {
                        if (next_cap > INT64_MAX / 2) break;
                        int64_t new_cap = next_cap * 2;
                        int64_t* new_nf = (int64_t*)scratch_realloc(&next_hdr,
                            (size_t)next_cap * sizeof(int64_t),
                            (size_t)new_cap * sizeof(int64_t));
                        if (!new_nf) break;
                        next_front = new_nf;
                        next_cap = new_cap;
                    }
                    next_front[next_len++] = nbr;

                    /* Emit if within depth range */
                    if (depth >= min_depth) {
                        if (out_count >= out_cap) {
                            if (out_cap > INT64_MAX / 2) break;
                            int64_t new_oc = out_cap * 2;
                            /* Grow all three buffers atomically — alloc new
                             * copies first, commit only if all succeed. */
                            td_t *ns_h = NULL, *ne_h = NULL, *nd_h = NULL;
                            size_t old_sz = (size_t)out_cap * sizeof(int64_t);
                            size_t new_sz = (size_t)new_oc * sizeof(int64_t);
                            int64_t* ns = (int64_t*)scratch_alloc(&ns_h, new_sz);
                            int64_t* ne = (int64_t*)scratch_alloc(&ne_h, new_sz);
                            int64_t* nd_buf = (int64_t*)scratch_alloc(&nd_h, new_sz);
                            if (!ns || !ne || !nd_buf) {
                                scratch_free(ns_h); scratch_free(ne_h); scratch_free(nd_h);
                                break;
                            }
                            memcpy(ns, out_start, old_sz);
                            memcpy(ne, out_end, old_sz);
                            memcpy(nd_buf, out_depth, old_sz);
                            scratch_free(start_hdr); scratch_free(end_hdr); scratch_free(depth_hdr);
                            start_hdr = ns_h; end_hdr = ne_h; depth_hdr = nd_h;
                            out_start = ns; out_end = ne; out_depth = nd_buf;
                            out_cap = new_oc;
                        }
                        out_start[out_count] = start_node;
                        out_end[out_count] = nbr;
                        out_depth[out_count] = depth;
                        out_count++;
                    }
                }
                } /* end for ci (CSR directions) */
            }

            scratch_free(front_hdr);
            front_hdr = next_hdr;
            frontier = next_front;
            front_len = next_len;
        }

        scratch_free(front_hdr);
        td_release(visited_sel);
    }

cleanup:;
    /* Build output table */
    td_t* v_start = td_vec_from_raw(TD_I64, out_start, out_count);
    td_t* v_end   = td_vec_from_raw(TD_I64, out_end,   out_count);
    td_t* v_depth = td_vec_from_raw(TD_I64, out_depth, out_count);
    scratch_free(start_hdr); scratch_free(end_hdr); scratch_free(depth_hdr);

    if (!v_start || TD_IS_ERR(v_start) || !v_end || TD_IS_ERR(v_end) ||
        !v_depth || TD_IS_ERR(v_depth)) {
        if (v_start && !TD_IS_ERR(v_start)) td_release(v_start);
        if (v_end && !TD_IS_ERR(v_end)) td_release(v_end);
        if (v_depth && !TD_IS_ERR(v_depth)) td_release(v_depth);
        return TD_ERR_PTR(TD_ERR_OOM);
    }

    int64_t start_sym = td_sym_intern("_start", 6);
    int64_t end_sym   = td_sym_intern("_end", 4);
    int64_t depth_sym = td_sym_intern("_depth", 6);

    td_t* result = td_table_new(3);
    if (!result || TD_IS_ERR(result)) {
        td_release(v_start); td_release(v_end); td_release(v_depth);
        return TD_ERR_PTR(TD_ERR_OOM);
    }
    td_t* tmp = td_table_add_col(result, start_sym, v_start);
    if (!tmp || TD_IS_ERR(tmp)) { td_release(v_start); td_release(v_end); td_release(v_depth); td_release(result); return TD_ERR_PTR(TD_ERR_OOM); }
    result = tmp;
    tmp = td_table_add_col(result, end_sym, v_end);
    if (!tmp || TD_IS_ERR(tmp)) { td_release(v_start); td_release(v_end); td_release(v_depth); td_release(result); return TD_ERR_PTR(TD_ERR_OOM); }
    result = tmp;
    tmp = td_table_add_col(result, depth_sym, v_depth);
    if (!tmp || TD_IS_ERR(tmp)) { td_release(v_start); td_release(v_end); td_release(v_depth); td_release(result); return TD_ERR_PTR(TD_ERR_OOM); }
    result = tmp;
    td_release(v_start); td_release(v_end); td_release(v_depth);
    return result;
}

/* exec_shortest_path: BFS from src to dst with parent tracking */
static td_t* exec_shortest_path(td_graph_t* g, td_op_t* op,
                                 td_t* src_val, td_t* dst_val) {
    td_op_ext_t* ext = find_ext(g, op->id);
    if (!ext) return TD_ERR_PTR(TD_ERR_NYI);

    td_rel_t* rel = (td_rel_t*)ext->graph.rel;
    if (!rel) return TD_ERR_PTR(TD_ERR_SCHEMA);
    uint8_t direction = ext->graph.direction;
    td_csr_t* csr = (direction == 1) ? &rel->rev : &rel->fwd;
    td_csr_t* csr_rev = &rel->rev;
    int n_csrs = (direction == 2) ? 2 : 1;
    td_csr_t* csrs[2] = { csr, csr_rev };
    int64_t bfs_n_nodes = csr->n_nodes;
    if (direction == 2 && csr_rev->n_nodes > bfs_n_nodes)
        bfs_n_nodes = csr_rev->n_nodes;
    uint8_t max_depth = ext->graph.max_depth;

    /* Extract single I64 values */
    int64_t src_node, dst_node;
    if (td_is_atom(src_val)) {
        src_node = src_val->i64;
    } else {
        if (src_val->len == 0) return TD_ERR_PTR(TD_ERR_RANGE);
        src_node = ((int64_t*)td_data(src_val))[0];
    }
    if (td_is_atom(dst_val)) {
        dst_node = dst_val->i64;
    } else {
        if (dst_val->len == 0) return TD_ERR_PTR(TD_ERR_RANGE);
        dst_node = ((int64_t*)td_data(dst_val))[0];
    }

    if (src_node < 0 || src_node >= bfs_n_nodes ||
        dst_node < 0 || dst_node >= bfs_n_nodes)
        return TD_ERR_PTR(TD_ERR_RANGE);

    /* Special case: src == dst */
    if (src_node == dst_node) {
        td_t* v_node = td_vec_from_raw(TD_I64, &src_node, 1);
        int64_t zero = 0;
        td_t* v_depth = td_vec_from_raw(TD_I64, &zero, 1);
        if (!v_node || TD_IS_ERR(v_node) || !v_depth || TD_IS_ERR(v_depth)) {
            if (v_node && !TD_IS_ERR(v_node)) td_release(v_node);
            if (v_depth && !TD_IS_ERR(v_depth)) td_release(v_depth);
            return TD_ERR_PTR(TD_ERR_OOM);
        }
        td_t* result = td_table_new(2);
        if (!result || TD_IS_ERR(result)) { td_release(v_node); td_release(v_depth); return TD_ERR_PTR(TD_ERR_OOM); }
        td_t* tmp = td_table_add_col(result, td_sym_intern("_node", 5), v_node);
        if (!tmp || TD_IS_ERR(tmp)) { td_release(v_node); td_release(v_depth); td_release(result); return TD_ERR_PTR(TD_ERR_OOM); }
        result = tmp;
        tmp = td_table_add_col(result, td_sym_intern("_depth", 6), v_depth);
        if (!tmp || TD_IS_ERR(tmp)) { td_release(v_node); td_release(v_depth); td_release(result); return TD_ERR_PTR(TD_ERR_OOM); }
        result = tmp;
        td_release(v_node); td_release(v_depth);
        return result;
    }

    /* Allocate parent array (-1 = unvisited) */
    td_t* parent_hdr;
    int64_t* parent = (int64_t*)scratch_alloc(&parent_hdr,
                                               (size_t)bfs_n_nodes * sizeof(int64_t));
    if (!parent) return TD_ERR_PTR(TD_ERR_OOM);
    memset(parent, 0xFF, (size_t)bfs_n_nodes * sizeof(int64_t)); /* -1 */
    parent[src_node] = src_node;

    /* BFS queue */
    td_t* queue_hdr;
    int64_t q_cap = 1024;
    int64_t* queue = (int64_t*)scratch_alloc(&queue_hdr, (size_t)q_cap * sizeof(int64_t));
    if (!queue) { scratch_free(parent_hdr); return TD_ERR_PTR(TD_ERR_OOM); }
    queue[0] = src_node;
    int64_t q_start = 0, q_end = 1;
    bool found = false;

    for (uint8_t depth = 1; depth <= max_depth && !found; depth++) {
        int64_t level_end = q_end;
        for (int64_t qi = q_start; qi < level_end && !found; qi++) {
            int64_t node = queue[qi];
            for (int ci = 0; ci < n_csrs && !found; ci++) {
                td_csr_t* cur_csr = csrs[ci];
                if (node < 0 || node >= cur_csr->n_nodes) continue;
                int64_t cnt;
                int64_t* nbrs = td_csr_neighbors(cur_csr, node, &cnt);
                for (int64_t j = 0; j < cnt; j++) {
                    int64_t nbr = nbrs[j];
                    if (nbr < 0 || nbr >= bfs_n_nodes) continue;
                    if (parent[nbr] != -1) continue;
                    parent[nbr] = node;

                    if (nbr == dst_node) { found = true; break; }

                    /* Grow queue if needed */
                    if (q_end >= q_cap) {
                        if (q_cap > INT64_MAX / 2) { found = false; goto bfs_done; }
                        int64_t new_cap = q_cap * 2;
                        int64_t* new_q = (int64_t*)scratch_realloc(&queue_hdr,
                            (size_t)q_cap * sizeof(int64_t),
                            (size_t)new_cap * sizeof(int64_t));
                        if (!new_q) { found = false; goto bfs_done; }
                        queue = new_q;
                        q_cap = new_cap;
                    }
                    queue[q_end++] = nbr;
                }
            } /* end for ci (CSR directions) */
        }
        q_start = level_end;
    }

bfs_done:
    scratch_free(queue_hdr);

    if (!found) {
        scratch_free(parent_hdr);
        return TD_ERR_PTR(TD_ERR_RANGE);
    }

    /* Reconstruct path */
    int64_t path_buf[256];
    int64_t path_len = 0;
    int64_t cur = dst_node;
    while (cur != src_node && path_len < 255) {
        path_buf[path_len++] = cur;
        cur = parent[cur];
    }
    if (path_len < 256)
        path_buf[path_len++] = src_node;
    scratch_free(parent_hdr);

    /* Reverse path */
    for (int64_t i = 0; i < path_len / 2; i++) {
        int64_t tmp = path_buf[i];
        path_buf[i] = path_buf[path_len - 1 - i];
        path_buf[path_len - 1 - i] = tmp;
    }

    /* Build output table */
    td_t* v_node = td_vec_from_raw(TD_I64, path_buf, path_len);
    td_t* v_depth = td_vec_new(TD_I64, path_len);
    if (!v_node || TD_IS_ERR(v_node) || !v_depth || TD_IS_ERR(v_depth)) {
        if (v_node && !TD_IS_ERR(v_node)) td_release(v_node);
        if (v_depth && !TD_IS_ERR(v_depth)) td_release(v_depth);
        return TD_ERR_PTR(TD_ERR_OOM);
    }
    v_depth->len = path_len;
    int64_t* dep_data = (int64_t*)td_data(v_depth);
    for (int64_t i = 0; i < path_len; i++) dep_data[i] = i;

    int64_t node_sym  = td_sym_intern("_node", 5);
    int64_t depth_sym = td_sym_intern("_depth", 6);
    td_t* result = td_table_new(2);
    if (!result || TD_IS_ERR(result)) { td_release(v_node); td_release(v_depth); return TD_ERR_PTR(TD_ERR_OOM); }
    td_t* tmp = td_table_add_col(result, node_sym, v_node);
    if (!tmp || TD_IS_ERR(tmp)) { td_release(v_node); td_release(v_depth); td_release(result); return TD_ERR_PTR(TD_ERR_OOM); }
    result = tmp;
    tmp = td_table_add_col(result, depth_sym, v_depth);
    if (!tmp || TD_IS_ERR(tmp)) { td_release(v_node); td_release(v_depth); td_release(result); return TD_ERR_PTR(TD_ERR_OOM); }
    result = tmp;
    td_release(v_node); td_release(v_depth);
    return result;
}

/* exec_wco_join: Worst-Case Optimal Join via general Leapfrog Triejoin */
static td_t* exec_wco_join(td_graph_t* g, td_op_t* op) {
    td_op_ext_t* ext = find_ext(g, op->id);
    if (!ext) return TD_ERR_PTR(TD_ERR_NYI);

    td_rel_t** rels = (td_rel_t**)ext->wco.rels;
    uint8_t n_rels = ext->wco.n_rels;
    uint8_t n_vars = ext->wco.n_vars;

    if (!rels || n_rels == 0) return TD_ERR_PTR(TD_ERR_SCHEMA);
    if (n_vars > LFTJ_MAX_VARS) return TD_ERR_PTR(TD_ERR_NYI);

    /* Validate sorted CSR */
    for (uint8_t r = 0; r < n_rels; r++) {
        if (!rels[r] || !rels[r]->fwd.sorted)
            return TD_ERR_PTR(TD_ERR_DOMAIN);
    }

    /* Build binding plan */
    lftj_enum_ctx_t ctx;
    memset(&ctx, 0, sizeof(ctx));
    if (!lftj_build_default_plan(&ctx, rels, n_rels, n_vars))
        return TD_ERR_PTR(TD_ERR_NYI);

    /* Allocate output buffers */
    int64_t out_cap = 4096;
    td_t* col_data_block;
    int64_t** col_data = (int64_t**)scratch_alloc(&col_data_block,
                              (size_t)n_vars * sizeof(int64_t*));
    if (!col_data) {
        scratch_free(col_data_block);
        return TD_ERR_PTR(TD_ERR_OOM);
    }

    for (uint8_t v = 0; v < n_vars; v++) {
        td_t* h = td_alloc((size_t)out_cap * sizeof(int64_t));
        if (!h) {
            for (uint8_t j = 0; j < v; j++) td_free(ctx.buf_hdrs[j]);
            scratch_free(col_data_block);
            return TD_ERR_PTR(TD_ERR_OOM);
        }
        ctx.buf_hdrs[v] = h;
        col_data[v] = (int64_t*)td_data(h);
    }

    ctx.col_data = col_data;
    ctx.out_count = 0;
    ctx.out_cap = out_cap;
    ctx.oom = false;

    /* Run general LFTJ enumeration */
    lftj_enumerate(&ctx, 0);

    if (ctx.oom) {
        for (uint8_t v = 0; v < n_vars; v++) td_free(ctx.buf_hdrs[v]);
        scratch_free(col_data_block);
        return TD_ERR_PTR(TD_ERR_OOM);
    }

    /* Build output table */
    td_t* result = td_table_new(n_vars);
    if (!result || TD_IS_ERR(result)) {
        for (uint8_t v = 0; v < n_vars; v++) td_free(ctx.buf_hdrs[v]);
        scratch_free(col_data_block);
        return TD_ERR_PTR(TD_ERR_OOM);
    }

    for (uint8_t v = 0; v < n_vars; v++) {
        td_t* vec = td_vec_from_raw(TD_I64, ctx.col_data[v], ctx.out_count);
        td_free(ctx.buf_hdrs[v]);
        if (!vec || TD_IS_ERR(vec)) {
            for (uint8_t j = v + 1; j < n_vars; j++) td_free(ctx.buf_hdrs[j]);
            scratch_free(col_data_block);
            td_release(result);
            return TD_ERR_PTR(TD_ERR_OOM);
        }
        char name_buf[12];
        int n = snprintf(name_buf, sizeof(name_buf), "_v%d", v);
        int64_t name_id = td_sym_intern(name_buf, (size_t)n);
        td_t* new_result = td_table_add_col(result, name_id, vec);
        td_release(vec);
        if (!new_result || TD_IS_ERR(new_result)) {
            for (uint8_t j = v + 1; j < n_vars; j++) td_free(ctx.buf_hdrs[j]);
            scratch_free(col_data_block);
            td_release(result);
            return TD_ERR_PTR(TD_ERR_OOM);
        }
        result = new_result;
    }

    scratch_free(col_data_block);
    return result;
}

/* ============================================================================
 * Recursive executor
 * ============================================================================ */

static td_t* exec_node(td_graph_t* g, td_op_t* op) {
    if (!op) return TD_ERR_PTR(TD_ERR_NYI);

    switch (op->opcode) {
        case OP_SCAN: {
            td_op_ext_t* ext = find_ext(g, op->id);
            if (!ext) return TD_ERR_PTR(TD_ERR_NYI);

            /* Resolve table: pad[0..1] stores table_id+1 (0 = default g->table) */
            uint16_t stored_table_id = 0;
            memcpy(&stored_table_id, ext->base.pad, sizeof(uint16_t));
            td_t* scan_tbl;
            if (stored_table_id > 0 && g->tables && (stored_table_id - 1) < g->n_tables) {
                scan_tbl = g->tables[stored_table_id - 1];
            } else {
                scan_tbl = g->table;
            }
            if (!scan_tbl) return TD_ERR_PTR(TD_ERR_SCHEMA);
            td_t* col = td_table_get_col(scan_tbl, ext->sym);
            if (!col) return TD_ERR_PTR(TD_ERR_SCHEMA);
            if (col->type == TD_MAPCOMMON)
                return materialize_mapcommon(col);
            if (TD_IS_PARTED(col->type)) {
                /* Concat parted segments into flat vector (cold path) */
                int8_t base = (int8_t)TD_PARTED_BASETYPE(col->type);
                td_t** sps = (td_t**)td_data(col);
                uint8_t sba = (base == TD_SYM && col->len > 0 && sps[0])
                            ? sps[0]->attrs : 0;
                int64_t total = td_parted_nrows(col);
                td_t* flat = typed_vec_new(base, sba, total);
                if (!flat || TD_IS_ERR(flat)) return TD_ERR_PTR(TD_ERR_OOM);
                flat->len = total;
                td_t** segs = sps;
                size_t esz = (size_t)td_sym_elem_size(base, sba);
                int64_t off = 0;
                for (int64_t s = 0; s < col->len; s++) {
                    if (segs[s] && segs[s]->len > 0) {
                        memcpy((char*)td_data(flat) + off * esz,
                               td_data(segs[s]), (size_t)segs[s]->len * esz);
                        off += segs[s]->len;
                    }
                }
                return flat;
            }
            td_retain(col);
            return col;
        }

        case OP_CONST: {
            td_op_ext_t* ext = find_ext(g, op->id);
            if (!ext || !ext->literal) return TD_ERR_PTR(TD_ERR_NYI);
            td_retain(ext->literal);
            return ext->literal;
        }

        /* Unary element-wise */
        case OP_NEG: case OP_ABS: case OP_NOT: case OP_SQRT:
        case OP_LOG: case OP_EXP: case OP_CEIL: case OP_FLOOR:
        case OP_ISNULL: case OP_CAST:
        /* Binary element-wise */
        case OP_ADD: case OP_SUB: case OP_MUL: case OP_DIV: case OP_MOD:
        case OP_EQ: case OP_NE: case OP_LT: case OP_LE:
        case OP_GT: case OP_GE: case OP_AND: case OP_OR:
        case OP_MIN2: case OP_MAX2: {
            /* Try compiled expression first (fuses entire subtree) */
            if (g->table) {
                int64_t nr = td_table_nrows(g->table);
                if (nr > 0) {
                    td_expr_t ex;
                    if (expr_compile(g, g->table, op, &ex)) {
                        td_t* vec = expr_eval_full(&ex, nr);
                        if (vec && !TD_IS_ERR(vec)) return vec;
                    }
                }
            }
            /* Fallback: recursive per-node evaluation */
            if (op->arity == 1) {
                td_t* input = exec_node(g, op->inputs[0]);
                if (!input || TD_IS_ERR(input)) return input;
                td_t* result = exec_elementwise_unary(g, op, input);
                td_release(input);
                return result;
            } else {
                td_t* lhs = exec_node(g, op->inputs[0]);
                td_t* rhs = exec_node(g, op->inputs[1]);
                if (!lhs || TD_IS_ERR(lhs)) { if (rhs && !TD_IS_ERR(rhs)) td_release(rhs); return lhs; }
                if (!rhs || TD_IS_ERR(rhs)) { td_release(lhs); return rhs; }
                td_t* result = exec_elementwise_binary(g, op, lhs, rhs);
                td_release(lhs);
                td_release(rhs);
                return result;
            }
        }

        /* Reductions */
        case OP_SUM: case OP_PROD: case OP_MIN: case OP_MAX:
        case OP_COUNT: case OP_AVG: case OP_FIRST: case OP_LAST:
        case OP_STDDEV: case OP_STDDEV_POP: case OP_VAR: case OP_VAR_POP: {
            td_t* input = exec_node(g, op->inputs[0]);
            if (!input || TD_IS_ERR(input)) return input;
            td_t* result = exec_reduction(g, op, input);
            td_release(input);
            return result;
        }

        case OP_FILTER: {
            /* HAVING fusion: FILTER(GROUP) — evaluate the predicate against
             * the GROUP result rather than the original input table.
             * SCAN nodes in the predicate tree resolve column names via
             * g->table, so we temporarily swap it to the GROUP output. */
            td_op_t* filter_child = op->inputs[0];
            if (filter_child && filter_child->opcode == OP_GROUP) {
                td_t* group_result = exec_node(g, filter_child);
                if (!group_result || TD_IS_ERR(group_result))
                    return group_result;

                td_t* saved_table = g->table;
                td_t* saved_sel   = g->selection;
                g->table     = group_result;
                g->selection = NULL;

                td_t* pred = exec_node(g, op->inputs[1]);

                g->table     = saved_table;
                g->selection = saved_sel;

                if (!pred || TD_IS_ERR(pred)) {
                    td_release(group_result);
                    return pred;
                }

                td_t* result = exec_filter(g, op, group_result, pred);
                td_release(pred);
                td_release(group_result);
                return result;
            }

            td_t* input = exec_node(g, op->inputs[0]);
            td_t* pred  = exec_node(g, op->inputs[1]);
            if (!input || TD_IS_ERR(input)) { if (pred && !TD_IS_ERR(pred)) td_release(pred); return input; }
            if (!pred || TD_IS_ERR(pred)) { td_release(input); return pred; }

            /* Lazy filter: convert predicate to TD_SEL bitmap instead of
             * materializing a compacted table.  Only for TABLE inputs —
             * downstream ops (group-by) consume the bitmap directly;
             * boundary ops (sort/join/window) compact on demand.
             * Vector inputs must still materialize immediately since
             * downstream ops like COUNT rely on compacted length. */
            if (pred->type == TD_BOOL && input->type == TD_TABLE) {
                td_t* new_sel = td_sel_from_pred(pred);
                td_release(pred);
                if (!new_sel || TD_IS_ERR(new_sel)) { td_release(input); return new_sel; }

                if (g->selection) {
                    /* Chained filter: AND with existing selection */
                    td_t* merged = td_sel_and(g->selection, new_sel);
                    td_release(new_sel);
                    td_release(g->selection);
                    g->selection = merged;
                } else {
                    g->selection = new_sel;
                }
                return input;  /* original table, not compacted */
            }

            /* Eager filter for vector inputs and non-BOOL predicates */
            td_t* result = exec_filter(g, op, input, pred);
            td_release(input);
            td_release(pred);
            return result;
        }

        case OP_SORT: {
            td_t* input = exec_node(g, op->inputs[0]);
            if (!input || TD_IS_ERR(input)) return input;
            td_t* tbl = (input->type == TD_TABLE) ? input : g->table;
            /* Compact lazy selection before sort (needs dense data) */
            if (g->selection && tbl && !TD_IS_ERR(tbl) && tbl->type == TD_TABLE) {
                td_t* compacted = sel_compact(g, tbl, g->selection);
                if (input != g->table) td_release(input);
                td_release(g->selection);
                g->selection = NULL;
                input = compacted;
                tbl = compacted;
            }
            td_t* result = exec_sort(g, op, tbl, 0);
            if (input != g->table) td_release(input);
            return result;
        }

        case OP_GROUP: {
            td_t* tbl = g->table;
            td_t* owned_tbl = NULL;

            /* Factorized pipeline: detect OP_EXPAND (factorized) → OP_GROUP.
             * When the group key is _src and there's a factorized expand node
             * in the graph, execute the expand first and pipe its output as
             * the group input table.  This connects the expand→group pipeline
             * that would otherwise disconnect since GROUP reads g->table. */
            {
                td_op_ext_t* gext = find_ext(g, op->id);
                if (gext && gext->n_keys == 1) {
                    td_op_ext_t* kx = find_ext(g, gext->keys[0]->id);
                    int64_t src_sym = td_sym_intern("_src", 4);
                    if (kx && kx->base.opcode == OP_SCAN && kx->sym == src_sym) {
                        /* Find the factorized OP_EXPAND connected to this GROUP.
                         * The expand must be the one whose output the GROUP
                         * is scanning (connected via OP_SCAN inputs). */
                        for (uint32_t ei = 0; ei < g->ext_count; ei++) {
                            td_op_ext_t* ex = g->ext_nodes[ei];
                            if (ex && ex->base.id < g->node_count
                                && g->nodes[ex->base.id].opcode == OP_EXPAND
                                && ex->graph.factorized) {
                                td_op_t* expand_op = &g->nodes[ex->base.id];
                                td_t* expand_result = exec_node(g, expand_op);
                                if (!expand_result || TD_IS_ERR(expand_result))
                                    return expand_result;
                                if (expand_result->type == TD_TABLE) {
                                    td_t* saved = g->table;
                                    g->table = expand_result;
                                    td_t* result = exec_group(g, op, expand_result, 0);
                                    g->table = saved;
                                    td_release(expand_result);
                                    return result;
                                }
                                td_release(expand_result);
                                break;
                            }
                        }
                    }
                }
            }

            /* Compact lazy selection if group-by has expression inputs.
             * Expression evaluation (expr_eval_full / exec_node) runs on
             * all nrows — with lazy filter that's the full unfiltered table.
             * Compacting first shrinks the input so expressions evaluate on
             * only the passing rows. For simple SCAN inputs this is skipped
             * and exec_group uses the bitmap for segment-level skip. */
            if (g->selection && g->selection->type == TD_SEL) {
                td_op_ext_t* gext = find_ext(g, op->id);
                if (gext) {
                    bool needs = false;
                    for (uint8_t k = 0; k < gext->n_keys && !needs; k++) {
                        td_op_ext_t* kx = find_ext(g, gext->keys[k]->id);
                        if (!kx || kx->base.opcode != OP_SCAN)
                            needs = true;
                    }
                    for (uint8_t a = 0; a < gext->n_aggs && !needs; a++) {
                        if (gext->agg_ops[a] == OP_COUNT) continue;
                        td_op_ext_t* ax = find_ext(g, gext->agg_ins[a]->id);
                        if (!ax || (ax->base.opcode != OP_SCAN
                                    && !(ax->base.opcode == OP_CONST
                                         && ax->literal)))
                            needs = true;
                    }
                    if (needs) {
                        td_t* compacted = sel_compact(g, tbl, g->selection);
                        if (!compacted || TD_IS_ERR(compacted)) return compacted;
                        td_release(g->selection);
                        g->selection = NULL;
                        owned_tbl = compacted;
                        tbl = compacted;
                    }
                }
            }
            td_t* result = exec_group(g, op, tbl, 0);
            if (owned_tbl) td_release(owned_tbl);
            return result;
        }

        case OP_JOIN: {
            td_t* left = exec_node(g, op->inputs[0]);
            td_t* right = exec_node(g, op->inputs[1]);
            if (!left || TD_IS_ERR(left)) { if (right && !TD_IS_ERR(right)) td_release(right); return left; }
            if (!right || TD_IS_ERR(right)) { td_release(left); return right; }
            /* Compact lazy selection before join (needs dense data) */
            if (g->selection && left && !TD_IS_ERR(left) && left->type == TD_TABLE) {
                td_t* compacted = sel_compact(g, left, g->selection);
                td_release(left);
                td_release(g->selection);
                g->selection = NULL;
                left = compacted;
            }
            td_t* result = exec_join(g, op, left, right);
            td_release(left);
            td_release(right);
            return result;
        }

        case OP_WINDOW: {
            td_t* input = exec_node(g, op->inputs[0]);
            if (!input || TD_IS_ERR(input)) return input;
            td_t* wdf = (input->type == TD_TABLE) ? input : g->table;
            /* Compact lazy selection before window (needs dense data) */
            if (g->selection && wdf && !TD_IS_ERR(wdf) && wdf->type == TD_TABLE) {
                td_t* compacted = sel_compact(g, wdf, g->selection);
                if (input != g->table) td_release(input);
                td_release(g->selection);
                g->selection = NULL;
                input = compacted;
                wdf = compacted;
            }
            td_t* result = exec_window(g, op, wdf);
            if (input != g->table) td_release(input);
            return result;
        }

        case OP_HEAD: {
            td_op_ext_t* ext = find_ext(g, op->id);
            int64_t n = ext ? ext->sym : 10;

            /* Fused sort+limit: detect SORT child → only gather N rows */
            td_op_t* child_op = op->inputs[0];
            if (child_op && child_op->opcode == OP_SORT) {
                td_t* sort_input = exec_node(g, child_op->inputs[0]);
                if (!sort_input || TD_IS_ERR(sort_input)) return sort_input;
                td_t* tbl = (sort_input->type == TD_TABLE) ? sort_input : g->table;
                /* Compact lazy selection before sort */
                if (g->selection && tbl && !TD_IS_ERR(tbl) && tbl->type == TD_TABLE) {
                    td_t* compacted = sel_compact(g, tbl, g->selection);
                    if (sort_input != g->table) td_release(sort_input);
                    td_release(g->selection);
                    g->selection = NULL;
                    sort_input = compacted;
                    tbl = compacted;
                }
                td_t* result = exec_sort(g, child_op, tbl, n);
                if (sort_input != g->table) td_release(sort_input);
                return result;
            }

            /* HEAD(GROUP) optimization: pass limit hint to exec_group
             * so it can short-circuit the per-partition loop when all
             * GROUP BY keys are MAPCOMMON.  The normal HEAD logic below
             * still trims the result to N rows regardless. */
            td_t* input;
            if (child_op && child_op->opcode == OP_GROUP) {
                td_t* tbl = g->table;
                if (!tbl || TD_IS_ERR(tbl)) return tbl;
                td_t* owned_tbl = NULL;
                if (g->selection && tbl->type == TD_TABLE) {
                    int needs = 0;
                    int64_t nc = td_table_ncols(tbl);
                    for (int64_t c = 0; c < nc; c++) {
                        td_t* col = td_table_get_col_idx(tbl, c);
                        if (col && !TD_IS_PARTED(col->type)
                            && col->type != TD_MAPCOMMON) {
                            needs = 1; break;
                        }
                    }
                    if (needs) {
                        td_t* compacted = sel_compact(g, tbl, g->selection);
                        if (!compacted || TD_IS_ERR(compacted)) return compacted;
                        td_release(g->selection);
                        g->selection = NULL;
                        owned_tbl = compacted;
                        tbl = compacted;
                    }
                }
                input = exec_group(g, child_op, tbl, n);
                if (owned_tbl) td_release(owned_tbl);
            } else if (child_op && child_op->opcode == OP_FILTER) {
                /* HEAD(FILTER): early-termination filter — gather only
                 * the first N matching rows instead of all matches. */
                td_t* filter_input = exec_node(g, child_op->inputs[0]);
                if (!filter_input || TD_IS_ERR(filter_input))
                    return filter_input;

                /* Compact lazy selection before filter evaluation */
                td_t* ftbl = (filter_input->type == TD_TABLE)
                           ? filter_input : g->table;
                if (g->selection && ftbl && ftbl->type == TD_TABLE) {
                    td_t* compacted = sel_compact(g, ftbl, g->selection);
                    if (filter_input != g->table) td_release(filter_input);
                    td_release(g->selection);
                    g->selection = NULL;
                    filter_input = compacted;
                    ftbl = compacted;
                }

                /* Swap table for predicate evaluation */
                td_t* saved_table = g->table;
                g->table = ftbl;
                td_t* pred = exec_node(g, child_op->inputs[1]);
                g->table = saved_table;

                if (!pred || TD_IS_ERR(pred)) {
                    if (filter_input != saved_table)
                        td_release(filter_input);
                    return pred;
                }

                td_t* result = exec_filter_head(ftbl, pred, n);
                td_release(pred);
                if (filter_input != saved_table)
                    td_release(filter_input);
                return result;
            } else {
                input = exec_node(g, op->inputs[0]);
            }
            if (!input || TD_IS_ERR(input)) return input;
            if (input->type == TD_TABLE) {
                int64_t ncols = td_table_ncols(input);
                int64_t nrows = td_table_nrows(input);
                if (n > nrows) n = nrows;
                td_t* result = td_table_new(ncols);
                for (int64_t c = 0; c < ncols; c++) {
                    td_t* col = td_table_get_col_idx(input, c);
                    int64_t name_id = td_table_col_name(input, c);
                    if (!col) continue;
                    if (col->type == TD_MAPCOMMON) {
                        td_t* mc_head = materialize_mapcommon_head(col, n);
                        if (mc_head && !TD_IS_ERR(mc_head)) {
                            result = td_table_add_col(result, name_id, mc_head);
                            td_release(mc_head);
                        }
                        continue;
                    }
                    if (TD_IS_PARTED(col->type)) {
                        /* Copy first n rows from parted segments */
                        int8_t base = (int8_t)TD_PARTED_BASETYPE(col->type);
                        td_t** sp = (td_t**)td_data(col);
                        uint8_t ba = (base == TD_SYM && col->len > 0 && sp[0])
                                   ? sp[0]->attrs : 0;
                        uint8_t esz = td_sym_elem_size(base, ba);
                        td_t* head_vec = typed_vec_new(base, ba, n);
                        if (head_vec && !TD_IS_ERR(head_vec)) {
                            head_vec->len = n;
                            td_t** segs = (td_t**)td_data(col);
                            int64_t remaining = n;
                            int64_t dst_off = 0;
                            for (int64_t s = 0; s < col->len && remaining > 0; s++) {
                                int64_t take = segs[s]->len;
                                if (take > remaining) take = remaining;
                                memcpy((char*)td_data(head_vec) + dst_off * esz,
                                       td_data(segs[s]), (size_t)take * esz);
                                dst_off += take;
                                remaining -= take;
                            }
                        }
                        result = td_table_add_col(result, name_id, head_vec);
                        td_release(head_vec);
                    } else {
                        /* Flat column: direct copy */
                        uint8_t esz = col_esz(col);
                        td_t* head_vec = col_vec_new(col, n);
                        if (head_vec && !TD_IS_ERR(head_vec)) {
                            head_vec->len = n;
                            memcpy(td_data(head_vec), td_data(col),
                                   (size_t)n * esz);
                        }
                        result = td_table_add_col(result, name_id, head_vec);
                        td_release(head_vec);
                    }
                }
                td_release(input);
                return result;
            }
            if (n > input->len) n = input->len;
            /* Materialized copy for vector head */
            uint8_t esz = col_esz(input);
            td_t* result = col_vec_new(input, n);
            if (result && !TD_IS_ERR(result)) {
                result->len = n;
                memcpy(td_data(result), td_data(input), (size_t)n * esz);
            }
            td_release(input);
            return result;
        }

        case OP_TAIL: {
            td_op_ext_t* ext = find_ext(g, op->id);
            td_t* input = exec_node(g, op->inputs[0]);
            if (!input || TD_IS_ERR(input)) return input;
            int64_t n = ext ? ext->sym : 10;
            if (input->type == TD_TABLE) {
                int64_t ncols = td_table_ncols(input);
                int64_t nrows = td_table_nrows(input);
                if (n > nrows) n = nrows;
                int64_t skip = nrows - n;
                td_t* result = td_table_new(ncols);
                for (int64_t c = 0; c < ncols; c++) {
                    td_t* col = td_table_get_col_idx(input, c);
                    int64_t name_id = td_table_col_name(input, c);
                    if (!col) continue;
                    if (col->type == TD_MAPCOMMON) {
                        /* Materialize last N rows from MAPCOMMON partitions */
                        td_t** mc_ptrs = (td_t**)td_data(col);
                        td_t* kv = mc_ptrs[0];
                        td_t* rc = mc_ptrs[1];
                        int64_t n_parts = kv->len;
                        size_t esz = (size_t)col_esz(kv);
                        const char* kdata = (const char*)td_data(kv);
                        const int64_t* counts = (const int64_t*)td_data(rc);
                        td_t* flat = col_vec_new(kv, n);
                        if (flat && !TD_IS_ERR(flat)) {
                            flat->len = n;
                            char* out = (char*)td_data(flat);
                            /* Walk partitions from end, fill output from end */
                            int64_t remaining = n;
                            int64_t dst = n;
                            for (int64_t p = n_parts - 1; p >= 0 && remaining > 0; p--) {
                                int64_t take = counts[p];
                                if (take > remaining) take = remaining;
                                dst -= take;
                                for (int64_t r = 0; r < take; r++)
                                    memcpy(out + (dst + r) * esz, kdata + (size_t)p * esz, esz);
                                remaining -= take;
                            }
                        }
                        result = td_table_add_col(result, name_id, flat);
                        td_release(flat);
                        continue;
                    }
                    if (TD_IS_PARTED(col->type)) {
                        /* Copy last N rows from parted segments */
                        int8_t base = (int8_t)TD_PARTED_BASETYPE(col->type);
                        td_t** tsp = (td_t**)td_data(col);
                        uint8_t tba = (base == TD_SYM && col->len > 0 && tsp[0])
                                    ? tsp[0]->attrs : 0;
                        uint8_t esz = td_sym_elem_size(base, tba);
                        td_t* tail_vec = typed_vec_new(base, tba, n);
                        if (tail_vec && !TD_IS_ERR(tail_vec)) {
                            tail_vec->len = n;
                            td_t** segs = (td_t**)td_data(col);
                            int64_t remaining = n;
                            int64_t dst = n;
                            for (int64_t s = col->len - 1; s >= 0 && remaining > 0; s--) {
                                int64_t take = segs[s]->len;
                                if (take > remaining) take = remaining;
                                dst -= take;
                                memcpy((char*)td_data(tail_vec) + (size_t)dst * esz,
                                       (char*)td_data(segs[s]) + (size_t)(segs[s]->len - take) * esz,
                                       (size_t)take * esz);
                                remaining -= take;
                            }
                        }
                        result = td_table_add_col(result, name_id, tail_vec);
                        td_release(tail_vec);
                    } else {
                        /* Flat column: direct copy */
                        uint8_t esz = col_esz(col);
                        td_t* tail_vec = col_vec_new(col, n);
                        if (tail_vec && !TD_IS_ERR(tail_vec)) {
                            tail_vec->len = n;
                            memcpy(td_data(tail_vec),
                                   (char*)td_data(col) + (size_t)skip * esz,
                                   (size_t)n * esz);
                        }
                        result = td_table_add_col(result, name_id, tail_vec);
                        td_release(tail_vec);
                    }
                }
                td_release(input);
                return result;
            }
            if (n > input->len) n = input->len;
            int64_t skip = input->len - n;
            uint8_t esz = col_esz(input);
            td_t* result = col_vec_new(input, n);
            if (result && !TD_IS_ERR(result)) {
                result->len = n;
                memcpy(td_data(result),
                       (char*)td_data(input) + (size_t)skip * esz,
                       (size_t)n * esz);
            }
            td_release(input);
            return result;
        }

        case OP_IF: {
            return exec_if(g, op);
        }

        case OP_LIKE: {
            return exec_like(g, op);
        }

        case OP_ILIKE: {
            return exec_ilike(g, op);
        }

        case OP_UPPER: case OP_LOWER: case OP_TRIM: {
            return exec_string_unary(g, op);
        }
        case OP_STRLEN: {
            return exec_strlen(g, op);
        }
        case OP_SUBSTR: {
            return exec_substr(g, op);
        }
        case OP_REPLACE: {
            return exec_replace(g, op);
        }
        case OP_CONCAT: {
            return exec_concat(g, op);
        }

        case OP_EXTRACT: {
            return exec_extract(g, op);
        }

        case OP_DATE_TRUNC: {
            return exec_date_trunc(g, op);
        }

        case OP_ALIAS: {
            return exec_node(g, op->inputs[0]);
        }

        case OP_MATERIALIZE: {
            return exec_node(g, op->inputs[0]);
        }

        case OP_SELECT: {
            /* Column projection: select/compute columns from input table */
            td_t* input = exec_node(g, op->inputs[0]);
            if (!input || TD_IS_ERR(input)) return input;
            if (input->type != TD_TABLE) {
                td_release(input);
                return TD_ERR_PTR(TD_ERR_NYI);
            }
            td_op_ext_t* ext = find_ext(g, op->id);
            if (!ext) { td_release(input); return TD_ERR_PTR(TD_ERR_NYI); }
            uint8_t n_cols = ext->sort.n_cols;
            td_op_t** columns = ext->sort.columns;
            td_t* result = td_table_new(n_cols);

            /* Set g->table so SCAN nodes inside expressions resolve correctly */
            td_t* saved_table = g->table;
            g->table = input;

            for (uint8_t c = 0; c < n_cols; c++) {
                if (columns[c]->opcode == OP_SCAN) {
                    /* Direct column reference — copy from input table */
                    td_op_ext_t* col_ext = find_ext(g, columns[c]->id);
                    if (!col_ext) continue;
                    int64_t name_id = col_ext->sym;
                    td_t* src_col = td_table_get_col(input, name_id);
                    if (src_col) {
                        td_retain(src_col);
                        result = td_table_add_col(result, name_id, src_col);
                        td_release(src_col);
                    }
                } else {
                    /* Expression column — evaluate against input table */
                    td_t* vec = exec_node(g, columns[c]);
                    if (vec && !TD_IS_ERR(vec)) {
                        /* Synthetic name: _expr_0, _expr_1, ... */
                        char name_buf[16];
                        int n = 0;
                        name_buf[n++] = '_'; name_buf[n++] = 'e';
                        if (c >= 100) name_buf[n++] = '0' + (c / 100);
                        if (c >= 10)  name_buf[n++] = '0' + ((c / 10) % 10);
                        name_buf[n++] = '0' + (c % 10);
                        int64_t name_id = td_sym_intern(name_buf, (size_t)n);
                        result = td_table_add_col(result, name_id, vec);
                        td_release(vec);
                    }
                }
            }

            g->table = saved_table;
            td_release(input);
            return result;
        }

        case OP_EXPAND: {
            td_t* src = exec_node(g, op->inputs[0]);
            if (!src || TD_IS_ERR(src)) return src;
            td_t* result = exec_expand(g, op, src);
            td_release(src);
            return result;
        }

        case OP_VAR_EXPAND: {
            td_t* start = exec_node(g, op->inputs[0]);
            if (!start || TD_IS_ERR(start)) return start;
            td_t* result = exec_var_expand(g, op, start);
            td_release(start);
            return result;
        }

        case OP_SHORTEST_PATH: {
            td_t* src = exec_node(g, op->inputs[0]);
            td_t* dst = exec_node(g, op->inputs[1]);
            if (!src || TD_IS_ERR(src)) {
                if (dst && !TD_IS_ERR(dst)) td_release(dst);
                return src;
            }
            if (!dst || TD_IS_ERR(dst)) { td_release(src); return dst; }
            td_t* result = exec_shortest_path(g, op, src, dst);
            td_release(src);
            td_release(dst);
            return result;
        }

        case OP_WCO_JOIN: {
            return exec_wco_join(g, op);
        }

        default:
            return TD_ERR_PTR(TD_ERR_NYI);
    }
}

/* ============================================================================
 * td_execute -- top-level entry point (lazy pool init)
 * ============================================================================ */

td_t* td_execute(td_graph_t* g, td_op_t* root) {
    if (!g || !root) return TD_ERR_PTR(TD_ERR_NYI);

    /* Lazy-init the global thread pool on first call */
    td_pool_t* pool = td_pool_get();

    /* Reset cancellation flag at the start of each query */
    if (pool)
        atomic_store_explicit(&pool->cancelled, 0, memory_order_relaxed);

    td_t* result = exec_node(g, root);

    /* Final compaction: if a lazy selection remains unconsumed (e.g., filter
     * followed directly by a terminal node), materialize it now. */
    if (g->selection && result && !TD_IS_ERR(result)
        && result->type == TD_TABLE) {
        td_t* compacted = sel_compact(g, result, g->selection);
        td_release(result);
        td_release(g->selection);
        g->selection = NULL;
        result = compacted;
    }
    return result;
}
