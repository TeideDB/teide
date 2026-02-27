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

#include "vec.h"
#include <string.h>

/* --------------------------------------------------------------------------
 * Capacity helpers
 *
 * A vector's capacity is determined by its buddy order:
 *   capacity = (2^order - 32) / elem_size
 * When len reaches capacity, realloc to next power-of-2 data size.
 * -------------------------------------------------------------------------- */

static int64_t vec_capacity(td_t* vec) {
    size_t block_size = (size_t)1 << vec->order;
    size_t data_space = block_size - 32;  /* 32B td_t header */
    uint8_t esz = td_sym_elem_size(vec->type, vec->attrs);
    if (esz == 0) return 0;
    return (int64_t)(data_space / esz);
}

/* --------------------------------------------------------------------------
 * td_vec_new
 * -------------------------------------------------------------------------- */

td_t* td_vec_new(int8_t type, int64_t capacity) {
    if (type <= 0 || type >= TD_TYPE_COUNT)
        return TD_ERR_PTR(TD_ERR_TYPE);
    if (type == TD_SYM)
        return td_sym_vec_new(TD_SYM_W64, capacity);  /* default: global sym IDs */
    if (capacity < 0) return TD_ERR_PTR(TD_ERR_RANGE);

    uint8_t esz = td_elem_size(type);
    size_t data_size = (size_t)capacity * esz;
    if (esz > 1 && data_size / esz != (size_t)capacity)
        return TD_ERR_PTR(TD_ERR_OOM);

    td_t* v = td_alloc(data_size);
    if (!v || TD_IS_ERR(v)) return v;

    v->type = type;
    v->len = 0;
    v->attrs = 0;
    memset(v->nullmap, 0, 16);

    return v;
}

/* --------------------------------------------------------------------------
 * td_sym_vec_new — create a TD_SYM vector with adaptive index width
 *
 * sym_width: TD_SYM_W8, TD_SYM_W16, TD_SYM_W32, or TD_SYM_W64
 * capacity:  number of elements (rows)
 * -------------------------------------------------------------------------- */

td_t* td_sym_vec_new(uint8_t sym_width, int64_t capacity) {
    if ((sym_width & ~TD_SYM_W_MASK) != 0)
        return TD_ERR_PTR(TD_ERR_TYPE);
    if (capacity < 0) return TD_ERR_PTR(TD_ERR_RANGE);

    uint8_t esz = (uint8_t)TD_SYM_ELEM(sym_width);
    size_t data_size = (size_t)capacity * esz;
    if (esz > 1 && data_size / esz != (size_t)capacity)
        return TD_ERR_PTR(TD_ERR_OOM);

    td_t* v = td_alloc(data_size);
    if (!v || TD_IS_ERR(v)) return v;

    v->type = TD_SYM;
    v->len = 0;
    v->attrs = sym_width;  /* lower 2 bits encode width */
    memset(v->nullmap, 0, 16);

    return v;
}

/* --------------------------------------------------------------------------
 * td_vec_append
 * -------------------------------------------------------------------------- */

td_t* td_vec_append(td_t* vec, const void* elem) {
    if (!vec || TD_IS_ERR(vec)) return vec;
    if (vec->type <= 0 || vec->type >= TD_TYPE_COUNT)
        return TD_ERR_PTR(TD_ERR_TYPE);

    /* COW: if shared, copy first */
    vec = td_cow(vec);
    if (!vec || TD_IS_ERR(vec)) return vec;

    uint8_t esz = td_sym_elem_size(vec->type, vec->attrs);
    int64_t cap = vec_capacity(vec);

    /* Grow if needed */
    if (vec->len >= cap) {
        size_t new_data_size = (size_t)(vec->len + 1) * esz;
        /* Round up to next power of 2 block */
        if (new_data_size < 32) new_data_size = 32;
        else {
            size_t s = 32;
            while (s < new_data_size) {
                if (s > SIZE_MAX / 2) return TD_ERR_PTR(TD_ERR_OOM);
                s *= 2;
            }
            new_data_size = s;
        }
        td_t* new_vec = td_scratch_realloc(vec, new_data_size);
        if (!new_vec || TD_IS_ERR(new_vec)) return new_vec;
        vec = new_vec;
    }

    /* Append element */
    char* dst = (char*)td_data(vec) + vec->len * esz;
    memcpy(dst, elem, esz);
    vec->len++;

    return vec;
}

/* --------------------------------------------------------------------------
 * td_vec_set
 * -------------------------------------------------------------------------- */

td_t* td_vec_set(td_t* vec, int64_t idx, const void* elem) {
    if (!vec || TD_IS_ERR(vec)) return vec;
    if (idx < 0 || idx >= vec->len)
        return TD_ERR_PTR(TD_ERR_RANGE);

    /* COW: if shared, copy first */
    vec = td_cow(vec);
    if (!vec || TD_IS_ERR(vec)) return vec;

    uint8_t esz = td_sym_elem_size(vec->type, vec->attrs);
    char* dst = (char*)td_data(vec) + idx * esz;
    memcpy(dst, elem, esz);

    return vec;
}

/* --------------------------------------------------------------------------
 * td_vec_get
 * -------------------------------------------------------------------------- */

void* td_vec_get(td_t* vec, int64_t idx) {
    if (!vec || TD_IS_ERR(vec)) return NULL;

    /* Slice path: redirect to parent */
    if (vec->attrs & TD_ATTR_SLICE) {
        td_t* parent = vec->slice_parent;
        int64_t offset = vec->slice_offset;
        if (idx < 0 || idx >= vec->len) return NULL;
        uint8_t esz = td_sym_elem_size(parent->type, parent->attrs);
        return (char*)td_data(parent) + (offset + idx) * esz;
    }

    if (idx < 0 || idx >= vec->len) return NULL;
    uint8_t esz = td_sym_elem_size(vec->type, vec->attrs);
    return (char*)td_data(vec) + idx * esz;
}

/* --------------------------------------------------------------------------
 * td_vec_slice  (zero-copy view)
 * -------------------------------------------------------------------------- */

td_t* td_vec_slice(td_t* vec, int64_t offset, int64_t len) {
    if (!vec || TD_IS_ERR(vec)) return vec;
    if (offset < 0 || len < 0 || offset > vec->len || len > vec->len - offset)
        return TD_ERR_PTR(TD_ERR_RANGE);

    /* If input is already a slice, resolve to ultimate parent */
    td_t* parent = vec;
    int64_t parent_offset = offset;
    if (vec->attrs & TD_ATTR_SLICE) {
        parent = vec->slice_parent;
        parent_offset = vec->slice_offset + offset;
    }

    /* Allocate a header-only block for the slice view */
    td_t* s = td_alloc(0);
    if (!s || TD_IS_ERR(s)) return s;

    s->type = parent->type;
    s->attrs = TD_ATTR_SLICE | (parent->attrs & TD_SYM_W_MASK);
    s->len = len;
    s->slice_parent = parent;
    s->slice_offset = parent_offset;

    /* Retain the parent so it stays alive */
    td_retain(parent);

    return s;
}

/* --------------------------------------------------------------------------
 * td_vec_concat
 * -------------------------------------------------------------------------- */

td_t* td_vec_concat(td_t* a, td_t* b) {
    if (!a || TD_IS_ERR(a)) return a;
    if (!b || TD_IS_ERR(b)) return b;
    if (a->type != b->type)
        return TD_ERR_PTR(TD_ERR_TYPE);

    uint8_t a_esz = td_sym_elem_size(a->type, a->attrs);
    uint8_t b_esz = td_sym_elem_size(b->type, b->attrs);
    /* Use the wider of the two widths for SYM columns */
    uint8_t out_attrs = (a_esz >= b_esz) ? a->attrs : b->attrs;
    uint8_t esz = (a_esz >= b_esz) ? a_esz : b_esz;

    int64_t total_len = a->len + b->len;
    if (total_len < a->len) return TD_ERR_PTR(TD_ERR_OOM); /* overflow */
    size_t data_size = (size_t)total_len * esz;
    if (esz > 1 && data_size / esz != (size_t)total_len)
        return TD_ERR_PTR(TD_ERR_OOM); /* multiplication overflow */

    td_t* result = td_alloc(data_size);
    if (!result || TD_IS_ERR(result)) return result;

    result->type = a->type;
    result->len = total_len;
    result->attrs = out_attrs;
    memset(result->nullmap, 0, 16);

    /* For SYM with mismatched widths, widen element-by-element */
    if (a->type == TD_SYM && a_esz != b_esz) {
        void* dst = td_data(result);
        for (int64_t i = 0; i < a->len; i++) {
            int64_t val = td_read_sym(td_data(a), i, a->type, a->attrs);
            td_write_sym(dst, i, (uint64_t)val, result->type, result->attrs);
        }
        for (int64_t i = 0; i < b->len; i++) {
            int64_t val = td_read_sym(td_data(b), i, b->type, b->attrs);
            td_write_sym(dst, a->len + i, (uint64_t)val, result->type, result->attrs);
        }
    } else {
        /* Same width: fast memcpy path */
        void* a_data = (a->attrs & TD_ATTR_SLICE) ?
            ((char*)td_data(a->slice_parent) + a->slice_offset * esz) :
            td_data(a);
        memcpy(td_data(result), a_data, (size_t)a->len * esz);

        void* b_data = (b->attrs & TD_ATTR_SLICE) ?
            ((char*)td_data(b->slice_parent) + b->slice_offset * esz) :
            td_data(b);
        memcpy((char*)td_data(result) + (size_t)a->len * esz, b_data,
               (size_t)b->len * esz);
    }

    /* LIST/TABLE columns hold child pointers — retain them */
    if (a->type == TD_LIST || a->type == TD_TABLE) {
        td_t** ptrs = (td_t**)td_data(result);
        for (int64_t i = 0; i < total_len; i++) {
            if (ptrs[i]) td_retain(ptrs[i]);
        }
    }

    return result;
}

/* --------------------------------------------------------------------------
 * td_vec_from_raw
 * -------------------------------------------------------------------------- */

td_t* td_vec_from_raw(int8_t type, const void* data, int64_t count) {
    if (type <= 0 || type >= TD_TYPE_COUNT)
        return TD_ERR_PTR(TD_ERR_TYPE);
    if (count < 0) return TD_ERR_PTR(TD_ERR_RANGE);

    /* TD_SYM defaults to W64 (global sym IDs) */
    uint8_t sym_w = (type == TD_SYM) ? TD_SYM_W64 : 0;
    uint8_t esz = td_sym_elem_size(type, sym_w);
    size_t data_size = (size_t)count * esz;

    td_t* v = td_alloc(data_size);
    if (!v || TD_IS_ERR(v)) return v;

    v->type = type;
    v->len = count;
    v->attrs = sym_w;
    memset(v->nullmap, 0, 16);

    memcpy(td_data(v), data, data_size);

    /* LIST/TABLE elements are child pointers — retain them */
    if (type == TD_LIST || type == TD_TABLE) {
        td_t** ptrs = (td_t**)td_data(v);
        for (int64_t i = 0; i < count; i++) {
            if (ptrs[i]) td_retain(ptrs[i]);
        }
    }

    return v;
}

/* --------------------------------------------------------------------------
 * Null bitmap operations
 *
 * Inline: for vectors with <=128 elements, bits stored in nullmap[16] (128 bits).
 * External: for >128 elements, allocate a U8 vector bitmap via ext_nullmap.
 * -------------------------------------------------------------------------- */

void td_vec_set_null(td_t* vec, int64_t idx, bool is_null) {
    if (!vec || TD_IS_ERR(vec)) return;
    if (vec->attrs & TD_ATTR_SLICE) return; /* cannot set null on slice — COW first */
    if (idx < 0 || idx >= vec->len) return;

    /* Mark HAS_NULLS if setting a null */
    if (is_null) vec->attrs |= TD_ATTR_HAS_NULLS;

    if (!(vec->attrs & TD_ATTR_NULLMAP_EXT)) {
        /* Inline nullmap path (<=128 elements) */
        if (idx < 128) {
            int byte_idx = (int)(idx / 8);
            int bit_idx = (int)(idx % 8);
            if (is_null)
                vec->nullmap[byte_idx] |= (uint8_t)(1u << bit_idx);
            else
                vec->nullmap[byte_idx] &= (uint8_t)~(1u << bit_idx);
            return;
        }
        /* Need to promote to external nullmap */
        int64_t bitmap_len = (vec->len + 7) / 8;
        td_t* ext = td_vec_new(TD_U8, bitmap_len);
        if (!ext || TD_IS_ERR(ext)) return;
        ext->len = bitmap_len;
        /* Copy existing inline bits */
        memcpy(td_data(ext), vec->nullmap, 16);
        /* Zero remaining bytes */
        if (bitmap_len > 16)
            memset((char*)td_data(ext) + 16, 0, (size_t)(bitmap_len - 16));
        vec->attrs |= TD_ATTR_NULLMAP_EXT;
        vec->ext_nullmap = ext;
    }

    /* External nullmap path */
    td_t* ext = vec->ext_nullmap;
    /* Grow external bitmap if needed */
    int64_t needed_bytes = (idx / 8) + 1;
    if (needed_bytes > ext->len) {
        int64_t new_len = (vec->len + 7) / 8;
        if (new_len < needed_bytes) new_len = needed_bytes;
        size_t new_data_size = (size_t)new_len;
        int64_t old_len = ext->len;
        td_t* new_ext = td_scratch_realloc(ext, new_data_size);
        if (!new_ext || TD_IS_ERR(new_ext)) return;
        /* Zero new bytes */
        if (new_len > old_len)
            memset((char*)td_data(new_ext) + old_len, 0,
                   (size_t)(new_len - old_len));
        new_ext->len = new_len;
        vec->ext_nullmap = new_ext;
        ext = new_ext;
    }

    uint8_t* bits = (uint8_t*)td_data(ext);
    int byte_idx = (int)(idx / 8);
    int bit_idx = (int)(idx % 8);
    if (is_null)
        bits[byte_idx] |= (uint8_t)(1u << bit_idx);
    else
        bits[byte_idx] &= (uint8_t)~(1u << bit_idx);
}

bool td_vec_is_null(td_t* vec, int64_t idx) {
    if (!vec || TD_IS_ERR(vec)) return false;
    if (idx < 0 || idx >= vec->len) return false;
    if (!(vec->attrs & TD_ATTR_HAS_NULLS)) return false;

    if (vec->attrs & TD_ATTR_NULLMAP_EXT) {
        td_t* ext = vec->ext_nullmap;
        int64_t byte_idx = idx / 8;
        if (byte_idx >= ext->len) return false;
        uint8_t* bits = (uint8_t*)td_data(ext);
        return (bits[byte_idx] >> (idx % 8)) & 1;
    }

    /* Inline nullmap */
    if (idx >= 128) return false;
    int byte_idx = (int)(idx / 8);
    int bit_idx = (int)(idx % 8);
    return (vec->nullmap[byte_idx] >> bit_idx) & 1;
}
