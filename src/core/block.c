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

#include "block.h"

/* Weak stub for td_alloc — replaced by buddy allocator at link time.
 * Uses td_vm_alloc (mmap) — page-aligned and zero-filled. */
__attribute__((weak))
td_t* td_alloc(size_t size) {
    if (size < 32) size = 32;
    size = (size + 4095) & ~(size_t)4095;
    void* p = td_vm_alloc(size);
    if (!p) return TD_ERR_PTR(TD_ERR_OOM);
    return (td_t*)p;
}

size_t td_block_size(td_t* v) {
    if (td_is_atom(v)) return 32;
    /* LIST (type=0) stores child pointers */
    if (v->type == TD_LIST) return 32 + (size_t)td_len(v) * sizeof(td_t*);
    /* TABLE stores schema slot + ncols column pointers */
    if (v->type == TD_TABLE) return 32 + (size_t)(td_len(v) + 1) * sizeof(td_t*);
    /* TD_SEL: variable layout — meta + seg_flags + seg_popcnt + bits */
    if (v->type == TD_SEL) {
        int64_t nrows = td_len(v);
        if (nrows < 0) return 32;
        uint32_t n_segs = (uint32_t)((nrows + TD_MORSEL_ELEMS - 1) / TD_MORSEL_ELEMS);
        uint32_t n_words = (uint32_t)((nrows + 63) / 64);
        size_t dsz = sizeof(td_sel_meta_t);
        dsz += (n_segs + 7u) & ~(size_t)7;           /* seg_flags, 8-aligned */
        dsz += ((size_t)n_segs * 2 + 7u) & ~(size_t)7; /* seg_popcnt, 8-aligned */
        dsz += (size_t)n_words * 8;                   /* bits */
        return 32 + dsz;
    }
    /* Vectors: header (32 bytes) + len * elem_size.
     * Use td_sym_elem_size for SYM columns to respect narrow widths. */
    int8_t t = td_type(v);
    if (t <= 0 || t >= TD_TYPE_COUNT) return 32;
    return 32 + (size_t)td_len(v) * td_sym_elem_size(t, v->attrs);
}

td_t* td_block_copy(td_t* src) {
    size_t sz = td_block_size(src);
    td_t* dst = td_alloc(sz);
    if (!dst) return TD_ERR_PTR(TD_ERR_OOM);
    /* Save allocator metadata before memcpy overwrites the header */
    uint8_t new_mmod = dst->mmod;
    uint8_t new_order = dst->order;
    memcpy(dst, src, sz);
    dst->mmod = new_mmod;
    dst->order = new_order;
    atomic_store_explicit(&dst->rc, 1, memory_order_relaxed);
    /* TODO: td_retain_owned_refs(dst) should be called here to retain
     * child pointers for STR/LIST/TABLE types. Currently the function is
     * static in arena.c — callers of td_block_copy must ensure child refs
     * are retained separately, or use td_alloc_copy() which handles this. */
    return dst;
}
