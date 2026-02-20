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

#include <teide/td.h>
#include <string.h>

/* --------------------------------------------------------------------------
 * Layout size computation
 *
 * Data payload after 32-byte td_t header:
 *   td_sel_meta_t          16 bytes
 *   seg_flags[n_segs]      align8(n_segs) bytes
 *   seg_popcnt[n_segs]     align8(n_segs * 2) bytes
 *   bits[n_words]          n_words * 8 bytes
 * -------------------------------------------------------------------------- */

static size_t sel_data_size(int64_t nrows) {
    uint32_t n_segs = (uint32_t)((nrows + TD_MORSEL_ELEMS - 1) / TD_MORSEL_ELEMS);
    uint32_t n_words = (uint32_t)((nrows + 63) / 64);

    size_t sz = sizeof(td_sel_meta_t);
    sz += (n_segs + 7u) & ~(size_t)7;           /* seg_flags, 8-aligned */
    sz += ((size_t)n_segs * 2 + 7u) & ~(size_t)7; /* seg_popcnt, 8-aligned */
    sz += (size_t)n_words * 8;                   /* bits */
    return sz;
}

/* --------------------------------------------------------------------------
 * td_sel_new — allocate a selection with all bits zero (no rows pass)
 * -------------------------------------------------------------------------- */

td_t* td_sel_new(int64_t nrows) {
    if (nrows < 0) return TD_ERR_PTR(TD_ERR_RANGE);

    size_t dsz = sel_data_size(nrows);
    td_t* s = td_alloc(dsz);
    if (!s || TD_IS_ERR(s)) return s;

    s->type = TD_SEL;
    s->len  = nrows;
    memset(td_data(s), 0, dsz);

    td_sel_meta_t* m = td_sel_meta(s);
    m->total_pass = 0;
    m->n_segs = (uint32_t)((nrows + TD_MORSEL_ELEMS - 1) / TD_MORSEL_ELEMS);
    /* seg_flags[] already zero = TD_SEL_NONE, seg_popcnt[] = 0, bits[] = 0 */

    return s;
}

/* --------------------------------------------------------------------------
 * td_sel_recompute — rebuild seg_flags + seg_popcnt from bits[]
 *
 * Called after direct writes into bits[] (e.g., fused predicate evaluation).
 * -------------------------------------------------------------------------- */

void td_sel_recompute(td_t* sel) {
    if (!sel || sel->type != TD_SEL) return;

    td_sel_meta_t* m = td_sel_meta(sel);
    uint8_t*  flags  = td_sel_flags(sel);
    uint16_t* pcnt   = td_sel_popcnt(sel);
    uint64_t* bits   = td_sel_bits(sel);

    int64_t total = 0;
    int64_t nrows = sel->len;
    uint32_t n_segs = m->n_segs;

    for (uint32_t seg = 0; seg < n_segs; seg++) {
        int64_t seg_start = (int64_t)seg * TD_MORSEL_ELEMS;
        int64_t seg_rows  = nrows - seg_start;
        if (seg_rows > TD_MORSEL_ELEMS) seg_rows = TD_MORSEL_ELEMS;

        /* Count bits in this segment's words */
        uint32_t word_start = (uint32_t)(seg_start / 64);
        uint32_t word_end   = (uint32_t)((seg_start + seg_rows + 63) / 64);
        int64_t seg_pop = 0;
        for (uint32_t w = word_start; w < word_end; w++)
            seg_pop += __builtin_popcountll(bits[w]);

        /* Handle partial last word: mask out trailing bits beyond nrows */
        if (seg == n_segs - 1 && (nrows & 63)) {
            uint32_t last_w = word_end - 1;
            uint32_t valid_bits = (uint32_t)(nrows & 63);
            uint64_t trail_mask = (1ULL << valid_bits) - 1;
            /* Subtract overcounted trailing bits */
            seg_pop -= __builtin_popcountll(bits[last_w] & ~trail_mask);
        }

        pcnt[seg] = (uint16_t)seg_pop;
        total += seg_pop;

        if (seg_pop == 0)
            flags[seg] = TD_SEL_NONE;
        else if (seg_pop == seg_rows)
            flags[seg] = TD_SEL_ALL;
        else
            flags[seg] = TD_SEL_MIX;
    }

    m->total_pass = total;
}

/* --------------------------------------------------------------------------
 * td_sel_from_pred — convert a TD_BOOL byte-per-row vector to TD_SEL
 * -------------------------------------------------------------------------- */

td_t* td_sel_from_pred(td_t* pred) {
    if (!pred || TD_IS_ERR(pred)) return pred;
    if (pred->type != TD_BOOL) return TD_ERR_PTR(TD_ERR_TYPE);

    int64_t nrows = pred->len;
    td_t* sel = td_sel_new(nrows);
    if (!sel || TD_IS_ERR(sel)) return sel;

    /* Pack byte-per-row into bitpacked uint64_t words */
    uint64_t* bits = td_sel_bits(sel);
    const uint8_t* src = (const uint8_t*)td_data(pred);

    int64_t full_words = nrows / 64;
    for (int64_t w = 0; w < full_words; w++) {
        uint64_t word = 0;
        const uint8_t* p = src + w * 64;
        for (int b = 0; b < 64; b++)
            word |= (uint64_t)(p[b] != 0) << b;
        bits[w] = word;
    }

    /* Remainder bits */
    int64_t rem = nrows & 63;
    if (rem) {
        uint64_t word = 0;
        const uint8_t* p = src + full_words * 64;
        for (int64_t b = 0; b < rem; b++)
            word |= (uint64_t)(p[b] != 0) << b;
        bits[full_words] = word;
    }

    td_sel_recompute(sel);
    return sel;
}

/* --------------------------------------------------------------------------
 * td_sel_and — AND two selections of equal length, returns new TD_SEL
 * -------------------------------------------------------------------------- */

td_t* td_sel_and(td_t* a, td_t* b) {
    if (!a || TD_IS_ERR(a)) return a;
    if (!b || TD_IS_ERR(b)) return b;
    if (a->type != TD_SEL || b->type != TD_SEL)
        return TD_ERR_PTR(TD_ERR_TYPE);
    if (a->len != b->len)
        return TD_ERR_PTR(TD_ERR_RANGE);

    int64_t nrows = a->len;
    td_t* out = td_sel_new(nrows);
    if (!out || TD_IS_ERR(out)) return out;

    uint64_t* dst = td_sel_bits(out);
    const uint64_t* sa = td_sel_bits(a);
    const uint64_t* sb = td_sel_bits(b);
    uint32_t n_words = (uint32_t)((nrows + 63) / 64);

    for (uint32_t w = 0; w < n_words; w++)
        dst[w] = sa[w] & sb[w];

    td_sel_recompute(out);
    return out;
}
