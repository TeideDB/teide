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

#ifndef TD_LFTJ_H
#define TD_LFTJ_H

#include <teide/td.h>
#include "store/csr.h"

/* Trie iterator over sorted CSR adjacency list */
typedef struct td_lftj_iter {
    int64_t* targets;        /* pointer into CSR targets data */
    int64_t  start;          /* current range start */
    int64_t  end;            /* current range end */
    int64_t  pos;            /* current position in [start, end) */
} td_lftj_iter_t;

/* O(1) */
static inline int64_t lftj_key(td_lftj_iter_t* it) {
    return it->targets[it->pos];
}

static inline bool lftj_at_end(td_lftj_iter_t* it) {
    return it->pos >= it->end;
}

static inline void lftj_next(td_lftj_iter_t* it) {
    it->pos++;
}

/* O(log degree) - binary search within [pos, end) */
static inline void lftj_seek(td_lftj_iter_t* it, int64_t v) {
    int64_t lo = it->pos, hi = it->end;
    while (lo < hi) {
        int64_t mid = lo + (hi - lo) / 2;
        if (it->targets[mid] < v) lo = mid + 1;
        else hi = mid;
    }
    it->pos = lo;
}

/* Open trie level: set iterator to a node's adjacency list */
static inline void lftj_open(td_lftj_iter_t* it, td_csr_t* csr, int64_t parent) {
    int64_t* o = (int64_t*)td_data(csr->offsets);
    it->targets = (int64_t*)td_data(csr->targets);
    it->start = o[parent];
    it->end   = o[parent + 1];
    it->pos   = it->start;
}

/* Leapfrog search: intersect k sorted iterators */
bool leapfrog_search(td_lftj_iter_t** iters, int k, int64_t* out);

#endif /* TD_LFTJ_H */
