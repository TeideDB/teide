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

#ifndef TD_CSR_H
#define TD_CSR_H

#include <teide/td.h>

/* Compressed Sparse Row edge index.
 *
 * offsets[i]..offsets[i+1] gives the range in targets[] for node i's neighbors.
 * Stored as td_t I64 vectors — same allocator, mmap, COW as everything else.
 *
 * If sorted == true, targets within each adjacency list are sorted ascending.
 * Required for OP_WCO_JOIN (Leapfrog Triejoin).
 */
typedef struct td_csr {
    td_t*    offsets;      /* I64 vec, length = n_nodes + 1                 */
    td_t*    targets;      /* I64 vec, length = n_edges                     */
    td_t*    rowmap;       /* I64 vec, length = n_edges (CSR pos -> prop row)*/
    td_t*    props;        /* optional edge property table (td_t TD_TABLE)  */
    int64_t  n_nodes;
    int64_t  n_edges;
    bool     sorted;       /* targets sorted per adjacency list             */
} td_csr_t;

/* Relationship: double-indexed CSR (forward + reverse).
 *
 * from_table/to_table are opaque IDs assigned by the caller (planner).
 * libteide does not manage a table registry -- it just stores the IDs
 * so the caller can identify which tables this rel connects.
 */
typedef struct td_rel {
    uint16_t    from_table;
    uint16_t    to_table;
    int64_t     name_sym;     /* relationship name as symbol ID */
    td_csr_t    fwd;          /* src -> dst */
    td_csr_t    rev;          /* dst -> src */
} td_rel_t;

/* O(1) neighbor range lookup — caller must ensure node is in [0, n_nodes). */
static inline int64_t td_csr_degree(td_csr_t* csr, int64_t node) {
    if (!csr || !csr->offsets || node < 0 || node >= csr->n_nodes) return 0;
    int64_t* o = (int64_t*)td_data(csr->offsets);
    return o[node + 1] - o[node];
}

static inline int64_t* td_csr_neighbors(td_csr_t* csr, int64_t node, int64_t* out_count) {
    if (!csr || !csr->offsets || !csr->targets || node < 0 || node >= csr->n_nodes) {
        if (out_count) *out_count = 0;
        return NULL;
    }
    int64_t* o = (int64_t*)td_data(csr->offsets);
    int64_t* t = (int64_t*)td_data(csr->targets);
    *out_count = o[node + 1] - o[node];
    return &t[o[node]];
}

#endif /* TD_CSR_H */
