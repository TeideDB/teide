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

#include "csr.h"
#include "mem/sys.h"
#include <string.h>
#include <stdio.h>
#include <sys/stat.h>
#include <errno.h>

/* Forward declaration */
static void csr_free(td_csr_t* csr);

/* --------------------------------------------------------------------------
 * CSR construction helpers
 * -------------------------------------------------------------------------- */

/* Pair for sorting edges */
typedef struct {
    int64_t src;
    int64_t dst;
    int64_t row;  /* original row index for rowmap */
} edge_pair_t;

/* Comparison by src then dst */
static int cmp_edge_by_src(const void* a, const void* b) {
    const edge_pair_t* ea = (const edge_pair_t*)a;
    const edge_pair_t* eb = (const edge_pair_t*)b;
    if (ea->src < eb->src) return -1;
    if (ea->src > eb->src) return 1;
    if (ea->dst < eb->dst) return -1;
    if (ea->dst > eb->dst) return 1;
    return 0;
}

/* Comparison by dst then src (for reverse CSR) */
static int cmp_edge_by_dst(const void* a, const void* b) {
    const edge_pair_t* ea = (const edge_pair_t*)a;
    const edge_pair_t* eb = (const edge_pair_t*)b;
    if (ea->dst < eb->dst) return -1;
    if (ea->dst > eb->dst) return 1;
    if (ea->src < eb->src) return -1;
    if (ea->src > eb->src) return 1;
    return 0;
}

/* Sort targets within each adjacency list (for LFTJ) */
static void csr_sort_adjacency_lists(td_csr_t* csr) {
    int64_t* offsets = (int64_t*)td_data(csr->offsets);
    int64_t* targets = (int64_t*)td_data(csr->targets);
    int64_t* rowmap = csr->rowmap ? (int64_t*)td_data(csr->rowmap) : NULL;

    for (int64_t node = 0; node < csr->n_nodes; node++) {
        int64_t start = offsets[node];
        int64_t end = offsets[node + 1];
        int64_t deg = end - start;
        if (deg <= 1) continue;

        /* Simple insertion sort — adjacency lists are typically small */
        for (int64_t i = start + 1; i < end; i++) {
            int64_t key = targets[i];
            int64_t row_key = rowmap ? rowmap[i] : 0;
            int64_t j = i - 1;
            while (j >= start && targets[j] > key) {
                targets[j + 1] = targets[j];
                if (rowmap) rowmap[j + 1] = rowmap[j];
                j--;
            }
            targets[j + 1] = key;
            if (rowmap) rowmap[j + 1] = row_key;
        }
    }
}

/* Build CSR from sorted edge pairs.
 * pairs must be sorted by the 'key' field (src for fwd, dst for rev). */
static td_err_t csr_build_from_pairs(edge_pair_t* pairs, int64_t n_edges,
                                      int64_t n_nodes, bool is_reverse,
                                      bool sort_targets, td_csr_t* out) {
    out->n_nodes = n_nodes;
    out->props = NULL;

    /* Count valid edges (those within [0, n_nodes) range) */
    int64_t valid_edges = 0;
    for (int64_t i = 0; i < n_edges; i++) {
        int64_t key = is_reverse ? pairs[i].dst : pairs[i].src;
        if (key >= 0 && key < n_nodes) valid_edges++;
    }
    out->n_edges = valid_edges;

    /* Allocate offsets (n_nodes + 1) */
    out->offsets = td_vec_new(TD_I64, n_nodes + 1);
    if (!out->offsets || TD_IS_ERR(out->offsets)) return TD_ERR_OOM;
    out->offsets->len = n_nodes + 1;
    int64_t* off = (int64_t*)td_data(out->offsets);
    memset(off, 0, (size_t)(n_nodes + 1) * sizeof(int64_t));

    /* Allocate targets */
    out->targets = td_vec_new(TD_I64, valid_edges > 0 ? valid_edges : 1);
    if (!out->targets || TD_IS_ERR(out->targets)) {
        td_release(out->offsets); out->offsets = NULL;
        return TD_ERR_OOM;
    }
    out->targets->len = valid_edges;
    int64_t* tgt = (int64_t*)td_data(out->targets);

    /* Allocate rowmap */
    out->rowmap = td_vec_new(TD_I64, valid_edges > 0 ? valid_edges : 1);
    if (!out->rowmap || TD_IS_ERR(out->rowmap)) {
        td_release(out->offsets); out->offsets = NULL;
        td_release(out->targets); out->targets = NULL;
        return TD_ERR_OOM;
    }
    out->rowmap->len = valid_edges;
    int64_t* rmap = (int64_t*)td_data(out->rowmap);

    /* Count degrees */
    for (int64_t i = 0; i < n_edges; i++) {
        int64_t key = is_reverse ? pairs[i].dst : pairs[i].src;
        if (key >= 0 && key < n_nodes) off[key + 1]++;
    }

    /* Prefix sum */
    for (int64_t i = 1; i <= n_nodes; i++)
        off[i] += off[i - 1];

    /* Fill targets + rowmap using a position array */
    td_t* pos_hdr = td_alloc((size_t)(n_nodes > 0 ? n_nodes : 1) * sizeof(int64_t));
    if (!pos_hdr) {
        td_release(out->offsets); out->offsets = NULL;
        td_release(out->targets); out->targets = NULL;
        td_release(out->rowmap); out->rowmap = NULL;
        return TD_ERR_OOM;
    }
    int64_t* pos = (int64_t*)td_data(pos_hdr);
    if (n_nodes > 0)
        memcpy(pos, off, (size_t)n_nodes * sizeof(int64_t));

    for (int64_t i = 0; i < n_edges; i++) {
        int64_t key = is_reverse ? pairs[i].dst : pairs[i].src;
        int64_t val = is_reverse ? pairs[i].src : pairs[i].dst;
        if (key >= 0 && key < n_nodes) {
            int64_t p = pos[key]++;
            tgt[p] = val;
            rmap[p] = pairs[i].row;
        }
    }
    td_free(pos_hdr);

    /* Sort within adjacency lists if requested */
    if (sort_targets) {
        csr_sort_adjacency_lists(out);
        out->sorted = true;
    } else {
        out->sorted = false;
    }

    return TD_OK;
}

/* --------------------------------------------------------------------------
 * td_rel_from_edges — build from explicit edge table
 * -------------------------------------------------------------------------- */

td_rel_t* td_rel_from_edges(td_t* edge_table,
                             const char* src_col, const char* dst_col,
                             int64_t n_src_nodes, int64_t n_dst_nodes,
                             bool sort_targets) {
    if (!edge_table || TD_IS_ERR(edge_table) || edge_table->type != TD_TABLE)
        return NULL;

    int64_t src_sym = td_sym_intern(src_col, strlen(src_col));
    int64_t dst_sym = td_sym_intern(dst_col, strlen(dst_col));

    td_t* src_vec = td_table_get_col(edge_table, src_sym);
    td_t* dst_vec = td_table_get_col(edge_table, dst_sym);
    if (!src_vec || !dst_vec) return NULL;
    if (src_vec->type != TD_I64 || dst_vec->type != TD_I64) return NULL;

    int64_t n_edges = src_vec->len;
    if (n_edges != dst_vec->len) return NULL;
    if (n_src_nodes < 0 || n_dst_nodes < 0) return NULL;

    /* Build edge pairs */
    td_t* pairs_hdr = td_alloc((size_t)n_edges * sizeof(edge_pair_t));
    if (!pairs_hdr) return NULL;
    edge_pair_t* pairs = (edge_pair_t*)td_data(pairs_hdr);

    int64_t* src_data = (int64_t*)td_data(src_vec);
    int64_t* dst_data = (int64_t*)td_data(dst_vec);
    for (int64_t i = 0; i < n_edges; i++) {
        pairs[i].src = src_data[i];
        pairs[i].dst = dst_data[i];
        pairs[i].row = i;
    }

    /* Allocate rel */
    td_rel_t* rel = (td_rel_t*)td_sys_alloc(sizeof(td_rel_t));
    if (!rel) { td_free(pairs_hdr); return NULL; }
    memset(rel, 0, sizeof(td_rel_t));
    rel->name_sym = -1;

    /* Build forward CSR (sorted by src) */
    /* qsort is from libc, not an external dep */
    qsort(pairs, (size_t)n_edges, sizeof(edge_pair_t), cmp_edge_by_src);
    td_err_t err = csr_build_from_pairs(pairs, n_edges, n_src_nodes, false,
                                         sort_targets, &rel->fwd);
    if (err != TD_OK) {
        td_free(pairs_hdr);
        td_sys_free(rel);
        return NULL;
    }

    /* Build reverse CSR (sorted by dst) */
    qsort(pairs, (size_t)n_edges, sizeof(edge_pair_t), cmp_edge_by_dst);
    err = csr_build_from_pairs(pairs, n_edges, n_dst_nodes, true,
                                sort_targets, &rel->rev);
    if (err != TD_OK) {
        td_free(pairs_hdr);
        csr_free(&rel->fwd);
        td_sys_free(rel);
        return NULL;
    }

    td_free(pairs_hdr);
    return rel;
}

/* --------------------------------------------------------------------------
 * td_rel_build — build from FK column in source table
 * -------------------------------------------------------------------------- */

td_rel_t* td_rel_build(td_t* from_table, const char* fk_col,
                         int64_t n_target_nodes, bool sort_targets) {
    if (!from_table || TD_IS_ERR(from_table) || from_table->type != TD_TABLE)
        return NULL;

    int64_t fk_sym = td_sym_intern(fk_col, strlen(fk_col));
    td_t* fk_vec = td_table_get_col(from_table, fk_sym);
    if (!fk_vec || fk_vec->type != TD_I64) return NULL;
    if (n_target_nodes < 0) return NULL;

    int64_t n_edges = fk_vec->len;
    int64_t n_src_nodes = td_table_nrows(from_table);

    /* Build edge pairs: src = row index, dst = fk value */
    td_t* pairs_hdr = td_alloc((size_t)n_edges * sizeof(edge_pair_t));
    if (!pairs_hdr) return NULL;
    edge_pair_t* pairs = (edge_pair_t*)td_data(pairs_hdr);

    int64_t* fk_data = (int64_t*)td_data(fk_vec);
    for (int64_t i = 0; i < n_edges; i++) {
        pairs[i].src = i;
        pairs[i].dst = fk_data[i];
        pairs[i].row = i;
    }

    td_rel_t* rel = (td_rel_t*)td_sys_alloc(sizeof(td_rel_t));
    if (!rel) { td_free(pairs_hdr); return NULL; }
    memset(rel, 0, sizeof(td_rel_t));
    rel->name_sym = -1;

    /* Build forward CSR */
    qsort(pairs, (size_t)n_edges, sizeof(edge_pair_t), cmp_edge_by_src);
    td_err_t err = csr_build_from_pairs(pairs, n_edges, n_src_nodes, false,
                                         sort_targets, &rel->fwd);
    if (err != TD_OK) {
        td_free(pairs_hdr);
        td_sys_free(rel);
        return NULL;
    }

    /* Build reverse CSR */
    qsort(pairs, (size_t)n_edges, sizeof(edge_pair_t), cmp_edge_by_dst);
    err = csr_build_from_pairs(pairs, n_edges, n_target_nodes, true,
                                sort_targets, &rel->rev);
    if (err != TD_OK) {
        td_free(pairs_hdr);
        csr_free(&rel->fwd);
        td_sys_free(rel);
        return NULL;
    }

    td_free(pairs_hdr);
    return rel;
}

/* --------------------------------------------------------------------------
 * CSR free
 * -------------------------------------------------------------------------- */

static void csr_free(td_csr_t* csr) {
    if (csr->offsets) td_release(csr->offsets);
    if (csr->targets) td_release(csr->targets);
    if (csr->rowmap) td_release(csr->rowmap);
    if (csr->props) td_release(csr->props);
    csr->offsets = NULL;
    csr->targets = NULL;
    csr->rowmap = NULL;
    csr->props = NULL;
}

void td_rel_free(td_rel_t* rel) {
    if (!rel) return;
    csr_free(&rel->fwd);
    csr_free(&rel->rev);
    td_sys_free(rel);
}

/* --------------------------------------------------------------------------
 * CSR persistence — save/load/mmap using existing column file format
 * -------------------------------------------------------------------------- */

static td_err_t csr_save(td_csr_t* csr, const char* dir, const char* prefix) {
    char path[1024];
    int len;

    len = snprintf(path, sizeof(path), "%s/%s_offsets.col", dir, prefix);
    if (len < 0 || (size_t)len >= sizeof(path)) return TD_ERR_IO;
    td_err_t err = td_col_save(csr->offsets, path);
    if (err != TD_OK) return err;

    len = snprintf(path, sizeof(path), "%s/%s_targets.col", dir, prefix);
    if (len < 0 || (size_t)len >= sizeof(path)) return TD_ERR_IO;
    err = td_col_save(csr->targets, path);
    if (err != TD_OK) return err;

    if (csr->rowmap) {
        len = snprintf(path, sizeof(path), "%s/%s_rowmap.col", dir, prefix);
        if (len < 0 || (size_t)len >= sizeof(path)) return TD_ERR_IO;
        err = td_col_save(csr->rowmap, path);
        if (err != TD_OK) return err;
    }

    return TD_OK;
}

static td_err_t csr_load_impl(td_csr_t* csr, const char* dir, const char* prefix,
                                bool use_mmap) {
    char path[1024];
    int len;

    len = snprintf(path, sizeof(path), "%s/%s_offsets.col", dir, prefix);
    if (len < 0 || (size_t)len >= sizeof(path)) return TD_ERR_IO;
    csr->offsets = use_mmap ? td_col_mmap(path) : td_col_load(path);
    if (!csr->offsets || TD_IS_ERR(csr->offsets)) {
        csr->offsets = NULL;
        return TD_ERR_IO;
    }

    len = snprintf(path, sizeof(path), "%s/%s_targets.col", dir, prefix);
    if (len < 0 || (size_t)len >= sizeof(path)) return TD_ERR_IO;
    csr->targets = use_mmap ? td_col_mmap(path) : td_col_load(path);
    if (!csr->targets || TD_IS_ERR(csr->targets)) {
        td_release(csr->offsets); csr->offsets = NULL;
        csr->targets = NULL;
        return TD_ERR_IO;
    }

    len = snprintf(path, sizeof(path), "%s/%s_rowmap.col", dir, prefix);
    if (len < 0 || (size_t)len >= sizeof(path)) return TD_ERR_IO;
    csr->rowmap = use_mmap ? td_col_mmap(path) : td_col_load(path);
    if (!csr->rowmap || TD_IS_ERR(csr->rowmap)) {
        /* rowmap is optional — ignore error */
        csr->rowmap = NULL;
    }

    if (csr->offsets->len < 1) {
        td_release(csr->offsets); csr->offsets = NULL;
        td_release(csr->targets); csr->targets = NULL;
        if (csr->rowmap) { td_release(csr->rowmap); csr->rowmap = NULL; }
        return TD_ERR_IO;
    }
    csr->n_nodes = csr->offsets->len - 1;
    csr->n_edges = csr->targets->len;

    /* Consistency: offsets[n_nodes] must equal targets->len */
    int64_t* off_data = (int64_t*)td_data(csr->offsets);
    if (off_data[csr->n_nodes] != csr->n_edges) {
        td_release(csr->offsets); csr->offsets = NULL;
        td_release(csr->targets); csr->targets = NULL;
        if (csr->rowmap) { td_release(csr->rowmap); csr->rowmap = NULL; }
        return TD_ERR_IO;
    }

    csr->sorted = false;  /* caller sets if known */
    csr->props = NULL;

    return TD_OK;
}

td_err_t td_rel_save(td_rel_t* rel, const char* dir) {
    if (!rel || !dir) return TD_ERR_IO;

    /* Create directory */
    if (mkdir(dir, 0755) != 0 && errno != EEXIST) return TD_ERR_IO;

    td_err_t err = csr_save(&rel->fwd, dir, "fwd");
    if (err != TD_OK) return err;

    err = csr_save(&rel->rev, dir, "rev");
    if (err != TD_OK) return err;

    /* Save metadata (from_table, to_table, name_sym, sorted flags) */
    char path[1024];
    int len = snprintf(path, sizeof(path), "%s/meta.col", dir);
    if (len < 0 || (size_t)len >= sizeof(path)) return TD_ERR_IO;

    /* Pack metadata into an I64 vector: [from_table, to_table, name_sym, fwd_sorted, rev_sorted] */
    int64_t meta_data[5];
    meta_data[0] = (int64_t)rel->from_table;
    meta_data[1] = (int64_t)rel->to_table;
    meta_data[2] = rel->name_sym;
    meta_data[3] = rel->fwd.sorted ? 1 : 0;
    meta_data[4] = rel->rev.sorted ? 1 : 0;
    td_t* meta_vec = td_vec_from_raw(TD_I64, meta_data, 5);
    if (!meta_vec || TD_IS_ERR(meta_vec)) return TD_ERR_OOM;
    err = td_col_save(meta_vec, path);
    td_release(meta_vec);

    return err;
}

static td_rel_t* rel_load_impl(const char* dir, bool use_mmap) {
    if (!dir) return NULL;

    td_rel_t* rel = (td_rel_t*)td_sys_alloc(sizeof(td_rel_t));
    if (!rel) return NULL;
    memset(rel, 0, sizeof(td_rel_t));

    td_err_t err = csr_load_impl(&rel->fwd, dir, "fwd", use_mmap);
    if (err != TD_OK) { td_sys_free(rel); return NULL; }

    err = csr_load_impl(&rel->rev, dir, "rev", use_mmap);
    if (err != TD_OK) {
        csr_free(&rel->fwd);
        td_sys_free(rel);
        return NULL;
    }

    /* Load metadata */
    char path[1024];
    int len = snprintf(path, sizeof(path), "%s/meta.col", dir);
    if (len >= 0 && (size_t)len < sizeof(path)) {
        td_t* meta = use_mmap ? td_col_mmap(path) : td_col_load(path);
        if (meta && !TD_IS_ERR(meta) && meta->len >= 5) {
            int64_t* md = (int64_t*)td_data(meta);
            rel->from_table = (uint16_t)md[0];
            rel->to_table = (uint16_t)md[1];
            rel->name_sym = md[2];
            rel->fwd.sorted = md[3] != 0;
            rel->rev.sorted = md[4] != 0;
            td_release(meta);
        } else if (meta && !TD_IS_ERR(meta)) {
            td_release(meta);
        }
    }

    return rel;
}

td_rel_t* td_rel_load(const char* dir) {
    return rel_load_impl(dir, false);
}

td_rel_t* td_rel_mmap(const char* dir) {
    return rel_load_impl(dir, true);
}
