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

#include "splay.h"
#include <string.h>
#include <stdio.h>
#include <sys/stat.h>
#include <errno.h>
#include <dirent.h>

/* --------------------------------------------------------------------------
 * Splayed table: directory of column files + .d schema file
 *
 * Format:
 *   dir/.d        — I64 vector of column name symbol IDs
 *   dir/<colname> — column file per column
 *
 * No symlink check: local-trust file format; path traversal checks
 * (rejecting '/', '\\', '..', leading '.') cover main attack vector.
 * -------------------------------------------------------------------------- */

/* --------------------------------------------------------------------------
 * td_splay_save — save a table to a splayed table directory
 * -------------------------------------------------------------------------- */

td_err_t td_splay_save(td_t* tbl, const char* dir, const char* sym_path) {
    if (!tbl || TD_IS_ERR(tbl)) return TD_ERR_TYPE;
    if (!dir) return TD_ERR_IO;

    /* sym_path saving is Phase 2 — ignore for now */
    (void)sym_path;

    /* Create directory */
    if (mkdir(dir, 0755) != 0 && errno != EEXIST) return TD_ERR_IO;

    int64_t ncols = td_table_ncols(tbl);

    /* Save .d schema file */
    td_t* schema = td_table_schema(tbl);
    if (schema) {
        /* Path buffer limited to 1024 chars — paths exceeding this are silently skipped. */
        char path[1024];
        int path_len = snprintf(path, sizeof(path), "%s/.d", dir);
        if (path_len < 0 || (size_t)path_len >= sizeof(path)) return TD_ERR_IO;
        td_err_t err = td_col_save(schema, path);
        if (err != TD_OK) return err;
    }

    /* Save each column */
    for (int64_t c = 0; c < ncols; c++) {
        td_t* col = td_table_get_col_idx(tbl, c);
        int64_t name_id = td_table_col_name(tbl, c);
        if (!col) continue;

        /* Get column name string */
        td_t* name_atom = td_sym_str(name_id);
        if (!name_atom) continue;

        const char* name = td_str_ptr(name_atom);
        size_t name_len = td_str_len(name_atom);

        /* Reject names with path separators, traversal, or starting with '.' */
        if (name_len == 0 || name[0] == '.' ||
            memchr(name, '/', name_len) || memchr(name, '\\', name_len) ||
            memchr(name, '\0', name_len))
            continue;

        char path[1024];
        int path_len = snprintf(path, sizeof(path), "%s/%.*s", dir, (int)name_len, name);
        if (path_len < 0 || (size_t)path_len >= sizeof(path)) continue;

        td_err_t err = td_col_save(col, path);
        /* On partial failure, columns 0..c-1 remain on disk.
         * Caller should clean up or use atomic rename for safe writes. */
        if (err != TD_OK) return err;
    }

    return TD_OK;
}

/* --------------------------------------------------------------------------
 * td_splay_load — load a splayed table from a directory
 * -------------------------------------------------------------------------- */

td_t* td_splay_load(const char* dir) {
    if (!dir) return TD_ERR_PTR(TD_ERR_IO);

    /* Path buffer limited to 1024 chars — paths exceeding this are silently skipped. */
    /* Load .d schema */
    char path[1024];
    int path_len = snprintf(path, sizeof(path), "%s/.d", dir);
    if (path_len < 0 || (size_t)path_len >= sizeof(path))
        return TD_ERR_PTR(TD_ERR_IO);
    td_t* schema = td_col_load(path);
    if (!schema || TD_IS_ERR(schema)) return schema;

    int64_t ncols = schema->len;
    int64_t* name_ids = (int64_t*)td_data(schema);

    td_t* tbl = td_table_new(ncols);
    if (!tbl || TD_IS_ERR(tbl)) {
        td_release(schema);
        return tbl;
    }

    /* Load each column */
    for (int64_t c = 0; c < ncols; c++) {
        int64_t name_id = name_ids[c];
        td_t* name_atom = td_sym_str(name_id);
        if (!name_atom) continue;

        const char* name = td_str_ptr(name_atom);
        size_t name_len = td_str_len(name_atom);

        /* Reject names with path separators, traversal, or starting with '.' */
        if (name_len == 0 || name[0] == '.' ||
            memchr(name, '/', name_len) || memchr(name, '\\', name_len) ||
            memchr(name, '\0', name_len))
            continue;

        /* Path buffer limited to 1024 chars — paths exceeding this are silently skipped. */
        path_len = snprintf(path, sizeof(path), "%s/%.*s", dir, (int)name_len, name);
        if (path_len < 0 || (size_t)path_len >= sizeof(path)) continue;

        td_t* col = td_col_load(path);
        if (!col || TD_IS_ERR(col)) {
            td_release(schema);
            td_release(tbl);
            return col ? col : TD_ERR_PTR(TD_ERR_IO);
        }

        td_t* new_df = td_table_add_col(tbl, name_id, col);
        if (!new_df || TD_IS_ERR(new_df)) {
            td_release(col);
            td_release(schema);
            td_release(tbl);
            return new_df ? new_df : TD_ERR_PTR(TD_ERR_OOM);
        }
        td_release(col); /* table_add_col retains; drop our ref */
        tbl = new_df;
    }

    td_release(schema);
    return tbl;
}

/* --------------------------------------------------------------------------
 * td_read_splayed — zero-copy splayed table load via mmap (mmod=1)
 *
 * Nearly identical to td_splay_load, but uses td_col_mmap for each column
 * file. The .d schema is still loaded via td_col_load (small, buddy copy).
 * -------------------------------------------------------------------------- */

td_t* td_read_splayed(const char* dir, const char* sym_path) {
    if (!dir) return TD_ERR_PTR(TD_ERR_IO);

    /* sym_path loading is Phase 2 — ignore for now */
    (void)sym_path;

    /* Path buffer limited to 1024 chars — paths exceeding this are silently skipped. */
    /* Load .d schema (small, use td_col_load — buddy copy is fine) */
    char path[1024];
    int path_len = snprintf(path, sizeof(path), "%s/.d", dir);
    if (path_len < 0 || (size_t)path_len >= sizeof(path))
        return TD_ERR_PTR(TD_ERR_IO);
    td_t* schema = td_col_load(path);
    if (!schema || TD_IS_ERR(schema)) return schema;

    int64_t ncols = schema->len;
    int64_t* name_ids = (int64_t*)td_data(schema);

    td_t* tbl = td_table_new(ncols);
    if (!tbl || TD_IS_ERR(tbl)) {
        td_release(schema);
        return tbl;
    }

    /* Load each column via mmap (zero-copy) */
    for (int64_t c = 0; c < ncols; c++) {
        int64_t name_id = name_ids[c];
        td_t* name_atom = td_sym_str(name_id);
        if (!name_atom) continue;

        const char* name = td_str_ptr(name_atom);
        size_t name_len = td_str_len(name_atom);

        if (name_len == 0 || name[0] == '.' ||
            memchr(name, '/', name_len) || memchr(name, '\\', name_len) ||
            memchr(name, '\0', name_len))
            continue;

        path_len = snprintf(path, sizeof(path), "%s/%.*s", dir, (int)name_len, name);
        if (path_len < 0 || (size_t)path_len >= sizeof(path)) continue;

        td_t* col = td_col_mmap(path);
        if (!col || TD_IS_ERR(col)) {
            td_release(schema);
            td_release(tbl);
            return col ? col : TD_ERR_PTR(TD_ERR_IO);
        }

        td_t* new_df = td_table_add_col(tbl, name_id, col);
        if (!new_df || TD_IS_ERR(new_df)) {
            td_release(col);
            td_release(schema);
            td_release(tbl);
            return new_df ? new_df : TD_ERR_PTR(TD_ERR_OOM);
        }
        td_release(col); /* table_add_col retains; drop our ref */
        tbl = new_df;
    }

    td_release(schema);
    return tbl;
}
