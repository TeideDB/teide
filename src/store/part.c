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

#define _POSIX_C_SOURCE 200809L
#include "part.h"
#include "mem/sys.h"
#include <string.h>
#include <stdio.h>
#include <dirent.h>

/* Validate YYYY.MM.DD format: exactly 10 chars, dots at pos 4/7,
 * month 01-12, day 01-31. */
static bool is_date_dir(const char* name) {
    if (strlen(name) != 10) return false;
    if (name[4] != '.' || name[7] != '.') return false;
    for (int i = 0; i < 10; i++) {
        if (i == 4 || i == 7) continue;
        if (name[i] < '0' || name[i] > '9') return false;
    }
    int month = (name[5] - '0') * 10 + (name[6] - '0');
    int day   = (name[8] - '0') * 10 + (name[9] - '0');
    return month >= 1 && month <= 12 && day >= 1 && day <= 31;
}

/* Check if string is a pure integer (digits only, possibly with leading minus). */
static bool is_integer_str(const char* s) {
    if (!*s) return false;
    if (*s == '-') s++;
    if (!*s) return false;
    for (; *s; s++)
        if (*s < '0' || *s > '9') return false;
    return true;
}

/* Infer MAPCOMMON sub-type from partition directory names. */
static uint8_t infer_mc_type(char** part_dirs, int64_t part_count) {
    bool all_date = true, all_int = true;
    for (int64_t i = 0; i < part_count; i++) {
        if (all_date && !is_date_dir(part_dirs[i])) all_date = false;
        if (all_int && !is_integer_str(part_dirs[i])) all_int = false;
        if (!all_date && !all_int) break;
    }
    if (all_date) return TD_MC_DATE;
    if (all_int) return TD_MC_I64;
    return TD_MC_SYM;
}

/* Parse "YYYY.MM.DD" → days since 2000-01-01 (Postgres epoch).
 * Uses inverse of Hinnant's civil_from_days algorithm (same as exec.c). */
static int32_t parse_date_dir(const char* name) {
    int64_t y = (name[0]-'0')*1000 + (name[1]-'0')*100 +
                (name[2]-'0')*10   + (name[3]-'0');
    int64_t m = (name[5]-'0')*10 + (name[6]-'0');
    int64_t d = (name[8]-'0')*10 + (name[9]-'0');
    y -= (m <= 2);
    int64_t era = (y >= 0 ? y : y - 399) / 400;
    uint64_t yoe = (uint64_t)(y - era * 400);
    uint64_t doy = (153 * (m > 2 ? (uint64_t)m-3 : (uint64_t)m+9) + 2)/5 + (uint64_t)d - 1;
    uint64_t doe = yoe*365 + yoe/4 - yoe/100 + doy;
    return (int32_t)(era * 146097 + (int64_t)doe - 719468 - 10957);
}

/* Parse integer string → int64_t. Caller guarantees is_integer_str(). */
static int64_t parse_int_dir(const char* s) {
    int neg = 0;
    if (*s == '-') { neg = 1; s++; }
    int64_t v = 0;
    for (; *s; s++) v = v * 10 + (*s - '0');
    return neg ? -v : v;
}

/* --------------------------------------------------------------------------
 * Partitioned table: date-partitioned directory of splayed tables
 *
 * Format:
 *   db_root/sym              — global symbol intern table
 *   db_root/YYYY.MM.DD/      — partition directories
 *   db_root/YYYY.MM.DD/table — splayed table per partition
 *
 * No symlink check: local-trust file format; path traversal checks
 * cover main attack vector.
 * -------------------------------------------------------------------------- */

/* --------------------------------------------------------------------------
 * td_part_load — load a partitioned table
 *
 * Discovers partition directories, loads each splayed table, and
 * concatenates columns across partitions.
 * -------------------------------------------------------------------------- */

td_t* td_part_load(const char* db_root, const char* table_name) {
    if (!db_root || !table_name) return TD_ERR_PTR(TD_ERR_IO);

    /* Validate table_name: no path separators or traversal */
    if (strchr(table_name, '/') || strchr(table_name, '\\') ||
        strstr(table_name, "..") || table_name[0] == '.')
        return TD_ERR_PTR(TD_ERR_IO);

    /* Scan db_root for partition directories (YYYY.MM.DD format) */
    DIR* d = opendir(db_root);
    if (!d) return TD_ERR_PTR(TD_ERR_IO);

    /* Collect partition directory names */
    char** part_dirs = NULL;
    int64_t part_count = 0;
    int64_t part_cap = 0;

    struct dirent* ent;
    while ((ent = readdir(d)) != NULL) {
        /* Skip . and .. and non-directories */
        if (ent->d_name[0] == '.') continue;

        /* Partition directory name format validation is intentionally loose:
         * accepts any sequence of digits and dots (e.g. "2024.01.15", "1.2.3").
         * The actual format is not strictly enforced here -- invalid entries
         * will simply fail during splay load and be caught there.
         * Non-conforming entries are harmless and silently skipped. */
        bool valid = false;
        for (const char* c = ent->d_name; *c; c++) {
            if (*c == '.') { valid = true; continue; }
            if (*c >= '0' && *c <= '9') continue;
            valid = false; break;
        }
        if (!valid) continue;

        if (part_count >= part_cap) {
            part_cap = part_cap == 0 ? 16 : part_cap * 2;
            char** tmp = (char**)td_sys_realloc(part_dirs, (size_t)part_cap * sizeof(char*));
            if (!tmp) break; /* OOM — stop collecting */
            part_dirs = tmp;
        }
        char* dup = td_sys_strdup(ent->d_name);
        if (!dup) break;
        part_dirs[part_count] = dup;
        part_count++;
    }
    closedir(d);

    if (part_count == 0) {
        /* No partition directories found in db_root */
        td_sys_free(part_dirs);
        return TD_ERR_PTR(TD_ERR_IO);
    }

    /* Sort partition names for deterministic order.
     * O(n^2) but partition count is typically small (< 1000 daily partitions). */
    for (int64_t i = 0; i < part_count - 1; i++) {
        for (int64_t j = i + 1; j < part_count; j++) {
            if (strcmp(part_dirs[i], part_dirs[j]) > 0) {
                char* tmp = part_dirs[i];
                part_dirs[i] = part_dirs[j];
                part_dirs[j] = tmp;
            }
        }
    }

    /* Load first partition to get schema. */
    char path[1024];
    int n = snprintf(path, sizeof(path), "%s/%s/%s", db_root, part_dirs[0], table_name);
    if (n < 0 || (size_t)n >= sizeof(path)) {
        for (int64_t i = 0; i < part_count; i++) td_sys_free(part_dirs[i]);
        td_sys_free(part_dirs);
        return TD_ERR_PTR(TD_ERR_IO);
    }
    td_t* first = td_splay_load(path);
    if (!first || TD_IS_ERR(first)) {
        for (int64_t i = 0; i < part_count; i++) td_sys_free(part_dirs[i]);
        td_sys_free(part_dirs);
        return first;
    }

    if (part_count == 1) {
        for (int64_t i = 0; i < part_count; i++) td_sys_free(part_dirs[i]);
        td_sys_free(part_dirs);
        return first;
    }

    /* Load remaining partitions and concatenate */
    int64_t ncols = td_table_ncols(first);
    /* Accumulate rows from all partitions */
    td_t** all_dfs = (td_t**)td_sys_alloc((size_t)part_count * sizeof(td_t*));
    if (!all_dfs) {
        td_release(first);
        for (int64_t i = 0; i < part_count; i++) td_sys_free(part_dirs[i]);
        td_sys_free(part_dirs);
        return TD_ERR_PTR(TD_ERR_OOM);
    }
    all_dfs[0] = first;

    int64_t fail_count = 0;
    for (int64_t p = 1; p < part_count; p++) {
        n = snprintf(path, sizeof(path), "%s/%s/%s", db_root, part_dirs[p], table_name);
        if (n < 0 || (size_t)n >= sizeof(path)) { all_dfs[p] = NULL; fail_count++; continue; }
        all_dfs[p] = td_splay_load(path);
        if (!all_dfs[p] || TD_IS_ERR(all_dfs[p])) {
            all_dfs[p] = NULL;
            fail_count++;
        }
    }
    if (fail_count > 0) {
        /* One or more partition splay loads failed -- abort entire load */
        for (int64_t p = 0; p < part_count; p++) {
            if (all_dfs[p] && !TD_IS_ERR(all_dfs[p]))
                td_release(all_dfs[p]);
            td_sys_free(part_dirs[p]);
        }
        td_sys_free(all_dfs);
        td_sys_free(part_dirs);
        return TD_ERR_PTR(TD_ERR_IO);
    }

    /* Build combined table by concatenating columns */
    td_t* result = td_table_new(ncols);
    for (int64_t c = 0; c < ncols; c++) {
        int64_t name_id = td_table_col_name(first, c);
        td_t* combined = td_table_get_col_idx(first, c);
        if (!combined) continue;
        td_retain(combined);

        for (int64_t p = 1; p < part_count; p++) {
            if (!all_dfs[p] || TD_IS_ERR(all_dfs[p])) continue;
            td_t* part_col = td_table_get_col_idx(all_dfs[p], c);
            if (part_col) {
                td_t* new_combined = td_vec_concat(combined, part_col);
                td_release(combined);
                if (!new_combined || TD_IS_ERR(new_combined)) {
                    combined = NULL;
                    break;
                }
                combined = new_combined;
            }
        }

        if (!combined) {
            td_release(result);
            result = NULL;
            break;
        }
        result = td_table_add_col(result, name_id, combined);
        td_release(combined);
        if (!result || TD_IS_ERR(result)) break;
    }

    /* Cleanup */
    for (int64_t p = 0; p < part_count; p++) {
        if (all_dfs[p] && !TD_IS_ERR(all_dfs[p]))
            td_release(all_dfs[p]);
        td_sys_free(part_dirs[p]);
    }
    td_sys_free(all_dfs);
    td_sys_free(part_dirs);

    return result ? result : TD_ERR_PTR(TD_ERR_OOM);
}

/* --------------------------------------------------------------------------
 * td_read_parted — zero-copy open of a partitioned table
 *
 * Builds parted columns (TD_PARTED_BASE + base_type) where each segment
 * is an mmap'd vector from td_read_splayed. Also builds a MAPCOMMON column
 * with partition key names and row counts.
 * -------------------------------------------------------------------------- */

td_t* td_read_parted(const char* db_root, const char* table_name) {
    if (!db_root || !table_name) return TD_ERR_PTR(TD_ERR_IO);

    /* Validate table_name: no path separators or traversal */
    if (strchr(table_name, '/') || strchr(table_name, '\\') ||
        strstr(table_name, "..") || table_name[0] == '.')
        return TD_ERR_PTR(TD_ERR_IO);

    /* Build sym_path. */
    char sym_path[1024];
    int sn = snprintf(sym_path, sizeof(sym_path), "%s/sym", db_root);
    if (sn < 0 || (size_t)sn >= sizeof(sym_path))
        return TD_ERR_PTR(TD_ERR_IO);

    /* Load global symfile */
    td_err_t sym_err = td_sym_load(sym_path);
    if (sym_err != TD_OK) return TD_ERR_PTR(sym_err);

    /* Scan db_root for partition directories */
    DIR* d = opendir(db_root);
    if (!d) return TD_ERR_PTR(TD_ERR_IO);

    char** part_dirs = NULL;
    int64_t part_count = 0;
    int64_t part_cap = 0;

    struct dirent* ent;
    while ((ent = readdir(d)) != NULL) {
        if (ent->d_name[0] == '.') continue;
        if (strcmp(ent->d_name, "sym") == 0) continue;

        /* Partition directory name format validation is intentionally loose:
         * accepts any sequence of digits and dots (e.g. "2024.01.15").
         * Invalid entries fail during splay open and are caught there. */
        bool valid = false;
        for (const char* c = ent->d_name; *c; c++) {
            if (*c == '.') { valid = true; continue; }
            if (*c >= '0' && *c <= '9') continue;
            valid = false; break;
        }
        if (!valid) continue;

        if (part_count >= part_cap) {
            part_cap = part_cap == 0 ? 16 : part_cap * 2;
            char** tmp = (char**)td_sys_realloc(part_dirs, (size_t)part_cap * sizeof(char*));
            if (!tmp) break;
            part_dirs = tmp;
        }
        char* dup = td_sys_strdup(ent->d_name);
        if (!dup) break;
        part_dirs[part_count++] = dup;
    }
    closedir(d);

    if (part_count == 0) {
        /* No partition directories found in db_root */
        td_sys_free(part_dirs);
        return TD_ERR_PTR(TD_ERR_IO);
    }

    /* Sort partition names for deterministic order.
     * O(n^2) but partition count is typically small (< 1000 daily partitions). */
    for (int64_t i = 0; i < part_count - 1; i++) {
        for (int64_t j = i + 1; j < part_count; j++) {
            if (strcmp(part_dirs[i], part_dirs[j]) > 0) {
                char* tmp = part_dirs[i];
                part_dirs[i] = part_dirs[j];
                part_dirs[j] = tmp;
            }
        }
    }

    /* Open each partition via td_read_splayed */
    td_t** part_tables = (td_t**)td_sys_alloc((size_t)part_count * sizeof(td_t*));
    if (!part_tables) goto fail_dirs;
    memset(part_tables, 0, (size_t)part_count * sizeof(td_t*));

    char path[1024];
    for (int64_t p = 0; p < part_count; p++) {
        int pn = snprintf(path, sizeof(path), "%s/%s/%s", db_root, part_dirs[p], table_name);
        if (pn < 0 || (size_t)pn >= sizeof(path)) {
            part_tables[p] = NULL;
            goto fail_tables;
        }
        part_tables[p] = td_read_splayed(path, sym_path);
        if (!part_tables[p] || TD_IS_ERR(part_tables[p])) {
            part_tables[p] = NULL;
            goto fail_tables;
        }
    }

    /* Get schema from first partition */
    int64_t ncols = td_table_ncols(part_tables[0]);
    if (ncols <= 0) goto fail_tables;

    /* Infer MAPCOMMON sub-type from partition directory names */
    uint8_t mc_type = infer_mc_type(part_dirs, part_count);

    /* Build result table: 1 MAPCOMMON + ncols data columns */
    td_t* result = td_table_new(ncols + 2);
    if (!result || TD_IS_ERR(result)) goto fail_tables;

    /* ---- MAPCOMMON column (first) ---- */
    {
        /* key_values type matches inferred partition key type */
        int8_t kv_type = (mc_type == TD_MC_DATE) ? TD_DATE
                       : (mc_type == TD_MC_I64)  ? TD_I64
                       :                           TD_SYM;
        td_t* key_values = td_vec_new(kv_type, part_count);
        td_t* row_counts = td_vec_new(TD_I64, part_count);
        if (!key_values || TD_IS_ERR(key_values) ||
            !row_counts || TD_IS_ERR(row_counts)) {
            if (key_values && !TD_IS_ERR(key_values)) td_release(key_values);
            if (row_counts && !TD_IS_ERR(row_counts)) td_release(row_counts);
            td_release(result);
            goto fail_tables;
        }

        int64_t* rc_data = (int64_t*)td_data(row_counts);
        if (mc_type == TD_MC_DATE) {
            int32_t* kv_data = (int32_t*)td_data(key_values);
            for (int64_t p = 0; p < part_count; p++) {
                kv_data[p] = parse_date_dir(part_dirs[p]);
                rc_data[p] = td_table_nrows(part_tables[p]);
            }
        } else if (mc_type == TD_MC_I64) {
            int64_t* kv_data = (int64_t*)td_data(key_values);
            for (int64_t p = 0; p < part_count; p++) {
                kv_data[p] = parse_int_dir(part_dirs[p]);
                rc_data[p] = td_table_nrows(part_tables[p]);
            }
        } else {
            int64_t* kv_data = (int64_t*)td_data(key_values);
            for (int64_t p = 0; p < part_count; p++) {
                kv_data[p] = td_sym_intern(part_dirs[p], strlen(part_dirs[p]));
                rc_data[p] = td_table_nrows(part_tables[p]);
            }
        }
        key_values->len = part_count;
        row_counts->len = part_count;

        td_t* mapcommon = td_alloc(2 * sizeof(td_t*));
        if (!mapcommon || TD_IS_ERR(mapcommon)) {
            td_release(key_values);
            td_release(row_counts);
            td_release(result);
            goto fail_tables;
        }
        mapcommon->type = TD_MAPCOMMON;
        mapcommon->len = 2;
        mapcommon->attrs = mc_type;
        memset(mapcommon->nullmap, 0, 16);

        td_t** mc_ptrs = (td_t**)td_data(mapcommon);
        mc_ptrs[0] = key_values;  td_retain(key_values);
        mc_ptrs[1] = row_counts;  td_retain(row_counts);

        const char* mc_name = (mc_type == TD_MC_DATE) ? "date" : "part";
        int64_t part_name_id = td_sym_intern(mc_name, strlen(mc_name));
        result = td_table_add_col(result, part_name_id, mapcommon);
        if (!result || TD_IS_ERR(result)) {
            td_release(mapcommon);
            td_release(key_values);
            td_release(row_counts);
            goto fail_tables;
        }

        td_release(mapcommon);
        td_release(key_values);
        td_release(row_counts);
    }

    /* ---- Data columns (after MAPCOMMON) ---- */
    for (int64_t c = 0; c < ncols; c++) {
        int64_t name_id = td_table_col_name(part_tables[0], c);
        td_t* first_seg = td_table_get_col_idx(part_tables[0], c);
        if (!first_seg) continue;

        td_t* parted = td_alloc((size_t)part_count * sizeof(td_t*));
        if (!parted || TD_IS_ERR(parted)) {
            td_release(result);
            goto fail_tables;
        }
        parted->type = TD_PARTED_BASE + first_seg->type;
        parted->len = part_count;
        parted->attrs = 0;
        memset(parted->nullmap, 0, 16);

        td_t** segs = (td_t**)td_data(parted);
        for (int64_t p = 0; p < part_count; p++) {
            td_t* seg = td_table_get_col_idx(part_tables[p], c);
            if (!seg) {
                segs[p] = NULL;
                continue;
            }
            td_retain(seg);
            segs[p] = seg;
            td_vm_advise_willneed(td_data(seg),
                                  (size_t)seg->len * td_sym_elem_size(seg->type, seg->attrs));
        }

        result = td_table_add_col(result, name_id, parted);
        td_release(parted);
        if (!result || TD_IS_ERR(result)) goto fail_tables;
    }

    /* Release partition sub-tables (segment vectors survive via retain) */
    for (int64_t p = 0; p < part_count; p++) {
        if (part_tables[p]) td_release(part_tables[p]);
        td_sys_free(part_dirs[p]);
    }
    td_sys_free(part_tables);
    td_sys_free(part_dirs);

    return result;

fail_tables:
    for (int64_t p = 0; p < part_count; p++) {
        if (part_tables[p] && !TD_IS_ERR(part_tables[p]))
            td_release(part_tables[p]);
    }
    td_sys_free(part_tables);

fail_dirs:
    for (int64_t p = 0; p < part_count; p++)
        td_sys_free(part_dirs[p]);
    td_sys_free(part_dirs);

    return TD_ERR_PTR(TD_ERR_IO);
}
