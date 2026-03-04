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

#include "col.h"
#include <string.h>
#include <stdio.h>
#include <stdatomic.h>

/* Magic numbers for extended column formats */
#define STR_LIST_MAGIC  0x4C525453U  /* "STRL" */
#define LIST_MAGIC      0x4754534CU  /* "LSTG" */
#define TABLE_MAGIC     0x4C425454U  /* "TTBL" */

/* --------------------------------------------------------------------------
 * Column file format:
 *   Bytes 0-15:  nullmap (inline) or zeroed (ext_nullmap / no nulls)
 *   Bytes 16-31: mmod=0, order=0, type, attrs, rc=0, len
 *   Bytes 32+:   raw element data
 *   (if TD_ATTR_NULLMAP_EXT): appended (len+7)/8 bitmap bytes
 *
 * On-disk format IS the in-memory format (zero deserialization on load).
 * -------------------------------------------------------------------------- */

/* Explicit allowlist of types that are safe to serialize as raw bytes.
 * Only fixed-size scalar types -- pointer-bearing types (STR, LIST, TABLE)
 * and non-scalar types are excluded. */
static bool is_serializable_type(int8_t t) {
    switch (t) {
    case TD_BOOL: case TD_U8:   case TD_CHAR:  case TD_I16:
    case TD_I32:  case TD_I64:  case TD_F64:
    case TD_DATE: case TD_TIME: case TD_TIMESTAMP: case TD_GUID:
    case TD_SYM:
        return true;
    default:
        return false;
    }
}

/* --------------------------------------------------------------------------
 * String list detection: TD_LIST whose elements are all TD_ATOM_STR
 * -------------------------------------------------------------------------- */

static bool is_str_list(td_t* v) {
    if (!v || TD_IS_ERR(v)) return false;
    if (v->type != TD_LIST) return false;
    td_t** slots = (td_t**)td_data(v);
    for (int64_t i = 0; i < v->len; i++) {
        td_t* elem = slots[i];
        if (!elem || TD_IS_ERR(elem)) return false;
        if (elem->type != TD_ATOM_STR) return false;
    }
    return true;
}

/* --------------------------------------------------------------------------
 * col_save_str_list -- serialize a list of string atoms
 *
 * Format: [4B magic "STRL"][8B count][for each: 4B len + data bytes]
 * -------------------------------------------------------------------------- */

static td_err_t col_save_str_list(td_t* list, FILE* f) {
    uint32_t magic = STR_LIST_MAGIC;
    if (fwrite(&magic, 4, 1, f) != 1) return TD_ERR_IO;

    int64_t count = list->len;
    if (fwrite(&count, 8, 1, f) != 1) return TD_ERR_IO;

    td_t** slots = (td_t**)td_data(list);
    for (int64_t i = 0; i < count; i++) {
        td_t* s = slots[i];
        const char* sp = td_str_ptr(s);
        size_t slen = td_str_len(s);
        uint32_t len32 = (uint32_t)slen;
        if (fwrite(&len32, 4, 1, f) != 1) return TD_ERR_IO;
        if (slen > 0 && fwrite(sp, 1, slen, f) != slen) return TD_ERR_IO;
    }
    return TD_OK;
}

/* --------------------------------------------------------------------------
 * col_load_str_list -- deserialize a string list from mapped data
 *
 * ptr points past the 4B magic. remaining = bytes available.
 * -------------------------------------------------------------------------- */

static td_t* col_load_str_list(const uint8_t* ptr, size_t remaining) {
    if (remaining < 8) return TD_ERR_PTR(TD_ERR_CORRUPT);
    int64_t count;
    memcpy(&count, ptr, 8);
    ptr += 8; remaining -= 8;

    if (count < 0 || (uint64_t)count > remaining)
        return TD_ERR_PTR(TD_ERR_CORRUPT);

    td_t* list = td_list_new(count);
    if (!list || TD_IS_ERR(list)) return list;

    for (int64_t i = 0; i < count; i++) {
        if (remaining < 4) { td_release(list); return TD_ERR_PTR(TD_ERR_CORRUPT); }
        uint32_t slen;
        memcpy(&slen, ptr, 4);
        ptr += 4; remaining -= 4;

        if (slen > remaining) { td_release(list); return TD_ERR_PTR(TD_ERR_CORRUPT); }
        td_t* s = td_str((const char*)ptr, (size_t)slen);
        if (!s || TD_IS_ERR(s)) { td_release(list); return s; }
        ptr += slen; remaining -= slen;

        list = td_list_append(list, s);
        td_release(s);  /* list_append retains */
        if (!list || TD_IS_ERR(list)) return list;
    }
    return list;
}

/* --------------------------------------------------------------------------
 * Recursive element serialization for generic lists and tables
 *
 * Recursive element format:
 *   [1B type]
 *   atoms (type < 0):
 *     TD_ATOM_STR: [4B len][data bytes]
 *     other:       [8B raw value]
 *   vectors with is_serializable_type: [8B len][raw data]
 *   TD_LIST: [8B count][recursive elements...]
 *   TD_TABLE: [8B ncols][8B nrows][for each col: 8B name_sym + recursive col]
 * -------------------------------------------------------------------------- */

static td_err_t col_write_recursive(td_t* obj, FILE* f);

static td_err_t col_write_recursive(td_t* obj, FILE* f) {
    if (!obj || TD_IS_ERR(obj)) return TD_ERR_TYPE;

    int8_t type = obj->type;
    if (fwrite(&type, 1, 1, f) != 1) return TD_ERR_IO;

    if (type < 0) {
        /* Atom */
        if (type == TD_ATOM_STR) {
            const char* sp = td_str_ptr(obj);
            size_t slen = td_str_len(obj);
            uint32_t len32 = (uint32_t)slen;
            if (fwrite(&len32, 4, 1, f) != 1) return TD_ERR_IO;
            if (slen > 0 && fwrite(sp, 1, slen, f) != slen) return TD_ERR_IO;
        } else {
            /* Fixed-size atom: write 8 bytes of the value union */
            if (fwrite(&obj->i64, 8, 1, f) != 1) return TD_ERR_IO;
        }
        return TD_OK;
    }

    if (is_serializable_type(type)) {
        /* Fixed-size vector: write len + raw data */
        int64_t len = obj->len;
        if (fwrite(&len, 8, 1, f) != 1) return TD_ERR_IO;
        uint8_t esz = td_sym_elem_size(type, obj->attrs);
        size_t data_size = (size_t)len * esz;
        if (data_size > 0 && fwrite(td_data(obj), 1, data_size, f) != data_size)
            return TD_ERR_IO;
        return TD_OK;
    }

    if (type == TD_LIST) {
        int64_t count = obj->len;
        if (fwrite(&count, 8, 1, f) != 1) return TD_ERR_IO;
        td_t** slots = (td_t**)td_data(obj);
        for (int64_t i = 0; i < count; i++) {
            td_err_t err = col_write_recursive(slots[i], f);
            if (err != TD_OK) return err;
        }
        return TD_OK;
    }

    if (type == TD_TABLE) {
        int64_t ncols = td_table_ncols(obj);
        int64_t nrows = td_table_nrows(obj);
        if (fwrite(&ncols, 8, 1, f) != 1) return TD_ERR_IO;
        if (fwrite(&nrows, 8, 1, f) != 1) return TD_ERR_IO;
        for (int64_t c = 0; c < ncols; c++) {
            int64_t name_sym = td_table_col_name(obj, c);
            if (fwrite(&name_sym, 8, 1, f) != 1) return TD_ERR_IO;
            td_t* col = td_table_get_col_idx(obj, c);
            td_err_t err = col_write_recursive(col, f);
            if (err != TD_OK) return err;
        }
        return TD_OK;
    }

    return TD_ERR_NYI;
}

/* Read recursive element from mapped buffer */
static td_t* col_read_recursive(const uint8_t** pp, size_t* remaining);

static td_t* col_read_recursive(const uint8_t** pp, size_t* remaining) {
    if (*remaining < 1) return TD_ERR_PTR(TD_ERR_CORRUPT);
    int8_t type;
    memcpy(&type, *pp, 1);
    *pp += 1; *remaining -= 1;

    if (type < 0) {
        /* Atom */
        if (type == TD_ATOM_STR) {
            if (*remaining < 4) return TD_ERR_PTR(TD_ERR_CORRUPT);
            uint32_t slen;
            memcpy(&slen, *pp, 4);
            *pp += 4; *remaining -= 4;
            if (slen > *remaining) return TD_ERR_PTR(TD_ERR_CORRUPT);
            td_t* s = td_str((const char*)*pp, (size_t)slen);
            *pp += slen; *remaining -= slen;
            return s;
        } else {
            /* Fixed atom: 8 bytes */
            if (*remaining < 8) return TD_ERR_PTR(TD_ERR_CORRUPT);
            int64_t val;
            memcpy(&val, *pp, 8);
            *pp += 8; *remaining -= 8;

            td_t* atom = td_alloc(0);
            if (!atom || TD_IS_ERR(atom)) return atom;
            atom->type = type;
            atom->i64 = val;
            return atom;
        }
    }

    if (is_serializable_type(type)) {
        /* Fixed-size vector */
        if (*remaining < 8) return TD_ERR_PTR(TD_ERR_CORRUPT);
        int64_t len;
        memcpy(&len, *pp, 8);
        *pp += 8; *remaining -= 8;
        if (len < 0) return TD_ERR_PTR(TD_ERR_CORRUPT);

        uint8_t esz = td_type_sizes[type];
        if (esz > 0 && (uint64_t)len > SIZE_MAX / esz)
            return TD_ERR_PTR(TD_ERR_CORRUPT);
        size_t data_size = (size_t)len * esz;
        if (data_size > *remaining) return TD_ERR_PTR(TD_ERR_CORRUPT);

        td_t* vec = td_vec_new(type, len);
        if (!vec || TD_IS_ERR(vec)) return vec;
        vec->len = len;
        if (data_size > 0)
            memcpy(td_data(vec), *pp, data_size);
        *pp += data_size; *remaining -= data_size;
        return vec;
    }

    if (type == TD_LIST) {
        if (*remaining < 8) return TD_ERR_PTR(TD_ERR_CORRUPT);
        int64_t count;
        memcpy(&count, *pp, 8);
        *pp += 8; *remaining -= 8;
        if (count < 0) return TD_ERR_PTR(TD_ERR_CORRUPT);

        td_t* list = td_list_new(count);
        if (!list || TD_IS_ERR(list)) return list;
        for (int64_t i = 0; i < count; i++) {
            td_t* elem = col_read_recursive(pp, remaining);
            if (!elem || TD_IS_ERR(elem)) { td_release(list); return elem; }
            list = td_list_append(list, elem);
            td_release(elem);
            if (!list || TD_IS_ERR(list)) return list;
        }
        return list;
    }

    if (type == TD_TABLE) {
        if (*remaining < 16) return TD_ERR_PTR(TD_ERR_CORRUPT);
        int64_t ncols, nrows;
        memcpy(&ncols, *pp, 8);
        *pp += 8; *remaining -= 8;
        memcpy(&nrows, *pp, 8);
        *pp += 8; *remaining -= 8;
        (void)nrows;  /* nrows is reconstructed from columns */

        if (ncols < 0) return TD_ERR_PTR(TD_ERR_CORRUPT);
        td_t* tbl = td_table_new(ncols);
        if (!tbl || TD_IS_ERR(tbl)) return tbl;

        for (int64_t c = 0; c < ncols; c++) {
            if (*remaining < 8) { td_release(tbl); return TD_ERR_PTR(TD_ERR_CORRUPT); }
            int64_t name_sym;
            memcpy(&name_sym, *pp, 8);
            *pp += 8; *remaining -= 8;

            td_t* col = col_read_recursive(pp, remaining);
            if (!col || TD_IS_ERR(col)) { td_release(tbl); return col; }
            tbl = td_table_add_col(tbl, name_sym, col);
            td_release(col);  /* table_add_col retains */
            if (!tbl || TD_IS_ERR(tbl)) return tbl;
        }
        return tbl;
    }

    return TD_ERR_PTR(TD_ERR_NYI);
}

/* --------------------------------------------------------------------------
 * col_save_list -- serialize a generic TD_LIST
 * -------------------------------------------------------------------------- */

static td_err_t col_save_list(td_t* list, FILE* f) {
    uint32_t magic = LIST_MAGIC;
    if (fwrite(&magic, 4, 1, f) != 1) return TD_ERR_IO;
    return col_write_recursive(list, f);
}

/* --------------------------------------------------------------------------
 * col_save_table -- serialize a TD_TABLE
 * -------------------------------------------------------------------------- */

static td_err_t col_save_table(td_t* tbl, FILE* f) {
    uint32_t magic = TABLE_MAGIC;
    if (fwrite(&magic, 4, 1, f) != 1) return TD_ERR_IO;
    return col_write_recursive(tbl, f);
}

/* --------------------------------------------------------------------------
 * td_col_save -- write a vector to a column file
 * -------------------------------------------------------------------------- */

td_err_t td_col_save(td_t* vec, const char* path) {
    if (!vec || TD_IS_ERR(vec)) return TD_ERR_TYPE;
    if (!path) return TD_ERR_IO;

    /* String list: TD_LIST of TD_ATOM_STR atoms */
    if (is_str_list(vec)) {
        FILE* f = fopen(path, "wb");
        if (!f) return TD_ERR_IO;
        td_err_t err = col_save_str_list(vec, f);
        fclose(f);
        return err;
    }

    /* Generic list */
    if (vec->type == TD_LIST) {
        FILE* f = fopen(path, "wb");
        if (!f) return TD_ERR_IO;
        td_err_t err = col_save_list(vec, f);
        fclose(f);
        return err;
    }

    /* Table */
    if (vec->type == TD_TABLE) {
        FILE* f = fopen(path, "wb");
        if (!f) return TD_ERR_IO;
        td_err_t err = col_save_table(vec, f);
        fclose(f);
        return err;
    }

    /* Explicit allowlist of serializable types */
    if (!is_serializable_type(vec->type))
        return TD_ERR_NYI;

    FILE* f = fopen(path, "wb");
    if (!f) return TD_ERR_IO;

    /* Write a clean header (mmod=0, rc=0) */
    td_t header;
    memcpy(&header, vec, 32);
    header.mmod = 0;
    header.order = 0;
    atomic_store_explicit(&header.rc, 0, memory_order_relaxed);

    /* Clear slice field; preserve ext_nullmap flag for bitmap append */
    header.attrs &= ~TD_ATTR_SLICE;
    if (!(header.attrs & TD_ATTR_HAS_NULLS)) {
        memset(header.nullmap, 0, 16);
        header.attrs &= ~TD_ATTR_NULLMAP_EXT;
    } else if (header.attrs & TD_ATTR_NULLMAP_EXT) {
        /* Ext bitmap appended after data; zero pointer bytes in header */
        memset(header.nullmap, 0, 16);
    }

    size_t written = fwrite(&header, 1, 32, f);
    if (written != 32) { fclose(f); return TD_ERR_IO; }

    /* Write data */
    if (vec->len < 0) { fclose(f); return TD_ERR_CORRUPT; }
    uint8_t esz = td_sym_elem_size(vec->type, vec->attrs);
    if (esz == 0 && vec->len > 0) { fclose(f); return TD_ERR_TYPE; }
    /* Overflow check: ensure len*esz fits in size_t with 32-byte header room */
    if ((uint64_t)vec->len > (SIZE_MAX - 32) / (esz ? esz : 1)) {
        fclose(f);
        return TD_ERR_IO;
    }
    size_t data_size = (size_t)vec->len * esz;

    void* data;
    if (vec->attrs & TD_ATTR_SLICE) {
        /* Validate slice bounds before computing data pointer */
        td_t* parent = vec->slice_parent;
        if (!parent || vec->slice_offset < 0 ||
            vec->slice_offset + vec->len > parent->len) {
            fclose(f);
            return TD_ERR_IO;
        }
        data = (char*)td_data(parent) + vec->slice_offset * esz;
    } else {
        data = td_data(vec);
    }

    if (data_size > 0) {
        written = fwrite(data, 1, data_size, f);
        if (written != data_size) { fclose(f); return TD_ERR_IO; }
    }

    /* Append external nullmap bitmap after data */
    if ((vec->attrs & TD_ATTR_HAS_NULLS) &&
        (vec->attrs & TD_ATTR_NULLMAP_EXT) && vec->ext_nullmap) {
        size_t bitmap_len = ((size_t)vec->len + 7) / 8;
        written = fwrite(td_data(vec->ext_nullmap), 1, bitmap_len, f);
        if (written != bitmap_len) { fclose(f); return TD_ERR_IO; }
    }

    /* No fsync; durability not guaranteed on power failure. */
    fclose(f);
    return TD_OK;
}

/* --------------------------------------------------------------------------
 * td_col_load -- load a column file via mmap (zero deserialization)
 * -------------------------------------------------------------------------- */

td_t* td_col_load(const char* path) {
    if (!path) return TD_ERR_PTR(TD_ERR_IO);

    /* Read file into temp mmap for validation, then copy to buddy block.
     * This avoids the mmap lifecycle problem (mmod=1 blocks are never freed). */
    size_t mapped_size = 0;
    void* ptr = td_vm_map_file(path, &mapped_size);
    if (!ptr) return TD_ERR_PTR(TD_ERR_IO);

    /* Check for extended format magic numbers (first 4 bytes) */
    if (mapped_size >= 4) {
        uint32_t magic;
        memcpy(&magic, ptr, 4);

        if (magic == STR_LIST_MAGIC) {
            td_t* result = col_load_str_list((const uint8_t*)ptr + 4, mapped_size - 4);
            td_vm_unmap_file(ptr, mapped_size);
            return result;
        }
        if (magic == LIST_MAGIC || magic == TABLE_MAGIC) {
            const uint8_t* p = (const uint8_t*)ptr + 4;
            size_t rem = mapped_size - 4;
            td_t* result = col_read_recursive(&p, &rem);
            td_vm_unmap_file(ptr, mapped_size);
            return result;
        }
    }

    if (mapped_size < 32) {
        td_vm_unmap_file(ptr, mapped_size);
        return TD_ERR_PTR(TD_ERR_CORRUPT);
    }

    td_t* tmp = (td_t*)ptr;

    /* Validate type from untrusted file data -- allowlist only */
    if (!is_serializable_type(tmp->type)) {
        td_vm_unmap_file(ptr, mapped_size);
        return TD_ERR_PTR(TD_ERR_NYI);
    }
    if (tmp->len < 0) {
        td_vm_unmap_file(ptr, mapped_size);
        return TD_ERR_PTR(TD_ERR_CORRUPT);
    }

    uint8_t esz = td_sym_elem_size(tmp->type, tmp->attrs);
    if (esz == 0 && tmp->len > 0) {
        td_vm_unmap_file(ptr, mapped_size);
        return TD_ERR_PTR(TD_ERR_TYPE);
    }
    if ((uint64_t)tmp->len * esz > SIZE_MAX - 32) {
        td_vm_unmap_file(ptr, mapped_size);
        return TD_ERR_PTR(TD_ERR_IO);
    }
    size_t data_size = (size_t)tmp->len * esz;
    if (32 + data_size > mapped_size) {
        td_vm_unmap_file(ptr, mapped_size);
        return TD_ERR_PTR(TD_ERR_CORRUPT);
    }

    /* Check for appended ext_nullmap bitmap */
    bool has_ext_nullmap = (tmp->attrs & TD_ATTR_HAS_NULLS) &&
                           (tmp->attrs & TD_ATTR_NULLMAP_EXT);
    size_t bitmap_len = has_ext_nullmap ? ((size_t)tmp->len + 7) / 8 : 0;
    if (has_ext_nullmap && 32 + data_size + bitmap_len > mapped_size) {
        td_vm_unmap_file(ptr, mapped_size);
        return TD_ERR_PTR(TD_ERR_CORRUPT);
    }

    /* Allocate buddy block and copy file data */
    td_t* vec = td_alloc(data_size);
    if (!vec || TD_IS_ERR(vec)) {
        td_vm_unmap_file(ptr, mapped_size);
        return vec ? vec : TD_ERR_PTR(TD_ERR_OOM);
    }
    uint8_t saved_order = vec->order;  /* preserve buddy order */
    memcpy(vec, ptr, 32 + data_size);

    /* Restore external nullmap if present */
    if (has_ext_nullmap) {
        td_t* ext = td_vec_new(TD_U8, (int64_t)bitmap_len);
        if (!ext || TD_IS_ERR(ext)) {
            td_vm_unmap_file(ptr, mapped_size);
            td_free(vec);
            return TD_ERR_PTR(TD_ERR_OOM);
        }
        ext->len = (int64_t)bitmap_len;
        memcpy(td_data(ext), (char*)ptr + 32 + data_size, bitmap_len);
        td_vm_unmap_file(ptr, mapped_size);
        vec->ext_nullmap = ext;
    } else {
        td_vm_unmap_file(ptr, mapped_size);
    }

    /* Fix up header for buddy-allocated block */
    vec->mmod = 0;
    vec->order = saved_order;
    vec->attrs &= ~TD_ATTR_SLICE;
    if (!has_ext_nullmap)
        vec->attrs &= ~TD_ATTR_NULLMAP_EXT;
    atomic_store_explicit(&vec->rc, 1, memory_order_relaxed);

    return vec;
}

/* --------------------------------------------------------------------------
 * td_col_mmap -- zero-copy column load via mmap (mmod=1)
 *
 * Returns a td_t* backed directly by the file's mmap region.
 * MAP_PRIVATE gives COW semantics -- only the header page gets a private
 * copy when we write mmod/rc. All data pages stay shared with page cache.
 * td_release -> td_free -> munmap.
 * -------------------------------------------------------------------------- */

td_t* td_col_mmap(const char* path) {
    if (!path) return TD_ERR_PTR(TD_ERR_IO);

    size_t mapped_size = 0;
    void* ptr = td_vm_map_file(path, &mapped_size);
    if (!ptr) return TD_ERR_PTR(TD_ERR_IO);

    if (mapped_size < 32) {
        td_vm_unmap_file(ptr, mapped_size);
        return TD_ERR_PTR(TD_ERR_CORRUPT);
    }

    td_t* vec = (td_t*)ptr;

    /* Validate type from untrusted file data -- allowlist only */
    if (!is_serializable_type(vec->type)) {
        td_vm_unmap_file(ptr, mapped_size);
        return TD_ERR_PTR(TD_ERR_NYI);
    }
    if (vec->len < 0) {
        td_vm_unmap_file(ptr, mapped_size);
        return TD_ERR_PTR(TD_ERR_CORRUPT);
    }

    uint8_t esz = td_sym_elem_size(vec->type, vec->attrs);
    /* Overflow check: ensure len*esz fits in size_t with 32-byte header room */
    if ((uint64_t)vec->len > (SIZE_MAX - 32) / (esz ? esz : 1)) {
        td_vm_unmap_file(ptr, mapped_size);
        return TD_ERR_PTR(TD_ERR_IO);
    }
    size_t data_size = (size_t)vec->len * esz;
    if (32 + data_size > mapped_size) {
        td_vm_unmap_file(ptr, mapped_size);
        return TD_ERR_PTR(TD_ERR_CORRUPT);
    }

    /* Validate that file size matches expected layout.
     * td_free() reconstructs the munmap size using the same formula. */
    bool has_ext_nullmap = (vec->attrs & TD_ATTR_HAS_NULLS) &&
                           (vec->attrs & TD_ATTR_NULLMAP_EXT);
    size_t bitmap_len = has_ext_nullmap ? ((size_t)vec->len + 7) / 8 : 0;
    size_t expected = 32 + data_size + bitmap_len;
    if (expected != mapped_size) {
        td_vm_unmap_file(ptr, mapped_size);
        return TD_ERR_PTR(TD_ERR_IO);
    }

    /* Restore external nullmap: allocate buddy-backed copy
     * (ext_nullmap must be a proper td_t for ref counting) */
    if (has_ext_nullmap) {
        td_t* ext = td_vec_new(TD_U8, (int64_t)bitmap_len);
        if (!ext || TD_IS_ERR(ext)) {
            td_vm_unmap_file(ptr, mapped_size);
            return TD_ERR_PTR(TD_ERR_OOM);
        }
        ext->len = (int64_t)bitmap_len;
        memcpy(td_data(ext), (char*)ptr + 32 + data_size, bitmap_len);
        vec->ext_nullmap = ext;
    }

    /* Patch header -- MAP_PRIVATE COW: only the header page gets copied */
    vec->mmod = 1;
    vec->order = 0;
    vec->attrs &= ~TD_ATTR_SLICE;
    if (!has_ext_nullmap)
        vec->attrs &= ~TD_ATTR_NULLMAP_EXT;
    atomic_store_explicit(&vec->rc, 1, memory_order_relaxed);

    return vec;
}
