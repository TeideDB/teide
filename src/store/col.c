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
 * td_col_save -- write a vector to a column file
 * -------------------------------------------------------------------------- */

td_err_t td_col_save(td_t* vec, const char* path) {
    if (!vec || TD_IS_ERR(vec)) return TD_ERR_TYPE;
    if (!path) return TD_ERR_IO;
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
