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

#include "atom.h"
#include <string.h>

/* --------------------------------------------------------------------------
 * Simple atom constructors
 *
 * Pattern: allocate 0-byte data block (just the 32B header), set type to
 * negative tag, store value in header union field.
 * -------------------------------------------------------------------------- */

td_t* td_bool(bool val) {
    td_t* v = td_alloc(0);
    if (TD_IS_ERR(v)) return v;
    v->type = TD_ATOM_BOOL;
    v->b8 = val ? 1 : 0;
    return v;
}

td_t* td_u8(uint8_t val) {
    td_t* v = td_alloc(0);
    if (TD_IS_ERR(v)) return v;
    v->type = TD_ATOM_U8;
    v->u8 = val;
    return v;
}

td_t* td_char(char val) {
    td_t* v = td_alloc(0);
    if (TD_IS_ERR(v)) return v;
    v->type = TD_ATOM_CHAR;
    v->c8 = val;
    return v;
}

td_t* td_i16(int16_t val) {
    td_t* v = td_alloc(0);
    if (TD_IS_ERR(v)) return v;
    v->type = TD_ATOM_I16;
    v->i16 = val;
    return v;
}

td_t* td_i32(int32_t val) {
    td_t* v = td_alloc(0);
    if (TD_IS_ERR(v)) return v;
    v->type = TD_ATOM_I32;
    v->i32 = val;
    return v;
}

td_t* td_i64(int64_t val) {
    td_t* v = td_alloc(0);
    if (TD_IS_ERR(v)) return v;
    v->type = TD_ATOM_I64;
    v->i64 = val;
    return v;
}

td_t* td_f64(double val) {
    td_t* v = td_alloc(0);
    if (TD_IS_ERR(v)) return v;
    v->type = TD_ATOM_F64;
    v->f64 = val;
    return v;
}

/* --------------------------------------------------------------------------
 * String atom: SSO for <= 7 bytes, long string via CHAR vector for > 7
 * -------------------------------------------------------------------------- */

td_t* td_str(const char* s, size_t len) {
    if (len < 7) {
        /* SSO path: store inline in header (< 7 leaves room for NUL).
         * Exactly 7 bytes would fill all of sdata[7] with no NUL terminator,
         * so 7-byte strings fall through to the long-string path. */
        td_t* v = td_alloc(0);
        if (TD_IS_ERR(v)) return v;
        v->type = TD_ATOM_STR;
        v->slen = (uint8_t)len;
        if (len > 0) memcpy(v->sdata, s, len);
        v->sdata[len] = '\0';
        return v;
    }
    /* Long string: allocate a CHAR vector to hold the data, store pointer.
     * Allocate len+1 and null-terminate for C string compatibility — callers
     * (including ctypes c_char_p) may read until '\0'. */
    size_t data_size = len + 1;
    td_t* chars = td_alloc(data_size);
    if (!chars || TD_IS_ERR(chars)) return chars;
    chars->type = TD_CHAR;
    chars->len = (int64_t)len;
    memcpy(td_data(chars), s, len);
    ((char*)td_data(chars))[len] = '\0';

    td_t* v = td_alloc(0);
    if (TD_IS_ERR(v)) {
        td_free(chars);
        return v;
    }
    v->type = TD_ATOM_STR;
    v->obj = chars;
    return v;
}

/* --------------------------------------------------------------------------
 * Symbol atom: intern ID stored as i64
 * -------------------------------------------------------------------------- */

td_t* td_sym(int64_t id) {
    td_t* v = td_alloc(0);
    if (TD_IS_ERR(v)) return v;
    v->type = TD_ATOM_SYM;
    v->i64 = id;
    return v;
}

/* --------------------------------------------------------------------------
 * Date/Time/Timestamp atoms
 *
 * All atom constructors accept int64_t and store in the i64 union field
 * (atoms are scalar wrappers — always 8 bytes in the union). The vector
 * element sizes differ: DATE=4, TIME=4, TIMESTAMP=8. When broadcasting
 * an atom to a vector (materialize_broadcast_input), the value must be
 * narrowed to the correct element width.
 * -------------------------------------------------------------------------- */

td_t* td_date(int64_t val) {
    td_t* v = td_alloc(0);
    if (TD_IS_ERR(v)) return v;
    v->type = -TD_DATE;
    v->i64 = val;
    return v;
}

td_t* td_time(int64_t val) {
    td_t* v = td_alloc(0);
    if (TD_IS_ERR(v)) return v;
    v->type = -TD_TIME;
    v->i64 = val;
    return v;
}

td_t* td_timestamp(int64_t val) {
    td_t* v = td_alloc(0);
    if (TD_IS_ERR(v)) return v;
    v->type = -TD_TIMESTAMP;
    v->i64 = val;
    return v;
}

/* --------------------------------------------------------------------------
 * GUID atom: 16 bytes stored in a U8 vector, pointer in obj field
 * -------------------------------------------------------------------------- */

td_t* td_guid(const uint8_t* bytes) {
    /* Allocate U8 vector of length 16 */
    td_t* vec = td_alloc(16);
    if (!vec || TD_IS_ERR(vec)) return vec;
    vec->type = TD_U8;
    vec->len = 16;
    memcpy(td_data(vec), bytes, 16);

    td_t* v = td_alloc(0);
    if (TD_IS_ERR(v)) {
        td_free(vec);
        return v;
    }
    v->type = -TD_GUID;
    v->obj = vec;
    return v;
}
