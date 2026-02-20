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

/* Element sizes indexed by positive type tag. */
const uint8_t td_type_sizes[TD_TYPE_COUNT] = {
    /* [TD_LIST]      =  0 */ 8,   /* pointer-sized (td_t*) */
    /* [TD_BOOL]      =  1 */ 1,
    /* [TD_U8]        =  2 */ 1,
    /* [TD_CHAR]      =  3 */ 1,
    /* [TD_I16]       =  4 */ 2,
    /* [TD_I32]       =  5 */ 4,
    /* [TD_I64]       =  6 */ 8,
    /* [TD_F64]       =  7 */ 8,
    /*                =  8 */ 0,
    /* [TD_DATE]      =  9 */ 4,
    /* [TD_TIME]      = 10 */ 4,
    /* [TD_TIMESTAMP] = 11 */ 8,
    /* [TD_GUID]      = 12 */ 16,
    /* [TD_TABLE]     = 13 */ 8,   /* pointer-sized (td_t*) */
    /*                = 14 */ 0,
    /*                = 15 */ 0,
    /* [TD_SEL]       = 16 */ 0,   /* variable-size layout, no elem_size */
    /*                = 17 */ 0,
    /*                = 18 */ 0,
    /*                = 19 */ 0,
    /* [TD_SYM]       = 20 */ 8,   /* W64 default; narrow widths use td_sym_elem_size */
};
