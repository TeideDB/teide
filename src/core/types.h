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

#ifndef TD_TYPES_H
#define TD_TYPES_H

/*
 * types.h — Internal types header.
 *
 * The canonical type definitions (td_t, type constants, attribute flags)
 * live in <teide/td.h> (the public header).
 * Internal .c files can include either td.h directly or types.h.
 */
#include <teide/td.h>

/* --------------------------------------------------------------------------
 * Type classification helpers (operate on positive type tags)
 * -------------------------------------------------------------------------- */
/* Numeric: BOOL, U8, CHAR, I16, I32, I64, F64 */
#define TD_IS_NUMERIC(t) ((t) >= TD_BOOL && (t) <= TD_F64)

/* Integer: BOOL, U8, CHAR, I16, I32, I64 */
#define TD_IS_INTEGER(t) ((t) >= TD_BOOL && (t) <= TD_I64)

/* Float: F64 only */
#define TD_IS_FLOAT(t)   ((t) == TD_F64)

#endif /* TD_TYPES_H */
