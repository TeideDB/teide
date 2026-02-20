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

#ifndef TD_BLOCK_H
#define TD_BLOCK_H

/*
 * block.h — Internal block header utilities.
 *
 * Provides td_block_size() and td_block_copy(). The core td_t struct and
 * accessor macros (td_type, td_is_atom, td_is_vec, td_len, td_data,
 * td_elem_size) are defined in <teide/td.h>.
 */
#include <teide/td.h>
#include <string.h>

/* Compute total block size in bytes (header + data) */
size_t td_block_size(td_t* v);

/* Allocate a new block and shallow-copy header + data from src.
 * WARNING: Does NOT retain child refs (STR/LIST/TABLE pointers). Callers
 * must retain children separately, or use td_alloc_copy() which handles this.
 * Requires td_alloc (declared in td.h, provided by the buddy allocator). */
td_t* td_block_copy(td_t* src);

#endif /* TD_BLOCK_H */
