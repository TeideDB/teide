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

#ifndef TD_MEM_SYS_H
#define TD_MEM_SYS_H

#include <stddef.h>
#include <stdint.h>

/* --------------------------------------------------------------------------
 * System-level mmap allocator for infrastructure that can't use the buddy
 * allocator (cross-thread lifetime, bootstrap, global state).
 *
 * Every allocation is tracked. td_mem_stats() reports the totals so users
 * can see the full memory footprint.
 *
 * Each allocation prepends a 32-byte header (stores mmap size + user size),
 * so td_sys_free() needs no size argument.
 * -------------------------------------------------------------------------- */

void* td_sys_alloc(size_t size);
void* td_sys_realloc(void* ptr, size_t new_size);
void  td_sys_free(void* ptr);
char* td_sys_strdup(const char* s);

/* Read current sys allocator counters (called by td_mem_stats in arena.c) */
void  td_sys_get_stat(int64_t* out_current, int64_t* out_peak);

#endif /* TD_MEM_SYS_H */
