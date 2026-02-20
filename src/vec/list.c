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

#include "list.h"
#include <string.h>

/* --------------------------------------------------------------------------
 * Capacity helpers (same pattern as vec.c)
 * -------------------------------------------------------------------------- */

static int64_t list_capacity(td_t* list) {
    size_t block_size = (size_t)1 << list->order;
    size_t data_space = block_size - 32;  /* 32B td_t header */
    return (int64_t)(data_space / sizeof(td_t*));
}

/* --------------------------------------------------------------------------
 * td_list_new
 * -------------------------------------------------------------------------- */

td_t* td_list_new(int64_t capacity) {
    if (capacity < 0) return TD_ERR_PTR(TD_ERR_RANGE);
    if ((uint64_t)capacity > SIZE_MAX / sizeof(td_t*))
        return TD_ERR_PTR(TD_ERR_OOM);
    size_t data_size = (size_t)capacity * sizeof(td_t*);

    td_t* list = td_alloc(data_size);
    if (!list || TD_IS_ERR(list)) return list;

    list->type = TD_LIST;
    list->len = 0;
    list->attrs = 0;
    memset(list->nullmap, 0, 16);

    return list;
}

/* --------------------------------------------------------------------------
 * td_list_append
 * -------------------------------------------------------------------------- */

td_t* td_list_append(td_t* list, td_t* item) {
    if (!list || TD_IS_ERR(list)) return list;

    /* COW if shared */
    list = td_cow(list);
    if (!list || TD_IS_ERR(list)) return list;

    int64_t cap = list_capacity(list);

    /* Grow if needed */
    if (list->len >= cap) {
        size_t new_data_size = (size_t)(list->len + 1) * sizeof(td_t*);
        if (new_data_size < 32) new_data_size = 32;
        else {
            size_t s = 32;
            while (s < new_data_size) {
                if (s > SIZE_MAX / 2) return TD_ERR_PTR(TD_ERR_OOM);
                s *= 2;
            }
            new_data_size = s;
        }
        td_t* new_list = td_scratch_realloc(list, new_data_size);
        if (!new_list || TD_IS_ERR(new_list)) return new_list;
        list = new_list;
    }

    /* Store item pointer and retain it */
    td_t** slots = (td_t**)td_data(list);
    slots[list->len] = item;
    if (item) td_retain(item);
    list->len++;

    return list;
}

/* --------------------------------------------------------------------------
 * td_list_get
 * -------------------------------------------------------------------------- */

td_t* td_list_get(td_t* list, int64_t idx) {
    if (!list || TD_IS_ERR(list)) return NULL;
    if (idx < 0 || idx >= list->len) return NULL;

    td_t** slots = (td_t**)td_data(list);
    return slots[idx];
}

/* --------------------------------------------------------------------------
 * td_list_set
 * -------------------------------------------------------------------------- */

td_t* td_list_set(td_t* list, int64_t idx, td_t* item) {
    if (!list || TD_IS_ERR(list)) return list;
    if (idx < 0 || idx >= list->len)
        return TD_ERR_PTR(TD_ERR_RANGE);

    /* COW if shared */
    list = td_cow(list);
    if (!list || TD_IS_ERR(list)) return list;

    td_t** slots = (td_t**)td_data(list);

    /* Release old item */
    td_t* old = slots[idx];
    if (old) td_release(old);

    /* Store new item and retain it */
    slots[idx] = item;
    if (item) td_retain(item);

    return list;
}
