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

#include "fvec.h"
#include "mem/sys.h"
#include <string.h>
#include <stdio.h>

td_ftable_t* td_ftable_new(uint16_t n_cols) {
    td_ftable_t* ft = (td_ftable_t*)td_sys_alloc(sizeof(td_ftable_t));
    if (!ft) return NULL;
    memset(ft, 0, sizeof(td_ftable_t));

    ft->columns = (td_fvec_t*)td_sys_alloc((size_t)n_cols * sizeof(td_fvec_t));
    if (!ft->columns) {
        td_sys_free(ft);
        return NULL;
    }
    memset(ft->columns, 0, (size_t)n_cols * sizeof(td_fvec_t));
    ft->n_cols = n_cols;

    return ft;
}

void td_ftable_free(td_ftable_t* ft) {
    if (!ft) return;

    if (ft->columns) {
        for (uint16_t i = 0; i < ft->n_cols; i++) {
            if (ft->columns[i].vec) td_release(ft->columns[i].vec);
        }
        td_sys_free(ft->columns);
    }
    if (ft->semijoin) td_release(ft->semijoin);
    td_sys_free(ft);
}

td_t* td_ftable_materialize(td_ftable_t* ft) {
    if (!ft || ft->n_cols == 0) return TD_ERR_PTR(TD_ERR_TYPE);

    td_t* tbl = td_table_new(ft->n_cols);
    if (!tbl || TD_IS_ERR(tbl)) return tbl;

    for (uint16_t c = 0; c < ft->n_cols; c++) {
        td_fvec_t* fv = &ft->columns[c];
        if (!fv->vec) continue;

        td_t* col;
        if (fv->cur_idx >= 0) {
            /* Flat: replicate single value */
            col = td_vec_new(fv->vec->type, fv->cardinality);
            if (!col || TD_IS_ERR(col)) { td_release(tbl); return col; }
            col->len = fv->cardinality;
            void* val = td_vec_get(fv->vec, fv->cur_idx);
            uint8_t esz = td_elem_size(fv->vec->type);
            char* dst = (char*)td_data(col);
            for (int64_t r = 0; r < fv->cardinality; r++)
                memcpy(dst + r * esz, val, esz);
        } else {
            /* Unflat: use as-is */
            col = fv->vec;
            td_retain(col);
        }

        char name_buf[8];
        int n = snprintf(name_buf, sizeof(name_buf), "_c%d", c);
        int64_t name_id = td_sym_intern(name_buf, (size_t)n);
        tbl = td_table_add_col(tbl, name_id, col);
        td_release(col);
    }

    return tbl;
}
