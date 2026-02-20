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

#include "err.h"

static const char* err_strings[] = {
    [TD_OK]          = "ok",
    [TD_ERR_OOM]     = "out of memory",
    [TD_ERR_TYPE]    = "type error",
    [TD_ERR_RANGE]   = "range error",
    [TD_ERR_LENGTH]  = "length mismatch",
    [TD_ERR_RANK]    = "rank error",
    [TD_ERR_DOMAIN]  = "domain error",
    [TD_ERR_NYI]     = "not yet implemented",
    [TD_ERR_IO]      = "I/O error",
    [TD_ERR_SCHEMA]  = "schema error",
    [TD_ERR_CORRUPT] = "corrupt data",
    [TD_ERR_CANCEL]  = "query cancelled",
};

#define ERR_STRING_COUNT (sizeof(err_strings) / sizeof(err_strings[0]))

const char* td_err_str(td_err_t e) {
    if ((unsigned)e >= ERR_STRING_COUNT) return "unknown error";
    return err_strings[e];
}
