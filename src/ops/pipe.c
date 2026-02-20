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

#include "pipe.h"
#include "mem/sys.h"
#include <string.h>
#ifndef _WIN32
#include <unistd.h>
#endif

/* --------------------------------------------------------------------------
 * td_pipe_new
 *
 * Allocate a new pipe structure with all fields zeroed and spill_fd = -1.
 * -------------------------------------------------------------------------- */

td_pipe_t* td_pipe_new(void) {
    td_pipe_t* p = (td_pipe_t*)td_sys_alloc(sizeof(td_pipe_t));
    if (!p) return NULL;
    /* L3: Zero-init the entire struct before setting individual fields,
       ensuring no uninitialized pointers or state. */
    memset(p, 0, sizeof(*p));
    p->spill_fd = -1;

    return p;
}

/* --------------------------------------------------------------------------
 * td_pipe_free
 *
 * Free a pipe. Closes the spill file descriptor if it was opened.
 * Does NOT recursively free upstream input pipes.
 * -------------------------------------------------------------------------- */

void td_pipe_free(td_pipe_t* pipe) {
    if (!pipe) return;

    if (pipe->spill_fd >= 0) {
        close(pipe->spill_fd);
    }

    td_sys_free(pipe);
}
