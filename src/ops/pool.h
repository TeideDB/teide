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

#ifndef TD_POOL_H
#define TD_POOL_H

/*
 * pool.h -- Persistent thread pool for parallel morsel dispatch.
 *
 * Workers sleep on a semaphore and wake when td_pool_dispatch() submits tasks.
 * The main thread participates as worker 0 (no thread spawned for it).
 * Each worker initializes its own thread-local heap via td_heap_init().
 */

#include "core/platform.h"
#include <teide/td.h>

/* Callback: process elements [start, end) with the given worker_id */
typedef void (*td_pool_fn)(void* ctx, uint32_t worker_id, int64_t start, int64_t end);

/* A single work item in the task ring */
typedef struct {
    td_pool_fn  fn;
    void*       ctx;
    int64_t     start;
    int64_t     end;
} td_pool_task_t;

/* Thread pool */
struct td_pool {
    td_thread_t*       threads;       /* worker thread handles [n_workers] */
    uint32_t           n_workers;     /* number of background threads (nproc - 1) */
    _Atomic(uint32_t)  shutdown;

    /* SPMC task ring (single producer = main, multi consumer = workers + main) */
    td_pool_task_t*    tasks;         /* ring buffer [task_cap] */
    uint32_t           task_cap;      /* power of 2 */
    uint32_t           task_head;     /* next to write (main only, no atomic needed) */
    _Atomic(uint32_t)  task_tail;     /* next to claim (workers, atomic_fetch_add) */
    _Atomic(uint32_t)  task_count;    /* total tasks submitted this dispatch */

    /* Barrier */
    _Atomic(uint32_t)  pending;       /* decremented by each task completion */
    td_sem_t           work_ready;    /* workers sleep here */

    /* Query cancellation — set by td_cancel(), checked per-morsel */
    _Atomic(uint32_t)  cancelled;
};

/* Total workers = n_workers + 1 (main thread is worker 0) */
#define td_pool_total_workers(p) ((p)->n_workers + 1)

/* Initialize pool with n_workers background threads.
 * Pass 0 to auto-detect (nproc - 1). */
td_err_t td_pool_create(td_pool_t* pool, uint32_t n_workers);

/* Shutdown and free all resources */
void td_pool_free(td_pool_t* pool);

/* Dispatch fn over [0, total_elems) partitioned into morsel-sized tasks.
 * Blocks until all tasks complete. Main thread participates as worker 0. */
void td_pool_dispatch(td_pool_t* pool, td_pool_fn fn, void* ctx, int64_t total_elems);

/* Dispatch exactly n_tasks tasks, each with range [i, i+1).
 * Used for partition-parallel workloads where each task is one partition. */
void td_pool_dispatch_n(td_pool_t* pool, td_pool_fn fn, void* ctx, uint32_t n_tasks);

/* Global pool lifecycle (lazy singleton) */
td_pool_t* td_pool_get(void);

#endif /* TD_POOL_H */
