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

#ifndef TD_HEAP_H
#define TD_HEAP_H

/*
 * heap.h -- Rayforce-style per-thread heap allocator (zero-prefix layout).
 *
 * Each thread owns one td_heap_t. Blocks are allocated from self-aligned
 * mmap'd pools via buddy splitting. td_t IS the block — no prefix.
 *
 * Pool metadata (heap_id, pool_order) is stored in a pool header at
 * offset 0 of each self-aligned pool (first min-block reserved).
 * Pool base is derived in O(1): ptr & ~(pool_size - 1).
 *
 * Free-list prev/next overlay nullmap bytes 0-15 of td_t (unused when free).
 * rc == 0 indicates a free block (replaces the old td_blk_t.used flag).
 *
 * Cross-thread free uses a foreign_blocks list (checked via pool heap_id).
 */

#include <teide/td.h>
#include <stdint.h>

/* --------------------------------------------------------------------------
 * Constants
 * -------------------------------------------------------------------------- */
#define TD_HEAP_POOL_ORDER  25      /* 32 MB standard pool */
#define TD_HEAP_MAX_ORDER   38      /* 256 GB max pool */
#define TD_HEAP_FL_SIZE     (TD_HEAP_MAX_ORDER + 1)
#define TD_MAX_POOLS        512

/* --------------------------------------------------------------------------
 * Block size helper
 * -------------------------------------------------------------------------- */
#define BSIZEOF(o)    ((size_t)1 << (o))

/* --------------------------------------------------------------------------
 * Pool header: first min-block (64B) of each self-aligned pool.
 *
 * Overlaid on bytes 0-15 of the td_t at pool offset 0.
 * The td_t at pool offset 0 has rc=1 (prevents coalescing) and
 * order=TD_ORDER_MIN (correct for buddy math).
 * -------------------------------------------------------------------------- */
typedef struct {
    uint16_t heap_id;     /* owning heap ID (for cross-thread free) */
    uint8_t  pool_order;  /* pool's top order */
    uint8_t  _pad[5];
    void*    vm_base;     /* original mmap base (for td_vm_free on Windows) */
} td_pool_hdr_t;

_Static_assert(sizeof(td_pool_hdr_t) <= 16,
               "td_pool_hdr_t must fit in td_t nullmap (16 bytes)");

/* --------------------------------------------------------------------------
 * Circular sentinel freelist (Rayforce-style)
 *
 * Each freelist[order] is a sentinel node with prev/next pointers at
 * offsets 0/8 — same layout as td_t.fl_prev/fl_next. This makes
 * fl_remove() work without knowing which freelist the block belongs to,
 * enabling safe cross-heap buddy coalescing.
 *
 * Empty list: sentinel.prev = sentinel.next = &sentinel.
 * -------------------------------------------------------------------------- */
typedef struct {
    td_t* fl_prev;   /* offset 0 — same as td_t.fl_prev */
    td_t* fl_next;   /* offset 8 — same as td_t.fl_next */
} td_fl_head_t;

static inline void fl_init(td_fl_head_t* h) {
    h->fl_prev = (td_t*)h;
    h->fl_next = (td_t*)h;
}

static inline bool fl_empty(const td_fl_head_t* h) {
    return h->fl_next == (const td_t*)h;
}

/* Unlink a block from whatever circular list it belongs to.
 * Works across heaps — no head pointer needed. */
static inline void fl_remove(td_t* blk) {
    blk->fl_prev->fl_next = blk->fl_next;
    blk->fl_next->fl_prev = blk->fl_prev;
}

/* --------------------------------------------------------------------------
 * Pool tracking entry (in td_heap_t)
 * -------------------------------------------------------------------------- */
typedef struct {
    void*   base;         /* pool base address (self-aligned) */
    uint8_t pool_order;   /* pool order for munmap sizing */
} td_pool_entry_t;

/* --------------------------------------------------------------------------
 * Pool derivation helpers
 *
 * td_pool_of: derive pool header from any block pointer.
 *
 * All pools are self-aligned (pool base = multiple of pool_size). Standard
 * pools (32 MB) are derived in O(1) via a single AND mask. Oversized pools
 * (> 32 MB) use a downward walk at 32 MB stride to find the pool header.
 *
 * Pool header validation: order == TD_ORDER_MIN, mmod == 0, rc == 1.
 * These conditions uniquely identify pool header blocks — cascade/split
 * blocks always have order > TD_ORDER_MIN.
 * -------------------------------------------------------------------------- */

static inline td_pool_hdr_t* td_pool_of(td_t* v) {
    /* Standard pools (32 MB, self-aligned): one AND gives the base.
     * Oversized pools need a downward walk but are rare. */
    uintptr_t stride = BSIZEOF(TD_HEAP_POOL_ORDER);  /* 32 MB */
    uintptr_t base = (uintptr_t)v & ~(stride - 1);
    td_pool_hdr_t* hdr = (td_pool_hdr_t*)base;

    /* Fast path: standard pool header at 32 MB boundary (99%+ of calls) */
    if (TD_LIKELY(hdr->pool_order == TD_HEAP_POOL_ORDER))
        return hdr;

    /* Slow path: oversized pool — walk downward at 32 MB stride */
    if (hdr->pool_order > TD_HEAP_POOL_ORDER &&
        hdr->pool_order <= TD_HEAP_MAX_ORDER &&
        (uintptr_t)v < base + BSIZEOF(hdr->pool_order))
        return hdr;

    for (;;) {
        if (base < stride) break;
        base -= stride;
        hdr = (td_pool_hdr_t*)base;
        td_t* hdr_blk = (td_t*)base;
        if (hdr_blk->order == TD_ORDER_MIN &&
            hdr_blk->mmod == 0 &&
            atomic_load_explicit(&hdr_blk->rc,
                                 memory_order_relaxed) == 1) {
            if (hdr->pool_order >= TD_HEAP_POOL_ORDER &&
                hdr->pool_order <= TD_HEAP_MAX_ORDER &&
                (uintptr_t)v < base + BSIZEOF(hdr->pool_order))
                return hdr;
        }
    }
    return (td_pool_hdr_t*)((uintptr_t)v & ~(stride - 1));
}

/* --------------------------------------------------------------------------
 * Buddy derivation: uses self-aligned pool base
 * -------------------------------------------------------------------------- */
static inline td_t* td_buddy_of(td_t* v, uint8_t order, uintptr_t pool_base) {
    return (td_t*)(pool_base + (((uintptr_t)v - pool_base) ^ BSIZEOF(order)));
}

/* --------------------------------------------------------------------------
 * Slab cache for small blocks (orders 6-10, i.e., 64B-1024B)
 * -------------------------------------------------------------------------- */
typedef struct {
    int64_t  count;
    td_t*    stack[TD_SLAB_CACHE_SIZE];
} td_slab_t;

#define TD_SLAB_MIN       TD_ORDER_MIN
#define TD_SLAB_MAX       (TD_ORDER_MIN + TD_SLAB_ORDERS - 1)
#define IS_SLAB_ORDER(o)  ((o) >= TD_SLAB_MIN && (o) <= TD_SLAB_MAX)
#define SLAB_INDEX(o)     ((o) - TD_SLAB_MIN)

/* --------------------------------------------------------------------------
 * Per-thread heap
 * -------------------------------------------------------------------------- */
typedef struct td_heap {
    uint64_t        avail;                       /* bitmask: bit N set = freelist[N] non-empty */
    uint16_t        id;                          /* heap identity (for cross-thread free) */
    td_t*           foreign;                     /* cross-heap freed blocks (singly-linked via fl_next) */
    td_slab_t       slabs[TD_SLAB_ORDERS];       /* small-block slab caches */
    td_fl_head_t    freelist[TD_HEAP_FL_SIZE];   /* circular sentinel per order */
    td_mem_stats_t  stats;
    uint32_t        pool_count;                  /* number of tracked pools */
    td_pool_entry_t pools[TD_MAX_POOLS];         /* pool tracking for destroy/merge */
} td_heap_t;

/* --------------------------------------------------------------------------
 * Pool-list scan: find which pool a block belongs to without reading the
 * remote pool header (avoids cold cache line 32MB away on hot path).
 * Returns pool index in h->pools[], or -1 if block is foreign.
 * -------------------------------------------------------------------------- */
static inline int heap_find_pool(const td_heap_t* h, const void* ptr) {
    uintptr_t addr = (uintptr_t)ptr;
    for (uint32_t i = 0; i < h->pool_count; i++) {
        uintptr_t pb = (uintptr_t)h->pools[i].base;
        if (addr >= pb && addr < pb + BSIZEOF(h->pools[i].pool_order))
            return (int)i;
    }
    return -1;
}

/* --------------------------------------------------------------------------
 * Thread-local state
 * -------------------------------------------------------------------------- */
extern TD_TLS td_heap_t*     td_tl_heap;
extern TD_TLS td_mem_stats_t td_tl_stats;

/* --------------------------------------------------------------------------
 * Global heap registry: look up any heap by ID so foreign blocks can be
 * returned to their owning heap instead of accumulating on the freeing heap.
 * -------------------------------------------------------------------------- */
#define TD_HEAP_REGISTRY_SIZE 1024
extern td_heap_t* td_heap_registry[TD_HEAP_REGISTRY_SIZE];

/* --------------------------------------------------------------------------
 * Scratch arena: bump-allocator backed by buddy-allocated pages.
 * O(1) push (pointer bump), O(n_backing) reset (free all backing blocks).
 * -------------------------------------------------------------------------- */
#define TD_ARENA_MAX_BACKING  64
#define TD_ARENA_BLOCK_ORDER  16   /* 64 KB backing blocks */

typedef struct {
    td_t*   backing[TD_ARENA_MAX_BACKING];
    int     n_backing;
    char*   ptr;
    char*   end;
} td_scratch_arena_t;

static inline void td_scratch_arena_init(td_scratch_arena_t* a) {
    a->n_backing = 0;
    a->ptr = NULL;
    a->end = NULL;
}

void* td_scratch_arena_push(td_scratch_arena_t* a, size_t nbytes);
void  td_scratch_arena_reset(td_scratch_arena_t* a);

#endif /* TD_HEAP_H */
