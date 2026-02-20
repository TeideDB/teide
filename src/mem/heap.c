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

#include "heap.h"
#include "sys.h"
#include "core/platform.h"
#include <string.h>

/* --------------------------------------------------------------------------
 * Static asserts
 * -------------------------------------------------------------------------- */
_Static_assert(sizeof(td_pool_hdr_t) <= 16,
               "td_pool_hdr_t must fit in nullmap (16 bytes)");

/* --------------------------------------------------------------------------
 * Thread-local state
 * -------------------------------------------------------------------------- */
TD_TLS td_heap_t*     td_tl_heap  = NULL;
TD_TLS td_mem_stats_t td_tl_stats;

/* --------------------------------------------------------------------------
 * Heap ID counter + global registry
 * -------------------------------------------------------------------------- */
static _Atomic(uint16_t) g_heap_id_next = 1;
td_heap_t* td_heap_registry[TD_HEAP_REGISTRY_SIZE];

/* --------------------------------------------------------------------------
 * Parallel flag
 * -------------------------------------------------------------------------- */
_Atomic(uint32_t) td_parallel_flag = 0;

/* --------------------------------------------------------------------------
 * Helpers
 * -------------------------------------------------------------------------- */

static uint8_t ceil_log2(size_t n) {
    if (n <= 1) return 0;
    return (uint8_t)(64 - __builtin_clzll(n - 1));
}

uint8_t td_order_for_size(size_t data_size) {
    if (data_size > SIZE_MAX - 32) return TD_HEAP_MAX_ORDER + 1;
    size_t total = data_size + 32;  /* 32B td_t header (no prefix) */
    uint8_t k = ceil_log2(total);
    if (k < TD_ORDER_MIN) k = TD_ORDER_MIN;
    return k;
}

/* --------------------------------------------------------------------------
 * Pool management
 *
 * Self-aligned pools: pool base = ptr & ~(pool_size - 1).
 * First min-block (64B at offset 0) reserved for pool header.
 * Remaining space split via cascading buddy split.
 *
 * For oversized blocks (order > POOL_ORDER), pool_order = order + 1
 * so the cascading split produces a right-half block of the needed order.
 * -------------------------------------------------------------------------- */

static bool heap_add_pool(td_heap_t* h, uint8_t order);

/* --------------------------------------------------------------------------
 * Freelist operations (circular sentinel via fl_prev/fl_next)
 *
 * Each freelist[order] is a td_fl_head_t sentinel. fl_remove() unlinks a
 * block from ANY circular list without needing the head pointer — enabling
 * safe cross-heap buddy coalescing.
 * -------------------------------------------------------------------------- */

TD_INLINE void heap_insert_block(td_heap_t* h, td_t* blk, uint8_t order) {
    td_fl_head_t* head = &h->freelist[order];
    td_t* first = head->fl_next;
    blk->fl_prev = (td_t*)head;
    blk->fl_next = first;
    first->fl_prev = blk;
    head->fl_next = blk;
    atomic_store_explicit(&blk->rc, 0, memory_order_relaxed);  /* free marker */
    blk->order = order;
    h->avail |= (1ULL << order);
}

/* heap_remove_block: currently unused — retained for future coalescing paths */
static void __attribute__((unused))
heap_remove_block(td_heap_t* h, td_t* blk, uint8_t order) {
    fl_remove(blk);  /* circular unlink — works across heaps */
    if (fl_empty(&h->freelist[order]))
        h->avail &= ~(1ULL << order);
}

TD_INLINE void heap_split_block(td_heap_t* h, td_t* blk,
                                uint8_t target_order, uint8_t block_order) {
    while (block_order > target_order) {
        block_order--;
        td_t* buddy = (td_t*)((char*)blk + BSIZEOF(block_order));
        buddy->mmod  = 0;
        buddy->order = block_order;
        heap_insert_block(h, buddy, block_order);
    }
}

/* --------------------------------------------------------------------------
 * Coalescing: merge block with buddies up to pool_order
 *
 * Pool header at offset 0 has rc=1 and order=TD_ORDER_MIN, so buddy
 * checks always fail before reaching the header. Safe sentinel.
 * -------------------------------------------------------------------------- */

static void heap_coalesce(td_heap_t* h, td_t* blk,
                          uintptr_t pool_base, uint8_t pool_order) {
    uint8_t order = blk->order;

    /* During parallel execution, skip coalescing entirely — buddies may
     * belong to other heaps' freelists, and fl_remove would corrupt them. */
    if (atomic_load_explicit(&td_parallel_flag, memory_order_relaxed) != 0) {
        heap_insert_block(h, blk, order);
        return;
    }

    for (;; order++) {
        if (order >= pool_order) break;

        td_t* buddy = td_buddy_of(blk, order, pool_base);
        __builtin_prefetch(buddy, 0, 1);

        uint32_t buddy_rc = atomic_load_explicit(&buddy->rc, memory_order_relaxed);
        if (buddy_rc != 0 || buddy->order != order) break;

        fl_remove(buddy);
        if (fl_empty(&h->freelist[order]))
            h->avail &= ~(1ULL << order);

        blk = (buddy < blk) ? buddy : blk;
    }

    heap_insert_block(h, blk, order);
}

/* --------------------------------------------------------------------------
 * heap_add_pool implementation
 * -------------------------------------------------------------------------- */

static bool heap_add_pool(td_heap_t* h, uint8_t order) {
    if (h->pool_count >= TD_MAX_POOLS) return false;

    uint8_t pool_order;
    if (order >= TD_HEAP_POOL_ORDER)
        pool_order = order + 1;  /* need one order larger for header + block */
    else
        pool_order = TD_HEAP_POOL_ORDER;

    if (pool_order > TD_HEAP_MAX_ORDER) return false;
    size_t pool_size = BSIZEOF(pool_order);

    void* mem = td_vm_alloc_aligned(pool_size, pool_size);
    if (!mem) return false;

    /* --- Write pool header at offset 0 --- */
    td_t* hdr_block = (td_t*)mem;
    memset(hdr_block, 0, BSIZEOF(TD_ORDER_MIN));
    hdr_block->mmod  = 0;
    hdr_block->order = TD_ORDER_MIN;
    atomic_store_explicit(&hdr_block->rc, 1, memory_order_relaxed);  /* sentinel: never free */

    td_pool_hdr_t* hdr = (td_pool_hdr_t*)hdr_block;  /* overlay on nullmap */
    hdr->heap_id    = h->id;
    hdr->pool_order = pool_order;
    hdr->vm_base    = mem;  /* on POSIX, same as aligned base */

    /* --- Cascading split: split from pool_order down to TD_ORDER_MIN.
     *     Right half of each split → freelist.
     *     Leftmost min-block = pool header (already set, rc=1). --- */
    for (uint8_t o = pool_order; o > TD_ORDER_MIN; o--) {
        td_t* right = (td_t*)((char*)mem + BSIZEOF(o - 1));
        right->mmod  = 0;
        right->order = (uint8_t)(o - 1);
        heap_insert_block(h, right, (uint8_t)(o - 1));
    }

    /* --- Track pool --- */
    h->pools[h->pool_count].base       = mem;
    h->pools[h->pool_count].pool_order = pool_order;
    h->pool_count++;

    return true;
}

/* --------------------------------------------------------------------------
 * Slab cache flush (with coalescing for GC effectiveness)
 * -------------------------------------------------------------------------- */

static void heap_flush_slabs(td_heap_t* h) {
    for (int i = 0; i < TD_SLAB_ORDERS; i++) {
        while (h->slabs[i].count > 0) {
            td_t* blk = h->slabs[i].stack[--h->slabs[i].count];
            int pidx = heap_find_pool(h, blk);
            uintptr_t pb;
            uint8_t po;
            if (pidx >= 0) {
                pb = (uintptr_t)h->pools[pidx].base;
                po = h->pools[pidx].pool_order;
            } else {
                td_pool_hdr_t* phdr = td_pool_of(blk);
                pb = (uintptr_t)phdr;
                po = phdr->pool_order;
            }
            heap_coalesce(h, blk, pb, po);
        }
    }
}

/* --------------------------------------------------------------------------
 * Foreign blocks flush
 *
 * When return_to_owner is true, returns each foreign block to its owning
 * heap (via pool header heap_id → global registry). This ensures workers
 * can reuse their pools across queries instead of allocating new ones.
 *
 * return_to_owner must only be true when workers are idle (on semaphore),
 * i.e. td_parallel_flag == 0. Otherwise coalesce into current heap.
 * -------------------------------------------------------------------------- */

static void heap_flush_foreign(td_heap_t* h, bool return_to_owner) {
    td_t* blk = h->foreign;
    while (blk) {
        td_t* next = blk->fl_next;
        if (return_to_owner) {
            td_pool_hdr_t* phdr = td_pool_of(blk);  /* GC path, not hot */
            uint16_t owner_id = phdr->heap_id;
            td_heap_t* owner = td_heap_registry[owner_id % TD_HEAP_REGISTRY_SIZE];
            if (owner && owner->id == owner_id && owner != h) {
                int pidx = heap_find_pool(owner, blk);
                uintptr_t pb;
                uint8_t po;
                if (pidx >= 0) {
                    pb = (uintptr_t)owner->pools[pidx].base;
                    po = owner->pools[pidx].pool_order;
                } else {
                    pb = (uintptr_t)phdr;
                    po = phdr->pool_order;
                }
                heap_coalesce(owner, blk, pb, po);
                blk = next;
                continue;
            }
        }
        /* Local coalesce */
        int pidx = heap_find_pool(h, blk);
        uintptr_t pb;
        uint8_t po;
        if (pidx >= 0) {
            pb = (uintptr_t)h->pools[pidx].base;
            po = h->pools[pidx].pool_order;
        } else {
            td_pool_hdr_t* phdr = td_pool_of(blk);
            pb = (uintptr_t)phdr;
            po = phdr->pool_order;
        }
        heap_coalesce(h, blk, pb, po);
        blk = next;
    }
    h->foreign = NULL;
}

/* --------------------------------------------------------------------------
 * Owned-reference helpers
 * -------------------------------------------------------------------------- */

static bool td_atom_str_is_sso(const td_t* s) {
    if (s->slen >= 1 && s->slen <= 7) return true;
    if (s->slen == 0 && s->obj == NULL) return true;
    return false;
}

static bool td_atom_owns_obj(const td_t* v) {
    if (v->type == TD_ATOM_GUID) return v->obj != NULL;
    if (v->type == TD_ATOM_STR) return !td_atom_str_is_sso(v);
    return false;
}

static void td_release_owned_refs(td_t* v) {
    if (!v || TD_IS_ERR(v)) return;

    if (td_is_atom(v)) {
        if (td_atom_owns_obj(v) && v->obj && !TD_IS_ERR(v->obj))
            td_release(v->obj);
        return;
    }

    if (v->attrs & TD_ATTR_SLICE) {
        if (v->slice_parent && !TD_IS_ERR(v->slice_parent))
            td_release(v->slice_parent);
        return;
    }

    if ((v->attrs & TD_ATTR_NULLMAP_EXT) &&
        v->ext_nullmap && !TD_IS_ERR(v->ext_nullmap))
        td_release(v->ext_nullmap);

    if (TD_IS_PARTED(v->type)) {
        int64_t n_segs = v->len;
        td_t** segs = (td_t**)td_data(v);
        for (int64_t i = 0; i < n_segs; i++) {
            if (segs[i] && !TD_IS_ERR(segs[i]))
                td_release(segs[i]);
        }
        return;
    }

    if (v->type == TD_MAPCOMMON) {
        td_t** ptrs = (td_t**)td_data(v);
        if (ptrs[0] && !TD_IS_ERR(ptrs[0])) td_release(ptrs[0]);
        if (ptrs[1] && !TD_IS_ERR(ptrs[1])) td_release(ptrs[1]);
        return;
    }

    if (v->type == TD_TABLE) {
        if (v->len < 0) return;
        td_t** slots = (td_t**)td_data(v);
        td_t* schema = slots[0];
        if (schema && !TD_IS_ERR(schema)) td_release(schema);

        td_t** cols = slots + 1;
        for (int64_t i = 0; i < v->len; i++) {
            td_t* col = cols[i];
            if (col && !TD_IS_ERR(col)) td_release(col);
        }
        return;
    }

    if (v->type == TD_LIST) {
        td_t** ptrs = (td_t**)td_data(v);
        for (int64_t i = 0; i < v->len; i++) {
            td_t* child = ptrs[i];
            if (child && !TD_IS_ERR(child)) td_release(child);
        }
    }
}

static void td_retain_owned_refs(td_t* v) {
    if (!v || TD_IS_ERR(v)) return;

    if (td_is_atom(v)) {
        if (td_atom_owns_obj(v) && v->obj && !TD_IS_ERR(v->obj))
            td_retain(v->obj);
        return;
    }

    if (v->attrs & TD_ATTR_SLICE) {
        if (v->slice_parent && !TD_IS_ERR(v->slice_parent))
            td_retain(v->slice_parent);
        return;
    }

    if ((v->attrs & TD_ATTR_NULLMAP_EXT) &&
        v->ext_nullmap && !TD_IS_ERR(v->ext_nullmap))
        td_retain(v->ext_nullmap);

    if (TD_IS_PARTED(v->type)) {
        int64_t n_segs = v->len;
        td_t** segs = (td_t**)td_data(v);
        for (int64_t i = 0; i < n_segs; i++) {
            if (segs[i] && !TD_IS_ERR(segs[i]))
                td_retain(segs[i]);
        }
        return;
    }

    if (v->type == TD_MAPCOMMON) {
        td_t** ptrs = (td_t**)td_data(v);
        if (ptrs[0] && !TD_IS_ERR(ptrs[0])) td_retain(ptrs[0]);
        if (ptrs[1] && !TD_IS_ERR(ptrs[1])) td_retain(ptrs[1]);
        return;
    }

    if (v->type == TD_TABLE) {
        td_t** slots = (td_t**)td_data(v);
        td_t* schema = slots[0];
        if (schema && !TD_IS_ERR(schema)) td_retain(schema);

        td_t** cols = slots + 1;
        for (int64_t i = 0; i < v->len; i++) {
            td_t* col = cols[i];
            if (col && !TD_IS_ERR(col)) td_retain(col);
        }
        return;
    }

    if (v->type == TD_LIST) {
        td_t** ptrs = (td_t**)td_data(v);
        for (int64_t i = 0; i < v->len; i++) {
            td_t* child = ptrs[i];
            if (child && !TD_IS_ERR(child)) td_retain(child);
        }
    }
}

static void td_detach_owned_refs(td_t* v) {
    if (!v || TD_IS_ERR(v)) return;

    if (td_is_atom(v)) {
        if (td_atom_owns_obj(v)) v->obj = NULL;
        return;
    }

    if (v->attrs & TD_ATTR_SLICE) {
        v->slice_parent = NULL;
        v->slice_offset = 0;
        v->attrs &= (uint8_t)~TD_ATTR_SLICE;
        return;
    }

    if (v->attrs & TD_ATTR_NULLMAP_EXT) {
        v->ext_nullmap = NULL;
        v->attrs &= (uint8_t)~TD_ATTR_NULLMAP_EXT;
    }

    if (TD_IS_PARTED(v->type)) {
        int64_t n_segs = v->len;
        td_t** segs = (td_t**)td_data(v);
        for (int64_t i = 0; i < n_segs; i++)
            segs[i] = NULL;
        return;
    }

    if (v->type == TD_MAPCOMMON) {
        td_t** ptrs = (td_t**)td_data(v);
        ptrs[0] = NULL;
        ptrs[1] = NULL;
        return;
    }

    if (v->type == TD_TABLE) {
        td_t** slots = (td_t**)td_data(v);
        slots[0] = NULL;
        v->len = 0;
        return;
    }

    if (v->type == TD_LIST) {
        v->len = 0;
    }
}

/* --------------------------------------------------------------------------
 * td_alloc
 * -------------------------------------------------------------------------- */

td_t* td_alloc(size_t data_size) {
    td_heap_t* h = td_tl_heap;
    if (TD_UNLIKELY(!h)) {
        td_heap_init();
        h = td_tl_heap;
        if (!h) return NULL;
    }

    uint8_t order = td_order_for_size(data_size);
    if (order > TD_HEAP_MAX_ORDER) return NULL;

    /* Slab fast path */
    if (TD_LIKELY(IS_SLAB_ORDER(order))) {
        int idx = SLAB_INDEX(order);
        if (TD_LIKELY(h->slabs[idx].count > 0)) {
            td_t* v = h->slabs[idx].stack[--h->slabs[idx].count];

            memset(v, 0, 32);
            v->mmod  = 0;
            v->order = order;
            atomic_store_explicit(&v->rc, 1, memory_order_relaxed);

            td_tl_stats.alloc_count++;
            td_tl_stats.slab_hits++;
            td_tl_stats.bytes_allocated += BSIZEOF(order);
            if (td_tl_stats.bytes_allocated > td_tl_stats.peak_bytes)
                td_tl_stats.peak_bytes = td_tl_stats.bytes_allocated;
            return v;
        }
    }

    /* Find free block via avail bitmask.
     * Avail bits can be stale from cross-heap fl_remove, so we loop
     * to find a genuinely non-empty freelist. */
    uint64_t candidates = h->avail & (UINT64_MAX << order);

    if (TD_UNLIKELY(candidates == 0)) {
        heap_flush_foreign(h, false);  /* always local in td_alloc */

        candidates = h->avail & (UINT64_MAX << order);

        if (candidates == 0) {
            if (!heap_add_pool(h, order)) return NULL;
            candidates = h->avail & (UINT64_MAX << order);
            if (candidates == 0) return NULL;
        }
    }

    /* Scan past stale avail bits (cross-heap fl_remove may have emptied lists) */
    uint8_t found_order;
    for (;;) {
        if (candidates == 0) {
            if (!heap_add_pool(h, order)) return NULL;
            candidates = h->avail & (UINT64_MAX << order);
            if (candidates == 0) return NULL;
        }
        found_order = (uint8_t)__builtin_ctzll(candidates);
        if (!fl_empty(&h->freelist[found_order])) break;
        /* Clear stale bit and try next */
        h->avail &= ~(1ULL << found_order);
        candidates &= ~(1ULL << found_order);
    }

    /* Pop from circular sentinel freelist */
    td_fl_head_t* head = &h->freelist[found_order];
    td_t* blk = head->fl_next;
    fl_remove(blk);
    if (fl_empty(head))
        h->avail &= ~(1ULL << found_order);

    /* Split down to requested order */
    heap_split_block(h, blk, order, found_order);

    /* Zero td_t header and set metadata */
    memset(blk, 0, 32);
    blk->mmod  = 0;
    blk->order = order;
    atomic_store_explicit(&blk->rc, 1, memory_order_relaxed);

    td_tl_stats.alloc_count++;
    td_tl_stats.bytes_allocated += BSIZEOF(order);
    if (td_tl_stats.bytes_allocated > td_tl_stats.peak_bytes)
        td_tl_stats.peak_bytes = td_tl_stats.bytes_allocated;

    return blk;
}

/* --------------------------------------------------------------------------
 * td_free
 * -------------------------------------------------------------------------- */

void td_free(td_t* v) {
    if (!v || TD_IS_ERR(v)) return;

    /* Guard: keep rc=1 while releasing children so buddy coalescing
     * won't merge this block prematurely (it checks buddy_rc==0). */
    atomic_store_explicit(&v->rc, 1, memory_order_relaxed);

    td_release_owned_refs(v);

    /* File-mapped: munmap */
    if (v->mmod == 1) {
        if (v->type == TD_TABLE || v->type == TD_LIST) return;
        if (v->type > 0 && v->type < TD_TYPE_COUNT) {
            uint8_t esz = td_sym_elem_size(v->type, v->attrs);
            size_t data_size = 32 + (size_t)v->len * esz;
            if (v->attrs & TD_ATTR_NULLMAP_EXT)
                data_size += ((size_t)v->len + 7) / 8;
            size_t mapped_size = (data_size + 4095) & ~(size_t)4095;
            td_vm_unmap_file(v, mapped_size);
        } else {
            td_vm_unmap_file(v, 4096);
        }
        td_tl_stats.free_count++;
        return;
    }

    /* Legacy mmod==2 guard */
    if (v->mmod == 2) return;

    td_heap_t* h = td_tl_heap;
    if (!h) return;

    uint8_t order = v->order;

    if (order < TD_ORDER_MIN || order > TD_HEAP_MAX_ORDER) return;

    size_t block_size = BSIZEOF(order);

    /* Derive ownership via pool-list scan (cache-hot, avoids reading
     * the remote pool header 32MB away). */
    int pidx = heap_find_pool(h, v);
    bool is_local = (pidx >= 0);

    /* Slab fast path (same heap only) */
    if (IS_SLAB_ORDER(order) && is_local) {
        int idx = SLAB_INDEX(order);
        if (h->slabs[idx].count < TD_SLAB_CACHE_SIZE) {
            /* Mark rc=1 so buddy coalescing skips slab-cached blocks.
             * Blocks freed via td_release arrive with rc=0; without this,
             * a buddy being freed would see rc==0 and incorrectly merge
             * with the slab-cached block, causing overlapping allocations. */
            atomic_store_explicit(&v->rc, 1, memory_order_relaxed);
            h->slabs[idx].stack[h->slabs[idx].count++] = v;
            td_tl_stats.free_count++;
            td_tl_stats.bytes_allocated -= block_size;
            return;
        }
    }

    /* Foreign: different heap */
    if (!is_local) {
        v->fl_next = h->foreign;
        h->foreign = v;
        td_tl_stats.free_count++;
        td_tl_stats.bytes_allocated -= block_size;
        return;
    }

    /* Coalescing — pass pool info from pools[] (already cache-hot) */
    heap_coalesce(h, v, (uintptr_t)h->pools[pidx].base,
                  h->pools[pidx].pool_order);

    td_tl_stats.free_count++;
    td_tl_stats.bytes_allocated -= block_size;
}

/* --------------------------------------------------------------------------
 * td_alloc_copy
 * -------------------------------------------------------------------------- */

td_t* td_alloc_copy(td_t* v) {
    if (!v || TD_IS_ERR(v)) return NULL;
    size_t data_size;
    if (td_is_atom(v)) {
        data_size = 0;
    } else if (v->type == TD_TABLE) {
        if (v->len < 0) return TD_ERR_PTR(TD_ERR_OOM);
        data_size = (size_t)(td_len(v) + 1) * sizeof(td_t*);
    } else if (TD_IS_PARTED(v->type) || v->type == TD_MAPCOMMON) {
        int64_t n_ptrs = v->len;
        if (v->type == TD_MAPCOMMON) n_ptrs = 2;
        if (n_ptrs < 0) return TD_ERR_PTR(TD_ERR_OOM);
        data_size = (size_t)n_ptrs * sizeof(td_t*);
    } else {
        int8_t t = td_type(v);
        if (t <= 0 || t >= TD_TYPE_COUNT)
            data_size = 0;
        else {
            uint8_t esz = td_sym_elem_size(t, v->attrs);
            if (v->len < 0 || (esz > 0 && (uint64_t)v->len > SIZE_MAX / esz))
                return TD_ERR_PTR(TD_ERR_OOM);
            data_size = (size_t)td_len(v) * esz;
        }
    }
    td_t* copy = td_alloc(data_size);
    if (!copy) return NULL;

    uint8_t new_order = copy->order;
    uint8_t new_mmod  = copy->mmod;
    memcpy(copy, v, 32 + data_size);
    copy->mmod  = new_mmod;
    copy->order = new_order;
    atomic_store_explicit(&copy->rc, 1, memory_order_relaxed);
    td_retain_owned_refs(copy);
    return copy;
}

/* --------------------------------------------------------------------------
 * td_scratch_alloc / td_scratch_realloc
 * -------------------------------------------------------------------------- */

td_t* td_scratch_alloc(size_t data_size) {
    return td_alloc(data_size);
}

td_t* td_scratch_realloc(td_t* v, size_t new_data_size) {
    td_t* new_v = td_alloc(new_data_size);
    if (!new_v) return NULL;
    if (v && !TD_IS_ERR(v)) {
        size_t old_data;
        if (td_is_atom(v))
            old_data = 0;
        else if (v->type == TD_LIST) {
            if (v->len < 0) { old_data = 0; }
            else old_data = (size_t)td_len(v) * sizeof(td_t*);
        } else if (v->type == TD_TABLE) {
            if (v->len < 0) { old_data = 0; }
            else old_data = (size_t)(td_len(v) + 1) * sizeof(td_t*);
        } else if (TD_IS_PARTED(v->type) || v->type == TD_MAPCOMMON) {
            int64_t n_ptrs = v->len;
            if (v->type == TD_MAPCOMMON) n_ptrs = 2;
            if (n_ptrs < 0) n_ptrs = 0;
            old_data = (size_t)n_ptrs * sizeof(td_t*);
        } else {
            int8_t t = td_type(v);
            old_data = (t > 0 && t < TD_TYPE_COUNT && v->len >= 0) ?
                       (size_t)td_len(v) * td_sym_elem_size(t, v->attrs) : 0;
        }
        /* Clamp old_data to actual allocation size */
        if (v->mmod == 0 && v->order >= TD_ORDER_MIN) {
            size_t alloc_data = BSIZEOF(v->order) - 32;
            if (old_data > alloc_data) old_data = alloc_data;
        }
        size_t copy_data = old_data < new_data_size ? old_data : new_data_size;
        uint8_t new_mmod = new_v->mmod;
        uint8_t new_order = new_v->order;
        memcpy(new_v, v, 32 + copy_data);
        new_v->mmod = new_mmod;
        new_v->order = new_order;
        atomic_store_explicit(&new_v->rc, 1, memory_order_relaxed);
        td_retain_owned_refs(new_v);
        td_detach_owned_refs(v);
        td_free(v);
    }
    return new_v;
}

/* --------------------------------------------------------------------------
 * td_mem_stats
 * -------------------------------------------------------------------------- */

void td_mem_stats(td_mem_stats_t* out) {
    *out = td_tl_stats;
    int64_t sc = 0, sp = 0;
    td_sys_get_stat(&sc, &sp);
    out->sys_current = (size_t)sc;
    out->sys_peak    = (size_t)sp;
}

/* --------------------------------------------------------------------------
 * Heap lifecycle
 * -------------------------------------------------------------------------- */

void td_heap_init(void) {
    if (td_tl_heap) return;

    size_t heap_sz = (sizeof(td_heap_t) + 4095) & ~(size_t)4095;
    td_heap_t* h = (td_heap_t*)td_vm_alloc(heap_sz);
    if (!h) return;
    memset(h, 0, heap_sz);

    h->id = atomic_fetch_add_explicit(&g_heap_id_next, 1, memory_order_relaxed);

    /* Register in global heap registry */
    td_heap_registry[h->id % TD_HEAP_REGISTRY_SIZE] = h;

    /* Initialize circular sentinel freelists */
    for (int i = 0; i < TD_HEAP_FL_SIZE; i++)
        fl_init(&h->freelist[i]);

    td_tl_heap = h;
    memset(&td_tl_stats, 0, sizeof(td_tl_stats));
}

void td_heap_destroy(void) {
    td_heap_t* h = td_tl_heap;
    if (!h) return;

    /* Unregister from global heap registry */
    td_heap_registry[h->id % TD_HEAP_REGISTRY_SIZE] = NULL;

    /* Skip flush_slabs and flush_foreign — all pools are about to be
     * munmap'd. Flushing would coalesce blocks and fl_remove buddies
     * from other heaps' freelists, which races with concurrent worker
     * destruction during td_pool_free(). */

    /* Munmap all tracked pools */
    for (uint32_t i = 0; i < h->pool_count; i++) {
        td_pool_hdr_t* hdr = (td_pool_hdr_t*)h->pools[i].base;
        td_vm_free(hdr->vm_base, BSIZEOF(h->pools[i].pool_order));
    }

    size_t heap_sz = (sizeof(td_heap_t) + 4095) & ~(size_t)4095;
    td_vm_free(h, heap_sz);
    td_tl_heap = NULL;
    memset(&td_tl_stats, 0, sizeof(td_tl_stats));
}

/* --------------------------------------------------------------------------
 * Return worker-pool blocks from this heap's freelists to their owners.
 *
 * After td_alloc flushes foreign blocks locally (coalesce + madvise),
 * worker-pool blocks sit on main's freelists with released physical pages.
 * This function walks the freelists, finds blocks whose pool header
 * heap_id != ours, removes them, and inserts into the owning worker heap.
 * Workers can then reuse their pools without allocating new ones.
 *
 * ONLY safe when workers are idle (on semaphore, td_parallel_flag == 0).
 * -------------------------------------------------------------------------- */

static void heap_return_foreign_freelist(td_heap_t* h) {
    for (int order = TD_ORDER_MIN; order < TD_HEAP_FL_SIZE; order++) {
        td_fl_head_t* head = &h->freelist[order];
        td_t* blk = head->fl_next;
        while (blk != (td_t*)head) {
            td_t* next = blk->fl_next;
            /* Use heap_find_pool on h first — if found, block is local */
            int pidx = heap_find_pool(h, blk);
            if (pidx < 0) {
                /* Foreign block — find owner via pool header (GC path) */
                td_pool_hdr_t* phdr = td_pool_of(blk);
                td_heap_t* owner = td_heap_registry[phdr->heap_id % TD_HEAP_REGISTRY_SIZE];
                if (owner && owner->id == phdr->heap_id) {
                    fl_remove(blk);
                    if (fl_empty(head))
                        h->avail &= ~(1ULL << order);
                    /* Coalesce on owner for defragmentation */
                    int opidx = heap_find_pool(owner, blk);
                    uintptr_t pb;
                    uint8_t po;
                    if (opidx >= 0) {
                        pb = (uintptr_t)owner->pools[opidx].base;
                        po = owner->pools[opidx].pool_order;
                    } else {
                        pb = (uintptr_t)phdr;
                        po = phdr->pool_order;
                    }
                    heap_coalesce(owner, blk, pb, po);
                }
            }
            blk = next;
        }
    }
}

void td_heap_gc(void) {
    td_heap_t* h = td_tl_heap;
    if (!h) return;

    bool safe = (atomic_load_explicit(&td_parallel_flag, memory_order_relaxed) == 0);

    /* Phase 1: Flush main heap's foreign blocks and slab caches.
     * When safe (workers idle), return foreign blocks to their owners
     * so worker pools become reusable. */
    heap_flush_foreign(h, safe);
    heap_flush_slabs(h);

    if (safe) {
        /* Phase 2: Return foreign blocks absorbed onto our freelists
         * back to their owning worker heaps. */
        heap_return_foreign_freelist(h);

        /* Phase 3: Flush foreign + slabs on all worker heaps.
         * Workers may have accumulated foreign blocks from other workers,
         * and slab caches prevent buddy coalescing needed for reclamation. */
        for (int hid = 0; hid < TD_HEAP_REGISTRY_SIZE; hid++) {
            td_heap_t* wh = td_heap_registry[hid];
            if (!wh || wh == h) continue;
            heap_flush_foreign(wh, true);
            heap_flush_slabs(wh);
        }

        /* Phase 4: Reclaim OVERSIZED empty pools.
         * Standard pools (pool_order == TD_HEAP_POOL_ORDER) are never
         * munmapped — physical pages released via madvise (phase 5)
         * re-fault cheaply on next query.
         * Only oversized pools (pool_order > TD_HEAP_POOL_ORDER) are
         * candidates — these are one-off large allocations.
         *
         * Emptiness is computed by walking all heaps' freelists and slab
         * caches to sum free capacity within the pool. This avoids atomic
         * live_count operations on the alloc/free hot path. */
        for (int hid = 0; hid < TD_HEAP_REGISTRY_SIZE; hid++) {
            td_heap_t* gh = td_heap_registry[hid];
            if (!gh) continue;

            for (uint32_t p = 0; p < gh->pool_count; ) {
                td_pool_hdr_t* phdr = (td_pool_hdr_t*)gh->pools[p].base;

                /* Skip standard pools and last-remaining pool */
                if (phdr->pool_order <= TD_HEAP_POOL_ORDER
                    || gh->pool_count <= 1) {
                    p++;
                    continue;
                }

                uint8_t po = phdr->pool_order;
                uintptr_t pb = (uintptr_t)phdr;
                uintptr_t pe = pb + BSIZEOF(po);
                /* Total usable capacity (minus header block) */
                size_t pool_capacity = BSIZEOF(po) - BSIZEOF(TD_ORDER_MIN);

                /* Sum free bytes: walk all heaps' freelists + slab caches */
                size_t free_bytes = 0;
                for (int scan_hid = 0; scan_hid < TD_HEAP_REGISTRY_SIZE; scan_hid++) {
                    td_heap_t* scan_h = td_heap_registry[scan_hid];
                    if (!scan_h) continue;
                    for (int ord = TD_ORDER_MIN; ord < TD_HEAP_FL_SIZE; ord++) {
                        td_fl_head_t* fh = &scan_h->freelist[ord];
                        td_t* blk = fh->fl_next;
                        while (blk != (td_t*)fh) {
                            if ((uintptr_t)blk >= pb && (uintptr_t)blk < pe)
                                free_bytes += BSIZEOF(ord);
                            blk = blk->fl_next;
                        }
                    }
                    for (int si = 0; si < TD_SLAB_ORDERS; si++) {
                        for (uint32_t j = 0; j < scan_h->slabs[si].count; j++) {
                            td_t* sb = scan_h->slabs[si].stack[j];
                            if ((uintptr_t)sb >= pb && (uintptr_t)sb < pe)
                                free_bytes += BSIZEOF(TD_SLAB_MIN + si);
                        }
                    }
                }

                if (free_bytes < pool_capacity) {
                    p++;
                    continue;  /* pool has live allocations */
                }

                /* Pool is empty — remove all blocks from all freelists
                 * and slab caches before munmap. */
                for (int scan_hid = 0; scan_hid < TD_HEAP_REGISTRY_SIZE; scan_hid++) {
                    td_heap_t* scan_h = td_heap_registry[scan_hid];
                    if (!scan_h) continue;
                    for (int ord = TD_ORDER_MIN; ord < TD_HEAP_FL_SIZE; ord++) {
                        td_fl_head_t* fh = &scan_h->freelist[ord];
                        td_t* blk = fh->fl_next;
                        while (blk != (td_t*)fh) {
                            td_t* next = blk->fl_next;
                            if ((uintptr_t)blk >= pb && (uintptr_t)blk < pe) {
                                fl_remove(blk);
                                if (fl_empty(fh))
                                    scan_h->avail &= ~(1ULL << ord);
                            }
                            blk = next;
                        }
                    }
                    for (int si = 0; si < TD_SLAB_ORDERS; si++) {
                        uint32_t dst = 0;
                        for (uint32_t j = 0; j < scan_h->slabs[si].count; j++) {
                            td_t* sb = scan_h->slabs[si].stack[j];
                            if ((uintptr_t)sb >= pb && (uintptr_t)sb < pe)
                                continue;
                            scan_h->slabs[si].stack[dst++] = sb;
                        }
                        scan_h->slabs[si].count = dst;
                    }
                }

                td_vm_free(phdr->vm_base, BSIZEOF(po));
                gh->pools[p] = gh->pools[--gh->pool_count];
                /* Don't increment p — check swapped entry */
            }
        }
    }

}

void td_heap_release_pages(void) {
    td_heap_t* h = td_tl_heap;
    if (!h) return;
    for (int i = 13; i < TD_HEAP_FL_SIZE; i++) {
        td_fl_head_t* head = &h->freelist[i];
        td_t* blk = head->fl_next;
        while (blk != (td_t*)head) {
            size_t bsize = BSIZEOF(i);
            if (bsize > 4096)
                td_vm_release((char*)blk + 4096, bsize - 4096);
            blk = blk->fl_next;
        }
    }
}

void td_heap_merge(td_heap_t* src) {
    td_heap_t* dst = td_tl_heap;
    if (!dst || !src) return;

    /* Transfer slabs: fit into dst cache, coalesce overflow */
    for (int i = 0; i < TD_SLAB_ORDERS; i++) {
        while (src->slabs[i].count > 0 && dst->slabs[i].count < TD_SLAB_CACHE_SIZE)
            dst->slabs[i].stack[dst->slabs[i].count++] =
                src->slabs[i].stack[--src->slabs[i].count];
        while (src->slabs[i].count > 0) {
            td_t* blk = src->slabs[i].stack[--src->slabs[i].count];
            int pidx = heap_find_pool(dst, blk);
            uintptr_t pb;
            uint8_t po;
            if (pidx >= 0) {
                pb = (uintptr_t)dst->pools[pidx].base;
                po = dst->pools[pidx].pool_order;
            } else {
                td_pool_hdr_t* phdr = td_pool_of(blk);
                pb = (uintptr_t)phdr;
                po = phdr->pool_order;
            }
            heap_coalesce(dst, blk, pb, po);
        }
    }

    /* Free foreign blocks via coalescing */
    td_t* fblk = src->foreign;
    while (fblk) {
        td_t* next = fblk->fl_next;
        int pidx = heap_find_pool(dst, fblk);
        uintptr_t pb;
        uint8_t po;
        if (pidx >= 0) {
            pb = (uintptr_t)dst->pools[pidx].base;
            po = dst->pools[pidx].pool_order;
        } else {
            td_pool_hdr_t* phdr = td_pool_of(fblk);
            pb = (uintptr_t)phdr;
            po = phdr->pool_order;
        }
        heap_coalesce(dst, fblk, pb, po);
        fblk = next;
    }
    src->foreign = NULL;

    /* Merge freelists: circular list splice (src chain into dst chain) */
    for (int i = TD_ORDER_MIN; i < TD_HEAP_FL_SIZE; i++) {
        if (fl_empty(&src->freelist[i])) continue;

        td_fl_head_t* src_head = &src->freelist[i];
        td_fl_head_t* dst_head = &dst->freelist[i];

        /* Splice: src's chain [src_first...src_last] into dst after sentinel */
        td_t* src_first = src_head->fl_next;
        td_t* src_last  = src_head->fl_prev;
        td_t* dst_first = dst_head->fl_next;

        /* src_first goes after dst sentinel */
        dst_head->fl_next = src_first;
        src_first->fl_prev = (td_t*)dst_head;

        /* src_last connects to old dst_first */
        src_last->fl_next = dst_first;
        dst_first->fl_prev = src_last;

        dst->avail |= (1ULL << i);

        /* Reset src sentinel to empty */
        fl_init(src_head);
    }

    src->avail = 0;

    /* Update pool headers: set heap_id to dst, transfer pool entries */
    for (uint32_t i = 0; i < src->pool_count; i++) {
        td_pool_hdr_t* hdr = (td_pool_hdr_t*)src->pools[i].base;
        hdr->heap_id = dst->id;

        if (dst->pool_count < TD_MAX_POOLS) {
            dst->pools[dst->pool_count++] = src->pools[i];
        }
    }
    src->pool_count = 0;
}

/* --------------------------------------------------------------------------
 * Scratch arena: bump allocator backed by buddy-allocated 64KB blocks
 * -------------------------------------------------------------------------- */

void* td_scratch_arena_push(td_scratch_arena_t* a, size_t nbytes) {
    /* 16-byte alignment */
    nbytes = (nbytes + 15) & ~(size_t)15;

    if (TD_LIKELY(a->ptr + nbytes <= a->end))
        goto bump;

    /* Need a new backing block */
    if (a->n_backing >= TD_ARENA_MAX_BACKING) return NULL;

    size_t block_data = BSIZEOF(TD_ARENA_BLOCK_ORDER) - 32;
    /* If request exceeds standard block, allocate exact-fit */
    size_t alloc_size = nbytes > block_data ? nbytes : block_data;
    td_t* blk = td_alloc(alloc_size);
    if (!blk) return NULL;
    a->backing[a->n_backing++] = blk;
    a->ptr = (char*)td_data(blk);
    a->end = (char*)blk + BSIZEOF(blk->order);

bump:;
    void* ret = a->ptr;
    a->ptr += nbytes;
    return ret;
}

void td_scratch_arena_reset(td_scratch_arena_t* a) {
    for (int i = 0; i < a->n_backing; i++)
        td_free(a->backing[i]);
    a->n_backing = 0;
    a->ptr = NULL;
    a->end = NULL;
}

/* --------------------------------------------------------------------------
 * Parallel begin / end
 * -------------------------------------------------------------------------- */

void td_parallel_begin(void) { atomic_store(&td_parallel_flag, 1); }
void td_parallel_end(void) {
    atomic_store(&td_parallel_flag, 0);
    td_heap_gc();
}
