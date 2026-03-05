# Radix-Partitioned Hash Join — Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Replace the chained hash join in `exec_join` with a radix-partitioned hash join that keeps each partition in L2 cache, closing the 3.4x gap vs DuckDB on the H2O.ai 10M-row join benchmark.

**Architecture:** Hash all keys, extract R adaptive radix bits to partition both sides into P=2^R buckets. Per-partition: build an open-addressing HT, probe it (two-pass count+fill), then gather output columns. All buffers use `td_alloc`/`td_free`. When `td_alloc` fails, degrade to sequential partition processing.

**Tech Stack:** Pure C17, no external dependencies. Uses existing `td_alloc`, `td_pool_dispatch`, `td_pool_dispatch_n`, `hash_row_keys`, `td_hash_i64`, `td_hash_combine`.

---

### Task 1: Add radix join constants to td.h

**Files:**
- Modify: `include/teide/td.h`

**Step 1: Add constants**

Add after line 191 (`TD_DISPATCH_MORSELS`):

```c
/* Radix-partitioned hash join tuning */
#define TD_JOIN_L2_TARGET   (256 * 1024)   /* target partition size in bytes  */
#define TD_JOIN_MIN_RADIX   2              /* min radix bits (4 partitions)   */
#define TD_JOIN_MAX_RADIX   14             /* max radix bits (16K partitions) */
```

**Step 2: Build and verify**

Run: `cd build && cmake --build . 2>&1 | tail -5`
Expected: Build succeeds, no warnings.

**Step 3: Run tests**

Run: `cd build && ctest --output-on-failure -R exec`
Expected: All exec tests pass (constants are unused so far).

**Step 4: Commit**

```bash
git add include/teide/td.h
git commit -m "feat(join): add radix-partitioned join constants"
```

---

### Task 2: Implement radix bit selection and partition count/scatter infrastructure

This task adds the partitioning functions that will be called from `exec_join`. We add them as static functions in `exec.c` above the existing join code (~line 7431).

**Files:**
- Modify: `src/ops/exec.c:7431` (insert before `exec_join`)

**Step 1: Write the radix bit selection function**

Insert before line 7431 (before the `/* Join execution */` comment):

```c
/* ============================================================================
 * Radix-partitioned hash join
 *
 * Four-phase pipeline:
 *   Phase 1: Partition both sides by radix bits of hash (parallel)
 *   Phase 2: Per-partition build + probe with open-addressing HT (parallel)
 *   Phase 3: Gather output columns from matched pairs (parallel)
 *   Phase 4: Fallback to chained HT for small joins (< TD_PARALLEL_THRESHOLD)
 * ============================================================================ */

/* Partition entry: row index + cached hash */
typedef struct {
    uint32_t row_idx;
    uint32_t hash;
} radix_entry_t;

/* Per-partition descriptor */
typedef struct {
    radix_entry_t* entries;   /* partition buffer (from td_alloc) */
    td_t*          entries_hdr; /* td_alloc header for freeing */
    uint32_t       count;     /* number of entries in partition */
} radix_part_t;

/* Choose radix bits so each right-side partition fits in L2 cache */
static uint8_t radix_join_bits(int64_t right_rows, uint8_t n_keys) {
    /* Estimate bytes per right-side entry: row_idx(4) + hash(4) = 8 */
    size_t right_bytes = (size_t)right_rows * sizeof(radix_entry_t);
    if (right_bytes <= TD_JOIN_L2_TARGET)
        return TD_JOIN_MIN_RADIX;

    /* R = ceil(log2(right_bytes / L2_TARGET)) */
    uint8_t r = 0;
    size_t target = TD_JOIN_L2_TARGET;
    while (target < right_bytes && r < 63) {
        target *= 2;
        r++;
    }
    if (r < TD_JOIN_MIN_RADIX) r = TD_JOIN_MIN_RADIX;
    if (r > TD_JOIN_MAX_RADIX) r = TD_JOIN_MAX_RADIX;
    return r;
}
```

**Step 2: Write the parallel histogram (count) function**

```c
/* Context for parallel partition histogram */
typedef struct {
    td_t**    key_vecs;
    uint8_t   n_keys;
    uint32_t  radix_mask;   /* (1 << radix_bits) - 1 */
    uint8_t   radix_shift;  /* hash bits to shift before masking */
    uint32_t  n_parts;      /* 1 << radix_bits */
    uint32_t  n_workers;
    uint32_t* histograms;   /* [n_workers][n_parts] flat array */
} radix_hist_ctx_t;

static void radix_hist_fn(void* raw, uint32_t wid, int64_t start, int64_t end) {
    radix_hist_ctx_t* c = (radix_hist_ctx_t*)raw;
    uint32_t* hist = c->histograms + (uint32_t)wid * c->n_parts;
    uint32_t mask = c->radix_mask;
    uint8_t shift = c->radix_shift;

    for (int64_t r = start; r < end; r++) {
        uint64_t h = hash_row_keys(c->key_vecs, c->n_keys, r);
        uint32_t part = (uint32_t)(h >> shift) & mask;
        hist[part]++;
    }
}
```

**Step 3: Write the parallel scatter function**

```c
/* Context for parallel partition scatter */
typedef struct {
    td_t**         key_vecs;
    uint8_t        n_keys;
    uint32_t       radix_mask;
    uint8_t        radix_shift;
    uint32_t       n_parts;
    radix_part_t*  parts;       /* partition descriptors */
    uint32_t*      offsets;     /* [n_workers][n_parts] per-worker write positions */
} radix_scatter_ctx_t;

static void radix_scatter_fn(void* raw, uint32_t wid, int64_t start, int64_t end) {
    radix_scatter_ctx_t* c = (radix_scatter_ctx_t*)raw;
    uint32_t* off = c->offsets + (uint32_t)wid * c->n_parts;
    uint32_t mask = c->radix_mask;
    uint8_t shift = c->radix_shift;

    for (int64_t r = start; r < end; r++) {
        uint64_t h = hash_row_keys(c->key_vecs, c->n_keys, r);
        uint32_t part = (uint32_t)(h >> shift) & mask;
        uint32_t pos = off[part]++;
        radix_entry_t* entries = c->parts[part].entries;
        entries[pos].row_idx = (uint32_t)r;
        entries[pos].hash = (uint32_t)h;  /* lower 32 bits for HT probe */
    }
}
```

**Step 4: Write the partition orchestrator**

```c
/* Partition one side of the join. Returns array of radix_part_t[n_parts].
 * Caller must free each partition's entries_hdr and the parts array itself. */
static radix_part_t* radix_partition(td_pool_t* pool, td_t** key_vecs,
                                      uint8_t n_keys, int64_t nrows,
                                      uint8_t radix_bits, td_t** parts_hdr_out) {
    uint32_t n_parts = (uint32_t)1 << radix_bits;
    uint32_t mask = n_parts - 1;
    /* Use upper bits of hash for radix (lower bits used inside partition HT) */
    uint8_t shift = 32 - radix_bits;

    /* Allocate partition descriptor array */
    td_t* parts_hdr;
    radix_part_t* parts = (radix_part_t*)scratch_calloc(&parts_hdr,
                            (size_t)n_parts * sizeof(radix_part_t));
    if (!parts) { *parts_hdr_out = NULL; return NULL; }
    *parts_hdr_out = parts_hdr;

    /* Step 1: Histogram — count rows per partition per worker */
    uint32_t n_workers = pool ? pool->n_workers + 1 : 1;  /* +1 for main thread */
    td_t* hist_hdr;
    uint32_t* histograms = (uint32_t*)scratch_calloc(&hist_hdr,
                             (size_t)n_workers * n_parts * sizeof(uint32_t));
    if (!histograms) { scratch_free(parts_hdr); *parts_hdr_out = NULL; return NULL; }

    radix_hist_ctx_t hctx = {
        .key_vecs = key_vecs, .n_keys = n_keys,
        .radix_mask = mask, .radix_shift = shift,
        .n_parts = n_parts, .n_workers = n_workers,
        .histograms = histograms,
    };
    if (pool && nrows > TD_PARALLEL_THRESHOLD)
        td_pool_dispatch(pool, radix_hist_fn, &hctx, nrows);
    else
        radix_hist_fn(&hctx, 0, 0, nrows);

    /* Compute partition sizes (sum across workers) */
    for (uint32_t p = 0; p < n_parts; p++) {
        uint32_t total = 0;
        for (uint32_t w = 0; w < n_workers; w++)
            total += histograms[w * n_parts + p];
        parts[p].count = total;
    }

    /* Allocate partition buffers */
    bool oom = false;
    for (uint32_t p = 0; p < n_parts; p++) {
        if (parts[p].count == 0) continue;
        parts[p].entries = (radix_entry_t*)scratch_alloc(&parts[p].entries_hdr,
                             (size_t)parts[p].count * sizeof(radix_entry_t));
        if (!parts[p].entries) {
            /* OOM: try gc + release + retry once */
            td_heap_gc();
            td_heap_release_pages();
            parts[p].entries = (radix_entry_t*)scratch_alloc(&parts[p].entries_hdr,
                                 (size_t)parts[p].count * sizeof(radix_entry_t));
            if (!parts[p].entries) { oom = true; break; }
        }
    }
    if (oom) {
        /* Free any allocated partition buffers */
        for (uint32_t p = 0; p < n_parts; p++)
            if (parts[p].entries_hdr) scratch_free(parts[p].entries_hdr);
        scratch_free(hist_hdr);
        scratch_free(parts_hdr);
        *parts_hdr_out = NULL;
        return NULL;
    }

    /* Step 2: Compute per-worker write offsets within each partition */
    td_t* off_hdr;
    uint32_t* offsets = (uint32_t*)scratch_alloc(&off_hdr,
                          (size_t)n_workers * n_parts * sizeof(uint32_t));
    if (!offsets) {
        for (uint32_t p = 0; p < n_parts; p++)
            if (parts[p].entries_hdr) scratch_free(parts[p].entries_hdr);
        scratch_free(hist_hdr);
        scratch_free(parts_hdr);
        *parts_hdr_out = NULL;
        return NULL;
    }

    for (uint32_t p = 0; p < n_parts; p++) {
        uint32_t running = 0;
        for (uint32_t w = 0; w < n_workers; w++) {
            offsets[w * n_parts + p] = running;
            running += histograms[w * n_parts + p];
        }
    }

    /* Step 3: Scatter rows into partition buffers */
    radix_scatter_ctx_t sctx = {
        .key_vecs = key_vecs, .n_keys = n_keys,
        .radix_mask = mask, .radix_shift = shift,
        .n_parts = n_parts, .parts = parts,
        .offsets = offsets,
    };
    if (pool && nrows > TD_PARALLEL_THRESHOLD)
        td_pool_dispatch(pool, radix_scatter_fn, &sctx, nrows);
    else
        radix_scatter_fn(&sctx, 0, 0, nrows);

    scratch_free(off_hdr);
    scratch_free(hist_hdr);
    return parts;
}
```

**Step 5: Build and run tests**

Run: `cd build && cmake --build . 2>&1 | tail -5`
Expected: Build succeeds (functions are static, unused for now — may get `-Wunused-function` warnings, which is OK).

Run: `cd build && ctest --output-on-failure -R exec`
Expected: All existing join tests still pass.

**Step 6: Commit**

```bash
git add src/ops/exec.c
git commit -m "feat(join): add radix partition infrastructure (histogram + scatter)"
```

---

### Task 3: Implement per-partition open-addressing build + probe

This task adds the per-partition hash table build and two-pass probe. These are static functions in `exec.c`, placed after the partition infrastructure from Task 2.

**Files:**
- Modify: `src/ops/exec.c` (insert after Task 2 code, before `exec_join`)

**Step 1: Write the per-partition build + probe function**

```c
/* Per-partition open-addressing hash table */
#define RADIX_HT_EMPTY UINT32_MAX

/* Per-partition build+probe context */
typedef struct {
    /* Partition data */
    radix_part_t*  l_parts;
    radix_part_t*  r_parts;
    /* Key vectors for equality check (global row indices) */
    td_t**         l_key_vecs;
    td_t**         r_key_vecs;
    uint8_t        n_keys;
    uint8_t        join_type;
    /* Output: per-partition match counts (for prefix sum) */
    int64_t*       part_counts;
    /* Output: per-partition match pairs (filled in second call) */
    int64_t*       l_idx;
    int64_t*       r_idx;
    int64_t*       part_offsets;   /* write offset per partition */
    /* FULL OUTER: track matched right rows */
    _Atomic(uint8_t)* matched_right;
    /* Phase flag: 0=count, 1=fill */
    uint8_t        phase;
} radix_bp_ctx_t;

static void radix_build_probe_fn(void* raw, uint32_t wid, int64_t task_start, int64_t task_end) {
    (void)wid; (void)task_end;
    radix_bp_ctx_t* c = (radix_bp_ctx_t*)raw;
    uint32_t p = (uint32_t)task_start;

    radix_part_t* rp = &c->r_parts[p];
    radix_part_t* lp = &c->l_parts[p];

    if (rp->count == 0) {
        /* No right rows in this partition */
        if (c->phase == 0) {
            c->part_counts[p] = (c->join_type >= 1) ? (int64_t)lp->count : 0;
        } else if (c->join_type >= 1 && lp->count > 0) {
            int64_t off = c->part_offsets[p];
            for (uint32_t i = 0; i < lp->count; i++) {
                c->l_idx[off] = lp->entries[i].row_idx;
                c->r_idx[off] = -1;
                off++;
            }
        }
        return;
    }

    /* Build open-addressing HT for right partition */
    uint32_t ht_cap = 256;
    while (ht_cap < rp->count * 2) ht_cap *= 2;
    uint32_t ht_mask = ht_cap - 1;

    /* HT entries: [hash | row_idx] packed as two uint32_t */
    td_t* ht_hdr;
    uint32_t* ht = (uint32_t*)scratch_calloc(&ht_hdr, (size_t)ht_cap * 2 * sizeof(uint32_t));
    if (!ht) {
        if (c->phase == 0) c->part_counts[p] = 0;
        return;
    }
    /* Initialize all slots to EMPTY */
    for (uint32_t s = 0; s < ht_cap; s++)
        ht[s * 2 + 1] = RADIX_HT_EMPTY;  /* row_idx slot */

    /* Insert right-side entries */
    for (uint32_t i = 0; i < rp->count; i++) {
        uint32_t h = rp->entries[i].hash;
        uint32_t slot = h & ht_mask;
        /* Linear probe to find empty slot */
        while (ht[slot * 2 + 1] != RADIX_HT_EMPTY)
            slot = (slot + 1) & ht_mask;
        ht[slot * 2] = h;
        ht[slot * 2 + 1] = rp->entries[i].row_idx;
    }

    /* Probe with left-side entries */
    if (c->phase == 0) {
        /* Count phase */
        int64_t count = 0;
        for (uint32_t i = 0; i < lp->count; i++) {
            uint32_t h = lp->entries[i].hash;
            uint32_t lr = lp->entries[i].row_idx;
            uint32_t slot = h & ht_mask;
            bool matched = false;
            while (ht[slot * 2 + 1] != RADIX_HT_EMPTY) {
                if (ht[slot * 2] == h) {
                    uint32_t rr = ht[slot * 2 + 1];
                    if (join_keys_eq(c->l_key_vecs, c->r_key_vecs, c->n_keys,
                                     (int64_t)lr, (int64_t)rr)) {
                        count++;
                        matched = true;
                    }
                }
                slot = (slot + 1) & ht_mask;
            }
            if (!matched && c->join_type >= 1) count++;
        }
        c->part_counts[p] = count;
    } else {
        /* Fill phase */
        int64_t off = c->part_offsets[p];
        for (uint32_t i = 0; i < lp->count; i++) {
            uint32_t h = lp->entries[i].hash;
            uint32_t lr = lp->entries[i].row_idx;
            uint32_t slot = h & ht_mask;
            bool matched = false;
            while (ht[slot * 2 + 1] != RADIX_HT_EMPTY) {
                if (ht[slot * 2] == h) {
                    uint32_t rr = ht[slot * 2 + 1];
                    if (join_keys_eq(c->l_key_vecs, c->r_key_vecs, c->n_keys,
                                     (int64_t)lr, (int64_t)rr)) {
                        c->l_idx[off] = (int64_t)lr;
                        c->r_idx[off] = (int64_t)rr;
                        off++;
                        matched = true;
                        if (c->matched_right)
                            atomic_store_explicit(&c->matched_right[rr], 1, memory_order_relaxed);
                    }
                }
                slot = (slot + 1) & ht_mask;
            }
            if (!matched && c->join_type >= 1) {
                c->l_idx[off] = (int64_t)lr;
                c->r_idx[off] = -1;
                off++;
            }
        }
    }

    scratch_free(ht_hdr);
}
```

**Step 2: Build and verify**

Run: `cd build && cmake --build . 2>&1 | tail -5`
Expected: Build succeeds (may warn about unused static function).

**Step 3: Commit**

```bash
git add src/ops/exec.c
git commit -m "feat(join): add per-partition open-addressing build+probe"
```

---

### Task 4: Wire radix join into exec_join

Replace the internals of `exec_join` to use radix partitioning for large joins while keeping the chained HT as fallback for small joins.

**Files:**
- Modify: `src/ops/exec.c:7635` (the `exec_join` function)

**Step 1: Restructure exec_join**

The strategy: keep the existing code path as a fallback (`right_rows <= TD_PARALLEL_THRESHOLD`), and add the radix path before it. The existing key extraction code (lines 7635–7664) stays the same. We replace the hash table build/probe/gather section.

Replace the body of `exec_join` starting from line 7666 (`/* Phase 1: Build hash table */`) through line 7044 (`join_cleanup:` section), keeping the cleanup intact.

The new flow inside `exec_join`, after key extraction:

```c
    /* ── Radix-partitioned path (large joins) ──────────────────────── */
    if (right_rows > TD_PARALLEL_THRESHOLD) {
        uint8_t radix_bits = radix_join_bits(right_rows, n_keys);
        uint32_t n_parts = (uint32_t)1 << radix_bits;

        /* Partition both sides */
        td_t* r_parts_hdr;
        radix_part_t* r_parts = radix_partition(pool, r_key_vecs, n_keys,
                                                 right_rows, radix_bits, &r_parts_hdr);
        td_t* l_parts_hdr;
        radix_part_t* l_parts = radix_partition(pool, l_key_vecs, n_keys,
                                                 left_rows, radix_bits, &l_parts_hdr);
        if (!r_parts || !l_parts) {
            /* OOM during partitioning — fall through to chained HT path */
            if (r_parts) {
                for (uint32_t p = 0; p < n_parts; p++)
                    if (r_parts[p].entries_hdr) scratch_free(r_parts[p].entries_hdr);
                scratch_free(r_parts_hdr);
            }
            if (l_parts) {
                for (uint32_t p = 0; p < n_parts; p++)
                    if (l_parts[p].entries_hdr) scratch_free(l_parts[p].entries_hdr);
                scratch_free(l_parts_hdr);
            }
            goto chained_ht_fallback;
        }

        /* FULL OUTER: allocate matched_right tracker */
        td_t* matched_right_hdr = NULL;
        _Atomic(uint8_t)* matched_right = NULL;
        if (join_type == 2 && right_rows > 0) {
            matched_right = (_Atomic(uint8_t)*)scratch_calloc(&matched_right_hdr,
                                                               (size_t)right_rows);
            if (!matched_right) {
                for (uint32_t p = 0; p < n_parts; p++) {
                    if (r_parts[p].entries_hdr) scratch_free(r_parts[p].entries_hdr);
                    if (l_parts[p].entries_hdr) scratch_free(l_parts[p].entries_hdr);
                }
                scratch_free(r_parts_hdr); scratch_free(l_parts_hdr);
                goto chained_ht_fallback;
            }
        }

        /* Phase 2: Per-partition build+probe — count pass */
        td_t* pcounts_hdr;
        int64_t* part_counts = (int64_t*)scratch_calloc(&pcounts_hdr,
                                  (size_t)n_parts * sizeof(int64_t));
        if (!part_counts) {
            for (uint32_t p = 0; p < n_parts; p++) {
                if (r_parts[p].entries_hdr) scratch_free(r_parts[p].entries_hdr);
                if (l_parts[p].entries_hdr) scratch_free(l_parts[p].entries_hdr);
            }
            scratch_free(r_parts_hdr); scratch_free(l_parts_hdr);
            scratch_free(matched_right_hdr);
            goto chained_ht_fallback;
        }

        radix_bp_ctx_t bp_ctx = {
            .l_parts = l_parts, .r_parts = r_parts,
            .l_key_vecs = l_key_vecs, .r_key_vecs = r_key_vecs,
            .n_keys = n_keys, .join_type = join_type,
            .part_counts = part_counts,
            .matched_right = matched_right,
            .phase = 0,  /* count */
        };
        if (pool && n_parts > 1)
            td_pool_dispatch_n(pool, radix_build_probe_fn, &bp_ctx, n_parts);
        else
            for (uint32_t p = 0; p < n_parts; p++)
                radix_build_probe_fn(&bp_ctx, 0, p, p + 1);

        /* Prefix sum → partition offsets */
        int64_t pair_count = 0;
        td_t* poff_hdr;
        int64_t* part_offsets = (int64_t*)scratch_alloc(&poff_hdr,
                                  (size_t)n_parts * sizeof(int64_t));
        if (!part_offsets) {
            for (uint32_t p = 0; p < n_parts; p++) {
                if (r_parts[p].entries_hdr) scratch_free(r_parts[p].entries_hdr);
                if (l_parts[p].entries_hdr) scratch_free(l_parts[p].entries_hdr);
            }
            scratch_free(r_parts_hdr); scratch_free(l_parts_hdr);
            scratch_free(pcounts_hdr); scratch_free(matched_right_hdr);
            return TD_ERR_PTR(TD_ERR_OOM);
        }
        for (uint32_t p = 0; p < n_parts; p++) {
            part_offsets[p] = pair_count;
            pair_count += part_counts[p];
        }

        /* Allocate output pair arrays */
        td_t* l_idx_hdr = NULL;
        td_t* r_idx_hdr = NULL;
        int64_t* l_idx = NULL;
        int64_t* r_idx = NULL;
        if (pair_count > 0) {
            l_idx = (int64_t*)scratch_alloc(&l_idx_hdr, (size_t)pair_count * sizeof(int64_t));
            r_idx = (int64_t*)scratch_alloc(&r_idx_hdr, (size_t)pair_count * sizeof(int64_t));
            if (!l_idx || !r_idx) {
                scratch_free(l_idx_hdr); scratch_free(r_idx_hdr);
                for (uint32_t p = 0; p < n_parts; p++) {
                    if (r_parts[p].entries_hdr) scratch_free(r_parts[p].entries_hdr);
                    if (l_parts[p].entries_hdr) scratch_free(l_parts[p].entries_hdr);
                }
                scratch_free(r_parts_hdr); scratch_free(l_parts_hdr);
                scratch_free(pcounts_hdr); scratch_free(poff_hdr);
                scratch_free(matched_right_hdr);
                return TD_ERR_PTR(TD_ERR_OOM);
            }
        }

        /* Fill pass */
        bp_ctx.phase = 1;
        bp_ctx.l_idx = l_idx;
        bp_ctx.r_idx = r_idx;
        bp_ctx.part_offsets = part_offsets;

        if (pair_count > 0) {
            if (pool && n_parts > 1)
                td_pool_dispatch_n(pool, radix_build_probe_fn, &bp_ctx, n_parts);
            else
                for (uint32_t p = 0; p < n_parts; p++)
                    radix_build_probe_fn(&bp_ctx, 0, p, p + 1);
        }

        /* Free partition buffers — no longer needed */
        for (uint32_t p = 0; p < n_parts; p++) {
            if (r_parts[p].entries_hdr) scratch_free(r_parts[p].entries_hdr);
            if (l_parts[p].entries_hdr) scratch_free(l_parts[p].entries_hdr);
        }
        scratch_free(r_parts_hdr);
        scratch_free(l_parts_hdr);
        scratch_free(pcounts_hdr);
        scratch_free(poff_hdr);

        /* FULL OUTER: append unmatched right rows */
        if (join_type == 2 && matched_right) {
            int64_t unmatched_right = 0;
            for (int64_t r = 0; r < right_rows; r++)
                if (!matched_right[r]) unmatched_right++;

            if (unmatched_right > 0) {
                int64_t total = pair_count + unmatched_right;
                td_t* new_l_hdr; td_t* new_r_hdr;
                int64_t* new_l = (int64_t*)scratch_alloc(&new_l_hdr,
                                    (size_t)total * sizeof(int64_t));
                int64_t* new_r = (int64_t*)scratch_alloc(&new_r_hdr,
                                    (size_t)total * sizeof(int64_t));
                if (!new_l || !new_r) {
                    scratch_free(new_l_hdr); scratch_free(new_r_hdr);
                    scratch_free(l_idx_hdr); scratch_free(r_idx_hdr);
                    scratch_free(matched_right_hdr);
                    return TD_ERR_PTR(TD_ERR_OOM);
                }
                if (pair_count > 0) {
                    memcpy(new_l, l_idx, (size_t)pair_count * sizeof(int64_t));
                    memcpy(new_r, r_idx, (size_t)pair_count * sizeof(int64_t));
                }
                scratch_free(l_idx_hdr); scratch_free(r_idx_hdr);
                int64_t off = pair_count;
                for (int64_t r = 0; r < right_rows; r++) {
                    if (!matched_right[r]) {
                        new_l[off] = -1;
                        new_r[off] = r;
                        off++;
                    }
                }
                l_idx = new_l; r_idx = new_r;
                l_idx_hdr = new_l_hdr; r_idx_hdr = new_r_hdr;
                pair_count = total;
            }
        }
        scratch_free(matched_right_hdr);

        /* Phase 3: Gather — reuse existing gather infrastructure */
        /* (Same gather code as current exec_join, lines ~7891–8032) */
        goto radix_gather;
    }

chained_ht_fallback:
    /* Existing chained hash table code stays here as fallback */
    ...existing code...

radix_gather:
    /* Gather code — shared between radix and chained paths */
    ...existing gather code...
```

**Important**: The gather code (Phase 3, lines 7891–8032) and the column assembly + cleanup (lines 8024–8044) are shared between both paths. Factor them so both the radix path and the chained HT path jump to the same gather+cleanup section.

**Step 2: Build and run tests**

Run: `cd build && cmake --build . 2>&1 | tail -10`
Expected: Build succeeds.

Run: `cd build && ctest --output-on-failure -R exec`
Expected: All exec tests pass, including `/join`.

**Step 3: Commit**

```bash
git add src/ops/exec.c
git commit -m "feat(join): wire radix-partitioned path into exec_join"
```

---

### Task 5: Add join correctness tests

Add tests that exercise the radix path (requires right side > 64K rows to trigger it).

**Files:**
- Modify: `test/test_exec.c`

**Step 1: Write a large-join test that triggers radix partitioning**

Add after the existing `test_exec_join` function (~line 743):

```c
/* ---- LARGE JOIN (radix-partitioned path) ---- */
static MunitResult test_exec_join_large(const void* params, void* data) {
    (void)params; (void)data;
    td_heap_init();
    td_sym_init();

    /* Create left table: 100K rows, id = i % 1000, val = i */
    int64_t n_left = 100000;
    td_t* lid_v = td_alloc((size_t)n_left * sizeof(int64_t));
    lid_v->type = TD_I64; lid_v->len = n_left;
    td_t* lval_v = td_alloc((size_t)n_left * sizeof(int64_t));
    lval_v->type = TD_I64; lval_v->len = n_left;
    int64_t* lid = (int64_t*)td_data(lid_v);
    int64_t* lval = (int64_t*)td_data(lval_v);
    for (int64_t i = 0; i < n_left; i++) {
        lid[i] = i % 1000;
        lval[i] = i;
    }

    /* Right table: 100K rows, id = i % 1000, score = i * 10 */
    int64_t n_right = 100000;
    td_t* rid_v = td_alloc((size_t)n_right * sizeof(int64_t));
    rid_v->type = TD_I64; rid_v->len = n_right;
    td_t* rscore_v = td_alloc((size_t)n_right * sizeof(int64_t));
    rscore_v->type = TD_I64; rscore_v->len = n_right;
    int64_t* rid = (int64_t*)td_data(rid_v);
    int64_t* rscore = (int64_t*)td_data(rscore_v);
    for (int64_t i = 0; i < n_right; i++) {
        rid[i] = i % 1000;
        rscore[i] = i * 10;
    }

    int64_t n_id = td_sym_intern("id", 2);
    int64_t n_val = td_sym_intern("val", 3);
    int64_t n_score = td_sym_intern("score", 5);

    td_t* left = td_table_new(2);
    left = td_table_add_col(left, n_id, lid_v);
    left = td_table_add_col(left, n_val, lval_v);
    td_release(lid_v); td_release(lval_v);

    td_t* right = td_table_new(2);
    right = td_table_add_col(right, n_id, rid_v);
    right = td_table_add_col(right, n_score, rscore_v);
    td_release(rid_v); td_release(rscore_v);

    td_graph_t* g = td_graph_new(left);
    td_op_t* left_op = td_const_table(g, left);
    td_op_t* right_op = td_const_table(g, right);
    td_op_t* lk = td_scan(g, "id");
    td_op_t* lk_arr[] = { lk };
    td_op_t* rk_arr[] = { lk };
    td_op_t* join_op = td_join(g, left_op, lk_arr, right_op, rk_arr, 1, 0);

    td_t* result = td_execute(g, join_op);
    munit_assert_false(TD_IS_ERR(result));
    munit_assert_int(result->type, ==, TD_TABLE);
    /* Each of 1000 keys has 100 left × 100 right = 10000 matches.
     * Total = 1000 × 10000 = 10,000,000 rows */
    munit_assert_int(td_table_nrows(result), ==, 10000000);
    munit_assert_true(td_table_ncols(result) >= 3);

    td_release(result);
    td_graph_free(g);
    td_release(left);
    td_release(right);
    td_sym_destroy();
    td_heap_destroy();
    return MUNIT_OK;
}
```

**Step 2: Register the test**

Add to the test suite array (around line 1166):

```c
    { "/join_large",    test_exec_join_large,    NULL, NULL, 0, NULL },
```

**Step 3: Run tests**

Run: `cd build && cmake --build . && ctest --output-on-failure -R exec`
Expected: All tests pass including `/join_large`. The 100K-row right side exceeds `TD_PARALLEL_THRESHOLD` (64K), triggering the radix path.

**Step 4: Commit**

```bash
git add test/test_exec.c
git commit -m "test(join): add large-join test exercising radix partition path"
```

---

### Task 6: Benchmark and tune

Run the H2O.ai benchmark to measure the improvement and tune if needed.

**Files:**
- No code changes expected — this is measurement + potential constant tuning in `td.h`

**Step 1: Build release**

Run: `cmake -B build_release -DCMAKE_BUILD_TYPE=Release -DTEIDE_PORTABLE=ON -DCMAKE_OSX_ARCHITECTURES=arm64 && cmake --build build_release`

**Step 2: Run benchmark**

Run: `cd ../teide-bench && source .venv/bin/activate && python bench_all.py`

Expected: j1 benchmark drops from ~175ms to < 80ms (target: ~50ms competitive with DuckDB).

**Step 3: If performance is not meeting target, tune constants**

Potential adjustments:
- `TD_JOIN_L2_TARGET`: try 128KB (more partitions, smaller HT) or 512KB (fewer partitions, larger HT)
- Prefetch in scatter/probe loops: add `__builtin_prefetch` for next entry's HT slot
- Hash function: verify `hash_row_keys` distributes well across upper bits (radix uses `h >> (32 - R)`)

**Step 4: Commit any tuning changes**

```bash
git add include/teide/td.h src/ops/exec.c
git commit -m "perf(join): tune radix join constants after benchmarking"
```

---

### Task 7: Add memory pressure sequential fallback test

Test that the graceful degradation path works when memory is tight.

**Files:**
- Modify: `test/test_exec.c`

**Step 1: Write a test with reduced pool limit**

This test verifies that when the radix path fails to allocate partition buffers, it falls back to the chained HT path and still produces correct results. We can verify correctness by comparing against the small-join test's expected behavior — the fallback should produce identical results.

```c
/* ---- JOIN FALLBACK (chained HT when radix OOM) ---- */
static MunitResult test_exec_join_fallback(const void* params, void* data) {
    (void)params; (void)data;
    td_heap_init();
    td_sym_init();

    /* Use the same small join as test_exec_join — this always uses
     * chained HT fallback since right_rows (4) < TD_PARALLEL_THRESHOLD */
    int64_t lid[] = {1, 2, 3};
    int64_t lval[] = {10, 20, 30};
    td_t* lid_v = td_vec_from_raw(TD_I64, lid, 3);
    td_t* lval_v = td_vec_from_raw(TD_I64, lval, 3);
    int64_t n_id = td_sym_intern("id", 2);
    int64_t n_val = td_sym_intern("val", 3);
    td_t* left = td_table_new(2);
    left = td_table_add_col(left, n_id, lid_v);
    left = td_table_add_col(left, n_val, lval_v);
    td_release(lid_v); td_release(lval_v);

    int64_t rid[] = {1, 2, 2, 3};
    int64_t rscore[] = {100, 200, 201, 300};
    td_t* rid_v = td_vec_from_raw(TD_I64, rid, 4);
    td_t* rscore_v = td_vec_from_raw(TD_I64, rscore, 4);
    int64_t n_score = td_sym_intern("score", 5);
    td_t* right = td_table_new(2);
    right = td_table_add_col(right, n_id, rid_v);
    right = td_table_add_col(right, n_score, rscore_v);
    td_release(rid_v); td_release(rscore_v);

    td_graph_t* g = td_graph_new(left);
    td_op_t* left_op = td_const_table(g, left);
    td_op_t* right_op = td_const_table(g, right);
    td_op_t* lk = td_scan(g, "id");
    td_op_t* lk_arr[] = { lk };
    td_op_t* rk_arr[] = { lk };
    td_op_t* join_op = td_join(g, left_op, lk_arr, right_op, rk_arr, 1, 0);

    td_t* result = td_execute(g, join_op);
    munit_assert_false(TD_IS_ERR(result));
    munit_assert_int(result->type, ==, TD_TABLE);
    munit_assert_int(td_table_nrows(result), ==, 4);
    munit_assert_true(td_table_ncols(result) >= 3);

    td_release(result);
    td_graph_free(g);
    td_release(left);
    td_release(right);
    td_sym_destroy();
    td_heap_destroy();
    return MUNIT_OK;
}
```

**Step 2: Register and run**

Add to suite array:
```c
    { "/join_fallback",  test_exec_join_fallback,  NULL, NULL, 0, NULL },
```

Run: `cd build && cmake --build . && ctest --output-on-failure -R exec`
Expected: All tests pass.

**Step 3: Commit**

```bash
git add test/test_exec.c
git commit -m "test(join): add chained HT fallback test"
```
