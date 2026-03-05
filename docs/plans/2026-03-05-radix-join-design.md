# Radix-Partitioned Hash Join — Design

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Replace the current chained hash join with a cache-efficient radix-partitioned hash join, and add out-of-core resilience through unified `td_alloc` memory management.

**Context:** Teide's `OP_JOIN` (opcode 63) already has parallel build, probe, and gather phases, but uses a chained hash table that thrashes cache on large datasets. On the H2O.ai 10M-row join benchmark (j1 — inner join on 3 keys), Teide runs at ~175ms vs DuckDB's ~52ms despite both being parallel. The gap comes from cache efficiency: DuckDB's radix-partitioned hash join keeps each partition in L2 cache during probing.

---

## Architecture

4-phase pipeline replacing the current chained hash join internals within `exec_join`:

```
  Left (probe) side          Right (build) side
       │                            │
       ▼                            ▼
  ┌─────────┐                 ┌─────────┐
  │Partition │  radix bits     │Partition │
  │  (P=2^R) │◄─── adaptive ──►│  (P=2^R) │
  └────┬─────┘                 └────┬─────┘
       │    per-partition            │
       │    ┌────────────────────────┘
       ▼    ▼
  ┌──────────────┐
  │ Build+Probe  │  one task per partition
  │ (open-addr   │  each partition fits L2
  │  hash table) │
  └──────┬───────┘
         ▼
  ┌──────────────┐
  │   Gather     │  materialize output columns
  └──────────────┘
```

**Fallback:** Right side < 64K rows (`TD_PARALLEL_THRESHOLD`) → skip partitioning, use current chained hash table. No point partitioning what already fits in cache.

---

## Phase 1: Adaptive Radix Bit Selection

Choose R so each right-side partition fits in L2 cache (~256KB):

```
R = ceil(log2(right_bytes / TD_JOIN_L2_TARGET))
R = clamp(R, TD_JOIN_MIN_RADIX, TD_JOIN_MAX_RADIX)
```

Constants:
- `TD_JOIN_L2_TARGET = 256 * 1024` (256KB, tunable)
- `TD_JOIN_MIN_RADIX = 2` (minimum 4 partitions for parallelism)
- `TD_JOIN_MAX_RADIX = 14` (maximum 16384 partitions to bound overhead)

Example for the benchmark: 10M rows × 8B × 3 keys = 240MB → R = 10, P = 1024 partitions × ~234KB each.

---

## Phase 2: Parallel Partitioning

Three-step count-scatter with pre-computed offsets (no atomics during scatter):

**Step 1 — Count (parallel):** Each thread processes its morsel range, hashes keys, extracts R radix bits, and builds a per-thread histogram `thread_counts[T][P]`.

**Step 2 — Prefix sum (sequential):** Compute global partition sizes and per-thread write offsets within each partition. Each thread gets a non-overlapping write region per partition.

**Step 3 — Scatter (parallel):** Each thread re-hashes its range and writes `{row_idx: uint32_t, hash: uint32_t}` entries (8 bytes each) into partition buffers at pre-computed offsets. Storing the full hash avoids re-hashing during build.

```
Thread 0    Thread 1    Thread 2       (T threads, P partitions)
┌────────┐ ┌────────┐ ┌────────┐
│count P0│ │count P0│ │count P0│  ─── histogram phase
│count P1│ │count P1│ │count P1│
│  ...   │ │  ...   │ │  ...   │
└────────┘ └────────┘ └────────┘
         ↓ prefix sum ↓
┌──────────────────────────────┐
│ partition 0: [T0 region][T1][T2] │  contiguous buffer per partition
│ partition 1: [T0 region][T1][T2] │  allocated via td_alloc
│  ...                             │
└──────────────────────────────┘
         ↓ scatter ↓
  each thread writes {row_idx, hash}
  to its pre-assigned region
```

Both left and right sides are partitioned using the same R radix bits from the same hash function, ensuring matching rows land in the same partition.

---

## Phase 3: Per-Partition Build + Probe

Each partition is processed independently — trivially parallel with one task per partition via `td_pool_dispatch_n`.

**Build:** Open-addressing hash table per partition. Capacity = `next_pow2(2 * right_count)`. Entries are `{hash: uint32_t, row_idx: uint32_t}` (8 bytes). Linear probing. Since the partition fits in L2, the entire build is cache-resident.

```c
typedef struct {
    uint32_t* entries;    // [hash, row_idx] pairs, open-addressing
    uint32_t  cap;        // power of 2
    uint32_t  mask;       // cap - 1
} radix_ht_t;
```

**Probe (two-pass per partition):**
1. **Count pass:** Walk left-side partition entries, probe HT, count matches. Produces `morsel_counts[]` per partition.
2. **Prefix sum:** Compute output offsets within partition.
3. **Fill pass:** Walk left-side again, write `(left_row_idx, right_row_idx)` global pairs at pre-computed offsets.

**Join types:** Inner/Left/Full outer all handled in the probe fill pass — same semantics as current code, operating on partition-local row indices that map back to global indices via the scatter buffers.

---

## Phase 4: Gather

Same as current implementation. Materialize output columns from matched `(left_idx, right_idx)` pairs. Parallel via `td_pool_dispatch` when `pair_count > TD_PARALLEL_THRESHOLD`.

---

## Memory Management — Unified through `td_alloc`

All partition buffers and hash tables use `td_alloc` / `td_free`. No separate mmap paths, no second allocator.

**Normal path:** `td_alloc(buffer_size)` → `td_data(v)` for buffer pointer → `td_free(v)` when done. Partition buffers (hundreds of KB to tens of MB) all fit within standard 32MB buddy pools at zero extra overhead.

**Pressure detection:** When `td_alloc` returns NULL:
1. Call `td_heap_gc()` to reclaim foreign blocks and release oversized pools
2. Call `td_heap_release_pages()` to MADV_DONTNEED free blocks
3. Retry `td_alloc` once
4. If still NULL → switch to sequential partition processing

**Sequential partition processing (graceful degradation):**
Instead of keeping all P partition buffers alive simultaneously, process and free one partition at a time:
- Build right-side partition HT → probe with left-side partition → emit pairs → free both buffers
- Peak memory drops from all P partitions (~80MB for 10M rows) to ~2 partitions (~500KB)

**OS-level page reclamation:** The existing `td_vm_release` (MADV_DONTNEED on Linux, MADV_FREE on macOS) on free buddy blocks is transparent to the join code. The buddy allocator already calls this via `td_heap_release_pages()`.

---

## Semi-Join Pre-Filters (Preserved)

The existing semi-join optimizations remain as pre-filters before partitioning:

- **ASP-Join** (lines 7694–7723 in exec.c): When left side is factorized and right is 2x larger, build a `TD_SEL` bitmap of left key values to skip non-matching right rows during partitioning.
- **S-Join** (lines 7742–7769): After build, extract distinct-key bitmap from right to filter left probe (single I64 key, inner join only).

These reduce the data volume before partitioning, making the radix join more efficient.

---

## Integration

**What changes:**

| File | Change |
|------|--------|
| `src/ops/exec.c` | Replace `exec_join` internals after semi-join pre-filters. Keep `OP_JOIN` opcode (63), same external interface. Current chained HT becomes the fallback for small joins. |
| `include/teide/td.h` | Add `TD_JOIN_L2_TARGET`, `TD_JOIN_MIN_RADIX`, `TD_JOIN_MAX_RADIX` constants. |

**What stays the same:**
- `OP_JOIN` opcode number (63) — no ABI change
- Join type semantics (0=inner, 1=left, 2=full outer)
- `td_join()` graph API — callers see no difference
- `OP_WINDOW_JOIN` (ASOF) — unchanged, separate code path
- `OP_WCO_JOIN` (LFTJ) — unchanged, separate code path
- Output format — same column gather producing `td_t*` table

---

## Testing

- Existing join correctness tests must continue to pass (same opcode, same semantics)
- Benchmark regression: j1 on 10M rows should be < 80ms (current: ~175ms, target: ~50ms)
- Memory pressure test: force small arena, join two tables that exceed it, verify sequential fallback works
- Partition balance test: verify radix distribution is uniform across partitions
- Edge cases: empty partitions, single-row partitions, all rows in one partition (skewed keys)

---

## Expected Outcome

The 10M-row j1 benchmark should drop from ~175ms to competitive with DuckDB's ~52ms. The radix partitioning ensures L2-cache-resident probes (the main bottleneck today), while the existing parallel dispatch infrastructure provides thread-level parallelism. Memory management stays unified through `td_alloc` with graceful degradation to sequential processing under pressure.
