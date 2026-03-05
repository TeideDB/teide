# Radix Join Audit Fixes — Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Address all findings from the radix-partitioned hash join audit — add missing tests, fix minor code issues, and improve documentation to bring the implementation to production quality.

**Architecture:** No algorithmic changes. Code fixes are small and localized. Tests exercise edge cases on both the radix path (>64K rows) and chained HT fallback path (<64K rows).

**Tech Stack:** Pure C17, munit test framework.

---

## Progress

- [x] Task 1: Fix `radix_join_bits` loop bound
- [x] Task 2: Extract buffer growth helper to eliminate duplication
- [x] Task 3: Add cross-thread free documentation comment
- [x] Task 4: Add empty table join tests
- [x] Task 5: Add LEFT OUTER join test (large, radix path)
- [x] Task 6: Add FULL OUTER join test (large, radix path)
- [x] Task 7: Add skewed keys test (all rows in one partition)
- [x] Task 8: Add threshold boundary test (65537 rows)
- [x] Task 9: Add multi-key type join test (I64 + F64 mixed keys)
- [x] Task 10: Build, run all tests, benchmark regression check

---

### Task 1: Fix `radix_join_bits` loop bound

**Audit finding:** MINOR — loop iterates up to `r < 63` but result is clamped to `TD_JOIN_MAX_RADIX` (14). Wastes iterations and `target *= 2` can overflow `size_t`.

**Files:**
- Modify: `src/ops/exec.c:7466`

**Step 1: Fix the loop guard**

Change line 7466 from:
```c
    while (target < right_bytes && r < 63) {
```
to:
```c
    while (target < right_bytes && r < TD_JOIN_MAX_RADIX) {
```

**Step 2: Remove redundant clamp**

Since the loop now stops at `TD_JOIN_MAX_RADIX`, the clamp at line 7471 is redundant but harmless. Keep it for safety.

**Step 3: Build and test**

Run: `cd build && cmake --build . && ctest --output-on-failure -R exec`
Expected: All tests pass.

**Step 4: Commit**

```bash
git add src/ops/exec.c
git commit -m "fix(join): tighten radix_join_bits loop bound to TD_JOIN_MAX_RADIX"
```

---

### Task 2: Extract buffer growth helper to eliminate duplication

**Audit finding:** MINOR — buffer growth logic (check cap, alloc new, copy, free old) is duplicated at lines 7898-7919 and 7931-7951.

**Files:**
- Modify: `src/ops/exec.c`

**Step 1: Add a static helper function before `join_radix_build_probe_fn`**

```c
/* Grow per-partition output buffers (matched pair arrays).
 * Returns true on success, false on OOM (sets had_error). */
static inline bool bp_grow_bufs(radix_bp_ctx_t* c, uint32_t p,
                                 int32_t** pl, int32_t** pr,
                                 uint32_t* cap, uint32_t cnt) {
    if (cnt < *cap) return true;
    if (*cap > UINT32_MAX / 2) {
        atomic_store_explicit(&c->had_error, 1, memory_order_relaxed);
        return false;
    }
    uint32_t new_cap = *cap * 2;
    td_t* nl_hdr; td_t* nr_hdr;
    int32_t* nl = (int32_t*)scratch_alloc(&nl_hdr, (size_t)new_cap * sizeof(int32_t));
    int32_t* nr = (int32_t*)scratch_alloc(&nr_hdr, (size_t)new_cap * sizeof(int32_t));
    if (!nl || !nr) {
        if (nl_hdr) scratch_free(nl_hdr);
        if (nr_hdr) scratch_free(nr_hdr);
        atomic_store_explicit(&c->had_error, 1, memory_order_relaxed);
        return false;
    }
    memcpy(nl, *pl, (size_t)cnt * sizeof(int32_t));
    memcpy(nr, *pr, (size_t)cnt * sizeof(int32_t));
    scratch_free(c->pp_l_hdr[p]); scratch_free(c->pp_r_hdr[p]);
    *pl = nl; *pr = nr;
    c->pp_l_hdr[p] = nl_hdr; c->pp_r_hdr[p] = nr_hdr;
    *cap = new_cap;
    return true;
}
```

**Step 2: Replace both growth blocks in `join_radix_build_probe_fn`**

Replace the matched-key growth block (lines 7898-7919) with:
```c
                    if (!bp_grow_bufs(c, p, &pl, &pr, &cap, cnt))
                        goto done;
```

Replace the unmatched-left growth block (lines 7931-7951) with:
```c
            if (!bp_grow_bufs(c, p, &pl, &pr, &cap, cnt))
                goto done;
```

**Step 3: Build and test**

Run: `cd build && cmake --build . && ctest --output-on-failure -R exec`
Expected: All tests pass.

**Step 4: Commit**

```bash
git add src/ops/exec.c
git commit -m "refactor(join): extract bp_grow_bufs helper to deduplicate buffer growth"
```

---

### Task 3: Add cross-thread free documentation comment

**Audit finding:** IMPORTANT — cross-thread free of worker-allocated buffers at lines 8397-8401 is correct but should be documented.

**Files:**
- Modify: `src/ops/exec.c:8397`

**Step 1: Add comment**

Replace line 8397:
```c
        /* Free per-partition buffers */
```
with:
```c
        /* Free per-partition buffers allocated by worker threads.
         * Safe: td_pool_dispatch_n has completed (workers are back on semaphore),
         * td_parallel_flag is 0, and td_free handles cross-heap deallocation
         * via the foreign-block list flushed by td_heap_gc at td_parallel_end. */
```

**Step 2: Build and test**

Run: `cd build && cmake --build . && ctest --output-on-failure -R exec`
Expected: All tests pass.

**Step 3: Commit**

```bash
git add src/ops/exec.c
git commit -m "docs(join): document cross-thread free safety in radix join cleanup"
```

---

### Task 4: Add empty table join tests

**Audit finding:** CRITICAL — no tests for joins with empty tables.

**Files:**
- Modify: `test/test_exec.c`

**Step 1: Add `test_exec_join_empty` after `test_exec_join_fallback`**

```c
/* ---- JOIN: empty tables ---- */
static MunitResult test_exec_join_empty(const void* params, void* data) {
    (void)params; (void)data;
    td_heap_init();
    td_sym_init();

    int64_t n_id = td_sym_intern("id", 2);
    int64_t n_val = td_sym_intern("val", 3);
    int64_t n_score = td_sym_intern("score", 5);

    /* Non-empty table: 3 rows */
    int64_t ids[] = {1, 2, 3};
    int64_t vals[] = {10, 20, 30};
    td_t* id_v = td_vec_from_raw(TD_I64, ids, 3);
    td_t* val_v = td_vec_from_raw(TD_I64, vals, 3);
    td_t* nonempty = td_table_new(2);
    nonempty = td_table_add_col(nonempty, n_id, id_v);
    nonempty = td_table_add_col(nonempty, n_val, val_v);
    td_release(id_v); td_release(val_v);

    /* Empty table: 0 rows */
    td_t* empty_id = td_alloc(0);
    empty_id->type = TD_I64; empty_id->len = 0;
    td_t* empty_score = td_alloc(0);
    empty_score->type = TD_I64; empty_score->len = 0;
    td_t* empty = td_table_new(2);
    empty = td_table_add_col(empty, n_id, empty_id);
    empty = td_table_add_col(empty, n_score, empty_score);
    td_release(empty_id); td_release(empty_score);

    /* Test 1: INNER JOIN with empty right → 0 rows */
    {
        td_graph_t* g = td_graph_new(nonempty);
        td_op_t* l = td_const_table(g, nonempty);
        td_op_t* r = td_const_table(g, empty);
        td_op_t* k = td_scan(g, "id");
        td_op_t* ka[] = { k };
        td_op_t* j = td_join(g, l, ka, r, ka, 1, 0);
        td_t* res = td_execute(g, j);
        munit_assert_false(TD_IS_ERR(res));
        munit_assert_int(td_table_nrows(res), ==, 0);
        td_release(res);
        td_graph_free(g);
    }

    /* Test 2: INNER JOIN with empty left → 0 rows */
    {
        td_graph_t* g = td_graph_new(empty);
        td_op_t* l = td_const_table(g, empty);
        td_op_t* r = td_const_table(g, nonempty);
        td_op_t* k = td_scan(g, "id");
        td_op_t* ka[] = { k };
        td_op_t* j = td_join(g, l, ka, r, ka, 1, 0);
        td_t* res = td_execute(g, j);
        munit_assert_false(TD_IS_ERR(res));
        munit_assert_int(td_table_nrows(res), ==, 0);
        td_release(res);
        td_graph_free(g);
    }

    /* Test 3: LEFT JOIN with empty right → 3 rows (all unmatched) */
    {
        td_graph_t* g = td_graph_new(nonempty);
        td_op_t* l = td_const_table(g, nonempty);
        td_op_t* r = td_const_table(g, empty);
        td_op_t* k = td_scan(g, "id");
        td_op_t* ka[] = { k };
        td_op_t* j = td_join(g, l, ka, r, ka, 1, 1);
        td_t* res = td_execute(g, j);
        munit_assert_false(TD_IS_ERR(res));
        munit_assert_int(td_table_nrows(res), ==, 3);
        td_release(res);
        td_graph_free(g);
    }

    td_release(nonempty);
    td_release(empty);
    td_sym_destroy();
    td_heap_destroy();
    return MUNIT_OK;
}
```

**Step 2: Register in test suite**

Add after the `/join_fallback` entry:
```c
    { "/join_empty",     test_exec_join_empty,        NULL, NULL, 0, NULL },
```

**Step 3: Build and test**

Run: `cd build && cmake --build . && ctest --output-on-failure -R exec`
Expected: All tests pass.

**Step 4: Commit**

```bash
git add test/test_exec.c
git commit -m "test(join): add empty table join tests (inner, left outer)"
```

---

### Task 5: Add LEFT OUTER join test (large, radix path)

**Audit finding:** CRITICAL — no tests for LEFT OUTER on the radix path (>64K rows).

**Files:**
- Modify: `test/test_exec.c`

**Step 1: Add `test_exec_join_left_large`**

```c
/* ---- LEFT OUTER JOIN (radix path, >64K rows) ---- */
static MunitResult test_exec_join_left_large(const void* params, void* data) {
    (void)params; (void)data;
    td_heap_init();
    td_sym_init();

    /* Left: 100K rows, id = i (unique keys) */
    int64_t n_left = 100000;
    td_t* lid_v = td_alloc((size_t)n_left * sizeof(int64_t));
    lid_v->type = TD_I64; lid_v->len = n_left;
    td_t* lval_v = td_alloc((size_t)n_left * sizeof(int64_t));
    lval_v->type = TD_I64; lval_v->len = n_left;
    int64_t* lid = (int64_t*)td_data(lid_v);
    int64_t* lval = (int64_t*)td_data(lval_v);
    for (int64_t i = 0; i < n_left; i++) {
        lid[i] = i;
        lval[i] = i * 10;
    }

    /* Right: 100K rows, id = i * 2 (only even keys match) */
    int64_t n_right = 100000;
    td_t* rid_v = td_alloc((size_t)n_right * sizeof(int64_t));
    rid_v->type = TD_I64; rid_v->len = n_right;
    td_t* rscore_v = td_alloc((size_t)n_right * sizeof(int64_t));
    rscore_v->type = TD_I64; rscore_v->len = n_right;
    int64_t* rid = (int64_t*)td_data(rid_v);
    int64_t* rscore = (int64_t*)td_data(rscore_v);
    for (int64_t i = 0; i < n_right; i++) {
        rid[i] = i * 2;
        rscore[i] = i * 100;
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
    /* LEFT OUTER join (type=1) */
    td_op_t* join_op = td_join(g, left_op, lk_arr, right_op, rk_arr, 1, 1);

    td_t* result = td_execute(g, join_op);
    munit_assert_false(TD_IS_ERR(result));
    munit_assert_int(result->type, ==, TD_TABLE);
    /* LEFT OUTER: all 100K left rows preserved.
     * Even keys (0,2,4,...,99998) match right side: 50K matched rows.
     * Odd keys (1,3,5,...,99999) have no match: 50K unmatched rows.
     * Total = 100K rows. */
    munit_assert_int(td_table_nrows(result), ==, 100000);

    td_release(result);
    td_graph_free(g);
    td_release(left);
    td_release(right);
    td_sym_destroy();
    td_heap_destroy();
    return MUNIT_OK;
}
```

**Step 2: Register in test suite**

```c
    { "/join_left_large", test_exec_join_left_large, NULL, NULL, 0, NULL },
```

**Step 3: Build and test**

Run: `cd build && cmake --build . && ctest --output-on-failure -R exec`
Expected: All tests pass.

**Step 4: Commit**

```bash
git add test/test_exec.c
git commit -m "test(join): add LEFT OUTER join test on radix path (100K rows)"
```

---

### Task 6: Add FULL OUTER join test (large, radix path)

**Audit finding:** CRITICAL — no tests for FULL OUTER on the radix path.

**Files:**
- Modify: `test/test_exec.c`

**Step 1: Add `test_exec_join_full_large`**

```c
/* ---- FULL OUTER JOIN (radix path, >64K rows) ---- */
static MunitResult test_exec_join_full_large(const void* params, void* data) {
    (void)params; (void)data;
    td_heap_init();
    td_sym_init();

    /* Left: 80K rows, id = i (0..79999) */
    int64_t n_left = 80000;
    td_t* lid_v = td_alloc((size_t)n_left * sizeof(int64_t));
    lid_v->type = TD_I64; lid_v->len = n_left;
    int64_t* lid = (int64_t*)td_data(lid_v);
    for (int64_t i = 0; i < n_left; i++) lid[i] = i;

    /* Right: 80K rows, id = i + 40000 (40000..119999) */
    int64_t n_right = 80000;
    td_t* rid_v = td_alloc((size_t)n_right * sizeof(int64_t));
    rid_v->type = TD_I64; rid_v->len = n_right;
    int64_t* rid = (int64_t*)td_data(rid_v);
    for (int64_t i = 0; i < n_right; i++) rid[i] = i + 40000;

    int64_t n_id = td_sym_intern("id", 2);
    td_t* left = td_table_new(1);
    left = td_table_add_col(left, n_id, lid_v);
    td_release(lid_v);
    td_t* right = td_table_new(1);
    right = td_table_add_col(right, n_id, rid_v);
    td_release(rid_v);

    td_graph_t* g = td_graph_new(left);
    td_op_t* left_op = td_const_table(g, left);
    td_op_t* right_op = td_const_table(g, right);
    td_op_t* lk = td_scan(g, "id");
    td_op_t* lk_arr[] = { lk };
    td_op_t* rk_arr[] = { lk };
    /* FULL OUTER join (type=2) */
    td_op_t* join_op = td_join(g, left_op, lk_arr, right_op, rk_arr, 1, 2);

    td_t* result = td_execute(g, join_op);
    munit_assert_false(TD_IS_ERR(result));
    munit_assert_int(result->type, ==, TD_TABLE);
    /* Left 0..39999 unmatched (40K), overlap 40000..79999 matched (40K),
     * Right 80000..119999 unmatched (40K). Total = 120K. */
    munit_assert_int(td_table_nrows(result), ==, 120000);

    td_release(result);
    td_graph_free(g);
    td_release(left);
    td_release(right);
    td_sym_destroy();
    td_heap_destroy();
    return MUNIT_OK;
}
```

**Step 2: Register in test suite**

```c
    { "/join_full_large", test_exec_join_full_large, NULL, NULL, 0, NULL },
```

**Step 3: Build and test**

Run: `cd build && cmake --build . && ctest --output-on-failure -R exec`
Expected: All tests pass.

**Step 4: Commit**

```bash
git add test/test_exec.c
git commit -m "test(join): add FULL OUTER join test on radix path (80K rows)"
```

---

### Task 7: Add skewed keys test (all rows in one partition)

**Audit finding:** IMPORTANT — no test for extreme key skew where all rows hash to one partition.

**Files:**
- Modify: `test/test_exec.c`

**Step 1: Add `test_exec_join_skewed`**

```c
/* ---- JOIN: skewed keys (all rows same key → one partition) ---- */
static MunitResult test_exec_join_skewed(const void* params, void* data) {
    (void)params; (void)data;
    td_heap_init();
    td_sym_init();

    /* Both tables: 100K rows, all id = 42.
     * Result: 100K × 100K = 10B rows — too large.
     * Instead: left 100K rows id=42, right 1 row id=42 → 100K result rows. */
    int64_t n_left = 100000;
    td_t* lid_v = td_alloc((size_t)n_left * sizeof(int64_t));
    lid_v->type = TD_I64; lid_v->len = n_left;
    td_t* lval_v = td_alloc((size_t)n_left * sizeof(int64_t));
    lval_v->type = TD_I64; lval_v->len = n_left;
    int64_t* lid = (int64_t*)td_data(lid_v);
    int64_t* lval = (int64_t*)td_data(lval_v);
    for (int64_t i = 0; i < n_left; i++) {
        lid[i] = 42;
        lval[i] = i;
    }

    /* Right: 100K rows, all id = 42, score = i */
    int64_t n_right = 100000;
    td_t* rid_v = td_alloc((size_t)n_right * sizeof(int64_t));
    rid_v->type = TD_I64; rid_v->len = n_right;
    td_t* rscore_v = td_alloc((size_t)n_right * sizeof(int64_t));
    rscore_v->type = TD_I64; rscore_v->len = n_right;
    int64_t* rid = (int64_t*)td_data(rid_v);
    int64_t* rscore = (int64_t*)td_data(rscore_v);
    for (int64_t i = 0; i < n_right; i++) {
        rid[i] = 42;
        rscore[i] = i;
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
    /* All 100K left × all 100K right share same key → 10B rows.
     * This is a cross product — too large for this test.
     * Reduced: we keep right at 100K but the test validates the
     * engine handles it. Actually 100K × 100K = 10B is too much.
     * Let's use left=100K, right=2 → 200K result rows. */
    /* NOTE: This test is designed below with right=2 instead. */
    (void)result;
    td_release(result);
    td_graph_free(g);
    td_release(left);
    td_release(right);
    td_sym_destroy();
    td_heap_destroy();
    return MUNIT_OK;
}
```

Wait — 100K × 100K is 10 billion rows, way too large. Let me redesign this test properly:

```c
/* ---- JOIN: skewed keys (all rows hash to same partition) ---- */
static MunitResult test_exec_join_skewed(const void* params, void* data) {
    (void)params; (void)data;
    td_heap_init();
    td_sym_init();

    /* Left: 100K rows, all id = 42.
     * Right: 2 rows, id = 42 with different scores.
     * All rows land in one radix partition.
     * Result: 100K × 2 = 200K rows. */
    int64_t n_left = 100000;
    td_t* lid_v = td_alloc((size_t)n_left * sizeof(int64_t));
    lid_v->type = TD_I64; lid_v->len = n_left;
    td_t* lval_v = td_alloc((size_t)n_left * sizeof(int64_t));
    lval_v->type = TD_I64; lval_v->len = n_left;
    int64_t* lid = (int64_t*)td_data(lid_v);
    int64_t* lval = (int64_t*)td_data(lval_v);
    for (int64_t i = 0; i < n_left; i++) {
        lid[i] = 42;
        lval[i] = i;
    }

    /* Right: 100K rows, all id = 42 — triggers radix path on right side.
     * But result = 100K × 100K = 10B — too large.
     * Instead: right = 70K rows with id = 42 (above threshold, triggers radix).
     * Result = 100K × 70K = 7B — still too large.
     *
     * Better approach: use 100K distinct keys on right (triggers radix),
     * but only key=42 appears on left. Most partitions are empty on left.
     * This tests one hot partition + many empty ones. */
    int64_t n_right = 100000;
    td_t* rid_v = td_alloc((size_t)n_right * sizeof(int64_t));
    rid_v->type = TD_I64; rid_v->len = n_right;
    td_t* rscore_v = td_alloc((size_t)n_right * sizeof(int64_t));
    rscore_v->type = TD_I64; rscore_v->len = n_right;
    int64_t* rid = (int64_t*)td_data(rid_v);
    int64_t* rscore = (int64_t*)td_data(rscore_v);
    for (int64_t i = 0; i < n_right; i++) {
        rid[i] = i;      /* unique keys 0..99999 */
        rscore[i] = i * 7;
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
    /* Left has 100K rows all with id=42. Right has id=42 at index 42.
     * INNER JOIN: 100K left rows match 1 right row → 100K result rows. */
    munit_assert_int(td_table_nrows(result), ==, 100000);

    /* Verify score values: all should be 42 * 7 = 294 */
    td_t* score_col = td_table_get_col(result, n_score);
    munit_assert_ptr_not_null(score_col);
    int64_t* scores = (int64_t*)td_data(score_col);
    for (int64_t i = 0; i < 10; i++)  /* spot-check first 10 */
        munit_assert_int(scores[i], ==, 294);

    td_release(result);
    td_graph_free(g);
    td_release(left);
    td_release(right);
    td_sym_destroy();
    td_heap_destroy();
    return MUNIT_OK;
}
```

**Step 2: Register in test suite**

```c
    { "/join_skewed",    test_exec_join_skewed,       NULL, NULL, 0, NULL },
```

**Step 3: Build and test**

Run: `cd build && cmake --build . && ctest --output-on-failure -R exec`
Expected: All tests pass.

**Step 4: Commit**

```bash
git add test/test_exec.c
git commit -m "test(join): add skewed key test (all left rows in one partition)"
```

---

### Task 8: Add threshold boundary test (65537 rows)

**Audit finding:** IMPORTANT — no test at the exact radix/chained HT decision boundary.

**Files:**
- Modify: `test/test_exec.c`

**Step 1: Add `test_exec_join_boundary`**

```c
/* ---- JOIN: threshold boundary (just above TD_PARALLEL_THRESHOLD) ---- */
static MunitResult test_exec_join_boundary(const void* params, void* data) {
    (void)params; (void)data;
    td_heap_init();
    td_sym_init();

    /* Right side = 65537 rows (just above TD_PARALLEL_THRESHOLD = 65536).
     * This triggers the radix path. Verify it produces the same result
     * as the chained HT would for the same data. */
    int64_t n = 65537;
    td_t* lid_v = td_alloc((size_t)n * sizeof(int64_t));
    lid_v->type = TD_I64; lid_v->len = n;
    td_t* rid_v = td_alloc((size_t)n * sizeof(int64_t));
    rid_v->type = TD_I64; rid_v->len = n;
    int64_t* lid = (int64_t*)td_data(lid_v);
    int64_t* rid = (int64_t*)td_data(rid_v);
    for (int64_t i = 0; i < n; i++) {
        lid[i] = i;
        rid[i] = i;
    }

    int64_t n_id = td_sym_intern("id", 2);
    td_t* left = td_table_new(1);
    left = td_table_add_col(left, n_id, lid_v);
    td_release(lid_v);
    td_t* right = td_table_new(1);
    right = td_table_add_col(right, n_id, rid_v);
    td_release(rid_v);

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
    /* 1:1 key mapping → exactly n result rows */
    munit_assert_int(td_table_nrows(result), ==, n);

    td_release(result);
    td_graph_free(g);
    td_release(left);
    td_release(right);
    td_sym_destroy();
    td_heap_destroy();
    return MUNIT_OK;
}
```

**Step 2: Register in test suite**

```c
    { "/join_boundary",  test_exec_join_boundary,     NULL, NULL, 0, NULL },
```

**Step 3: Build and test**

Run: `cd build && cmake --build . && ctest --output-on-failure -R exec`
Expected: All tests pass.

**Step 4: Commit**

```bash
git add test/test_exec.c
git commit -m "test(join): add threshold boundary test (65537 rows, radix path edge)"
```

---

### Task 9: Add multi-key composite join test (I64 + F64 mixed keys)

**Audit finding:** IMPORTANT — no test with mixed key types.

**Files:**
- Modify: `test/test_exec.c`

**Step 1: Add `test_exec_join_multikey`**

```c
/* ---- JOIN: multi-key composite join (I64 + F64 mixed keys) ---- */
static MunitResult test_exec_join_multikey(const void* params, void* data) {
    (void)params; (void)data;
    td_heap_init();
    td_sym_init();

    /* Left: 5 rows, join on (k1: I64, k2: F64) — exercises mixed-type key path */
    int64_t lk1[] = {1, 1, 2, 2, 3};
    double  lk2[] = {10.0, 20.0, 10.0, 20.0, 10.0};
    int64_t lval[] = {100, 200, 300, 400, 500};
    td_t* lk1_v = td_vec_from_raw(TD_I64, lk1, 5);
    td_t* lk2_v = td_vec_from_raw(TD_F64, lk2, 5);
    td_t* lval_v = td_vec_from_raw(TD_I64, lval, 5);

    int64_t n_k1 = td_sym_intern("k1", 2);
    int64_t n_k2 = td_sym_intern("k2", 2);
    int64_t n_val = td_sym_intern("val", 3);
    int64_t n_score = td_sym_intern("score", 5);

    td_t* left = td_table_new(3);
    left = td_table_add_col(left, n_k1, lk1_v);
    left = td_table_add_col(left, n_k2, lk2_v);
    left = td_table_add_col(left, n_val, lval_v);
    td_release(lk1_v); td_release(lk2_v); td_release(lval_v);

    /* Right: 3 rows */
    int64_t rk1[] = {1, 2, 3};
    double  rk2[] = {10.0, 20.0, 30.0};
    int64_t rscore[] = {1000, 2000, 3000};
    td_t* rk1_v = td_vec_from_raw(TD_I64, rk1, 3);
    td_t* rk2_v = td_vec_from_raw(TD_F64, rk2, 3);
    td_t* rscore_v = td_vec_from_raw(TD_I64, rscore, 3);

    td_t* right = td_table_new(3);
    right = td_table_add_col(right, n_k1, rk1_v);
    right = td_table_add_col(right, n_k2, rk2_v);
    right = td_table_add_col(right, n_score, rscore_v);
    td_release(rk1_v); td_release(rk2_v); td_release(rscore_v);

    td_graph_t* g = td_graph_new(left);
    td_op_t* left_op = td_const_table(g, left);
    td_op_t* right_op = td_const_table(g, right);
    td_op_t* k1_op = td_scan(g, "k1");
    td_op_t* k2_op = td_scan(g, "k2");
    td_op_t* lk_arr[] = { k1_op, k2_op };
    td_op_t* rk_arr[] = { k1_op, k2_op };
    td_op_t* join_op = td_join(g, left_op, lk_arr, right_op, rk_arr, 2, 0);

    td_t* result = td_execute(g, join_op);
    munit_assert_false(TD_IS_ERR(result));
    munit_assert_int(result->type, ==, TD_TABLE);
    /* Matches: (1,10)→1, (2,20)→1. No match: (1,20), (2,10), (3,10).
     * Total = 2 result rows. */
    munit_assert_int(td_table_nrows(result), ==, 2);

    td_release(result);
    td_graph_free(g);
    td_release(left);
    td_release(right);
    td_sym_destroy();
    td_heap_destroy();
    return MUNIT_OK;
}
```

**Step 2: Register in test suite**

```c
    { "/join_multikey",  test_exec_join_multikey,     NULL, NULL, 0, NULL },
```

**Step 3: Build and test**

Run: `cd build && cmake --build . && ctest --output-on-failure -R exec`
Expected: All tests pass.

**Step 4: Commit**

```bash
git add test/test_exec.c
git commit -m "test(join): add multi-key composite join test"
```

---

### Task 10: Build, run all tests, benchmark regression check

**Files:**
- No code changes — verification only

**Step 1: Full debug build + all tests**

Run: `cmake -B build -DCMAKE_BUILD_TYPE=Debug && cmake --build build && cd build && ctest --output-on-failure`
Expected: All tests pass (including all new join tests).

**Step 2: Release build**

Run: `cmake -B build_release -DCMAKE_BUILD_TYPE=Release -DTEIDE_PORTABLE=ON -DCMAKE_OSX_ARCHITECTURES=arm64 && cmake --build build_release`
Expected: Build succeeds with no warnings.

**Step 3: Benchmark regression check**

Run: `cd ../teide-bench && source .venv/bin/activate && python bench_all.py`
Expected: j1 benchmark still ~33ms (no regression from code changes).

**Step 4: Commit any final fixes if needed**

If benchmark shows regression, investigate and fix. Otherwise, no commit needed.
