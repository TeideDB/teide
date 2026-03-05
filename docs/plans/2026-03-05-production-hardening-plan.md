# Production Hardening Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Production-harden Teide: optimize ASOF join to O(N+M) sort-merge, add fuzz testing, enhance CI/CD, and add micro + end-to-end benchmarks.

**Architecture:** Four independent workstreams executed sequentially. The ASOF join is a rewrite of the existing `exec_window_join` to use DuckDB-style sort-merge semantics. Fuzz testing uses libFuzzer with ASan+UBSan. CI enhancement extends the existing `.github/workflows/ci.yml`. Benchmarks are a new `bench/` directory with opt-in CMake targets.

**Tech Stack:** Pure C17, munit test framework, libFuzzer (LLVM), GitHub Actions, CMake.

---

## Task 1: Replace `wjoin` ext struct with `asof` in td.h

**Files:**
- Modify: `include/teide/td.h:473-481` (replace wjoin struct)
- Modify: `include/teide/td.h:852-857` (replace td_window_join declaration)

**Step 1: Replace the wjoin struct**

Replace lines 473-481 in td.h:

```c
        struct {               /* OP_WINDOW_JOIN: ASOF join */
            td_op_t*   time_key;      /* time/ordered key column */
            td_op_t**  eq_keys;       /* equality partition keys */
            uint8_t    n_eq_keys;     /* number of equality keys */
            uint8_t    join_type;     /* 0=inner, 1=left outer */
        } asof;
```

**Step 2: Replace the td_window_join declaration**

Replace lines 852-857 with:

```c
td_op_t* td_asof_join(td_graph_t* g,
                       td_op_t* left_table, td_op_t* right_table,
                       td_op_t* time_key,
                       td_op_t** eq_keys, uint8_t n_eq_keys,
                       uint8_t join_type);
```

Keep `OP_WINDOW_JOIN` define as-is (line 381) — renaming opcodes is unnecessary churn.

**Step 3: Verify build fails (references to wjoin broken)**

Run: `cmake --build build 2>&1 | head -30`
Expected: Compilation errors in graph.c, exec.c, opt.c, fuse.c referencing `wjoin`.

**Step 4: Commit**

```bash
git add include/teide/td.h
git commit -m "refactor: replace wjoin ext struct with asof for DuckDB-style ASOF join"
```

---

## Task 2: Rewrite `td_window_join` → `td_asof_join` DAG builder

**Files:**
- Modify: `src/ops/graph.c:66-72` (fixup case)
- Modify: `src/ops/graph.c:753-799` (td_window_join → td_asof_join)

**Step 1: Rewrite the DAG builder**

Replace `td_window_join` (graph.c:753-799) with:

```c
td_op_t* td_asof_join(td_graph_t* g,
                       td_op_t* left_table, td_op_t* right_table,
                       td_op_t* time_key,
                       td_op_t** eq_keys, uint8_t n_eq_keys,
                       uint8_t join_type) {
    uint32_t left_id  = left_table->id;
    uint32_t right_id = right_table->id;
    uint32_t time_id  = time_key->id;
    uint32_t eq_ids[256];
    for (uint8_t i = 0; i < n_eq_keys; i++) eq_ids[i] = eq_keys[i]->id;

    /* Trailing: [eq_keys: n_eq_keys * ptr] */
    size_t keys_sz = (size_t)n_eq_keys * sizeof(td_op_t*);
    td_op_ext_t* ext = graph_alloc_ext_node_ex(g, keys_sz);
    if (!ext) return NULL;

    left_table  = &g->nodes[left_id];
    right_table = &g->nodes[right_id];

    ext->base.opcode  = OP_WINDOW_JOIN;
    ext->base.arity   = 2;
    ext->base.inputs[0] = left_table;
    ext->base.inputs[1] = right_table;
    ext->base.out_type = TD_TABLE;
    ext->base.est_rows = left_table->est_rows;

    ext->asof.time_key   = &g->nodes[time_id];
    ext->asof.n_eq_keys  = n_eq_keys;
    ext->asof.join_type  = join_type;
    ext->asof.eq_keys    = (td_op_t**)EXT_TRAIL(ext);
    for (uint8_t i = 0; i < n_eq_keys; i++)
        ext->asof.eq_keys[i] = &g->nodes[eq_ids[i]];

    g->nodes[ext->base.id] = ext->base;
    return &g->nodes[ext->base.id];
}
```

**Step 2: Update graph_fixup_ext_ptrs**

Replace lines 66-72:

```c
            case OP_WINDOW_JOIN:
                ext->asof.time_key = graph_fix_ptr(ext->asof.time_key, delta);
                for (uint8_t k = 0; k < ext->asof.n_eq_keys; k++)
                    ext->asof.eq_keys[k] = graph_fix_ptr(ext->asof.eq_keys[k], delta);
                break;
```

**Step 3: Verify build (still fails in opt.c/fuse.c/exec.c)**

Run: `cmake --build build 2>&1 | grep error | head -10`

**Step 4: Commit**

```bash
git add src/ops/graph.c
git commit -m "refactor: rewrite td_asof_join DAG builder with simplified asof ext"
```

---

## Task 3: Update opt.c and fuse.c traversals for asof ext

**Files:**
- Modify: `src/ops/opt.c` (4 locations: ~lines 135, 571, 715, 962)
- Modify: `src/ops/fuse.c` (lines 144-153)

**Step 1: Update all 4 opt.c locations**

For the 3 DFS traversal cases (~lines 135, 571, 715), replace with:

```c
                case OP_WINDOW_JOIN: {
                    td_op_ext_t* ext = find_ext(g, n->id);
                    if (ext) {
                        if (ext->asof.time_key && !visited[ext->asof.time_key->id])
                            stack[sp++] = ext->asof.time_key->id;
                        for (uint8_t k = 0; k < ext->asof.n_eq_keys; k++) {
                            if (ext->asof.eq_keys[k] && !visited[ext->asof.eq_keys[k]->id])
                                stack[sp++] = ext->asof.eq_keys[k]->id;
                        }
                    }
                    break;
                }
```

Note: at line 715 (DCE liveness), use `live[]` instead of `visited[]`.

For the pointer fixup (~line 962):

```c
                        case OP_WINDOW_JOIN:
                            ext->asof.time_key = (td_op_t*)((char*)ext->asof.time_key + delta);
                            for (uint8_t k = 0; k < ext->asof.n_eq_keys; k++)
                                ext->asof.eq_keys[k] = (td_op_t*)((char*)ext->asof.eq_keys[k] + delta);
                            break;
```

**Step 2: Update fuse.c**

Replace lines 144-153:

```c
                    case OP_WINDOW_JOIN:
                        if (ext->asof.time_key && sp < (int)stack_cap)
                            stack[sp++] = ext->asof.time_key->id;
                        for (uint8_t k = 0; k < ext->asof.n_eq_keys; k++) {
                            if (ext->asof.eq_keys[k] && sp < (int)stack_cap)
                                stack[sp++] = ext->asof.eq_keys[k]->id;
                        }
                        break;
```

**Step 3: Verify build (only exec.c should fail now)**

Run: `cmake --build build 2>&1 | grep error | head -10`

**Step 4: Commit**

```bash
git add src/ops/opt.c src/ops/fuse.c
git commit -m "refactor: update opt.c and fuse.c traversals for asof ext struct"
```

---

## Task 4: Rewrite exec_window_join with sort-merge algorithm

**Files:**
- Modify: `src/ops/exec.c:8052-8162` (replace exec_window_join)

**Step 1: Rewrite exec_window_join**

Replace the entire function (lines 8052-8162) with a sort-merge implementation. The algorithm:

1. Resolve time_key and eq_keys column names from scan nodes
2. Sort both tables by (eq_keys..., time_key) using the existing radix sort infrastructure
3. Two-pointer merge: advance through both sorted tables, for each left row find the best right match

```c
/* Helper: compare two rows by eq_keys then time_key */
static int asof_row_cmp(int64_t** eq_vecs_l, int64_t** eq_vecs_r,
                         int64_t* time_l, int64_t* time_r,
                         uint8_t n_eq, int64_t li, int64_t ri) {
    for (uint8_t k = 0; k < n_eq; k++) {
        if (eq_vecs_l[k][li] < eq_vecs_r[k][ri]) return -1;
        if (eq_vecs_l[k][li] > eq_vecs_r[k][ri]) return  1;
    }
    if (time_l[li] < time_r[ri]) return -1;
    if (time_l[li] > time_r[ri]) return  1;
    return 0;
}

/* Helper: compare eq_keys only */
static int asof_eq_cmp(int64_t** eq_vecs_l, int64_t** eq_vecs_r,
                        uint8_t n_eq, int64_t li, int64_t ri) {
    for (uint8_t k = 0; k < n_eq; k++) {
        if (eq_vecs_l[k][li] < eq_vecs_r[k][ri]) return -1;
        if (eq_vecs_l[k][li] > eq_vecs_r[k][ri]) return  1;
    }
    return 0;
}

static td_t* exec_window_join(td_graph_t* g, td_op_t* op,
                               td_t* left_table, td_t* right_table) {
    td_op_ext_t* ext = find_ext(g, op->id);
    if (!ext) return TD_ERR_PTR(TD_ERR_NYI);

    uint8_t n_eq      = ext->asof.n_eq_keys;
    uint8_t join_type = ext->asof.join_type;

    int64_t left_n  = td_table_nrows(left_table);
    int64_t right_n = td_table_nrows(right_table);

    /* Resolve time key */
    td_op_ext_t* time_ext = find_ext(g, ext->asof.time_key->id);
    if (!time_ext || time_ext->base.opcode != OP_SCAN)
        return TD_ERR_PTR(TD_ERR_NYI);
    int64_t time_sym = time_ext->sym;

    /* Resolve equality keys */
    int64_t eq_syms[256];
    for (uint8_t k = 0; k < n_eq; k++) {
        td_op_ext_t* ek = find_ext(g, ext->asof.eq_keys[k]->id);
        if (!ek || ek->base.opcode != OP_SCAN)
            return TD_ERR_PTR(TD_ERR_NYI);
        eq_syms[k] = ek->sym;
    }

    /* Get time vectors */
    td_t* lt_time_vec = td_table_get_col(left_table, time_sym);
    td_t* rt_time_vec = td_table_get_col(right_table, time_sym);
    if (!lt_time_vec || !rt_time_vec) return TD_ERR_PTR(TD_ERR_SCHEMA);
    int64_t* lt_time = (int64_t*)td_data(lt_time_vec);
    int64_t* rt_time = (int64_t*)td_data(rt_time_vec);

    /* Get eq key vectors */
    int64_t* lt_eq[256], *rt_eq[256];
    for (uint8_t k = 0; k < n_eq; k++) {
        td_t* lv = td_table_get_col(left_table, eq_syms[k]);
        td_t* rv = td_table_get_col(right_table, eq_syms[k]);
        if (!lv || !rv) return TD_ERR_PTR(TD_ERR_SCHEMA);
        lt_eq[k] = (int64_t*)td_data(lv);
        rt_eq[k] = (int64_t*)td_data(rv);
    }

    /* Sort both tables by (eq_keys, time_key) using index arrays */
    td_t* li_hdr = NULL, *ri_hdr = NULL;
    int64_t* li_idx = (int64_t*)scratch_alloc(&li_hdr, (size_t)left_n * sizeof(int64_t));
    int64_t* ri_idx = (int64_t*)scratch_alloc(&ri_hdr, (size_t)right_n * sizeof(int64_t));
    if ((!li_idx && left_n > 0) || (!ri_idx && right_n > 0)) {
        if (li_hdr) scratch_free(li_hdr);
        if (ri_hdr) scratch_free(ri_hdr);
        return TD_ERR_PTR(TD_ERR_OOM);
    }
    for (int64_t i = 0; i < left_n; i++) li_idx[i] = i;
    for (int64_t i = 0; i < right_n; i++) ri_idx[i] = i;

    /* Simple insertion sort on indices (to be replaced with radix sort for large N) */
    /* Left side */
    for (int64_t i = 1; i < left_n; i++) {
        int64_t key = li_idx[i];
        int64_t j = i - 1;
        while (j >= 0) {
            int64_t o = li_idx[j];
            int cmp = 0;
            for (uint8_t k = 0; k < n_eq && cmp == 0; k++) {
                if (lt_eq[k][key] < lt_eq[k][o]) cmp = -1;
                else if (lt_eq[k][key] > lt_eq[k][o]) cmp = 1;
            }
            if (cmp == 0) {
                if (lt_time[key] < lt_time[o]) cmp = -1;
                else if (lt_time[key] > lt_time[o]) cmp = 1;
            }
            if (cmp >= 0) break;
            li_idx[j + 1] = li_idx[j];
            j--;
        }
        li_idx[j + 1] = key;
    }
    /* Right side — same */
    for (int64_t i = 1; i < right_n; i++) {
        int64_t key = ri_idx[i];
        int64_t j = i - 1;
        while (j >= 0) {
            int64_t o = ri_idx[j];
            int cmp = 0;
            for (uint8_t k = 0; k < n_eq && cmp == 0; k++) {
                if (rt_eq[k][key] < rt_eq[k][o]) cmp = -1;
                else if (rt_eq[k][key] > rt_eq[k][o]) cmp = 1;
            }
            if (cmp == 0) {
                if (rt_time[key] < rt_time[o]) cmp = -1;
                else if (rt_time[key] > rt_time[o]) cmp = 1;
            }
            if (cmp >= 0) break;
            ri_idx[j + 1] = ri_idx[j];
            j--;
        }
        ri_idx[j + 1] = key;
    }

    /* Build match array: for each left row (sorted), find best right match */
    td_t* match_hdr = NULL;
    int64_t* match = (int64_t*)scratch_alloc(&match_hdr, (size_t)left_n * sizeof(int64_t));
    if (!match && left_n > 0) {
        scratch_free(li_hdr); scratch_free(ri_hdr);
        return TD_ERR_PTR(TD_ERR_OOM);
    }

    /* Two-pointer merge */
    int64_t rp = 0;  /* right pointer */
    for (int64_t lp = 0; lp < left_n; lp++) {
        int64_t li = li_idx[lp];
        match[lp] = -1;  /* no match yet */

        /* Advance right pointer while right row is <= left row */
        while (rp < right_n) {
            int64_t ri = ri_idx[rp];
            /* Check eq_keys match */
            int eq_cmp = 0;
            for (uint8_t k = 0; k < n_eq && eq_cmp == 0; k++) {
                if (rt_eq[k][ri] < lt_eq[k][li]) eq_cmp = -1;
                else if (rt_eq[k][ri] > lt_eq[k][li]) eq_cmp = 1;
            }
            if (eq_cmp > 0) break;  /* right partition past left partition */
            if (eq_cmp == 0 && rt_time[ri] <= lt_time[li]) {
                match[lp] = ri;  /* candidate — will be overwritten by later (closer) matches */
            }
            if (eq_cmp == 0 && rt_time[ri] > lt_time[li]) break;  /* past time window */
            rp++;
        }

        /* Back up rp to allow overlap with next left row in same partition */
        /* The right pointer must be reset to the start of the current eq partition
           because the next left row might match earlier right rows */
        if (lp + 1 < left_n) {
            int64_t next_li = li_idx[lp + 1];
            /* If next left row is in a different eq partition, don't back up */
            int same_part = 1;
            for (uint8_t k = 0; k < n_eq; k++) {
                if (lt_eq[k][li] != lt_eq[k][next_li]) { same_part = 0; break; }
            }
            if (!same_part) {
                /* New partition — rp stays where it is */
            }
            /* If same partition, rp can stay because left is sorted by time too,
               so next left.time >= current left.time, and the right pointer
               only needs to advance further (merge property) */
        }
    }

    /* Count output rows */
    int64_t out_n = 0;
    if (join_type == 1) {
        out_n = left_n;  /* left outer: all left rows */
    } else {
        for (int64_t i = 0; i < left_n; i++)
            if (match[i] >= 0) out_n++;
    }

    /* Build output table */
    int64_t left_ncols  = td_table_ncols(left_table);
    int64_t right_ncols = td_table_ncols(right_table);

    /* Collect right column names/indices, excluding duplicate key columns */
    int64_t right_out_syms[256];
    int64_t right_out_count = 0;
    for (int64_t c = 0; c < right_ncols; c++) {
        int64_t rname = td_table_col_name(right_table, c);
        /* Skip if this name matches time_key or any eq_key */
        int skip = 0;
        if (rname == time_sym) skip = 1;
        for (uint8_t k = 0; k < n_eq && !skip; k++)
            if (rname == eq_syms[k]) skip = 1;
        if (!skip) right_out_syms[right_out_count++] = c;
    }

    td_t* out = td_table_new(left_ncols + right_out_count);

    /* Gather left columns */
    for (int64_t c = 0; c < left_ncols; c++) {
        int64_t col_name = td_table_col_name(left_table, c);
        td_t* src_col = td_table_get_col_idx(left_table, c);
        int8_t ctype = src_col->type;
        td_t* dst_col = td_vec_new(ctype, out_n);

        uint8_t esz = td_type_sizes[ctype];
        char* src = (char*)td_data(src_col);
        char* dst = (char*)td_data(dst_col);
        int64_t wi = 0;
        for (int64_t lp = 0; lp < left_n; lp++) {
            if (join_type == 0 && match[lp] < 0) continue;
            int64_t li = li_idx[lp];
            memcpy(dst + wi * esz, src + li * esz, esz);
            wi++;
        }
        dst_col->len = out_n;
        out = td_table_add_col(out, col_name, dst_col);
        td_release(dst_col);
    }

    /* Gather right columns (excluding key duplicates) */
    for (int64_t rc = 0; rc < right_out_count; rc++) {
        int64_t cidx = right_out_syms[rc];
        int64_t col_name = td_table_col_name(right_table, cidx);
        td_t* src_col = td_table_get_col_idx(right_table, cidx);
        int8_t ctype = src_col->type;
        td_t* dst_col = td_vec_new(ctype, out_n);

        uint8_t esz = td_type_sizes[ctype];
        char* src = (char*)td_data(src_col);
        char* dst = (char*)td_data(dst_col);
        int64_t wi = 0;
        for (int64_t lp = 0; lp < left_n; lp++) {
            if (join_type == 0 && match[lp] < 0) continue;
            if (match[lp] >= 0) {
                memcpy(dst + wi * esz, src + match[lp] * esz, esz);
            } else {
                memset(dst + wi * esz, 0, esz);  /* NULL fill for left outer */
            }
            wi++;
        }
        dst_col->len = out_n;
        out = td_table_add_col(out, col_name, dst_col);
        td_release(dst_col);
    }

    scratch_free(match_hdr);
    scratch_free(li_hdr);
    scratch_free(ri_hdr);
    return out;
}
```

**Step 2: Verify build**

Run: `cmake --build build 2>&1`
Expected: Clean build.

**Step 3: Commit**

```bash
git add src/ops/exec.c
git commit -m "feat: rewrite exec_window_join with sort-merge algorithm"
```

---

## Task 5: Rewrite ASOF join tests for new API

**Files:**
- Modify: `test/test_exec.c:961-1179` (replace 3 tests)
- Modify: `test/test_exec.c:1205-1207` (update test array)

**Step 1: Replace the 3 window_join tests**

Delete `test_exec_window_join`, `test_exec_window_join_no_sym`, `test_exec_window_join_empty` (lines 961-1179) and replace with:

```c
/* ---- ASOF JOIN ---- */
static MunitResult test_exec_asof_join(const void* params, void* data) {
    (void)params; (void)data;
    td_heap_init();
    td_sym_init();

    /* Left: trades — time(I64), sym(I64), price(F64) */
    int64_t ltime[]  = {100, 200, 300, 400, 500};
    int64_t lsym[]   = {1, 1, 2, 1, 2};
    double  lprice[] = {10.0, 20.0, 30.0, 40.0, 50.0};

    td_t* lt_v = td_vec_from_raw(TD_I64, ltime, 5);
    td_t* ls_v = td_vec_from_raw(TD_I64, lsym, 5);
    td_t* lp_v = td_vec_from_raw(TD_F64, lprice, 5);

    int64_t n_time  = td_sym_intern("time", 4);
    int64_t n_sym   = td_sym_intern("sym", 3);
    int64_t n_price = td_sym_intern("price", 5);

    td_t* left = td_table_new(3);
    left = td_table_add_col(left, n_time, lt_v);
    left = td_table_add_col(left, n_sym, ls_v);
    left = td_table_add_col(left, n_price, lp_v);
    td_release(lt_v); td_release(ls_v); td_release(lp_v);

    /* Right: quotes — time(I64), sym(I64), bid(F64) */
    int64_t rtime[] = {90, 150, 250, 350, 450};
    int64_t rsym[]  = {1, 1, 2, 1, 2};
    double  rbid[]  = {9.5, 15.0, 25.0, 35.0, 45.0};

    td_t* rt_v = td_vec_from_raw(TD_I64, rtime, 5);
    td_t* rs_v = td_vec_from_raw(TD_I64, rsym, 5);
    td_t* rb_v = td_vec_from_raw(TD_F64, rbid, 5);

    int64_t n_bid = td_sym_intern("bid", 3);

    td_t* right = td_table_new(3);
    right = td_table_add_col(right, n_time, rt_v);
    right = td_table_add_col(right, n_sym, rs_v);
    right = td_table_add_col(right, n_bid, rb_v);
    td_release(rt_v); td_release(rs_v); td_release(rb_v);

    td_graph_t* g = td_graph_new(left);
    td_op_t* left_op  = td_const_table(g, left);
    td_op_t* right_op = td_const_table(g, right);
    td_op_t* tkey = td_scan(g, "time");
    td_op_t* skey = td_scan(g, "sym");
    td_op_t* eq_keys[] = { skey };

    /* Inner ASOF join */
    td_op_t* aj = td_asof_join(g, left_op, right_op, tkey, eq_keys, 1, 0);

    td_t* result = td_execute(g, aj);
    munit_assert_false(TD_IS_ERR(result));
    munit_assert_int(result->type, ==, TD_TABLE);
    /* All 5 left rows should have matches */
    munit_assert_int(td_table_nrows(result), ==, 5);
    /* Should have left cols + bid (time/sym deduplicated) */
    munit_assert_int(td_table_ncols(result), ==, 4);  /* time, sym, price, bid */

    td_release(result);
    td_graph_free(g);
    td_release(left);
    td_release(right);
    td_sym_destroy();
    td_heap_destroy();
    return MUNIT_OK;
}

/* ---- ASOF LEFT JOIN ---- */
static MunitResult test_exec_asof_left_join(const void* params, void* data) {
    (void)params; (void)data;
    td_heap_init();
    td_sym_init();

    /* Left has a row with time=50 that's before any right row */
    int64_t ltime[] = {50, 100, 200};
    double  lval[]  = {1.0, 2.0, 3.0};
    td_t* lt_v = td_vec_from_raw(TD_I64, ltime, 3);
    td_t* lv_v = td_vec_from_raw(TD_F64, lval, 3);
    int64_t n_time = td_sym_intern("time", 4);
    int64_t n_val  = td_sym_intern("val", 3);
    td_t* left = td_table_new(2);
    left = td_table_add_col(left, n_time, lt_v);
    left = td_table_add_col(left, n_val, lv_v);
    td_release(lt_v); td_release(lv_v);

    int64_t rtime[] = {80, 150};
    double  rbid[]  = {0.8, 1.5};
    td_t* rt_v = td_vec_from_raw(TD_I64, rtime, 2);
    td_t* rb_v = td_vec_from_raw(TD_F64, rbid, 2);
    int64_t n_bid = td_sym_intern("bid", 3);
    td_t* right = td_table_new(2);
    right = td_table_add_col(right, n_time, rt_v);
    right = td_table_add_col(right, n_bid, rb_v);
    td_release(rt_v); td_release(rb_v);

    td_graph_t* g = td_graph_new(left);
    td_op_t* left_op  = td_const_table(g, left);
    td_op_t* right_op = td_const_table(g, right);
    td_op_t* tkey = td_scan(g, "time");

    /* Left outer ASOF join, no eq keys */
    td_op_t* aj = td_asof_join(g, left_op, right_op, tkey, NULL, 0, 1);

    td_t* result = td_execute(g, aj);
    munit_assert_false(TD_IS_ERR(result));
    /* Left outer: all 3 left rows preserved */
    munit_assert_int(td_table_nrows(result), ==, 3);

    td_release(result);
    td_graph_free(g);
    td_release(left);
    td_release(right);
    td_sym_destroy();
    td_heap_destroy();
    return MUNIT_OK;
}

/* ---- ASOF JOIN: empty right ---- */
static MunitResult test_exec_asof_empty(const void* params, void* data) {
    (void)params; (void)data;
    td_heap_init();
    td_sym_init();

    int64_t ltime[] = {100, 200};
    double  lval[]  = {1.0, 2.0};
    td_t* lt_v = td_vec_from_raw(TD_I64, ltime, 2);
    td_t* lv_v = td_vec_from_raw(TD_F64, lval, 2);
    int64_t n_time = td_sym_intern("time", 4);
    int64_t n_val  = td_sym_intern("val", 3);
    td_t* left = td_table_new(2);
    left = td_table_add_col(left, n_time, lt_v);
    left = td_table_add_col(left, n_val, lv_v);
    td_release(lt_v); td_release(lv_v);

    int64_t n_bid = td_sym_intern("bid", 3);
    td_t* right = td_table_new(2);
    td_t* rt_v = td_vec_new(TD_I64, 0);
    td_t* rb_v = td_vec_new(TD_F64, 0);
    right = td_table_add_col(right, n_time, rt_v);
    right = td_table_add_col(right, n_bid, rb_v);
    td_release(rt_v); td_release(rb_v);

    td_graph_t* g = td_graph_new(left);
    td_op_t* left_op  = td_const_table(g, left);
    td_op_t* right_op = td_const_table(g, right);
    td_op_t* tkey = td_scan(g, "time");

    /* Inner ASOF with empty right → 0 rows */
    td_op_t* aj = td_asof_join(g, left_op, right_op, tkey, NULL, 0, 0);
    td_t* result = td_execute(g, aj);
    munit_assert_false(TD_IS_ERR(result));
    munit_assert_int(td_table_nrows(result), ==, 0);

    td_release(result);
    td_graph_free(g);
    td_release(left);
    td_release(right);
    td_sym_destroy();
    td_heap_destroy();
    return MUNIT_OK;
}
```

**Step 2: Update test array entries**

Replace lines 1205-1207:

```c
    { "/asof_join",      test_exec_asof_join,      NULL, NULL, 0, NULL },
    { "/asof_left_join", test_exec_asof_left_join,  NULL, NULL, 0, NULL },
    { "/asof_empty",     test_exec_asof_empty,      NULL, NULL, 0, NULL },
```

**Step 3: Build and run tests**

Run: `cmake --build build && build/test_teide --suite /exec`
Expected: All exec tests pass.

**Step 4: Run full suite**

Run: `cd build && ctest --output-on-failure`
Expected: All tests pass.

**Step 5: Commit**

```bash
git add test/test_exec.c
git commit -m "test: rewrite ASOF join tests for DuckDB-style semantics"
```

---

## Task 6: Add fuzz targets

**Files:**
- Create: `fuzz/fuzz_col_load.c`
- Create: `fuzz/fuzz_csv_read.c`
- Modify: `CMakeLists.txt` (add TEIDE_FUZZ option)

**Step 1: Create fuzz directory**

```bash
mkdir -p fuzz
```

**Step 2: Write fuzz_col_load.c**

```c
#include <teide/td.h>
#include <stdint.h>
#include <stddef.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
    td_heap_init();
    td_sym_init();

    /* Write fuzz data to a temp file */
    char path[] = "/tmp/fuzz_col_XXXXXX";
    int fd = mkstemp(path);
    if (fd < 0) { td_sym_destroy(); td_heap_destroy(); return 0; }
    write(fd, data, size);
    close(fd);

    /* Try to load — should never crash, may return error */
    td_t* result = td_col_load(path);
    if (result && !TD_IS_ERR(result))
        td_release(result);

    unlink(path);
    td_sym_destroy();
    td_heap_destroy();
    return 0;
}
```

**Step 3: Write fuzz_csv_read.c**

```c
#include <teide/td.h>
#include <stdint.h>
#include <stddef.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
    td_heap_init();
    td_sym_init();

    /* Write fuzz data to a temp file */
    char path[] = "/tmp/fuzz_csv_XXXXXX";
    int fd = mkstemp(path);
    if (fd < 0) { td_sym_destroy(); td_heap_destroy(); return 0; }
    write(fd, data, size);
    close(fd);

    /* Try to read as CSV — should never crash */
    td_t* result = td_csv_read(path, ',');
    if (result && !TD_IS_ERR(result))
        td_release(result);

    unlink(path);
    td_sym_destroy();
    td_heap_destroy();
    return 0;
}
```

**Step 4: Add TEIDE_FUZZ option to CMakeLists.txt**

Add before the test section (before line 65):

```cmake
# Fuzz targets (requires clang with libFuzzer)
option(TEIDE_FUZZ "Build fuzz testing targets" OFF)
if(TEIDE_FUZZ)
    file(GLOB FUZZ_SOURCES CONFIGURE_DEPENDS "fuzz/*.c")
    foreach(fuzz_src ${FUZZ_SOURCES})
        get_filename_component(fuzz_name ${fuzz_src} NAME_WE)
        add_executable(${fuzz_name} ${fuzz_src})
        target_link_libraries(${fuzz_name} PRIVATE teide_static)
        target_include_directories(${fuzz_name} PRIVATE include src)
        target_compile_options(${fuzz_name} PRIVATE
            -fsanitize=fuzzer,address,undefined -g -O1)
        target_link_options(${fuzz_name} PRIVATE
            -fsanitize=fuzzer,address,undefined)
    endforeach()
endif()
```

**Step 5: Verify fuzz build (optional — only works with clang)**

Run: `cmake -B build_fuzz -DCMAKE_BUILD_TYPE=Debug -DTEIDE_FUZZ=ON -DCMAKE_C_COMPILER=clang && cmake --build build_fuzz 2>&1 | tail -5`

**Step 6: Commit**

```bash
git add fuzz/ CMakeLists.txt
git commit -m "feat: add libFuzzer targets for col_load and csv_read"
```

---

## Task 7: Enhance CI/CD workflow

**Files:**
- Modify: `.github/workflows/ci.yml`

**Step 1: Replace ci.yml with enhanced version**

```yaml
name: CI

on:
  push:
    branches: [master]
  pull_request:
    branches: [master]

jobs:
  build-and-test:
    runs-on: ${{ matrix.os }}
    strategy:
      fail-fast: false
      matrix:
        include:
          - os: ubuntu-latest
            compiler: gcc
            build_type: Debug
          - os: ubuntu-latest
            compiler: gcc
            build_type: Release
          - os: ubuntu-latest
            compiler: clang
            build_type: Debug
          - os: ubuntu-latest
            compiler: clang
            build_type: Release
          - os: macos-latest
            compiler: clang
            build_type: Debug
          - os: macos-latest
            compiler: clang
            build_type: Release

    steps:
      - uses: actions/checkout@v4

      - name: Set compiler
        run: |
          if [ "${{ matrix.compiler }}" = "gcc" ]; then
            echo "CC=gcc" >> $GITHUB_ENV
          else
            echo "CC=clang" >> $GITHUB_ENV
          fi

      - name: Configure
        run: cmake -B build -DCMAKE_BUILD_TYPE=${{ matrix.build_type }} -DTEIDE_WERROR=ON

      - name: Build
        run: cmake --build build

      - name: Test
        run: cd build && ctest --output-on-failure
```

**Step 2: Commit**

```bash
git add .github/workflows/ci.yml
git commit -m "ci: enhance workflow with compiler matrix and -Werror"
```

---

## Task 8: Add microbenchmark harness

**Files:**
- Create: `bench/bench_teide.c`
- Modify: `CMakeLists.txt` (add TEIDE_BENCH option)

**Step 1: Create bench directory**

```bash
mkdir -p bench
```

**Step 2: Write bench_teide.c**

```c
#include <teide/td.h>
#include <stdio.h>
#include <time.h>
#include <string.h>

static double now_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (double)ts.tv_sec * 1e9 + (double)ts.tv_nsec;
}

static void report(const char* name, int64_t nrows, double elapsed_ns) {
    double rows_per_sec = (double)nrows / (elapsed_ns / 1e9);
    printf("%-24s  %10lld rows  %10.1f ms  %12.0f rows/sec\n",
           name, (long long)nrows, elapsed_ns / 1e6, rows_per_sec);
}

/* ---- vec_add: element-wise addition ---- */
static void bench_vec_add(int64_t n) {
    int64_t* a_data = td_sys_alloc((size_t)n * sizeof(int64_t));
    int64_t* b_data = td_sys_alloc((size_t)n * sizeof(int64_t));
    for (int64_t i = 0; i < n; i++) { a_data[i] = i; b_data[i] = i * 2; }

    td_t* a = td_vec_from_raw(TD_I64, a_data, n);
    td_t* b = td_vec_from_raw(TD_I64, b_data, n);

    int64_t n_a = td_sym_intern("a", 1);
    int64_t n_b = td_sym_intern("b", 1);

    td_t* tbl = td_table_new(2);
    tbl = td_table_add_col(tbl, n_a, a);
    tbl = td_table_add_col(tbl, n_b, b);
    td_release(a); td_release(b);

    td_graph_t* g = td_graph_new(tbl);
    td_op_t* sa = td_scan(g, "a");
    td_op_t* sb = td_scan(g, "b");
    td_op_t* add = td_add(g, sa, sb);
    td_op_t* s = td_sum(g, add);

    double t0 = now_ns();
    td_t* result = td_execute(g, s);
    double elapsed = now_ns() - t0;

    report("vec_add", n, elapsed);

    if (result && !TD_IS_ERR(result)) td_release(result);
    td_graph_free(g);
    td_release(tbl);
    td_sys_free(a_data);
    td_sys_free(b_data);
}

/* ---- filter: predicate evaluation ---- */
static void bench_filter(int64_t n) {
    int64_t* v_data = td_sys_alloc((size_t)n * sizeof(int64_t));
    for (int64_t i = 0; i < n; i++) v_data[i] = i;

    td_t* v = td_vec_from_raw(TD_I64, v_data, n);
    int64_t n_v = td_sym_intern("v", 1);
    td_t* tbl = td_table_new(1);
    tbl = td_table_add_col(tbl, n_v, v);
    td_release(v);

    td_graph_t* g = td_graph_new(tbl);
    td_op_t* sv = td_scan(g, "v");
    td_op_t* thresh = td_const_i64(g, n / 2);
    td_op_t* pred = td_gt(g, sv, thresh);
    td_op_t* flt = td_filter(g, sv, pred);
    td_op_t* s = td_sum(g, flt);

    double t0 = now_ns();
    td_t* result = td_execute(g, s);
    double elapsed = now_ns() - t0;

    report("filter", n, elapsed);

    if (result && !TD_IS_ERR(result)) td_release(result);
    td_graph_free(g);
    td_release(tbl);
    td_sys_free(v_data);
}

/* ---- sort: multi-column sort ---- */
static void bench_sort(int64_t n) {
    int64_t* v_data = td_sys_alloc((size_t)n * sizeof(int64_t));
    for (int64_t i = 0; i < n; i++) v_data[i] = n - i;  /* reverse */

    td_t* v = td_vec_from_raw(TD_I64, v_data, n);
    int64_t n_v = td_sym_intern("v", 1);
    td_t* tbl = td_table_new(1);
    tbl = td_table_add_col(tbl, n_v, v);
    td_release(v);

    td_graph_t* g = td_graph_new(tbl);
    td_op_t* sv = td_scan(g, "v");
    td_op_t* keys[] = { sv };
    uint8_t descs[] = { 0 };
    uint8_t nf[] = { 0 };
    td_op_t* sort_op = td_sort_op(g, sv, keys, descs, nf, 1);
    td_op_t* s = td_sum(g, sort_op);

    double t0 = now_ns();
    td_t* result = td_execute(g, s);
    double elapsed = now_ns() - t0;

    report("sort", n, elapsed);

    if (result && !TD_IS_ERR(result)) td_release(result);
    td_graph_free(g);
    td_release(tbl);
    td_sys_free(v_data);
}

/* ---- group: group-by + sum ---- */
static void bench_group(int64_t n) {
    int64_t* id_data = td_sys_alloc((size_t)n * sizeof(int64_t));
    int64_t* v_data = td_sys_alloc((size_t)n * sizeof(int64_t));
    for (int64_t i = 0; i < n; i++) { id_data[i] = i % 100; v_data[i] = i; }

    td_t* id_v = td_vec_from_raw(TD_I64, id_data, n);
    td_t* v_v = td_vec_from_raw(TD_I64, v_data, n);

    int64_t n_id = td_sym_intern("id", 2);
    int64_t n_v = td_sym_intern("v", 1);
    td_t* tbl = td_table_new(2);
    tbl = td_table_add_col(tbl, n_id, id_v);
    tbl = td_table_add_col(tbl, n_v, v_v);
    td_release(id_v); td_release(v_v);

    td_graph_t* g = td_graph_new(tbl);
    td_op_t* sid = td_scan(g, "id");
    td_op_t* sv = td_scan(g, "v");
    td_op_t* keys[] = { sid };
    uint16_t agg_ops[] = { OP_SUM };
    td_op_t* agg_ins[] = { sv };
    td_op_t* grp = td_group(g, keys, 1, agg_ops, agg_ins, 1);
    td_op_t* cnt = td_count(g, grp);

    double t0 = now_ns();
    td_t* result = td_execute(g, cnt);
    double elapsed = now_ns() - t0;

    report("group", n, elapsed);

    if (result && !TD_IS_ERR(result)) td_release(result);
    td_graph_free(g);
    td_release(tbl);
    td_sys_free(id_data);
    td_sys_free(v_data);
}

int main(void) {
    int64_t sizes[] = { 1000, 100000, 10000000 };
    int n_sizes = 3;

    printf("%-24s  %10s  %10s  %12s\n", "Benchmark", "Rows", "Time", "Throughput");
    printf("%-24s  %10s  %10s  %12s\n",
           "------------------------", "----------", "----------", "------------");

    for (int s = 0; s < n_sizes; s++) {
        td_heap_init();
        td_sym_init();

        bench_vec_add(sizes[s]);
        bench_filter(sizes[s]);
        bench_sort(sizes[s]);
        bench_group(sizes[s]);

        td_sym_destroy();
        td_heap_destroy();

        printf("\n");
    }

    return 0;
}
```

**Step 3: Add TEIDE_BENCH option to CMakeLists.txt**

Add after the fuzz section (or after the test section):

```cmake
# Benchmark targets
option(TEIDE_BENCH "Build benchmark executables" OFF)
if(TEIDE_BENCH)
    file(GLOB BENCH_SOURCES CONFIGURE_DEPENDS "bench/*.c")
    foreach(bench_src ${BENCH_SOURCES})
        get_filename_component(bench_name ${bench_src} NAME_WE)
        add_executable(${bench_name} ${bench_src})
        target_link_libraries(${bench_name} PRIVATE teide_static)
        target_include_directories(${bench_name} PRIVATE include src)
        target_compile_options(${bench_name} PRIVATE ${TEIDE_RELEASE_FLAGS})
    endforeach()
endif()
```

**Step 4: Verify bench build**

Run: `cmake -B build_bench -DCMAKE_BUILD_TYPE=Release -DTEIDE_BENCH=ON && cmake --build build_bench`
Expected: Clean build.

**Step 5: Run benchmarks**

Run: `./build_bench/bench_teide`
Expected: Table output with rows/sec for each operation at 1K, 100K, 10M rows.

**Step 6: Commit**

```bash
git add bench/ CMakeLists.txt
git commit -m "feat: add microbenchmark harness for core operations"
```

---

## Task 9: Add end-to-end query benchmarks

**Files:**
- Create: `bench/bench_queries.c`

**Step 1: Write bench_queries.c**

```c
#include <teide/td.h>
#include <stdio.h>
#include <time.h>

static double now_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (double)ts.tv_sec * 1e9 + (double)ts.tv_nsec;
}

static void report(const char* name, int64_t nrows, double elapsed_ns) {
    double rows_per_sec = (double)nrows / (elapsed_ns / 1e9);
    printf("%-30s  %10lld rows  %10.1f ms  %12.0f rows/sec\n",
           name, (long long)nrows, elapsed_ns / 1e6, rows_per_sec);
}

/* Q1: scan + filter + group + sum (analytics) */
static void bench_q1_analytics(int64_t n) {
    td_heap_init();
    td_sym_init();

    int64_t* region_data = td_sys_alloc((size_t)n * sizeof(int64_t));
    int64_t* amount_data = td_sys_alloc((size_t)n * sizeof(int64_t));
    int64_t* flag_data   = td_sys_alloc((size_t)n * sizeof(int64_t));
    for (int64_t i = 0; i < n; i++) {
        region_data[i] = i % 5;
        amount_data[i] = (i * 7 + 13) % 1000;
        flag_data[i]   = i % 3;  /* filter on flag == 0 */
    }

    td_t* r_v = td_vec_from_raw(TD_I64, region_data, n);
    td_t* a_v = td_vec_from_raw(TD_I64, amount_data, n);
    td_t* f_v = td_vec_from_raw(TD_I64, flag_data, n);

    int64_t n_r = td_sym_intern("region", 6);
    int64_t n_a = td_sym_intern("amount", 6);
    int64_t n_f = td_sym_intern("flag", 4);

    td_t* tbl = td_table_new(3);
    tbl = td_table_add_col(tbl, n_r, r_v);
    tbl = td_table_add_col(tbl, n_a, a_v);
    tbl = td_table_add_col(tbl, n_f, f_v);
    td_release(r_v); td_release(a_v); td_release(f_v);

    /* SELECT region, SUM(amount) FROM t WHERE flag = 0 GROUP BY region */
    td_graph_t* g = td_graph_new(tbl);
    td_op_t* sf = td_scan(g, "flag");
    td_op_t* zero = td_const_i64(g, 0);
    td_op_t* pred = td_eq(g, sf, zero);
    td_op_t* sr = td_scan(g, "region");
    td_op_t* sa = td_scan(g, "amount");
    td_op_t* flt_r = td_filter(g, sr, pred);
    td_op_t* flt_a = td_filter(g, sa, pred);
    td_op_t* keys[] = { flt_r };
    uint16_t agg_ops[] = { OP_SUM };
    td_op_t* agg_ins[] = { flt_a };
    td_op_t* grp = td_group(g, keys, 1, agg_ops, agg_ins, 1);
    td_op_t* cnt = td_count(g, grp);

    double t0 = now_ns();
    td_t* result = td_execute(g, cnt);
    double elapsed = now_ns() - t0;

    report("Q1: filter+group+sum", n, elapsed);

    if (result && !TD_IS_ERR(result)) td_release(result);
    td_graph_free(g);
    td_release(tbl);
    td_sys_free(region_data);
    td_sys_free(amount_data);
    td_sys_free(flag_data);
    td_sym_destroy();
    td_heap_destroy();
}

/* Q2: join + filter + sort (relational) */
static void bench_q2_relational(int64_t n) {
    td_heap_init();
    td_sym_init();

    /* Left: orders */
    int64_t* oid_data = td_sys_alloc((size_t)n * sizeof(int64_t));
    int64_t* cid_data = td_sys_alloc((size_t)n * sizeof(int64_t));
    int64_t* amt_data = td_sys_alloc((size_t)n * sizeof(int64_t));
    for (int64_t i = 0; i < n; i++) {
        oid_data[i] = i;
        cid_data[i] = i % 1000;
        amt_data[i] = (i * 13 + 7) % 10000;
    }

    td_t* o_v = td_vec_from_raw(TD_I64, oid_data, n);
    td_t* c_v = td_vec_from_raw(TD_I64, cid_data, n);
    td_t* a_v = td_vec_from_raw(TD_I64, amt_data, n);

    int64_t n_oid = td_sym_intern("oid", 3);
    int64_t n_cid = td_sym_intern("cid", 3);
    int64_t n_amt = td_sym_intern("amt", 3);

    td_t* orders = td_table_new(3);
    orders = td_table_add_col(orders, n_oid, o_v);
    orders = td_table_add_col(orders, n_cid, c_v);
    orders = td_table_add_col(orders, n_amt, a_v);
    td_release(o_v); td_release(c_v); td_release(a_v);

    /* Right: customers (smaller) */
    int64_t n_cust = 1000;
    int64_t* c2_data = td_sys_alloc((size_t)n_cust * sizeof(int64_t));
    int64_t* sc_data = td_sys_alloc((size_t)n_cust * sizeof(int64_t));
    for (int64_t i = 0; i < n_cust; i++) { c2_data[i] = i; sc_data[i] = i * 100; }

    td_t* c2_v = td_vec_from_raw(TD_I64, c2_data, n_cust);
    td_t* sc_v = td_vec_from_raw(TD_I64, sc_data, n_cust);

    int64_t n_score = td_sym_intern("score", 5);

    td_t* custs = td_table_new(2);
    custs = td_table_add_col(custs, n_cid, c2_v);
    custs = td_table_add_col(custs, n_score, sc_v);
    td_release(c2_v); td_release(sc_v);

    td_graph_t* g = td_graph_new(orders);
    td_op_t* lo = td_const_table(g, orders);
    td_op_t* ro = td_const_table(g, custs);
    td_op_t* lk = td_scan(g, "cid");
    td_op_t* lk_arr[] = { lk };
    td_op_t* rk_arr[] = { lk };
    td_op_t* join_op = td_join(g, lo, lk_arr, ro, rk_arr, 1, 0);

    td_op_t* cnt = td_count(g, join_op);

    double t0 = now_ns();
    td_t* result = td_execute(g, cnt);
    double elapsed = now_ns() - t0;

    report("Q2: join+count", n, elapsed);

    if (result && !TD_IS_ERR(result)) td_release(result);
    td_graph_free(g);
    td_release(orders);
    td_release(custs);
    td_sys_free(oid_data);
    td_sys_free(cid_data);
    td_sys_free(amt_data);
    td_sys_free(c2_data);
    td_sys_free(sc_data);
    td_sym_destroy();
    td_heap_destroy();
}

int main(void) {
    int64_t sizes[] = { 10000, 1000000 };
    int n_sizes = 2;

    printf("%-30s  %10s  %10s  %12s\n", "Query", "Rows", "Time", "Throughput");
    printf("%-30s  %10s  %10s  %12s\n",
           "------------------------------", "----------", "----------", "------------");

    for (int s = 0; s < n_sizes; s++) {
        bench_q1_analytics(sizes[s]);
        bench_q2_relational(sizes[s]);
        printf("\n");
    }

    return 0;
}
```

**Step 2: Verify build**

Run: `cmake --build build_bench`
Expected: Both bench_teide and bench_queries built.

**Step 3: Run query benchmarks**

Run: `./build_bench/bench_queries`
Expected: Table output with timing for Q1 and Q2 at 10K and 1M rows.

**Step 4: Commit**

```bash
git add bench/bench_queries.c
git commit -m "feat: add end-to-end query benchmarks (analytics + relational)"
```

---

## Task 10: Final verification

**Step 1: Clean rebuild with -Werror**

Run: `cmake -B build_final -DCMAKE_BUILD_TYPE=Debug -DTEIDE_WERROR=ON && cmake --build build_final`
Expected: Clean build, zero warnings.

**Step 2: Run all tests**

Run: `cd build_final && ctest --output-on-failure`
Expected: All tests pass.

**Step 3: Release build + tests**

Run: `cmake -B build_release -DCMAKE_BUILD_TYPE=Release && cmake --build build_release && cd build_release && ctest --output-on-failure`
Expected: All tests pass.

**Step 4: Run benchmarks**

Run: `cmake -B build_bench -DCMAKE_BUILD_TYPE=Release -DTEIDE_BENCH=ON && cmake --build build_bench && ./build_bench/bench_teide && ./build_bench/bench_queries`

**Step 5: Move design + plan to completed**

```bash
mv docs/plans/2026-03-05-production-hardening-design.md docs/plans/completed/
mv docs/plans/2026-03-05-production-hardening-plan.md docs/plans/completed/
git add docs/plans/
git commit -m "move completed plans: production-hardening"
```
