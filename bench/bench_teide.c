#include <teide/td.h>
#include <mem/sys.h>
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

static void bench_sort(int64_t n) {
    int64_t* v_data = td_sys_alloc((size_t)n * sizeof(int64_t));
    for (int64_t i = 0; i < n; i++) v_data[i] = n - i;

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

/* ---- asof_join: ASOF join on time-series ---- */
static void bench_asof_join(int64_t n) {
    /* Left: trades with time, sym, price */
    int64_t* lt_data = td_sys_alloc((size_t)n * sizeof(int64_t));
    int64_t* ls_data = td_sys_alloc((size_t)n * sizeof(int64_t));
    int64_t* lp_data = td_sys_alloc((size_t)n * sizeof(int64_t));
    for (int64_t i = 0; i < n; i++) {
        lt_data[i] = i * 10;
        ls_data[i] = i % 100;
        lp_data[i] = (i * 7 + 3) % 1000;
    }

    td_t* lt_v = td_vec_from_raw(TD_I64, lt_data, n);
    td_t* ls_v = td_vec_from_raw(TD_I64, ls_data, n);
    td_t* lp_v = td_vec_from_raw(TD_I64, lp_data, n);

    int64_t n_time  = td_sym_intern("time", 4);
    int64_t n_sym   = td_sym_intern("sym", 3);
    int64_t n_price = td_sym_intern("price", 5);

    td_t* left = td_table_new(3);
    left = td_table_add_col(left, n_time, lt_v);
    left = td_table_add_col(left, n_sym, ls_v);
    left = td_table_add_col(left, n_price, lp_v);
    td_release(lt_v); td_release(ls_v); td_release(lp_v);

    /* Right: quotes with time, sym, bid */
    int64_t* rt_data = td_sys_alloc((size_t)n * sizeof(int64_t));
    int64_t* rs_data = td_sys_alloc((size_t)n * sizeof(int64_t));
    int64_t* rb_data = td_sys_alloc((size_t)n * sizeof(int64_t));
    for (int64_t i = 0; i < n; i++) {
        rt_data[i] = i * 10 + 5;  /* offset by 5 from trades */
        rs_data[i] = i % 100;
        rb_data[i] = (i * 13 + 7) % 1000;
    }

    td_t* rt_v = td_vec_from_raw(TD_I64, rt_data, n);
    td_t* rs_v = td_vec_from_raw(TD_I64, rs_data, n);
    td_t* rb_v = td_vec_from_raw(TD_I64, rb_data, n);

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

    td_op_t* aj = td_asof_join(g, left_op, right_op, tkey, eq_keys, 1, 0);
    td_op_t* cnt = td_count(g, aj);

    double t0 = now_ns();
    td_t* result = td_execute(g, cnt);
    double elapsed = now_ns() - t0;

    report("asof_join", n, elapsed);

    if (result && !TD_IS_ERR(result)) td_release(result);
    td_graph_free(g);
    td_release(left);
    td_release(right);
    td_sys_free(lt_data); td_sys_free(ls_data); td_sys_free(lp_data);
    td_sys_free(rt_data); td_sys_free(rs_data); td_sys_free(rb_data);
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
        bench_asof_join(sizes[s]);

        td_sym_destroy();
        td_heap_destroy();

        printf("\n");
    }

    return 0;
}
