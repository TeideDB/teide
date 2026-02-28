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

#ifndef TD_H
#define TD_H

#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>
/* MSVC < 17.4 (cl 19.34) does not ship <stdatomic.h>; use Interlocked intrinsics
 * instead. _MSC_VER 1934 corresponds to VS 2022 17.4. */
#if !defined(_MSC_VER) || _MSC_VER >= 1934
  #include <stdatomic.h>
#else
  #include <windows.h>
  #define _Atomic(T)                          volatile T
  #define atomic_store_explicit(p, v, mo)     (*(p) = (v))
  #define atomic_load_explicit(p, mo)         (*(p))
  #define atomic_fetch_add_explicit(p, v, mo) _InterlockedExchangeAdd((volatile long*)(p), (long)(v))
  #define atomic_fetch_sub_explicit(p, v, mo) _InterlockedExchangeAdd((volatile long*)(p), -(long)(v))
  #define atomic_exchange_explicit(p, v, mo)  _InterlockedExchange((volatile long*)(p), (long)(v))
  #define atomic_compare_exchange_weak_explicit(p, exp, des, s, f) \
      (_InterlockedCompareExchange((volatile long*)(p), (long)(des), *(long*)(exp)) == *(long*)(exp))
  #define atomic_store(p, v)                  (*(p) = (v))
  #define atomic_thread_fence(mo)             MemoryBarrier()
  #define memory_order_relaxed 0
  #define memory_order_acquire 0
  #define memory_order_release 0
  #define memory_order_acq_rel 0
  #define memory_order_seq_cst 0
#endif

#ifdef __cplusplus
extern "C" {
#endif

/* ===== Platform Macros ===== */

#ifndef TD_LIKELY
#if defined(__GNUC__) || defined(__clang__)
  #define TD_LIKELY(x)   __builtin_expect(!!(x), 1)
  #define TD_UNLIKELY(x) __builtin_expect(!!(x), 0)
  #define TD_ALIGN(n)    __attribute__((aligned(n)))
  #define TD_INLINE      static inline __attribute__((always_inline))
#elif defined(_MSC_VER)
  #define TD_LIKELY(x)   (x)
  #define TD_UNLIKELY(x) (x)
  #define TD_ALIGN(n)    __declspec(align(n))
  #define TD_INLINE      static __forceinline
#else
  #define TD_LIKELY(x)   (x)
  #define TD_UNLIKELY(x) (x)
  #define TD_ALIGN(n)
  #define TD_INLINE      static inline
#endif
#endif /* TD_LIKELY */

#ifndef TD_ASSUME_ALIGNED
#if defined(__GNUC__) || defined(__clang__)
  #define TD_ASSUME_ALIGNED(p, n) __builtin_assume_aligned((p), (n))
#else
  #define TD_ASSUME_ALIGNED(p, n) (p)
#endif
#endif

#if defined(_MSC_VER)
  #define TD_TLS __declspec(thread)
#else
  #define TD_TLS _Thread_local
#endif

/* ===== Atomic Helpers ===== */

#if defined(_MSC_VER)
  #define td_atomic_inc(p)   _InterlockedIncrement((volatile long*)(p))
  #define td_atomic_dec(p)   _InterlockedDecrement((volatile long*)(p))
  #define td_atomic_load(p)  _InterlockedOr((volatile long*)(p), 0)
#else
  #define td_atomic_inc(p)   atomic_fetch_add_explicit(p, 1, memory_order_relaxed)
  #define td_atomic_dec(p)   atomic_fetch_sub_explicit(p, 1, memory_order_acq_rel)
  #define td_atomic_load(p)  atomic_load_explicit(p, memory_order_acquire)
#endif

/* ===== Type Constants ===== */

#define TD_LIST       0
#define TD_BOOL       1
#define TD_U8         2
#define TD_CHAR       3
#define TD_I16        4
#define TD_I32        5
#define TD_I64        6
#define TD_F64        7
#define TD_DATE       9
#define TD_TIME      10
#define TD_TIMESTAMP 11
#define TD_GUID      12
#define TD_TABLE     13
#define TD_SEL       16   /* selection bitmap (lazy filter) */

/* Unified dictionary-encoded string column (adaptive width) */
#define TD_SYM       20

/* Symbol width encoding (lower 2 bits of attrs when type == TD_SYM) */
#define TD_SYM_W_MASK   0x03
#define TD_SYM_W8       0x00   /* uint8_t  indices — dict ≤ 255 entries */
#define TD_SYM_W16      0x01   /* uint16_t indices — dict ≤ 65,535 */
#define TD_SYM_W32      0x02   /* uint32_t indices — dict ≤ 4,294,967,295 */
#define TD_SYM_W64      0x03   /* uint64_t indices — dict > 4B entries */

/* Helper macros */
#define TD_IS_SYM(t)         ((t) == TD_SYM)
#define TD_SYM_ELEM(attrs)   (1u << ((attrs) & TD_SYM_W_MASK))  /* 1,2,4,8 */

/* Dictionary metadata key (splay tree) */
#define TD_META_DICT  0x4449  /* "DI" — per-column dictionary */

/* Parted types: composite of TD_PARTED_BASE + base type */
#define TD_PARTED_BASE   32
#define TD_MAPCOMMON     64   /* virtual partition column */

/* MAPCOMMON inferred sub-types (stored in attrs field) */
#define TD_MC_SYM    0   /* opaque partition key strings */
#define TD_MC_DATE   1   /* YYYY.MM.DD partition directories */
#define TD_MC_I64    2   /* pure integer partition keys */

#define TD_IS_PARTED(t)       ((t) >= TD_PARTED_BASE && (t) < TD_MAPCOMMON)
#define TD_PARTED_BASETYPE(t) ((t) - TD_PARTED_BASE)

/* Atom variants (negative type tags) */
#define TD_ATOM_BOOL       (-TD_BOOL)
#define TD_ATOM_U8         (-TD_U8)
#define TD_ATOM_CHAR       (-TD_CHAR)
#define TD_ATOM_I16        (-TD_I16)
#define TD_ATOM_I32        (-TD_I32)
#define TD_ATOM_I64        (-TD_I64)
#define TD_ATOM_F64        (-TD_F64)
#define TD_ATOM_STR        (-8)
#define TD_ATOM_DATE       (-TD_DATE)
#define TD_ATOM_TIME       (-TD_TIME)
#define TD_ATOM_TIMESTAMP  (-TD_TIMESTAMP)
#define TD_ATOM_GUID       (-TD_GUID)
#define TD_ATOM_SYM        (-TD_SYM)

/* Number of types (positive range): must be > max type ID */
#define TD_TYPE_COUNT 21

/* ===== Attribute Flags ===== */

#define TD_ATTR_SLICE        0x10
#define TD_ATTR_NULLMAP_EXT  0x20
#define TD_ATTR_HAS_NULLS    0x40

/* ===== Morsel Constants ===== */

#define TD_MORSEL_ELEMS  1024

/* ===== Slab Cache Constants ===== */

#define TD_SLAB_CACHE_SIZE  64
#define TD_SLAB_ORDERS      5

/* ===== Heap Allocator Constants ===== */

#define TD_ORDER_MIN  6
#define TD_ORDER_MAX  30

/* ===== Parallel Threshold ===== */

#define TD_PARALLEL_THRESHOLD  (64 * TD_MORSEL_ELEMS)
#define TD_DISPATCH_MORSELS    8

/* ===== Error Handling ===== */

typedef enum {
    TD_OK = 0,
    TD_ERR_OOM,
    TD_ERR_TYPE,
    TD_ERR_RANGE,
    TD_ERR_LENGTH,
    TD_ERR_RANK,
    TD_ERR_DOMAIN,
    TD_ERR_NYI,
    TD_ERR_IO,
    TD_ERR_SCHEMA,
    TD_ERR_CORRUPT,
    TD_ERR_CANCEL
} td_err_t;

#define TD_ERR_PTR(e)   ((td_t*)(uintptr_t)(e))
#define TD_IS_ERR(p)    ((uintptr_t)(p) < 32)
#define TD_ERR_CODE(p)  ((td_err_t)(uintptr_t)(p))

const char* td_err_str(td_err_t e);

/* ===== Core Type: td_t (32-byte block/object header) ===== */

typedef union TD_ALIGN(32) td_t {
    /* Allocated: object header */
    struct {
        /* Bytes 0-15: nullable bitmask / slice / ext nullmap */
        union {
            uint8_t  nullmap[16];
            struct { union td_t* slice_parent; int64_t slice_offset; };
            struct { union td_t* ext_nullmap;  union td_t* sym_dict; };
        };
        /* Bytes 16-31: metadata + value */
        uint8_t  mmod;       /* 0=heap, 1=file-mmap */
        uint8_t  order;      /* block order (block size = 2^order) */
        int8_t   type;       /* negative=atom, positive=vector, 0=LIST */
        uint8_t  attrs;      /* attribute flags */
        _Atomic(uint32_t) rc; /* reference count (0=free) */
        union {
            uint8_t  b8;     /* BOOL atom */
            uint8_t  u8;     /* U8 atom */
            char     c8;     /* CHAR atom */
            int16_t  i16;    /* I16 atom */
            int32_t  i32;    /* I32 atom */
            uint32_t u32;
            int64_t  i64;    /* I64/SYMBOL/DATE/TIME/TIMESTAMP atom */
            double   f64;    /* F64 atom */
            union td_t* obj; /* pointer to child (long strings, GUID) */
            struct { uint8_t slen; char sdata[7]; }; /* SSO string (<=7 bytes) */
            int64_t  len;    /* vector element count */
        };
        uint8_t  data[];     /* element data (flexible array member) */
    };
    /* Free: buddy allocator block (fl_prev/fl_next overlay bytes 0-15) */
    struct {
        union td_t* fl_prev;
        union td_t* fl_next;
    };
} td_t;

/* Type sizes lookup table (defined in types.c) */
extern const uint8_t td_type_sizes[TD_TYPE_COUNT];

/* ===== Accessor Macros ===== */

#define td_type(v)       ((v)->type)
#define td_is_atom(v)    ((v)->type < 0)
#define td_is_vec(v)     ((v)->type > 0)
#define td_len(v)        ((v)->len)
static inline void* td_data_fn(td_t* v) {
    return TD_ASSUME_ALIGNED((void*)v->data, 32);
}
#define td_data(v)       td_data_fn(v)
#define td_elem_size(t)  (td_type_sizes[(t)])

/* SYM-aware element size: returns adaptive width for TD_SYM columns */
static inline uint8_t td_sym_elem_size(int8_t type, uint8_t attrs) {
    if (type == TD_SYM) return (uint8_t)TD_SYM_ELEM(attrs);
    return td_elem_size(type);
}

/* Read a dictionary index from a TD_SYM column (adaptive width) */
static inline int64_t td_read_sym(const void* data, int64_t row, int8_t type, uint8_t attrs) {
    (void)type; /* only TD_SYM now */
    switch (attrs & TD_SYM_W_MASK) {
        case TD_SYM_W8:  return ((const uint8_t*)data)[row];
        case TD_SYM_W16: return ((const uint16_t*)data)[row];
        case TD_SYM_W32: return ((const uint32_t*)data)[row];
        case TD_SYM_W64: return ((const int64_t*)data)[row];
    }
    return 0;
}

/* Write a dictionary index into a TD_SYM column (adaptive width) */
static inline void td_write_sym(void* data, int64_t row, uint64_t val, int8_t type, uint8_t attrs) {
    (void)type; /* only TD_SYM now */
    switch (attrs & TD_SYM_W_MASK) {
        case TD_SYM_W8:  ((uint8_t*)data)[row]  = (uint8_t)val;  break;
        case TD_SYM_W16: ((uint16_t*)data)[row] = (uint16_t)val; break;
        case TD_SYM_W32: ((uint32_t*)data)[row] = (uint32_t)val; break;
        case TD_SYM_W64: ((int64_t*)data)[row]  = (int64_t)val;  break;
    }
}

/* Determine optimal SYM width for a given dictionary size */
static inline uint8_t td_sym_dict_width(int64_t dict_size) {
    if (dict_size <= 255)        return TD_SYM_W8;
    if (dict_size <= 65535)      return TD_SYM_W16;
    if (dict_size <= 4294967295) return TD_SYM_W32;
    return TD_SYM_W64;
}

/* ===== Operation Graph ===== */

/* Opcodes — Sources */
#define OP_SCAN          1
#define OP_CONST         2

/* Opcodes — Unary element-wise (fuseable) */
#define OP_NEG          10
#define OP_ABS          11
#define OP_NOT          12
#define OP_SQRT         13
#define OP_LOG          14
#define OP_EXP          15
#define OP_CEIL         16
#define OP_FLOOR        17
#define OP_ISNULL       18
#define OP_CAST         19

/* Opcodes — Binary element-wise (fuseable) */
#define OP_ADD          20
#define OP_SUB          21
#define OP_MUL          22
#define OP_DIV          23
#define OP_MOD          24
#define OP_EQ           25
#define OP_NE           26
#define OP_LT           27
#define OP_LE           28
#define OP_GT           29
#define OP_GE           30
#define OP_AND          31
#define OP_OR           32
#define OP_MIN2         33
#define OP_MAX2         34
#define OP_IF           35
#define OP_LIKE         36
#define OP_UPPER        37
#define OP_LOWER        38
#define OP_STRLEN       39
#define OP_SUBSTR       40
#define OP_REPLACE      41
#define OP_TRIM         42
#define OP_CONCAT       43
#define OP_EXTRACT      45
#define OP_DATE_TRUNC   46

/* EXTRACT / DATE_TRUNC field identifiers */
#define TD_EXTRACT_YEAR    0
#define TD_EXTRACT_MONTH   1
#define TD_EXTRACT_DAY     2
#define TD_EXTRACT_HOUR    3
#define TD_EXTRACT_MINUTE  4
#define TD_EXTRACT_SECOND  5
#define TD_EXTRACT_DOW     6
#define TD_EXTRACT_DOY     7
#define TD_EXTRACT_EPOCH   8

/* Opcodes — Reductions (pipeline breakers) */
#define OP_SUM          50
#define OP_PROD         51
#define OP_MIN          52
#define OP_MAX          53
#define OP_COUNT        54
#define OP_AVG          55
#define OP_FIRST        56
#define OP_LAST         57
#define OP_COUNT_DISTINCT 58
#define OP_STDDEV       59

/* Opcodes — Structural (pipeline breakers) */
#define OP_FILTER       60
#define OP_SORT         61
#define OP_GROUP        62
#define OP_JOIN         63
#define OP_WINDOW_JOIN  64
#define OP_PROJECT      65
#define OP_SELECT       66
#define OP_HEAD         67
#define OP_TAIL         68

/* Opcodes — Window */
#define OP_WINDOW       72

/* Opcodes — Statistical aggregates */
#define OP_STDDEV_POP   73
#define OP_VAR          74
#define OP_VAR_POP      75
#define OP_ILIKE        76

/* Opcodes — Graph */
#define OP_EXPAND        80   /* 1-hop CSR neighbor expansion       */
#define OP_VAR_EXPAND    81   /* variable-length BFS/DFS            */
#define OP_SHORTEST_PATH 82   /* BFS shortest path                  */
#define OP_WCO_JOIN      83   /* worst-case optimal join (LFTJ)     */

/* Opcodes — Misc */
#define OP_ALIAS        70
#define OP_MATERIALIZE  71

/* Window function kinds (stored in func_kinds[]) */
#define TD_WIN_ROW_NUMBER    0
#define TD_WIN_RANK          1
#define TD_WIN_DENSE_RANK    2
#define TD_WIN_NTILE         3
#define TD_WIN_SUM           4
#define TD_WIN_AVG           5
#define TD_WIN_MIN           6
#define TD_WIN_MAX           7
#define TD_WIN_COUNT         8
#define TD_WIN_LAG           9
#define TD_WIN_LEAD         10
#define TD_WIN_FIRST_VALUE  11
#define TD_WIN_LAST_VALUE   12
#define TD_WIN_NTH_VALUE    13

/* Frame types */
#define TD_FRAME_ROWS    0
#define TD_FRAME_RANGE   1

/* Frame bounds */
#define TD_BOUND_UNBOUNDED_PRECEDING  0
#define TD_BOUND_N_PRECEDING          1
#define TD_BOUND_CURRENT_ROW          2
#define TD_BOUND_N_FOLLOWING          3
#define TD_BOUND_UNBOUNDED_FOLLOWING  4

/* Op flags */
#define OP_FLAG_FUSED        0x01
#define OP_FLAG_DEAD         0x02

/* Operation node (32 bytes, fits one cache line) */
typedef struct td_op {
    uint16_t       opcode;     /* OP_ADD, OP_SCAN, OP_FILTER, etc. */
    uint8_t        arity;      /* 0, 1, or 2 */
    uint8_t        flags;      /* FUSED, DEAD */
    int8_t         out_type;   /* inferred output type */
    uint8_t        pad[3];
    uint32_t       id;         /* unique node ID */
    uint32_t       est_rows;   /* estimated row count */
    struct td_op*  inputs[2];  /* NULL if unused */
} td_op_t;

/* Extended operation node for N-ary ops (heap-allocated, variable size) */
typedef struct td_op_ext {
    td_op_t base;              /* 32 bytes standard node */
    union {
        td_t*   literal;       /* OP_CONST: inline literal value */
        int64_t sym;           /* OP_SCAN: column name symbol ID */
        struct {               /* OP_GROUP: group-by specification */
            td_op_t**  keys;
            uint8_t    n_keys;
            uint8_t    n_aggs;
            uint16_t*  agg_ops;
            td_op_t**  agg_ins;
        };
        struct {               /* OP_SORT: multi-column sort */
            td_op_t**  columns;
            uint8_t*   desc;
            uint8_t*   nulls_first; /* 1=nulls first, 0=nulls last */
            uint8_t    n_cols;
        } sort;
        struct {               /* OP_JOIN: join specification */
            td_op_t**  left_keys;
            td_op_t**  right_keys;
            uint8_t    n_join_keys;
            uint8_t    join_type;  /* 0=inner, 1=left, 2=full */
        } join;
        struct {               /* OP_WINDOW: window functions */
            td_op_t**  part_keys;
            td_op_t**  order_keys;
            uint8_t*   order_descs;
            td_op_t**  func_inputs;
            uint8_t*   func_kinds;    /* TD_WIN_ROW_NUMBER etc. */
            int64_t*   func_params;   /* NTILE(n), LAG offset, etc. */
            uint8_t    n_part_keys;
            uint8_t    n_order_keys;
            uint8_t    n_funcs;
            uint8_t    frame_type;    /* TD_FRAME_ROWS / TD_FRAME_RANGE */
            uint8_t    frame_start;   /* TD_BOUND_* */
            uint8_t    frame_end;     /* TD_BOUND_* */
            int64_t    frame_start_n;
            int64_t    frame_end_n;
        } window;
        struct {  /* OP_EXPAND / OP_VAR_EXPAND / OP_SHORTEST_PATH */
            void*     rel;            /* td_rel_t* (opaque to public header) */
            void*     sip_sel;        /* td_t* TD_SEL bitmap for SIP source-side skip */
            uint8_t   direction;      /* 0=fwd, 1=rev, 2=both */
            uint8_t   min_depth;
            uint8_t   max_depth;
            uint8_t   path_tracking;
            uint8_t   factorized;     /* 1 = emit factorized output (fvec) */
        } graph;
        struct {  /* OP_WCO_JOIN */
            void**    rels;           /* td_rel_t** array */
            uint8_t   n_rels;
            uint8_t   n_vars;
        } wco;
    };
} td_op_ext_t;

/* Operation graph */
typedef struct td_graph {
    td_op_t*       nodes;       /* array of op nodes (malloc'd) */
    uint32_t       node_count;  /* number of nodes */
    uint32_t       node_cap;    /* allocated capacity */
    td_t*          table;       /* bound table (provides columns for OP_SCAN) */
    td_t**         tables;      /* table registry (indexed by table_id) */
    uint16_t       n_tables;    /* number of registered tables */
    td_op_ext_t**  ext_nodes;   /* tracked extended nodes for cleanup */
    uint32_t       ext_count;   /* number of extended nodes */
    uint32_t       ext_cap;     /* capacity of ext_nodes array */
    td_t*          selection;   /* TD_SEL bitmap — lazy filter (NULL = all pass) */
} td_graph_t;

/* ===== Morsel Iterator ===== */

typedef struct {
    td_t*    vec;          /* source vector */
    int64_t  offset;       /* current position (element index) */
    int64_t  len;          /* total length of vector */
    uint32_t elem_size;    /* bytes per element */
    int64_t  morsel_len;   /* elements in current morsel (<=TD_MORSEL_ELEMS) */
    void*    morsel_ptr;   /* pointer to current morsel data */
    uint8_t* null_bits;    /* current morsel null bitmap (or NULL) */
} td_morsel_t;

/* ===== Selection Bitmap (TD_SEL) ===== */

/* Segment flags — one per morsel (TD_MORSEL_ELEMS rows) */
#define TD_SEL_NONE  0   /* all bits 0 — skip entire morsel           */
#define TD_SEL_ALL   1   /* all bits 1 — process without bitmap check */
#define TD_SEL_MIX   2   /* mixed bits — must check per-row           */

/* Words per morsel segment: 1024 rows / 64 bits = 16 uint64_t */
#define TD_SEL_WORDS_PER_SEG  (TD_MORSEL_ELEMS / 64)

/* Inline metadata at td_data(sel) */
typedef struct {
    int64_t   total_pass;  /* total passing rows                      */
    uint32_t  n_segs;      /* ceil(nrows / TD_MORSEL_ELEMS)           */
    uint32_t  _pad;
} td_sel_meta_t;

/*
 * TD_SEL block layout (td_data offset 0):
 *
 *   td_sel_meta_t  meta        (16 bytes)
 *   uint8_t        seg_flags[] (n_segs, padded to 8-byte alignment)
 *   uint16_t       seg_popcnt[](n_segs, padded to 8-byte alignment)
 *   uint64_t       bits[]      (ceil(nrows/64) words)
 */

static inline td_sel_meta_t* td_sel_meta(td_t* s) {
    return (td_sel_meta_t*)td_data(s);
}
static inline uint8_t* td_sel_flags(td_t* s) {
    return (uint8_t*)td_data(s) + sizeof(td_sel_meta_t);
}
static inline uint16_t* td_sel_popcnt(td_t* s) {
    uint32_t n = td_sel_meta(s)->n_segs;
    return (uint16_t*)(td_sel_flags(s) + ((n + 7u) & ~7u));
}
static inline uint64_t* td_sel_bits(td_t* s) {
    uint32_t n = td_sel_meta(s)->n_segs;
    uint16_t* pc = td_sel_popcnt(s);
    return (uint64_t*)(pc + ((n + 3u) & ~3u));
}

/* Bit ops */
#define TD_SEL_BIT_TEST(bits, r)  ((bits)[(r) >> 6] & (1ULL << ((r) & 63)))
#define TD_SEL_BIT_SET(bits, r)   ((bits)[(r) >> 6] |= (1ULL << ((r) & 63)))
#define TD_SEL_BIT_CLR(bits, r)   ((bits)[(r) >> 6] &= ~(1ULL << ((r) & 63)))

/* ===== Executor Pipeline ===== */

typedef struct td_pipe {
    td_op_t*          op;            /* operation node */
    struct td_pipe*   inputs[2];     /* upstream pipes */
    td_morsel_t       state;         /* current morsel state */
    td_t*             materialized;  /* materialized intermediate (or NULL) */
    int               spill_fd;      /* file descriptor for spill (-1 if none) */
} td_pipe_t;

/* ===== Memory Statistics ===== */

typedef struct {
    size_t alloc_count;      /* td_alloc calls */
    size_t free_count;       /* td_free calls */
    size_t bytes_allocated;  /* currently allocated */
    size_t peak_bytes;       /* high-water mark */
    size_t slab_hits;        /* slab cache hits */
    size_t direct_count;     /* active direct mmaps */
    size_t direct_bytes;     /* bytes in direct mmaps */
    size_t sys_current;      /* sys allocator: current mmap'd bytes */
    size_t sys_peak;         /* sys allocator: peak mmap'd bytes */
} td_mem_stats_t;

/* ===== Forward Declarations (internal types) ===== */

typedef struct td_heap      td_heap_t;
typedef struct td_sym_table td_sym_table_t;
typedef struct td_sym_map   td_sym_map_t;
typedef struct td_pool      td_pool_t;
typedef struct td_task      td_task_t;
typedef struct td_dispatch  td_dispatch_t;
typedef struct td_csr       td_csr_t;
typedef struct td_rel       td_rel_t;

/* ===== Thread Types ===== */

#if defined(_WIN32)
  typedef void* td_thread_t;
#else
  typedef unsigned long td_thread_t;
#endif

typedef void (*td_thread_fn)(void* arg);

/* ===== Platform API ===== */

void* td_vm_alloc(size_t size);
void  td_vm_free(void* ptr, size_t size);
void* td_vm_map_file(const char* path, size_t* out_size);
void  td_vm_unmap_file(void* ptr, size_t size);
void  td_vm_advise_seq(void* ptr, size_t size);
void  td_vm_advise_willneed(void* ptr, size_t size);
void  td_vm_release(void* ptr, size_t size);
void* td_vm_alloc_aligned(size_t size, size_t alignment);

/* ===== Threading API ===== */

td_err_t td_thread_create(td_thread_t* t, td_thread_fn fn, void* arg);
td_err_t td_thread_join(td_thread_t t);
uint32_t td_thread_count(void);

void td_parallel_begin(void);
void td_parallel_end(void);
extern _Atomic(uint32_t) td_parallel_flag;

/* Reclaim fully-free pools by munmapping their regions. Called at
 * control points (e.g. between queries, end of parallel sections). */
void td_heap_gc(void);

/* Release physical pages for large free blocks (madvise DONTNEED).
 * Explicit opt-in — NOT called automatically by td_heap_gc(). Use
 * after long idle periods to reduce RSS. */
void td_heap_release_pages(void);

/* ===== Memory Allocator API ===== */

td_t*    td_alloc(size_t data_size);
/* NOTE: td_free supports cross-thread free via foreign_blocks list.
 * Blocks freed from a non-owning thread are deferred and coalesced
 * when the owning heap flushes foreign blocks. */
void     td_free(td_t* v);
td_t*    td_alloc_copy(td_t* v);
td_t*    td_scratch_alloc(size_t data_size);
td_t*    td_scratch_realloc(td_t* v, size_t new_data_size);

void     td_heap_init(void);
void     td_heap_destroy(void);
void     td_heap_merge(td_heap_t* src);

uint8_t  td_order_for_size(size_t data_size);
void     td_mem_stats(td_mem_stats_t* out);

/* ===== COW / Ref Counting API ===== */

void     td_retain(td_t* v);
void     td_release(td_t* v);
td_t*    td_cow(td_t* v);

/* ===== Atom Constructors ===== */

td_t* td_bool(bool val);
td_t* td_u8(uint8_t val);
td_t* td_char(char val);
td_t* td_i16(int16_t val);
td_t* td_i32(int32_t val);
td_t* td_i64(int64_t val);
td_t* td_f64(double val);
td_t* td_str(const char* s, size_t len);
td_t* td_sym(int64_t id);
td_t* td_date(int64_t val);
td_t* td_time(int64_t val);
td_t* td_timestamp(int64_t val);
td_t* td_guid(const uint8_t* bytes);

/* ===== Selection API ===== */

td_t* td_sel_new(int64_t nrows);              /* all-zero (no rows pass)       */
td_t* td_sel_from_pred(td_t* bool_vec);       /* convert TD_BOOL vec → TD_SEL  */
td_t* td_sel_and(td_t* a, td_t* b);           /* AND two selections            */
void  td_sel_recompute(td_t* sel);             /* rebuild seg_flags + popcounts */

/* ===== Vector API ===== */

td_t* td_vec_new(int8_t type, int64_t capacity);
td_t* td_sym_vec_new(uint8_t sym_width, int64_t capacity);  /* TD_SYM with adaptive width */
td_t* td_vec_append(td_t* vec, const void* elem);
td_t* td_vec_set(td_t* vec, int64_t idx, const void* elem);
void* td_vec_get(td_t* vec, int64_t idx);
td_t* td_vec_slice(td_t* vec, int64_t offset, int64_t len);
td_t* td_vec_concat(td_t* a, td_t* b);
td_t* td_vec_from_raw(int8_t type, const void* data, int64_t count);

/* Null bitmap ops */
void  td_vec_set_null(td_t* vec, int64_t idx, bool is_null);
bool  td_vec_is_null(td_t* vec, int64_t idx);

/* ===== String API ===== */

const char* td_str_ptr(td_t* s);
size_t      td_str_len(td_t* s);
int         td_str_cmp(td_t* a, td_t* b);

/* ===== List API ===== */

td_t* td_list_new(int64_t capacity);
td_t* td_list_append(td_t* list, td_t* item);
td_t* td_list_get(td_t* list, int64_t idx);
td_t* td_list_set(td_t* list, int64_t idx, td_t* item);

/* ===== Symbol Intern Table API ===== */

void     td_sym_init(void);
void     td_sym_destroy(void);
int64_t  td_sym_intern(const char* str, size_t len);
int64_t  td_sym_find(const char* str, size_t len);
td_t*    td_sym_str(int64_t id);
uint32_t td_sym_count(void);
td_err_t td_sym_save(const char* path);
td_err_t td_sym_load(const char* path);

/* ===== Table API ===== */

td_t*       td_table_new(int64_t ncols);
td_t*       td_table_add_col(td_t* tbl, int64_t name_id, td_t* col_vec);
td_t*       td_table_get_col(td_t* tbl, int64_t name_id);
td_t*       td_table_get_col_idx(td_t* tbl, int64_t idx);
int64_t     td_table_col_name(td_t* tbl, int64_t idx);
void        td_table_set_col_name(td_t* tbl, int64_t idx, int64_t name_id);
int64_t     td_table_ncols(td_t* tbl);
int64_t     td_table_nrows(td_t* tbl);
int64_t     td_parted_nrows(td_t* parted_col);
td_t*       td_table_schema(td_t* tbl);

/* ===== Morsel Iterator API ===== */

void td_morsel_init(td_morsel_t* m, td_t* vec);
void td_morsel_init_range(td_morsel_t* m, td_t* vec, int64_t start, int64_t end);
bool td_morsel_next(td_morsel_t* m);

/* ===== Operation Graph API ===== */

td_graph_t* td_graph_new(td_t* tbl);
void        td_graph_free(td_graph_t* g);

/* Source ops */
td_op_t* td_scan(td_graph_t* g, const char* col_name);
td_op_t* td_const_f64(td_graph_t* g, double val);
td_op_t* td_const_i64(td_graph_t* g, int64_t val);
td_op_t* td_const_bool(td_graph_t* g, bool val);
td_op_t* td_const_str(td_graph_t* g, const char* s);
td_op_t* td_const_vec(td_graph_t* g, td_t* vec);
td_op_t* td_const_table(td_graph_t* g, td_t* table);

/* Unary element-wise ops */
td_op_t* td_neg(td_graph_t* g, td_op_t* a);
td_op_t* td_abs(td_graph_t* g, td_op_t* a);
td_op_t* td_not(td_graph_t* g, td_op_t* a);
td_op_t* td_sqrt_op(td_graph_t* g, td_op_t* a);
td_op_t* td_log_op(td_graph_t* g, td_op_t* a);
td_op_t* td_exp_op(td_graph_t* g, td_op_t* a);
td_op_t* td_ceil_op(td_graph_t* g, td_op_t* a);
td_op_t* td_floor_op(td_graph_t* g, td_op_t* a);
td_op_t* td_isnull(td_graph_t* g, td_op_t* a);
td_op_t* td_cast(td_graph_t* g, td_op_t* a, int8_t target_type);

/* Binary element-wise ops */
td_op_t* td_add(td_graph_t* g, td_op_t* a, td_op_t* b);
td_op_t* td_sub(td_graph_t* g, td_op_t* a, td_op_t* b);
td_op_t* td_mul(td_graph_t* g, td_op_t* a, td_op_t* b);
td_op_t* td_div(td_graph_t* g, td_op_t* a, td_op_t* b);
td_op_t* td_mod(td_graph_t* g, td_op_t* a, td_op_t* b);
td_op_t* td_eq(td_graph_t* g, td_op_t* a, td_op_t* b);
td_op_t* td_ne(td_graph_t* g, td_op_t* a, td_op_t* b);
td_op_t* td_lt(td_graph_t* g, td_op_t* a, td_op_t* b);
td_op_t* td_le(td_graph_t* g, td_op_t* a, td_op_t* b);
td_op_t* td_gt(td_graph_t* g, td_op_t* a, td_op_t* b);
td_op_t* td_ge(td_graph_t* g, td_op_t* a, td_op_t* b);
td_op_t* td_and(td_graph_t* g, td_op_t* a, td_op_t* b);
td_op_t* td_or(td_graph_t* g, td_op_t* a, td_op_t* b);
td_op_t* td_min2(td_graph_t* g, td_op_t* a, td_op_t* b);
td_op_t* td_max2(td_graph_t* g, td_op_t* a, td_op_t* b);
td_op_t* td_if(td_graph_t* g, td_op_t* cond, td_op_t* then_val, td_op_t* else_val);
td_op_t* td_like(td_graph_t* g, td_op_t* input, td_op_t* pattern);
td_op_t* td_ilike(td_graph_t* g, td_op_t* input, td_op_t* pattern);
td_op_t* td_upper(td_graph_t* g, td_op_t* a);
td_op_t* td_lower(td_graph_t* g, td_op_t* a);
td_op_t* td_strlen(td_graph_t* g, td_op_t* a);
td_op_t* td_substr(td_graph_t* g, td_op_t* str, td_op_t* start, td_op_t* len);
td_op_t* td_replace(td_graph_t* g, td_op_t* str, td_op_t* from, td_op_t* to);
td_op_t* td_trim_op(td_graph_t* g, td_op_t* a);
td_op_t* td_concat(td_graph_t* g, td_op_t** args, int n);

/* Date/time extraction and truncation */
td_op_t* td_extract(td_graph_t* g, td_op_t* col, int64_t field);
td_op_t* td_date_trunc(td_graph_t* g, td_op_t* col, int64_t field);

/* Reduction ops */
td_op_t* td_sum(td_graph_t* g, td_op_t* a);
td_op_t* td_prod(td_graph_t* g, td_op_t* a);
td_op_t* td_min_op(td_graph_t* g, td_op_t* a);
td_op_t* td_max_op(td_graph_t* g, td_op_t* a);
td_op_t* td_count(td_graph_t* g, td_op_t* a);
td_op_t* td_avg(td_graph_t* g, td_op_t* a);
td_op_t* td_first(td_graph_t* g, td_op_t* a);
td_op_t* td_last(td_graph_t* g, td_op_t* a);
td_op_t* td_count_distinct(td_graph_t* g, td_op_t* a);

/* Structural ops */
td_op_t* td_filter(td_graph_t* g, td_op_t* input, td_op_t* predicate);
td_op_t* td_sort_op(td_graph_t* g, td_op_t* table_node,
                     td_op_t** keys, uint8_t* descs, uint8_t* nulls_first,
                     uint8_t n_cols);
td_op_t* td_group(td_graph_t* g, td_op_t** keys, uint8_t n_keys,
                   uint16_t* agg_ops, td_op_t** agg_ins, uint8_t n_aggs);
td_op_t* td_distinct(td_graph_t* g, td_op_t** keys, uint8_t n_keys);
td_op_t* td_join(td_graph_t* g,
                  td_op_t* left_table, td_op_t** left_keys,
                  td_op_t* right_table, td_op_t** right_keys,
                  uint8_t n_keys, uint8_t join_type);
td_op_t* td_window_join(td_graph_t* g,
                         td_op_t* left_table, td_op_t* right_table,
                         td_op_t* time_key, td_op_t* sym_key,
                         int64_t window_lo, int64_t window_hi,
                         uint16_t* agg_ops, td_op_t** agg_ins,
                         uint8_t n_aggs);
td_op_t* td_window_op(td_graph_t* g, td_op_t* table_node,
                       td_op_t** part_keys, uint8_t n_part,
                       td_op_t** order_keys, uint8_t* order_descs, uint8_t n_order,
                       uint8_t* func_kinds, td_op_t** func_inputs,
                       int64_t* func_params, uint8_t n_funcs,
                       uint8_t frame_type, uint8_t frame_start, uint8_t frame_end,
                       int64_t frame_start_n, int64_t frame_end_n);
td_op_t* td_project(td_graph_t* g, td_op_t* input,
                     td_op_t** cols, uint8_t n_cols);
td_op_t* td_select(td_graph_t* g, td_op_t* input,
                    td_op_t** cols, uint8_t n_cols);
td_op_t* td_head(td_graph_t* g, td_op_t* input, int64_t n);
td_op_t* td_tail(td_graph_t* g, td_op_t* input, int64_t n);
td_op_t* td_alias(td_graph_t* g, td_op_t* input, const char* name);
td_op_t* td_materialize(td_graph_t* g, td_op_t* input);

/* ===== Graph Ops ===== */

/* Multi-table support */
uint16_t td_graph_add_table(td_graph_t* g, td_t* table);
td_op_t* td_scan_table(td_graph_t* g, uint16_t table_id, const char* col_name);

/* Graph traversal */
td_op_t* td_expand(td_graph_t* g, td_op_t* src_nodes,
                    td_rel_t* rel, uint8_t direction);
td_op_t* td_var_expand(td_graph_t* g, td_op_t* start_nodes,
                        td_rel_t* rel, uint8_t direction,
                        uint8_t min_depth, uint8_t max_depth,
                        bool track_path);
td_op_t* td_shortest_path(td_graph_t* g, td_op_t* src, td_op_t* dst,
                           td_rel_t* rel, uint8_t max_depth);
td_op_t* td_wco_join(td_graph_t* g,
                      td_rel_t** rels, uint8_t n_rels,
                      uint8_t n_vars);

/* CSR / Relationship API */
td_rel_t* td_rel_build(td_t* from_table, const char* fk_col,
                         int64_t n_target_nodes, bool sort_targets);
td_rel_t* td_rel_from_edges(td_t* edge_table,
                             const char* src_col, const char* dst_col,
                             int64_t n_src_nodes, int64_t n_dst_nodes,
                             bool sort_targets);
td_err_t  td_rel_save(td_rel_t* rel, const char* dir);
td_rel_t* td_rel_load(const char* dir);
td_rel_t* td_rel_mmap(const char* dir);
void      td_rel_free(td_rel_t* rel);

/* ===== Optimizer API ===== */

td_op_t* td_optimize(td_graph_t* g, td_op_t* root);
void     td_fuse_pass(td_graph_t* g, td_op_t* root);

/* ===== Executor API ===== */

td_t* td_execute(td_graph_t* g, td_op_t* root);

/* ===== Storage API ===== */

/* Column file I/O */
td_err_t td_col_save(td_t* vec, const char* path);
td_t*    td_col_load(const char* path);
td_t*    td_col_mmap(const char* path);

/* Splayed table I/O */
td_err_t td_splay_save(td_t* tbl, const char* dir, const char* sym_path);
td_t*    td_splay_load(const char* dir);
td_t*    td_read_splayed(const char* dir, const char* sym_path);

/* Partitioned table */
td_t*    td_part_load(const char* db_root, const char* table_name);
td_t*    td_read_parted(const char* db_root, const char* table_name);

/* Metadata */
td_err_t td_meta_save_d(td_t* schema, const char* path);
td_t*    td_meta_load_d(const char* path);

/* ===== CSV API ===== */

td_t* td_read_csv(const char* path);
td_t* td_read_csv_opts(const char* path, char delimiter, bool header,
                        const int8_t* col_types, int32_t n_types);
td_err_t td_write_csv(td_t* table, const char* path);


/* ===== Pool / Parallel API ===== */

td_err_t td_pool_init(uint32_t n_workers);
void     td_pool_destroy(void);
void     td_cancel(void);

#ifdef __cplusplus
}
#endif

#endif /* TD_H */
