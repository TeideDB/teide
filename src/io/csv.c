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

/* ============================================================================
 * csv.c — Fast parallel CSV reader
 *
 * Design:
 *   1. mmap + MAP_POPULATE for zero-copy file access
 *   2. memchr-based newline scan for row offset discovery
 *   3. Single-pass: sample-based type inference, then parallel value parsing
 *   4. Inline integer/float parsers (bypass strtoll/strtod overhead)
 *   5. Parallel row parsing via td_pool_dispatch
 *   6. Per-worker local sym tables, merged post-parse on main thread
 * ============================================================================ */

#if defined(__linux__)
  #define _GNU_SOURCE
#endif

#include "csv.h"
#include "mem/sys.h"
#include "ops/pool.h"
#include "ops/hash.h"

#include <string.h>
#include <stdio.h>
#include <stdlib.h>  /* strtoll fallback for fast_i64 overflow */
#include <sys/stat.h>
#include <fcntl.h>
#ifndef _WIN32
#include <unistd.h>
#endif
#include <sys/mman.h>

/* --------------------------------------------------------------------------
 * Constants
 * -------------------------------------------------------------------------- */

#define CSV_MAX_COLS      256
#define CSV_SAMPLE_ROWS   100

/* --------------------------------------------------------------------------
 * mmap flags
 * -------------------------------------------------------------------------- */

#ifdef __linux__
  #define MMAP_FLAGS (MAP_PRIVATE | MAP_POPULATE)
#else
  #define MMAP_FLAGS MAP_PRIVATE
#endif

/* --------------------------------------------------------------------------
 * Scratch memory helpers (same pattern as exec.c).
 * Uses td_alloc/td_free (buddy allocator) instead of malloc/free.
 * -------------------------------------------------------------------------- */

static inline void* scratch_alloc(td_t** hdr_out, size_t nbytes) {
    td_t* h = td_alloc(nbytes);
    if (!h) { *hdr_out = NULL; return NULL; }
    *hdr_out = h;
    return td_data(h);
}

static inline void* scratch_realloc(td_t** hdr_out, size_t old_bytes, size_t new_bytes) {
    td_t* old_h = *hdr_out;
    td_t* new_h = td_alloc(new_bytes);
    if (!new_h) return NULL;
    void* new_p = td_data(new_h);
    if (old_h) {
        memcpy(new_p, td_data(old_h), old_bytes < new_bytes ? old_bytes : new_bytes);
        td_free(old_h);
    }
    *hdr_out = new_h;
    return new_p;
}

static inline void scratch_free(td_t* hdr) {
    if (hdr) td_free(hdr);
}

/* Hash uses wyhash from ops/hash.h (td_hash_bytes) — much faster than FNV-1a
 * for short strings typical in CSV columns. */

/* --------------------------------------------------------------------------
 * Per-worker local symbol table
 *
 * Workers must NOT call td_sym_intern() because it allocates string atoms
 * on the calling thread's arena. Worker arenas are destroyed when the pool
 * shuts down, but the global sym table outlives workers — creating dangling
 * pointers. Instead, each worker gets one local_sym_t per string column.
 * Strings are interned locally (no locks). After the parallel parse, the
 * main thread merges local tables into the global sym table.
 *
 * Workers init their local_sym on first use (same-thread alloc via scratch).
 * Main thread frees after merge (cross-thread free → return queue).
 * -------------------------------------------------------------------------- */

#define LSYM_INIT_BUCKETS 128
#define LSYM_INIT_STRS    64
#define LSYM_INIT_ARENA   4096
#define LSYM_LOAD_FACTOR  0.7

/* Pack worker_id + local_id into uint32_t for string columns.
 * Upper 8 bits = worker_id (max 256 workers).
 * Lower 24 bits = local_id (max 16M unique strings per worker per column). */
#define PACK_SYM(wid, lid) (((uint32_t)(wid) << 24) | (lid))
#define UNPACK_WID(packed) ((packed) >> 24)
#define UNPACK_LID(packed) ((packed) & 0x00FFFFFFu)

typedef struct {
    td_t*      buckets_hdr;
    uint64_t*  buckets;      /* (hash<<32) | (id+1), 0 = empty */
    uint32_t   bucket_cap;   /* power of 2 */
    td_t*      offsets_hdr;
    uint32_t*  offsets;      /* offsets[id] = byte offset into arena */
    td_t*      lens_hdr;
    uint32_t*  lens;         /* lens[id] = string length */
    uint32_t   count;
    uint32_t   cap;
    td_t*      arena_hdr;
    char*      arena;        /* growable buffer for string copies */
    size_t     arena_used;
    size_t     arena_cap;
} local_sym_t;

static void local_sym_init(local_sym_t* ls) {
    ls->bucket_cap = LSYM_INIT_BUCKETS;
    /* Use td_sys_alloc (mmap-backed) instead of scratch_alloc (buddy) because
     * workers allocate these buffers but the main thread frees them after merge.
     * Buddy allocator td_free() only works on the allocating thread's arena —
     * cross-thread free silently leaks.  td_sys_alloc/td_sys_free are safe
     * across threads (each allocation is an independent mmap). */
    ls->buckets = (uint64_t*)td_sys_alloc(ls->bucket_cap * sizeof(uint64_t));
    ls->buckets_hdr = NULL; /* unused with td_sys_alloc */
    ls->offsets = NULL;
    ls->offsets_hdr = NULL;
    ls->lens = NULL;
    ls->lens_hdr = NULL;
    ls->arena = NULL;
    ls->arena_hdr = NULL;
    ls->count = 0;
    ls->arena_used = 0;
    if (!ls->buckets) { ls->buckets = NULL; return; }
    memset(ls->buckets, 0, ls->bucket_cap * sizeof(uint64_t));
    ls->cap = LSYM_INIT_STRS;
    ls->offsets = (uint32_t*)td_sys_alloc(ls->cap * sizeof(uint32_t));
    if (!ls->offsets) { td_sys_free(ls->buckets); ls->buckets = NULL; return; }
    ls->lens = (uint32_t*)td_sys_alloc(ls->cap * sizeof(uint32_t));
    if (!ls->lens) { td_sys_free(ls->buckets); ls->buckets = NULL; td_sys_free(ls->offsets); ls->offsets = NULL; return; }
    ls->arena_cap = LSYM_INIT_ARENA;
    ls->arena = (char*)td_sys_alloc(ls->arena_cap);
    if (!ls->arena) { td_sys_free(ls->buckets); ls->buckets = NULL; td_sys_free(ls->offsets); ls->offsets = NULL; td_sys_free(ls->lens); ls->lens = NULL; return; }
}

static void local_sym_free(local_sym_t* ls) {
    if (ls->buckets) td_sys_free(ls->buckets);
    if (ls->offsets) td_sys_free(ls->offsets);
    if (ls->lens)    td_sys_free(ls->lens);
    if (ls->arena)   td_sys_free(ls->arena);
    memset(ls, 0, sizeof(*ls));
}

/* OOM during rehash degrades to O(n) lookup but doesn't crash.
 * Load factor 0.7 ensures probing terminates. */
static void local_sym_rehash(local_sym_t* ls) {
    if (ls->bucket_cap > UINT32_MAX / 2) return; /* overflow guard */
    uint32_t new_cap = ls->bucket_cap * 2;
    uint64_t* new_buckets = (uint64_t*)td_sys_alloc(new_cap * sizeof(uint64_t));
    if (!new_buckets) return;
    memset(new_buckets, 0, new_cap * sizeof(uint64_t));

    uint32_t new_mask = new_cap - 1;
    for (uint32_t i = 0; i < ls->bucket_cap; i++) {
        uint64_t e = ls->buckets[i];
        if (e == 0) continue;
        uint32_t h = (uint32_t)(e >> 32);
        uint32_t slot = h & new_mask;
        while (new_buckets[slot] != 0) slot = (slot + 1) & new_mask;
        new_buckets[slot] = e;
    }
    td_sys_free(ls->buckets);
    ls->buckets = new_buckets;
    ls->bucket_cap = new_cap;
}

static uint32_t local_sym_intern(local_sym_t* ls, const char* str, size_t len) {
    if (TD_UNLIKELY(!ls->buckets || !ls->offsets || !ls->lens || !ls->arena))
        return UINT32_MAX;
    uint32_t hash = (uint32_t)td_hash_bytes(str, len);
    uint32_t mask = ls->bucket_cap - 1;
    uint32_t slot = hash & mask;

    for (;;) {
        uint64_t e = ls->buckets[slot];
        if (e == 0) break;
        uint32_t e_hash = (uint32_t)(e >> 32);
        if (e_hash == hash) {
            uint32_t e_id = (uint32_t)(e & 0xFFFFFFFF) - 1;
            if (ls->lens[e_id] == (uint32_t)len &&
                memcmp(ls->arena + ls->offsets[e_id], str, len) == 0)
                return e_id;
        }
        slot = (slot + 1) & mask;
    }

    uint32_t new_id = ls->count;

    if (new_id >= ls->cap) {
        if (ls->cap > UINT32_MAX / 2) return UINT32_MAX;
        uint32_t new_cap = ls->cap * 2;
        uint32_t* new_offsets = (uint32_t*)td_sys_realloc(ls->offsets,
            new_cap * sizeof(uint32_t));
        if (TD_UNLIKELY(!new_offsets)) return UINT32_MAX;
        ls->offsets = new_offsets;
        uint32_t* new_lens = (uint32_t*)td_sys_realloc(ls->lens,
            new_cap * sizeof(uint32_t));
        if (TD_UNLIKELY(!new_lens)) return UINT32_MAX;
        ls->lens = new_lens;
        ls->cap = new_cap;
    }

    /* Guard against u32 truncation when storing arena offset */
    if (ls->arena_used > UINT32_MAX - len - 1) return UINT32_MAX;

    if (ls->arena_used + len > ls->arena_cap) {
        if (ls->arena_cap > SIZE_MAX / 2) return UINT32_MAX;
        size_t new_acap = ls->arena_cap * 2;
        while (new_acap < ls->arena_used + len) {
            if (new_acap > SIZE_MAX / 2) return UINT32_MAX;
            new_acap *= 2;
        }
        char* new_arena = (char*)td_sys_realloc(ls->arena, new_acap);
        if (TD_UNLIKELY(!new_arena)) return UINT32_MAX;
        ls->arena = new_arena;
        ls->arena_cap = new_acap;
    }
    memcpy(ls->arena + ls->arena_used, str, len);
    ls->offsets[new_id] = (uint32_t)ls->arena_used;
    ls->lens[new_id] = (uint32_t)len;
    ls->arena_used += len;
    ls->count++;

    uint64_t entry = ((uint64_t)hash << 32) | ((uint64_t)(new_id + 1));
    ls->buckets[slot] = entry;

    if ((double)ls->count / (double)ls->bucket_cap > LSYM_LOAD_FACTOR) {
        local_sym_rehash(ls);
    }

    return new_id;
}

/* --------------------------------------------------------------------------
 * Type inference
 * -------------------------------------------------------------------------- */

typedef enum {
    CSV_TYPE_UNKNOWN = 0,
    CSV_TYPE_BOOL,
    CSV_TYPE_I64,
    CSV_TYPE_F64,
    CSV_TYPE_STR,
    CSV_TYPE_DATE,
    CSV_TYPE_TIME,
    CSV_TYPE_TIMESTAMP
} csv_type_t;

static csv_type_t detect_type(const char* f, size_t len) {
    if (len == 0) return CSV_TYPE_UNKNOWN;

    /* NaN/Inf literals → float */
    if (len == 3) {
        if ((f[0]=='n'||f[0]=='N') && (f[1]=='a'||f[1]=='A') && (f[2]=='n'||f[2]=='N'))
            return CSV_TYPE_F64;
        if ((f[0]=='i'||f[0]=='I') && (f[1]=='n'||f[1]=='N') && (f[2]=='f'||f[2]=='F'))
            return CSV_TYPE_F64;
    }
    if ((len == 4 && (f[0]=='+' || f[0]=='-')) &&
        (f[1]=='i'||f[1]=='I') && (f[2]=='n'||f[2]=='N') && (f[3]=='f'||f[3]=='F'))
        return CSV_TYPE_F64;

    /* Boolean */
    if ((len == 4 && memcmp(f, "true", 4) == 0) ||
        (len == 5 && memcmp(f, "false", 5) == 0) ||
        (len == 4 && memcmp(f, "TRUE", 4) == 0) ||
        (len == 5 && memcmp(f, "FALSE", 5) == 0))
        return CSV_TYPE_BOOL;

    /* Numeric scan */
    const char* p = f;
    const char* end = f + len;
    if (*p == '-' || *p == '+') p++;
    bool has_dot = false, has_e = false, has_digit = false;
    while (p < end) {
        unsigned char c = (unsigned char)*p;
        if (c >= '0' && c <= '9') { has_digit = true; p++; continue; }
        if (c == '.' && !has_dot) { has_dot = true; p++; continue; }
        if ((c == 'e' || c == 'E') && !has_e) {
            has_e = true; p++;
            if (p < end && (*p == '-' || *p == '+')) p++;
            continue;
        }
        break;
    }
    if (p == end && has_digit) {
        if (!has_dot && !has_e) return CSV_TYPE_I64;
        return CSV_TYPE_F64;
    }

    /* Date: YYYY-MM-DD (exactly 10 chars) or Timestamp: YYYY-MM-DD{T| }HH:MM:SS */
    if (len >= 10 && f[4] == '-' && f[7] == '-') {
        bool is_date = true;
        for (int i = 0; i < 10; i++) {
            if (i == 4 || i == 7) continue;
            if ((unsigned)(f[i] - '0') > 9) { is_date = false; break; }
        }
        if (is_date) {
            if (len == 10) return CSV_TYPE_DATE;
            if (len >= 19 && (f[10] == 'T' || f[10] == ' ') &&
                f[13] == ':' && f[16] == ':') {
                const int tp[] = {11,12,14,15,17,18};
                bool is_ts = true;
                for (int i = 0; i < 6; i++) {
                    if ((unsigned)(f[tp[i]] - '0') > 9) { is_ts = false; break; }
                }
                if (is_ts) return CSV_TYPE_TIMESTAMP;
            }
        }
    }

    /* Time: HH:MM:SS[.ffffff] (at least 8 chars) */
    if (len >= 8 && f[2] == ':' && f[5] == ':') {
        const int tp[] = {0,1,3,4,6,7};
        bool is_time = true;
        for (int i = 0; i < 6; i++) {
            if ((unsigned)(f[tp[i]] - '0') > 9) { is_time = false; break; }
        }
        if (is_time) return CSV_TYPE_TIME;
    }

    return CSV_TYPE_STR;
}

static csv_type_t promote_csv_type(csv_type_t cur, csv_type_t obs) {
    if (cur == CSV_TYPE_UNKNOWN) return obs;
    if (obs == CSV_TYPE_UNKNOWN) return cur;
    if (cur == obs) return cur;
    if (cur == CSV_TYPE_STR || obs == CSV_TYPE_STR) return CSV_TYPE_STR;
    /* DATE + TIMESTAMP → TIMESTAMP */
    if ((cur == CSV_TYPE_DATE && obs == CSV_TYPE_TIMESTAMP) ||
        (cur == CSV_TYPE_TIMESTAMP && obs == CSV_TYPE_DATE))
        return CSV_TYPE_TIMESTAMP;
    /* Numeric promotion: BOOL ⊂ I64 ⊂ F64 (enum values 1 < 2 < 3) */
    if (cur <= CSV_TYPE_F64 && obs <= CSV_TYPE_F64) {
        if (cur == CSV_TYPE_F64 || obs == CSV_TYPE_F64) return CSV_TYPE_F64;
        if (cur == CSV_TYPE_I64 || obs == CSV_TYPE_I64) return CSV_TYPE_I64;
        return cur;
    }
    /* All other mixed types (e.g. DATE+I64, TIME+BOOL) → STR */
    return CSV_TYPE_STR;
}

/* --------------------------------------------------------------------------
 * Zero-copy field scanner
 *
 * Returns pointer past the field's trailing delimiter (or at newline/end).
 * Sets *out and *out_len to the field content. For unquoted fields, *out
 * points directly into the mmap buffer. For quoted fields with escaped
 * quotes, content is unescaped into esc_buf.
 * -------------------------------------------------------------------------- */

static const char* scan_field_quoted(const char* p, const char* buf_end,
                                     char delim,
                                     const char** out, size_t* out_len,
                                     char* esc_buf, char** dyn_esc) {
    p++; /* skip opening quote */
    const char* fld_start = p;
    bool has_escape = false;

    while (p < buf_end) {
        if (*p == '"') {
            if (p + 1 < buf_end && *(p + 1) == '"') {
                has_escape = true;
                p += 2;
            } else {
                break; /* closing quote */
            }
        } else {
            p++;
        }
    }
    size_t raw_len = (size_t)(p - fld_start);
    if (p < buf_end && *p == '"') p++; /* skip closing quote */

    if (has_escape) {
        /* raw_len >= output length (quotes are collapsed); no overflow. */
        char* dest = esc_buf;
        if (TD_UNLIKELY(raw_len > 8192)) {
            /* Field too large for stack buffer — dynamically allocate */
            dest = (char*)td_sys_alloc(raw_len);
            if (!dest) {
                /* OOM: fall back to raw (quotes remain) */
                *out = fld_start;
                *out_len = raw_len;
                goto advance;
            }
            *dyn_esc = dest;
        }
        size_t olen = 0;
        for (const char* s = fld_start; s < fld_start + raw_len; s++) {
            if (*s == '"' && s + 1 < fld_start + raw_len && *(s + 1) == '"') {
                dest[olen++] = '"';
                s++;
            } else {
                dest[olen++] = *s;
            }
        }
        *out = dest;
        *out_len = olen;
    } else {
        *out = fld_start;
        *out_len = raw_len;
    }

advance:
    /* Advance past delimiter */
    if (p < buf_end && *p == delim) p++;
    /* Don't advance past newline — caller handles row boundaries */
    return p;
}

TD_INLINE const char* scan_field(const char* p, const char* buf_end,
                                  char delim,
                                  const char** out, size_t* out_len,
                                  char* esc_buf, char** dyn_esc) {
    if (TD_UNLIKELY(p >= buf_end)) {
        *out = p;
        *out_len = 0;
        return p;
    }

    if (TD_LIKELY(*p != '"')) {
        /* Unquoted field — fast path */
        const char* s = p;
        while (p < buf_end && *p != delim && *p != '\n' && *p != '\r') p++;
        *out = s;
        *out_len = (size_t)(p - s);
        if (p < buf_end && *p == delim) return p + 1;
        return p;
    }

    return scan_field_quoted(p, buf_end, delim, out, out_len, esc_buf, dyn_esc);
}

/* --------------------------------------------------------------------------
 * Fast inline integer parser (replaces strtoll)
 * -------------------------------------------------------------------------- */

TD_INLINE int64_t fast_i64(const char* p, size_t len) {
    if (TD_UNLIKELY(len == 0)) return 0;

    const char* end = p + len;
    const char* start = p;
    bool neg = false;
    if (*p == '-') { neg = true; p++; }
    else if (*p == '+') { p++; }

    /* Count digit span; if >18, fall back to strtoll to avoid overflow */
    size_t digit_len = 0;
    for (const char* q = p; q < end && (unsigned char)(*q - '0') <= 9; q++)
        digit_len++;
    if (TD_UNLIKELY(digit_len > 18)) {
        /* max int64 = 20 chars; 31-byte limit safe for valid integers. */
        char tmp[32];
        size_t slen = (size_t)(end - start);
        if (slen > sizeof(tmp) - 1) slen = sizeof(tmp) - 1;
        memcpy(tmp, start, slen);
        tmp[slen] = '\0';
        return strtoll(tmp, NULL, 10);
    }

    uint64_t val = 0;
    while (p < end) {
        unsigned d = (unsigned char)*p - '0';
        if (d > 9) break; /* stop on non-digit */
        val = val * 10 + d;
        p++;
    }
    /* 18-digit values may exceed int64 range; fall back to strtoll for safety */
    if (TD_UNLIKELY(digit_len == 18)) {
        uint64_t limit = neg ? (uint64_t)INT64_MAX + 1u : (uint64_t)INT64_MAX;
        if (val > limit) {
            char tmp[32];
            size_t slen = (size_t)(end - start);
            if (slen > sizeof(tmp) - 1) slen = sizeof(tmp) - 1;
            memcpy(tmp, start, slen);
            tmp[slen] = '\0';
            return strtoll(tmp, NULL, 10);
        }
    }
    /* Negate in unsigned to avoid signed overflow UB */
    return neg ? (int64_t)(~val + 1u) : (int64_t)val;
}

/* --------------------------------------------------------------------------
 * Fast inline float parser (replaces strtod)
 *
 * Handles: [+-]digits[.digits][eE[+-]digits]
 * Uses pow10 lookup table for exponents up to +/-22.
 * -------------------------------------------------------------------------- */

static const double g_pow10[] = {
    1e0,  1e1,  1e2,  1e3,  1e4,  1e5,  1e6,  1e7,
    1e8,  1e9,  1e10, 1e11, 1e12, 1e13, 1e14, 1e15,
    1e16, 1e17, 1e18, 1e19, 1e20, 1e21, 1e22
};

TD_INLINE double fast_f64(const char* p, size_t len) {
    if (TD_UNLIKELY(len == 0)) return 0.0;

    /* NaN/Inf string literals — check before numeric parse */
    if (TD_UNLIKELY(len <= 4)) {
        if (len == 3 &&
            (p[0]=='n'||p[0]=='N') && (p[1]=='a'||p[1]=='A') && (p[2]=='n'||p[2]=='N'))
            return __builtin_nan("");
        if (len == 3 &&
            (p[0]=='i'||p[0]=='I') && (p[1]=='n'||p[1]=='N') && (p[2]=='f'||p[2]=='F'))
            return __builtin_inf();
        if (len == 4 && p[0] == '+' &&
            (p[1]=='i'||p[1]=='I') && (p[2]=='n'||p[2]=='N') && (p[3]=='f'||p[3]=='F'))
            return __builtin_inf();
        if (len == 4 && p[0] == '-' &&
            (p[1]=='i'||p[1]=='I') && (p[2]=='n'||p[2]=='N') && (p[3]=='f'||p[3]=='F'))
            return -__builtin_inf();
    }

    const char* start = p;
    const char* end = p + len;
    int negative = 0;
    if (*p == '-') { negative = 1; p++; }
    else if (*p == '+') { p++; }

    /* Integer part */
    uint64_t int_part = 0;
    int idigits = 0;
    while (p < end && (unsigned)(*p - '0') < 10) {
        int_part = int_part * 10 + (uint64_t)(*p - '0');
        idigits++;
        p++;
        if (idigits > 18) goto strtod_fallback;
    }
    /* 18-digit integer parts risk uint64 overflow in subsequent mul;
     * fall back to strtod for exact conversion. */
    if (TD_UNLIKELY(idigits == 18 && int_part > (uint64_t)999999999999999999ULL))
        goto strtod_fallback;
    double val = (double)int_part;

    /* Fractional part */
    if (p < end && *p == '.') {
        p++;
        uint64_t frac = 0;
        int frac_digits = 0;
        while (p < end && (unsigned)(*p - '0') < 10) {
            frac = frac * 10 + (uint64_t)(*p - '0');
            frac_digits++;
            p++;
            if (frac_digits > 18) {
                /* Cap fractional accumulation — skip remaining fractional digits */
                while (p < end && (unsigned)(*p - '0') < 10) p++;
                break;
            }
        }
        if (frac_digits > 0 && frac_digits <= 22) {
            val += (double)frac / g_pow10[frac_digits];
        } else if (frac_digits > 0) {
            double f = (double)frac;
            int d = frac_digits;
            while (d > 22) { f /= 1e22; d -= 22; }
            f /= g_pow10[d];
            val += f;
        }
    }

    /* Exponent */
    if (p < end && (*p == 'e' || *p == 'E')) {
        p++;
        int exp_neg = 0;
        if (p < end) {
            if (*p == '-') { exp_neg = 1; p++; }
            else if (*p == '+') { p++; }
        }
        int exp_val = 0;
        while (p < end && (unsigned)(*p - '0') < 10) {
            exp_val = exp_val * 10 + (*p - '0');
            /* Clamp exponent to avoid int overflow on crafted input (e.g. 1e9999999999).
             * 10^999 is far beyond double range (max ~10^308), so the result
             * will be inf/0.0 anyway, but the integer arithmetic stays defined. */
            if (exp_val > 999) exp_val = 999;
            p++;
        }
        if (exp_val <= 22) {
            if (exp_neg) val /= g_pow10[exp_val];
            else         val *= g_pow10[exp_val];
        } else {
            int e = exp_val;
            if (exp_neg) {
                while (e > 22) { val /= 1e22; e -= 22; }
                val /= g_pow10[e];
            } else {
                while (e > 22) { val *= 1e22; e -= 22; }
                val *= g_pow10[e];
            }
        }
    }

    return negative ? -val : val;

strtod_fallback:
    {
        char tmp[64];
        size_t slen = (size_t)(end - start);
        if (slen > sizeof(tmp) - 1) slen = sizeof(tmp) - 1;
        memcpy(tmp, start, slen);
        tmp[slen] = '\0';
        return strtod(tmp, NULL);
    }
}

/* --------------------------------------------------------------------------
 * Fast inline date/time parsers
 *
 * DATE:      YYYY-MM-DD        → int32_t  (days since 1970-01-01)
 * TIME:      HH:MM:SS[.ffffff] → int64_t  (microseconds since midnight)
 * TIMESTAMP: YYYY-MM-DD{T| }HH:MM:SS[.ffffff] → int64_t (µs since epoch)
 *
 * Uses Howard Hinnant's civil-calendar algorithm (public domain) for the
 * date→days conversion — O(1), no tables, no branches.
 * -------------------------------------------------------------------------- */

TD_INLINE int32_t civil_to_days(int y, int m, int d) {
    /* Shift Jan/Feb to months 10/11 of the previous year */
    if (m <= 2) { y--; m += 9; } else { m -= 3; }
    int era = (y >= 0 ? y : y - 399) / 400;
    int yoe = y - era * 400;
    int doy = (153 * m + 2) / 5 + d - 1;
    int doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    return (int32_t)(era * 146097 + doe - 719468);
}

TD_INLINE int32_t fast_date(const char* p, size_t len) {
    if (TD_UNLIKELY(len < 10)) return 0;
    int y = (p[0]-'0')*1000 + (p[1]-'0')*100 + (p[2]-'0')*10 + (p[3]-'0');
    int m = (p[5]-'0')*10 + (p[6]-'0');
    int d = (p[8]-'0')*10 + (p[9]-'0');
    if (TD_UNLIKELY(m < 1 || m > 12 || d < 1 || d > 31)) return 0;
    return civil_to_days(y, m, d);
}

TD_INLINE int64_t fast_time(const char* p, size_t len) {
    if (TD_UNLIKELY(len < 8)) return 0;
    int h  = (p[0]-'0')*10 + (p[1]-'0');
    int mi = (p[3]-'0')*10 + (p[4]-'0');
    int s  = (p[6]-'0')*10 + (p[7]-'0');
    if (TD_UNLIKELY(h > 23 || mi > 59 || s > 59)) return 0;
    int64_t us = (int64_t)h * 3600000000LL + (int64_t)mi * 60000000LL +
                 (int64_t)s * 1000000LL;
    /* Fractional seconds → microseconds */
    if (len > 8 && p[8] == '.') {
        int frac = 0, digits = 0;
        for (size_t i = 9; i < len && digits < 6; i++, digits++) {
            unsigned di = (unsigned char)p[i] - '0';
            if (di > 9) break;
            frac = frac * 10 + (int)di;
        }
        while (digits < 6) { frac *= 10; digits++; }
        us += frac;
    }
    return us;
}

TD_INLINE int64_t fast_timestamp(const char* p, size_t len) {
    if (TD_UNLIKELY(len < 19)) return 0;
    int32_t days = fast_date(p, 10);
    int64_t time_us = fast_time(p + 11, len - 11);
    return (int64_t)days * 86400000000LL + time_us;
}

/* --------------------------------------------------------------------------
 * Row offsets builder — memchr-accelerated
 *
 * Uses memchr (glibc: SIMD-accelerated ~15-20 GB/s) for newline scanning.
 * Fast path for quote-free files; falls back to byte-by-byte for quoted
 * fields with embedded newlines. Returns exact row count.
 *
 * Allocates offsets via scratch_alloc. Caller frees with scratch_free.
 * -------------------------------------------------------------------------- */

static int64_t build_row_offsets(const char* buf, size_t buf_size,
                                  size_t data_offset,
                                  int64_t** offsets_out, td_t** hdr_out) {
    const char* p = buf + data_offset;
    const char* end = buf + buf_size;

    /* Skip leading blank lines */
    while (p < end && (*p == '\r' || *p == '\n')) p++;
    if (p >= end) { *offsets_out = NULL; *hdr_out = NULL; return 0; }

    /* Estimate capacity: ~40 bytes per row + headroom.
     * 40 bytes/row is conservative for typical CSVs; realloc path handles
     * underestimates. */
    size_t remaining = (size_t)(end - p);
    int64_t est = (int64_t)(remaining / 40) + 16;
    td_t* hdr = NULL;
    int64_t* offs = (int64_t*)scratch_alloc(&hdr, (size_t)est * sizeof(int64_t));
    if (!offs) { *offsets_out = NULL; *hdr_out = NULL; return 0; }

    int64_t n = 0;
    offs[n++] = (int64_t)(p - buf);

    /* Check if file has any quotes — determines fast vs slow path */
    bool has_quotes = (memchr(p, '"', remaining) != NULL);

    if (TD_LIKELY(!has_quotes)) {
        /* Fast path: no quotes, use memchr for newlines.
         * Only scans for \n; pure \r line endings (old Mac) treated as single row. */
        for (;;) {
            const char* nl = (const char*)memchr(p, '\n', (size_t)(end - p));
            if (!nl) break;
            p = nl + 1;
            /* Skip \r and consecutive blank lines */
            while (p < end && (*p == '\r' || *p == '\n')) p++;
            if (p >= end) break;

            if (n >= est) {
                est *= 2;
                offs = (int64_t*)scratch_realloc(&hdr,
                    (size_t)n * sizeof(int64_t),
                    (size_t)est * sizeof(int64_t));
                if (!offs) { scratch_free(hdr); *offsets_out = NULL; *hdr_out = NULL; return 0; }
            }
            offs[n++] = (int64_t)(p - buf);
        }
    } else {
        /* Slow path: track quote parity, byte-by-byte */
        bool in_quote = false;
        while (p < end) {
            char c = *p;
            if (c == '"') {
                in_quote = !in_quote;
                p++;
            } else if (!in_quote && (c == '\n' || c == '\r')) {
                if (c == '\r' && p + 1 < end && *(p + 1) == '\n') p++;
                p++;
                while (p < end && (*p == '\r' || *p == '\n')) p++;
                if (p < end) {
                    if (n >= est) {
                        est *= 2;
                        offs = (int64_t*)scratch_realloc(&hdr,
                            (size_t)n * sizeof(int64_t),
                            (size_t)est * sizeof(int64_t));
                        if (!offs) { scratch_free(hdr); *offsets_out = NULL; *hdr_out = NULL; return 0; }
                    }
                    offs[n++] = (int64_t)(p - buf);
                }
            } else {
                p++;
            }
        }
    }

    *offsets_out = offs;
    *hdr_out = hdr;
    return n;
}

/* --------------------------------------------------------------------------
 * Merge per-worker local sym tables into global sym table and fix up
 * the packed (worker_id, local_id) values in string columns.
 *
 * Runs on the main thread so td_sym_intern allocates on the main arena
 * (which outlives workers). Uses VLAs for small arrays.
 * -------------------------------------------------------------------------- */

/* Context for parallel sym fixup dispatch */
typedef struct {
    uint32_t*  data;
    int64_t**  mappings;
    uint32_t*  mapping_counts; /* count per worker for bounds checking */
    uint32_t   n_workers;
} sym_fixup_ctx_t;

static void sym_fixup_fn(void* arg, uint32_t worker_id, int64_t start, int64_t end) {
    (void)worker_id;
    sym_fixup_ctx_t* c = (sym_fixup_ctx_t*)arg;
    uint32_t* restrict data = c->data;
    int64_t** restrict mappings = c->mappings;
    uint32_t* restrict mcounts = c->mapping_counts;
    for (int64_t r = start; r < end; r++) {
        uint32_t packed = data[r];
        uint32_t wid = UNPACK_WID(packed);
        if (wid >= c->n_workers) continue; /* corrupted packed value */
        uint32_t lid = UNPACK_LID(packed);
        if (mappings[wid] && lid < mcounts[wid])
            data[r] = (uint32_t)mappings[wid][lid];
        else
            data[r] = 0; /* fallback for corrupt packed value */
    }
}

static void merge_local_syms(local_sym_t* local_syms, uint32_t n_workers,
                              int n_cols, const csv_type_t* col_types,
                              void** col_data, int64_t n_rows,
                              td_pool_t* pool, int64_t* col_max_ids) {
    for (int c = 0; c < n_cols; c++) {
        if (col_types[c] != CSV_TYPE_STR) continue;

        /* Build per-worker mappings: local_id → global sym_id (VLA) */
        int64_t* mappings[n_workers];
        td_t* map_hdrs[n_workers];
        uint32_t mapping_counts[n_workers]; /* count per worker for lid bounds checking */
        for (uint32_t w = 0; w < n_workers; w++) {
            mappings[w] = NULL;
            map_hdrs[w] = NULL;
            mapping_counts[w] = 0;
        }

        for (uint32_t w = 0; w < n_workers; w++) {
            local_sym_t* ls = &local_syms[(size_t)w * (size_t)n_cols + (size_t)c];
            if (ls->count == 0) continue;

            mappings[w] = (int64_t*)scratch_alloc(&map_hdrs[w],
                                                    ls->count * sizeof(int64_t));
            if (!mappings[w]) continue;
            mapping_counts[w] = ls->count;
            for (uint32_t i = 0; i < ls->count; i++) {
                mappings[w][i] = td_sym_intern(
                    ls->arena + ls->offsets[i], ls->lens[i]);
                if (mappings[w][i] < 0) mappings[w][i] = 0;
            }
        }

        /* Track max global sym_id for adaptive width narrowing */
        int64_t max_id = 0;
        for (uint32_t w = 0; w < n_workers; w++) {
            for (uint32_t i = 0; i < mapping_counts[w]; i++) {
                if (mappings[w][i] > max_id) max_id = mappings[w][i];
            }
        }
        if (col_max_ids) col_max_ids[c] = max_id;

        /* Fix up column data: parallel unpack (wid, lid) → global sym_id.
         * Mappings are read-only at this point — no contention. */
        uint32_t* data = (uint32_t*)col_data[c];
        if (pool && n_rows > 1024) {
            sym_fixup_ctx_t ctx = { .data = data, .mappings = mappings,
                                    .mapping_counts = mapping_counts,
                                    .n_workers = n_workers };
            td_pool_dispatch(pool, sym_fixup_fn, &ctx, n_rows);
        } else {
            for (int64_t r = 0; r < n_rows; r++) {
                uint32_t packed = data[r];
                uint32_t wid = UNPACK_WID(packed);
                if (wid >= n_workers) continue;
                uint32_t lid = UNPACK_LID(packed);
                if (mappings[wid] && lid < mapping_counts[wid])
                    data[r] = (uint32_t)mappings[wid][lid];
                else
                    data[r] = 0; /* fallback for corrupt packed value */
            }
        }

        for (uint32_t w = 0; w < n_workers; w++) scratch_free(map_hdrs[w]);
    }
}

/* --------------------------------------------------------------------------
 * Parallel parse context and callback
 * -------------------------------------------------------------------------- */

typedef struct {
    const char*       buf;
    size_t            buf_size;
    const int64_t*    row_offsets;
    int64_t           n_rows;
    int               n_cols;
    char              delim;
    const csv_type_t* col_types;
    void**            col_data;     /* non-const: workers write parsed values into columns */
    local_sym_t*      local_syms;   /* [n_workers * n_cols] */
    uint32_t          n_workers;
} csv_par_ctx_t;

static void csv_parse_fn(void* arg, uint32_t worker_id,
                          int64_t start, int64_t end_row) {
    csv_par_ctx_t* ctx = (csv_par_ctx_t*)arg;
    char esc_buf[8192];
    const char* buf_end = ctx->buf + ctx->buf_size;
    local_sym_t* my_syms = ctx->local_syms
                           ? &ctx->local_syms[(size_t)worker_id * (size_t)ctx->n_cols]
                           : NULL;

    /* Lazy init: workers allocate their own local_sym buffers (same-thread) */
    if (my_syms) {
        for (int c = 0; c < ctx->n_cols; c++) {
            if (ctx->col_types[c] == CSV_TYPE_STR && my_syms[c].buckets == NULL)
                local_sym_init(&my_syms[c]);
        }
    }

    for (int64_t row = start; row < end_row; row++) {
        const char* p = ctx->buf + ctx->row_offsets[row];
        const char* row_end = (row + 1 < ctx->n_rows)
            ? ctx->buf + ctx->row_offsets[row + 1]
            : buf_end;

        for (int c = 0; c < ctx->n_cols; c++) {
            /* Guard: if past row boundary, fill remaining columns with defaults */
            if (p >= row_end) {
                for (; c < ctx->n_cols; c++) {
                    switch (ctx->col_types[c]) {
                        case CSV_TYPE_BOOL: ((uint8_t*)ctx->col_data[c])[row] = 0; break;
                        case CSV_TYPE_I64:  ((int64_t*)ctx->col_data[c])[row] = 0; break;
                        case CSV_TYPE_F64:  ((double*)ctx->col_data[c])[row] = 0.0; break;
                        case CSV_TYPE_DATE: ((int32_t*)ctx->col_data[c])[row] = 0; break;
                        case CSV_TYPE_TIME: case CSV_TYPE_TIMESTAMP:
                            ((int64_t*)ctx->col_data[c])[row] = 0; break;
                        case CSV_TYPE_STR:  ((uint32_t*)ctx->col_data[c])[row] = 0; break;
                        default: break;
                    }
                }
                break;
            }

            const char* fld;
            size_t flen;
            char* dyn_esc = NULL;
            p = scan_field(p, buf_end, ctx->delim, &fld, &flen, esc_buf, &dyn_esc);

            /* Strip trailing \r from last field of row */
            if (c == ctx->n_cols - 1 && flen > 0 && fld[flen - 1] == '\r')
                flen--;

            switch (ctx->col_types[c]) {
                case CSV_TYPE_BOOL: {
                    uint8_t v = (flen > 0 && (fld[0] == 't' || fld[0] == 'T' || fld[0] == '1')) ? 1 : 0;
                    ((uint8_t*)ctx->col_data[c])[row] = v;
                    break;
                }
                case CSV_TYPE_I64:
                    ((int64_t*)ctx->col_data[c])[row] = fast_i64(fld, flen);
                    break;
                case CSV_TYPE_F64:
                    ((double*)ctx->col_data[c])[row] = fast_f64(fld, flen);
                    break;
                case CSV_TYPE_DATE:
                    ((int32_t*)ctx->col_data[c])[row] = fast_date(fld, flen);
                    break;
                case CSV_TYPE_TIME:
                    ((int64_t*)ctx->col_data[c])[row] = fast_time(fld, flen);
                    break;
                case CSV_TYPE_TIMESTAMP:
                    ((int64_t*)ctx->col_data[c])[row] = fast_timestamp(fld, flen);
                    break;
                case CSV_TYPE_STR: {
                    uint32_t lid = local_sym_intern(&my_syms[c], fld, flen);
                    if (TD_UNLIKELY(lid == UINT32_MAX)) { if (dyn_esc) td_sys_free(dyn_esc); continue; }
                    ((uint32_t*)ctx->col_data[c])[row] = PACK_SYM(worker_id, lid);
                    break;
                }
                default:
                    break;
            }
            if (TD_UNLIKELY(dyn_esc != NULL)) td_sys_free(dyn_esc);
        }
    }
}

/* --------------------------------------------------------------------------
 * Serial parse fallback (small files or no thread pool)
 * -------------------------------------------------------------------------- */

static void csv_parse_serial(const char* buf, size_t buf_size,
                              const int64_t* row_offsets, int64_t n_rows,
                              int n_cols, char delim,
                              const csv_type_t* col_types, void** col_data) {
    char esc_buf[8192];
    const char* buf_end = buf + buf_size;

    for (int64_t row = 0; row < n_rows; row++) {
        const char* p = buf + row_offsets[row];
        const char* row_end = (row + 1 < n_rows)
            ? buf + row_offsets[row + 1]
            : buf_end;

        for (int c = 0; c < n_cols; c++) {
            /* Guard: if past row boundary, fill remaining columns with defaults */
            if (p >= row_end) {
                for (; c < n_cols; c++) {
                    switch (col_types[c]) {
                        case CSV_TYPE_BOOL: ((uint8_t*)col_data[c])[row] = 0; break;
                        case CSV_TYPE_I64:  ((int64_t*)col_data[c])[row] = 0; break;
                        case CSV_TYPE_F64:  ((double*)col_data[c])[row] = 0.0; break;
                        case CSV_TYPE_DATE: ((int32_t*)col_data[c])[row] = 0; break;
                        case CSV_TYPE_TIME: case CSV_TYPE_TIMESTAMP:
                            ((int64_t*)col_data[c])[row] = 0; break;
                        case CSV_TYPE_STR:  ((uint32_t*)col_data[c])[row] = 0; break;
                        default: break;
                    }
                }
                break;
            }

            const char* fld;
            size_t flen;
            char* dyn_esc = NULL;
            p = scan_field(p, buf_end, delim, &fld, &flen, esc_buf, &dyn_esc);

            /* Strip trailing \r from last field of row */
            if (c == n_cols - 1 && flen > 0 && fld[flen - 1] == '\r')
                flen--;

            switch (col_types[c]) {
                case CSV_TYPE_BOOL: {
                    uint8_t v = (flen > 0 && (fld[0] == 't' || fld[0] == 'T' || fld[0] == '1')) ? 1 : 0;
                    ((uint8_t*)col_data[c])[row] = v;
                    break;
                }
                case CSV_TYPE_I64:
                    ((int64_t*)col_data[c])[row] = fast_i64(fld, flen);
                    break;
                case CSV_TYPE_F64:
                    ((double*)col_data[c])[row] = fast_f64(fld, flen);
                    break;
                case CSV_TYPE_DATE:
                    ((int32_t*)col_data[c])[row] = fast_date(fld, flen);
                    break;
                case CSV_TYPE_TIME:
                    ((int64_t*)col_data[c])[row] = fast_time(fld, flen);
                    break;
                case CSV_TYPE_TIMESTAMP:
                    ((int64_t*)col_data[c])[row] = fast_timestamp(fld, flen);
                    break;
                case CSV_TYPE_STR: {
                    int64_t sym_id = td_sym_intern(fld, flen);
                    if (sym_id < 0) sym_id = 0;
                    ((uint32_t*)col_data[c])[row] = (uint32_t)sym_id;
                    break;
                }
                default:
                    break;
            }
            if (TD_UNLIKELY(dyn_esc != NULL)) td_sys_free(dyn_esc);
        }
    }
}

/* --------------------------------------------------------------------------
 * td_read_csv_opts — main CSV parser
 * -------------------------------------------------------------------------- */

td_t* td_read_csv_opts(const char* path, char delimiter, bool header,
                        const int8_t* col_types_in, int32_t n_types) {
    /* ---- 1. Open file and get size ---- */
    int fd = open(path, O_RDONLY);
    if (fd < 0) return TD_ERR_PTR(TD_ERR_IO);

    struct stat st;
    if (fstat(fd, &st) != 0 || st.st_size <= 0) {
        close(fd);
        return TD_ERR_PTR(TD_ERR_IO);
    }
    size_t file_size = (size_t)st.st_size;

    /* ---- 2. mmap the file ---- */
    char* buf = (char*)mmap(NULL, file_size, PROT_READ, MMAP_FLAGS, fd, 0);
    close(fd);
    if (buf == MAP_FAILED) return TD_ERR_PTR(TD_ERR_IO);

#ifdef __APPLE__
    madvise(buf, file_size, MADV_SEQUENTIAL);
#endif

    const char* buf_end = buf + file_size;
    td_t* result = NULL;

    /* ---- 3. Detect delimiter ---- */
    /* Delimiter auto-detected from header row only. Files where the header
     * has a different delimiter distribution than data rows may be misdetected;
     * pass an explicit delimiter for such files.  Scanning additional data rows
     * was considered but adds complexity for a rare edge case. */
    if (delimiter == 0) {
        int commas = 0, tabs = 0;
        for (const char* p = buf; p < buf_end && *p != '\n'; p++) {
            if (*p == ',') commas++;
            if (*p == '\t') tabs++;
        }
        delimiter = (tabs > commas) ? '\t' : ',';
    }

    /* ---- 4. Count columns from first line ---- */
    int ncols = 1;
    {
        const char* p = buf;
        bool in_quote = false;
        while (p < buf_end && (in_quote || (*p != '\n' && *p != '\r'))) {
            if (*p == '"') in_quote = !in_quote;
            else if (!in_quote && *p == delimiter) ncols++;
            p++;
        }
    }
    /* Columns beyond CSV_MAX_COLS (256) are silently dropped. */
    if (ncols > CSV_MAX_COLS) ncols = CSV_MAX_COLS;

    /* ---- 5. Parse header row ---- */
    const char* p = buf;
    char esc_buf[8192];
    int64_t col_name_ids[CSV_MAX_COLS];

    if (header) {
        for (int c = 0; c < ncols; c++) {
            const char* fld;
            size_t flen;
            char* dyn_esc = NULL;
            p = scan_field(p, buf_end, delimiter, &fld, &flen, esc_buf, &dyn_esc);
            col_name_ids[c] = td_sym_intern(fld, flen);
            if (dyn_esc) td_sys_free(dyn_esc);
        }
        while (p < buf_end && (*p == '\r' || *p == '\n')) p++;
    } else {
        for (int c = 0; c < ncols; c++) {
            char name[32];
            snprintf(name, sizeof(name), "V%d", c + 1);
            col_name_ids[c] = td_sym_intern(name, strlen(name));
        }
    }

    size_t data_offset = (size_t)(p - buf);

    /* ---- 6. Build row offsets (memchr-accelerated) ---- */
    td_t* row_offsets_hdr = NULL;
    int64_t* row_offsets = NULL;
    int64_t n_rows = build_row_offsets(buf, file_size, data_offset,
                                        &row_offsets, &row_offsets_hdr);

    if (n_rows == 0) {
        /* Empty file → empty table */
        td_t* tbl = td_table_new(ncols);
        if (!tbl || TD_IS_ERR(tbl)) goto fail_unmap;
        for (int c = 0; c < ncols; c++) {
            td_t* empty_vec = td_vec_new(TD_F64, 0);
            if (empty_vec && !TD_IS_ERR(empty_vec)) {
                tbl = td_table_add_col(tbl, col_name_ids[c], empty_vec);
                td_release(empty_vec);
            }
        }
        munmap(buf, file_size);
        return tbl;
    }

    /* ---- 7. Resolve column types ---- */
    int8_t resolved_types[CSV_MAX_COLS];
    if (col_types_in && n_types >= ncols) {
        /* Explicit types provided by caller — validate against known types */
        for (int c = 0; c < ncols; c++) {
            int8_t t = col_types_in[c];
            if (t < TD_BOOL || t >= TD_TYPE_COUNT || t == TD_TABLE) {
                /* Invalid type constant — fall through to error */
                goto fail_offsets;
            }
            resolved_types[c] = t;
        }
    } else if (!col_types_in) {
        /* Auto-infer from sample rows */
        csv_type_t col_types[CSV_MAX_COLS];
        memset(col_types, 0, (size_t)ncols * sizeof(csv_type_t));
        /* Type inference from first 100 rows. Heterogeneous CSVs with type
         * changes after row 100 will be mistyped. Use explicit schema
         * (col_types_in) for such files. */
        int64_t sample_n = (n_rows < CSV_SAMPLE_ROWS) ? n_rows : CSV_SAMPLE_ROWS;
        for (int64_t r = 0; r < sample_n; r++) {
            const char* rp = buf + row_offsets[r];
            for (int c = 0; c < ncols; c++) {
                const char* fld;
                size_t flen;
                char* dyn_esc = NULL;
                rp = scan_field(rp, buf_end, delimiter, &fld, &flen, esc_buf, &dyn_esc);
                csv_type_t t = detect_type(fld, flen);
                if (dyn_esc) td_sys_free(dyn_esc);
                col_types[c] = promote_csv_type(col_types[c], t);
            }
        }
        for (int c = 0; c < ncols; c++) {
            switch (col_types[c]) {
                case CSV_TYPE_BOOL:      resolved_types[c] = TD_BOOL;      break;
                case CSV_TYPE_I64:       resolved_types[c] = TD_I64;       break;
                case CSV_TYPE_F64:       resolved_types[c] = TD_F64;       break;
                case CSV_TYPE_DATE:      resolved_types[c] = TD_DATE;      break;
                case CSV_TYPE_TIME:      resolved_types[c] = TD_TIME;      break;
                case CSV_TYPE_TIMESTAMP: resolved_types[c] = TD_TIMESTAMP; break;
                default:                 resolved_types[c] = TD_SYM;       break;
            }
        }
    } else {
        /* col_types_in provided but too short — error */
        goto fail_offsets;
    }

    /* ---- 8. Allocate column vectors ---- */
    td_t* col_vecs[CSV_MAX_COLS];
    void* col_data[CSV_MAX_COLS];

    for (int c = 0; c < ncols; c++) {
        int8_t type = resolved_types[c];
        /* String columns: allocate TD_SYM at W32 (4B/elem) for PACK_SYM during parse.
         * After merge, narrow to W8/W16 if max sym ID permits. */
        col_vecs[c] = (type == TD_SYM) ? td_sym_vec_new(TD_SYM_W32, n_rows)
                                        : td_vec_new(type, n_rows);
        if (!col_vecs[c] || TD_IS_ERR(col_vecs[c])) {
            for (int j = 0; j < c; j++) td_release(col_vecs[j]);
            goto fail_offsets;
        }
        /* len set early so parallel workers can write to full extent;
         * parse errors return before table is used. */
        col_vecs[c]->len = n_rows;
        col_data[c] = td_data(col_vecs[c]);
    }

    /* Build csv_type_t array for parse functions (maps td types → csv types) */
    csv_type_t parse_types[CSV_MAX_COLS];
    for (int c = 0; c < ncols; c++) {
        switch (resolved_types[c]) {
            case TD_BOOL:      parse_types[c] = CSV_TYPE_BOOL;      break;
            case TD_I64:       parse_types[c] = CSV_TYPE_I64;       break;
            case TD_F64:       parse_types[c] = CSV_TYPE_F64;       break;
            case TD_DATE:      parse_types[c] = CSV_TYPE_DATE;      break;
            case TD_TIME:      parse_types[c] = CSV_TYPE_TIME;      break;
            case TD_TIMESTAMP: parse_types[c] = CSV_TYPE_TIMESTAMP; break;
            default:           parse_types[c] = CSV_TYPE_STR;       break;
        }
    }

    /* ---- 9. Parse data ---- */
    int64_t sym_max_ids[CSV_MAX_COLS];
    memset(sym_max_ids, 0, (size_t)ncols * sizeof(int64_t));
    {
        /* Check if any string columns exist */
        int has_str_cols = 0;
        for (int c = 0; c < ncols; c++) {
            if (parse_types[c] == CSV_TYPE_STR) { has_str_cols = 1; break; }
        }

        td_pool_t* pool = td_pool_get();
        bool use_parallel = pool && n_rows > 8192;

        /* PACK_SYM uses upper 8 bits for worker_id (IDs 0-255, max 256 workers).
         * If pool has >256 workers and string columns exist, fall back to serial
         * to avoid worker ID overflow in packed sym encoding. */
        if (use_parallel && has_str_cols && td_pool_total_workers(pool) > 256) {
            use_parallel = false;
        }

        if (use_parallel) {
            uint32_t n_workers = td_pool_total_workers(pool);

            /* Allocate per-worker local sym tables for string columns.
             * Zero-init so workers can lazy-init on first use. */
            td_t* local_syms_hdr = NULL;
            local_sym_t* local_syms = NULL;
            if (has_str_cols) {
                size_t lsym_sz = (size_t)n_workers * (size_t)ncols * sizeof(local_sym_t);
                local_syms = (local_sym_t*)scratch_alloc(&local_syms_hdr, lsym_sz);
                if (local_syms) {
                    memset(local_syms, 0, lsym_sz);
                } else {
                    use_parallel = false;
                }
            }

            if (use_parallel) {
                csv_par_ctx_t ctx = {
                    .buf         = buf,
                    .buf_size    = file_size,
                    .row_offsets = row_offsets,
                    .n_rows      = n_rows,
                    .n_cols      = ncols,
                    .delim       = delimiter,
                    .col_types   = parse_types,
                    .col_data    = col_data,
                    .local_syms  = local_syms,
                    .n_workers   = n_workers,
                };

                td_pool_dispatch(pool, csv_parse_fn, &ctx, n_rows);

                /* Merge local sym tables into global (main thread — safe) */
                if (has_str_cols && local_syms) {
                    merge_local_syms(local_syms, n_workers, ncols,
                                     parse_types, col_data, n_rows, pool,
                                     sym_max_ids);

                    for (uint32_t w = 0; w < n_workers; w++) {
                        for (int c = 0; c < ncols; c++) {
                            if (parse_types[c] == CSV_TYPE_STR)
                                local_sym_free(&local_syms[(size_t)w * (size_t)ncols + (size_t)c]);
                        }
                    }
                }
                scratch_free(local_syms_hdr);
            }
        }

        if (!use_parallel) {
            csv_parse_serial(buf, file_size, row_offsets, n_rows,
                             ncols, delimiter, parse_types, col_data);
            /* Serial path: scan string columns to find max sym ID */
            for (int c = 0; c < ncols; c++) {
                if (parse_types[c] != CSV_TYPE_STR) continue;
                const uint32_t* d = (const uint32_t*)col_data[c];
                uint32_t mx = 0;
                for (int64_t r = 0; r < n_rows; r++)
                    if (d[r] > mx) mx = d[r];
                sym_max_ids[c] = (int64_t)mx;
            }
        }
    }

    /* ---- 10. Narrow sym columns to optimal width ---- */
    for (int c = 0; c < ncols; c++) {
        if (resolved_types[c] != TD_SYM) continue;
        uint8_t new_w = td_sym_dict_width(sym_max_ids[c]);
        if (new_w >= TD_SYM_W32) continue; /* already at W32, no savings */
        td_t* narrow = td_sym_vec_new(new_w, n_rows);
        if (!narrow || TD_IS_ERR(narrow)) continue;
        narrow->len = n_rows;
        const uint32_t* src = (const uint32_t*)col_data[c];
        void* dst = td_data(narrow);
        if (new_w == TD_SYM_W8) {
            uint8_t* d = (uint8_t*)dst;
            for (int64_t r = 0; r < n_rows; r++) d[r] = (uint8_t)src[r];
        } else { /* TD_SYM_W16 */
            uint16_t* d = (uint16_t*)dst;
            for (int64_t r = 0; r < n_rows; r++) d[r] = (uint16_t)src[r];
        }
        td_release(col_vecs[c]);
        col_vecs[c] = narrow;
        col_data[c] = dst;
    }

    /* ---- 11. Build table ---- */
    {
        td_t* tbl = td_table_new(ncols);
        if (!tbl || TD_IS_ERR(tbl)) {
            for (int c = 0; c < ncols; c++) td_release(col_vecs[c]);
            goto fail_offsets;
        }

        for (int c = 0; c < ncols; c++) {
            tbl = td_table_add_col(tbl, col_name_ids[c], col_vecs[c]);
            td_release(col_vecs[c]);
        }

        result = tbl;
    }

    /* ---- 12. Cleanup ---- */
    scratch_free(row_offsets_hdr);
    munmap(buf, file_size);
    return result;

    /* Error paths */
fail_offsets:
    scratch_free(row_offsets_hdr);
fail_unmap:
    munmap(buf, file_size);
    return TD_ERR_PTR(TD_ERR_OOM);
}

/* --------------------------------------------------------------------------
 * td_read_csv — convenience wrapper with default options
 * -------------------------------------------------------------------------- */

td_t* td_read_csv(const char* path) {
    return td_read_csv_opts(path, 0, true, NULL, 0);
}

/* ============================================================================
 * td_write_csv — Write a table to a CSV file (RFC 4180)
 *
 * Writes header row with column names, then data rows.
 * Strings containing commas, quotes, or newlines are quoted.
 * Returns TD_OK on success, error code on failure.
 * ============================================================================ */

/* Write a string value, quoting if it contains special chars */
static void csv_write_str(FILE* fp, const char* s, size_t len) {
    int need_quote = 0;
    for (size_t i = 0; i < len; i++) {
        if (s[i] == ',' || s[i] == '"' || s[i] == '\n' || s[i] == '\r') {
            need_quote = 1;
            break;
        }
    }
    if (need_quote) {
        fputc('"', fp);
        for (size_t i = 0; i < len; i++) {
            if (s[i] == '"') fputc('"', fp);
            fputc(s[i], fp);
        }
        fputc('"', fp);
    } else {
        fwrite(s, 1, len, fp);
    }
}

td_err_t td_write_csv(td_t* table, const char* path) {
    if (!table || !path) return TD_ERR_TYPE;

    int64_t ncols = td_table_ncols(table);
    int64_t nrows = td_table_nrows(table);
    if (ncols <= 0) return TD_ERR_TYPE;

    FILE* fp = fopen(path, "w");
    if (!fp) return TD_ERR_IO;

    /* Header row: column names */
    for (int64_t c = 0; c < ncols; c++) {
        if (c > 0) fputc(',', fp);
        int64_t name_id = td_table_col_name(table, c);
        td_t* name_atom = td_sym_str(name_id);
        if (name_atom) {
            const char* s = td_str_ptr(name_atom);
            size_t slen = td_str_len(name_atom);
            csv_write_str(fp, s, slen);
        }
    }
    fputc('\n', fp);

    /* Data rows */
    for (int64_t r = 0; r < nrows; r++) {
        for (int64_t c = 0; c < ncols; c++) {
            if (c > 0) fputc(',', fp);
            td_t* col = td_table_get_col_idx(table, c);
            if (!col) continue;
            int8_t t = col->type;
            switch (t) {
            case TD_I64: {
                int64_t v = ((const int64_t*)td_data(col))[r];
                fprintf(fp, "%ld", (long)v);
                break;
            }
            case TD_I32: {
                int32_t v = ((const int32_t*)td_data(col))[r];
                fprintf(fp, "%d", v);
                break;
            }
            case TD_F64: {
                double v = ((const double*)td_data(col))[r];
                fprintf(fp, "%.17g", v);
                break;
            }
            case TD_BOOL: case TD_U8: {
                uint8_t v = ((const uint8_t*)td_data(col))[r];
                if (t == TD_BOOL) fputs(v ? "true" : "false", fp);
                else fprintf(fp, "%u", (unsigned)v);
                break;
            }
            case TD_SYM: {
                int64_t sym = td_read_sym(td_data(col), r, col->type, col->attrs);
                td_t* s = td_sym_str(sym);
                if (s) csv_write_str(fp, td_str_ptr(s), td_str_len(s));
                break;
            }
            default:
                break;
            }
        }
        fputc('\n', fp);
    }

    fclose(fp);
    return TD_OK;
}
