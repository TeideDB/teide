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

#ifndef TD_PLATFORM_H
#define TD_PLATFORM_H

#include <stddef.h>
#include <stdint.h>

/* --------------------------------------------------------------------------
 * OS detection
 * -------------------------------------------------------------------------- */
#if defined(__linux__)
  #define TD_OS_LINUX   1
#elif defined(__APPLE__) && defined(__MACH__)
  #define TD_OS_MACOS   1
#elif defined(_WIN32)
  #define TD_OS_WINDOWS 1
#else
  #error "Unsupported platform"
#endif

/* --------------------------------------------------------------------------
 * Compiler hints
 * -------------------------------------------------------------------------- */
#if !defined(TD_LIKELY)
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
#endif /* !TD_LIKELY */

/* --------------------------------------------------------------------------
 * Thread-local storage
 * -------------------------------------------------------------------------- */
#if !defined(TD_TLS)
#if defined(_MSC_VER)
  #define TD_TLS __declspec(thread)
#else
  #define TD_TLS _Thread_local
#endif
#endif /* !TD_TLS */

/* --------------------------------------------------------------------------
 * Atomics
 * -------------------------------------------------------------------------- */
#if !defined(td_atomic_inc)
#if defined(_MSC_VER)
  #include <intrin.h>
  #define td_atomic_inc(p)   _InterlockedIncrement((volatile long*)(p))
  #define td_atomic_dec(p)   _InterlockedDecrement((volatile long*)(p))
  #define td_atomic_load(p)  _InterlockedOr((volatile long*)(p), 0)
  #define td_atomic_store(p, v) _InterlockedExchange((volatile long*)(p), (long)(v))
  #define td_atomic_cas(p, expected, desired) \
      (_InterlockedCompareExchange((volatile long*)(p), (long)(desired), (long)(*(expected))) == (long)(*(expected)))
#else
  #include <stdatomic.h>
  #define td_atomic_inc(p)   atomic_fetch_add_explicit(p, 1, memory_order_relaxed)
  #define td_atomic_dec(p)   atomic_fetch_sub_explicit(p, 1, memory_order_acq_rel)
  #define td_atomic_load(p)  atomic_load_explicit(p, memory_order_acquire)
  #define td_atomic_store(p, v) atomic_store_explicit(p, v, memory_order_release)
  #define td_atomic_cas(p, expected, desired) \
      atomic_compare_exchange_strong_explicit(p, expected, desired, \
          memory_order_acq_rel, memory_order_acquire)
#endif
#endif /* !td_atomic_inc */

/* --------------------------------------------------------------------------
 * Pull in the public header for td_err_t, td_thread_t, VM API, etc.
 * This ensures all type/function declarations are consistent with td.h.
 * -------------------------------------------------------------------------- */
#include <teide/td.h>

/* --------------------------------------------------------------------------
 * Semaphore (platform-specific, not in the public header)
 * -------------------------------------------------------------------------- */
#if defined(TD_OS_WINDOWS)
  typedef void* td_sem_t;  /* HANDLE */
#elif defined(TD_OS_MACOS)
  #include <dispatch/dispatch.h>
  typedef dispatch_semaphore_t td_sem_t;
#else
  #include <semaphore.h>
  typedef sem_t td_sem_t;
#endif

td_err_t td_sem_init(td_sem_t* s, uint32_t initial_value);
void     td_sem_destroy(td_sem_t* s);
void     td_sem_wait(td_sem_t* s);
void     td_sem_signal(td_sem_t* s);

#endif /* TD_PLATFORM_H */
