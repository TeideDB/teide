#ifndef MUNIT_H
#define MUNIT_H

#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ---- Result codes ------------------------------------------------------ */

typedef enum {
    MUNIT_OK,
    MUNIT_FAIL,
    MUNIT_SKIP,
    MUNIT_ERROR
} MunitResult;

/* ---- Test function signature ------------------------------------------- */

typedef void* MunitTestSetup(const void* params, void* user_data);
typedef void  MunitTestTearDown(void* fixture);
typedef MunitResult (*MunitTestFunc)(const void* params, void* fixture);

/* ---- Test & Suite structures ------------------------------------------- */

typedef struct {
    const char*       name;     /* test name, e.g. "/my_test" */
    MunitTestFunc     test;     /* test function */
    MunitTestSetup*   setup;    /* optional setup (NULL if unused) */
    MunitTestTearDown* tear_down; /* optional teardown (NULL if unused) */
    unsigned int       options;  /* reserved */
    void*             parameters; /* reserved */
} MunitTest;

typedef struct MunitSuite {
    const char*          prefix;    /* suite prefix, e.g. "/vec" */
    MunitTest*           tests;     /* NULL-terminated array of tests */
    struct MunitSuite*   suites;    /* NULL-terminated array of child suites */
    unsigned int          iterations; /* reserved */
    unsigned int          options;    /* reserved */
} MunitSuite;

/* ---- Suite runner ------------------------------------------------------ */

int munit_suite_main(const MunitSuite* suite, void* user_data,
                     int argc, char* argv[]);

/* ---- Assertion macros -------------------------------------------------- */

/* Internal: report failure */
void munit_fail_at(const char* file, int line, const char* msg);

#define munit_assert(expr)                                                \
    do {                                                                  \
        if (!(expr)) {                                                    \
            munit_fail_at(__FILE__, __LINE__, "assertion failed: " #expr);\
            return MUNIT_FAIL;                                            \
        }                                                                 \
    } while (0)

#define munit_assert_true(expr) munit_assert(expr)
#define munit_assert_false(expr) munit_assert(!(expr))

#define munit_assert_int(a, op, b)                                        \
    do {                                                                  \
        long long munit_a_ = (long long)(a);                              \
        long long munit_b_ = (long long)(b);                              \
        if (!(munit_a_ op munit_b_)) {                                    \
            fprintf(stderr, "  %s:%d: assertion failed: %s " #op " %s "   \
                    "(%lld " #op " %lld)\n",                              \
                    __FILE__, __LINE__, #a, #b, munit_a_, munit_b_);      \
            return MUNIT_FAIL;                                            \
        }                                                                 \
    } while (0)

#define munit_assert_uint(a, op, b)                                       \
    do {                                                                  \
        unsigned long long munit_a_ = (unsigned long long)(a);            \
        unsigned long long munit_b_ = (unsigned long long)(b);            \
        if (!(munit_a_ op munit_b_)) {                                    \
            fprintf(stderr, "  %s:%d: assertion failed: %s " #op " %s "   \
                    "(%llu " #op " %llu)\n",                              \
                    __FILE__, __LINE__, #a, #b, munit_a_, munit_b_);      \
            return MUNIT_FAIL;                                            \
        }                                                                 \
    } while (0)

#define munit_assert_size(a, op, b) munit_assert_uint(a, op, b)

#define munit_assert_double(a, op, b)                                     \
    do {                                                                  \
        double munit_a_ = (double)(a);                                    \
        double munit_b_ = (double)(b);                                    \
        if (!(munit_a_ op munit_b_)) {                                    \
            fprintf(stderr, "  %s:%d: assertion failed: %s " #op " %s "   \
                    "(%g " #op " %g)\n",                                  \
                    __FILE__, __LINE__, #a, #b, munit_a_, munit_b_);      \
            return MUNIT_FAIL;                                            \
        }                                                                 \
    } while (0)

#define munit_assert_double_equal(a, b, prec)                             \
    do {                                                                  \
        double munit_a_ = (double)(a);                                    \
        double munit_b_ = (double)(b);                                    \
        double munit_tol_ = 1.0;                                          \
        for (int munit_i_ = 0; munit_i_ < (prec); munit_i_++)            \
            munit_tol_ *= 0.1;                                            \
        if (fabs(munit_a_ - munit_b_) > munit_tol_) {                    \
            fprintf(stderr, "  %s:%d: assertion failed: %s ~= %s "       \
                    "(%g ~= %g, tolerance %g)\n",                         \
                    __FILE__, __LINE__, #a, #b, munit_a_, munit_b_,       \
                    munit_tol_);                                          \
            return MUNIT_FAIL;                                            \
        }                                                                 \
    } while (0)

#define munit_assert_ptr_not_null(p)                                      \
    do {                                                                  \
        if ((p) == NULL) {                                                \
            fprintf(stderr, "  %s:%d: assertion failed: %s != NULL\n",    \
                    __FILE__, __LINE__, #p);                              \
            return MUNIT_FAIL;                                            \
        }                                                                 \
    } while (0)

#define munit_assert_null(p)                                              \
    do {                                                                  \
        if ((p) != NULL) {                                                \
            fprintf(stderr, "  %s:%d: assertion failed: %s == NULL\n",    \
                    __FILE__, __LINE__, #p);                              \
            return MUNIT_FAIL;                                            \
        }                                                                 \
    } while (0)

#define munit_assert_ptr_equal(a, b)                                      \
    do {                                                                  \
        const void* munit_a_ = (const void*)(a);                         \
        const void* munit_b_ = (const void*)(b);                         \
        if (munit_a_ != munit_b_) {                                       \
            fprintf(stderr, "  %s:%d: assertion failed: %s == %s "        \
                    "(%p == %p)\n",                                       \
                    __FILE__, __LINE__, #a, #b, munit_a_, munit_b_);      \
            return MUNIT_FAIL;                                            \
        }                                                                 \
    } while (0)

#define munit_assert_string_equal(a, b)                                   \
    do {                                                                  \
        const char* munit_a_ = (const char*)(a);                          \
        const char* munit_b_ = (const char*)(b);                          \
        if (munit_a_ == NULL || munit_b_ == NULL ||                       \
            strcmp(munit_a_, munit_b_) != 0) {                            \
            fprintf(stderr, "  %s:%d: assertion failed: %s == %s "        \
                    "(\"%s\" == \"%s\")\n",                               \
                    __FILE__, __LINE__, #a, #b,                           \
                    munit_a_ ? munit_a_ : "(null)",                      \
                    munit_b_ ? munit_b_ : "(null)");                     \
            return MUNIT_FAIL;                                            \
        }                                                                 \
    } while (0)

#define munit_assert_memory_equal(sz, a, b)                               \
    do {                                                                  \
        if (memcmp((a), (b), (sz)) != 0) {                                \
            fprintf(stderr, "  %s:%d: assertion failed: %zu bytes of "    \
                    "%s == %s\n", __FILE__, __LINE__,                     \
                    (size_t)(sz), #a, #b);                                \
            return MUNIT_FAIL;                                            \
        }                                                                 \
    } while (0)

#ifdef __cplusplus
}
#endif

#endif /* MUNIT_H */
