#include "munit.h"

/* ---- Internal helpers -------------------------------------------------- */

void munit_fail_at(const char* file, int line, const char* msg) {
    fprintf(stderr, "  %s:%d: %s\n", file, line, msg);
}

/* Concatenate two path segments.  Caller must free the result. */
static char* path_join(const char* a, const char* b) {
    size_t la = a ? strlen(a) : 0;
    size_t lb = b ? strlen(b) : 0;
    char* out = (char*)malloc(la + lb + 1);
    if (!out) return NULL;
    if (la) memcpy(out, a, la);
    if (lb) memcpy(out + la, b, lb);
    out[la + lb] = '\0';
    return out;
}

/* Check if `str` starts with `prefix`. */
static int starts_with(const char* str, const char* prefix) {
    return strncmp(str, prefix, strlen(prefix)) == 0;
}

/* ---- Run a single test ------------------------------------------------- */

static int run_test(const char* full_name, const MunitTest* test,
                    void* user_data) {
    fprintf(stderr, "  %-60s ", full_name);

    void* fixture = NULL;
    if (test->setup) fixture = test->setup(NULL, user_data);

    MunitResult result = test->test(NULL, fixture);

    if (test->tear_down) test->tear_down(fixture);

    switch (result) {
    case MUNIT_OK:   fprintf(stderr, "[ OK ]\n");   return 0;
    case MUNIT_SKIP: fprintf(stderr, "[ SKIP ]\n"); return 0;
    default:         fprintf(stderr, "[ FAIL ]\n"); return 1;
    }
}

/* ---- Recursive suite runner -------------------------------------------- */

static int run_suite(const MunitSuite* suite, const char* parent_prefix,
                     const char* filter, void* user_data,
                     int* total, int* failures) {
    char* prefix = path_join(parent_prefix, suite->prefix);
    if (!prefix) return -1;

    /* Run tests in this suite */
    if (suite->tests) {
        for (const MunitTest* t = suite->tests; t->name != NULL; t++) {
            char* name = path_join(prefix, t->name);
            if (!name) { free(prefix); return -1; }

            /* Apply --suite filter: run if name starts with filter */
            if (filter == NULL || starts_with(name, filter)) {
                (*total)++;
                *failures += run_test(name, t, user_data);
            }
            free(name);
        }
    }

    /* Recurse into child suites */
    if (suite->suites) {
        for (const MunitSuite* child = suite->suites;
             child->prefix != NULL; child++) {
            run_suite(child, prefix, filter, user_data, total, failures);
        }
    }

    free(prefix);
    return 0;
}

/* ---- Public entry point ------------------------------------------------ */

int munit_suite_main(const MunitSuite* suite, void* user_data,
                     int argc, char* argv[]) {
    const char* filter = NULL;

    /* Parse --suite argument */
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--suite") == 0 && i + 1 < argc) {
            filter = argv[i + 1];
            i++;
        }
    }

    int total = 0;
    int failures = 0;

    fprintf(stderr, "\nRunning tests...\n");
    run_suite(suite, "", filter, user_data, &total, &failures);

    fprintf(stderr, "\n%d of %d tests passed", total - failures, total);
    if (failures > 0) fprintf(stderr, " (%d FAILED)", failures);
    fprintf(stderr, "\n\n");

    return failures > 0 ? EXIT_FAILURE : EXIT_SUCCESS;
}
