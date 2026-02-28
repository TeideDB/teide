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

#include "munit.h"

/*
 * Test suites are declared in individual test_*.c files.
 * They are extern'd here and collected into the root suite.
 *
 * To add a new suite:
 *   1. Create test/test_foo.c with MunitTest tests and MunitSuite test_foo_suite
 *   2. Add: extern MunitSuite test_foo_suite;
 *   3. Add to child_suites[]: test_foo_suite,
 */

/* Extern declarations for module test suites */
extern MunitSuite test_err_suite;
extern MunitSuite test_block_suite;
extern MunitSuite test_buddy_suite;
extern MunitSuite test_cow_suite;
extern MunitSuite test_atom_suite;
extern MunitSuite test_vec_suite;
extern MunitSuite test_str_suite;
extern MunitSuite test_list_suite;
extern MunitSuite test_morsel_suite;
extern MunitSuite test_sym_suite;
extern MunitSuite test_table_suite;
extern MunitSuite test_graph_suite;
extern MunitSuite test_pool_suite;
extern MunitSuite test_store_suite;
extern MunitSuite test_csr_suite;

static MunitSuite child_suites[] = {
    /* { .prefix, .tests, .suites, .iterations, .options } */
    { "/err",    NULL, NULL, 0, 0 },
    { "/block",  NULL, NULL, 0, 0 },
    { "/buddy",  NULL, NULL, 0, 0 },
    { "/cow",    NULL, NULL, 0, 0 },
    { "/atom",   NULL, NULL, 0, 0 },
    { "/vec",    NULL, NULL, 0, 0 },
    { "/str",    NULL, NULL, 0, 0 },
    { "/list",   NULL, NULL, 0, 0 },
    { "/morsel", NULL, NULL, 0, 0 },
    { "/sym",    NULL, NULL, 0, 0 },
    { "/table",  NULL, NULL, 0, 0 },
    { "/graph",  NULL, NULL, 0, 0 },
    { "/pool",   NULL, NULL, 0, 0 },
    { "/store",  NULL, NULL, 0, 0 },
    { "/csr",    NULL, NULL, 0, 0 },
    { NULL, NULL, NULL, 0, 0 },        /* terminator */
};

static MunitSuite root_suite = {
    "",              /* prefix */
    NULL,            /* no tests at root level */
    child_suites,    /* child suites */
    0,               /* iterations */
    0,               /* options */
};

int main(int argc, char* argv[]) {
    /* Patch child suites with the real extern suite data */
    child_suites[0] = test_err_suite;
    child_suites[1] = test_block_suite;
    child_suites[2] = test_buddy_suite;
    child_suites[3] = test_cow_suite;
    child_suites[4] = test_atom_suite;
    child_suites[5] = test_vec_suite;
    child_suites[6] = test_str_suite;
    child_suites[7] = test_list_suite;
    child_suites[8] = test_morsel_suite;
    child_suites[9] = test_sym_suite;
    child_suites[10] = test_table_suite;
    child_suites[11] = test_graph_suite;
    child_suites[12] = test_pool_suite;
    child_suites[13] = test_store_suite;
    child_suites[14] = test_csr_suite;

    return munit_suite_main(&root_suite, NULL, argc, argv);
}
