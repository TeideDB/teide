#include <teide/td.h>
#include <stdint.h>
#include <stddef.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
    td_heap_init();
    td_sym_init();

    char path[] = "/tmp/fuzz_csv_XXXXXX";
    int fd = mkstemp(path);
    if (fd < 0) { td_sym_destroy(); td_heap_destroy(); return 0; }
    write(fd, data, size);
    close(fd);

    td_t* result = td_read_csv(path);
    if (result && !TD_IS_ERR(result))
        td_release(result);

    unlink(path);
    td_sym_destroy();
    td_heap_destroy();
    return 0;
}
