#include "BioAssert.h"

#include <stdio.h>
#include <stdlib.h>

void __bioAssertWithLocation(const char* file, unsigned line, const char* expr, const char* msg) {
    fprintf(stderr, "Internal Error: The assertion '%s' failed at %s:%u\n%s\n\n", expr, file, line, msg);
    abort();
}

