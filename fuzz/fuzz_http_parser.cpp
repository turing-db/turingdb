// AFL++ / stdin fuzzing harness for the HTTP parser.
// Reads arbitrary input from stdin, writes it into a NetBuffer,
// then runs HTTPParser::analyze() on it.

#include "HTTPParser.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <string>

#include "FatalException.h"
#include "NetBuffer.h"
#include "UriParser.h"

static int fuzzOne(const char* data, size_t size) {
    // Cap at NetBuffer size to avoid writing past the buffer.
    if (size > net::NetBuffer::BUFFER_SIZE) {
        size = net::NetBuffer::BUFFER_SIZE;
    }

    net::NetBuffer buffer;
    auto writer = buffer.getWriter();
    writer.writeString(data, size);

    net::HTTPParser<net::URIParser> parser(&buffer);

    try {
        auto result = parser.analyze();
        (void)result;
    } catch (const FatalException&) {
        throw;
    } catch (...) {
    }

    return 0;
}

#if defined(__AFL_COMPILER) && defined(__AFL_HAVE_MANUAL_CONTROL)
__AFL_FUZZ_INIT();

int main(int argc, char** argv) {
    (void)argc;
    (void)argv;

    __AFL_INIT();
    unsigned char* buf = __AFL_FUZZ_TESTCASE_BUF;

    while (__AFL_LOOP(10000)) {
        int len = __AFL_FUZZ_TESTCASE_LEN;
        fuzzOne(reinterpret_cast<const char*>(buf), len);
    }

    return 0;
}

#else
// Standalone / afl-gcc mode: read from stdin.
int main(int argc, char** argv) {
    (void)argc;
    (void)argv;

    std::string input;
    char buf[4096];
    while (size_t n = fread(buf, 1, sizeof(buf), stdin)) {
        input.append(buf, n);
    }

    return fuzzOne(input.data(), input.size());
}
#endif
