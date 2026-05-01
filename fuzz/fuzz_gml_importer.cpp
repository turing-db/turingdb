// AFL++ / stdin fuzzing harness for the GML importer.
// Reads arbitrary input from stdin and feeds it to GMLImporter::importContent().

#include "GMLImporter.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <string>
#include <memory>

#include "FatalException.h"
#include "Graph.h"
#include "JobSystem.h"
#include "Path.h"

static std::unique_ptr<db::JobSystem> g_jobSystem;

static void initOnce() {
    if (!g_jobSystem) {
        g_jobSystem = std::make_unique<db::JobSystem>();
        g_jobSystem->init();
    }
}

static int fuzzOne(const char* data, size_t size) {
    if (size > 256 * 1024) {
        return 0;
    }

    std::string_view input(data, size);

    try {
        auto graph = db::Graph::create();
        db::GMLImporter importer;
        (void)importer.importContent(*g_jobSystem, graph.get(), input);
    } catch (const FatalException&) {
        throw;
    } catch (const std::exception&) {
        // Expected for malformed GML.
    }

    return 0;
}

#if defined(__AFL_COMPILER) && defined(__AFL_HAVE_MANUAL_CONTROL)
__AFL_FUZZ_INIT();

int main(int argc, char** argv) {
    (void)argc;
    (void)argv;

    initOnce();

    __AFL_INIT();
    unsigned char* buf = __AFL_FUZZ_TESTCASE_BUF;

    while (__AFL_LOOP(10000)) {
        int len = __AFL_FUZZ_TESTCASE_LEN;
        fuzzOne(reinterpret_cast<const char*>(buf), len);
    }

    return 0;
}

#else
int main(int argc, char** argv) {
    (void)argc;
    (void)argv;

    initOnce();

    std::string input;
    char buf[4096];
    while (size_t n = fread(buf, 1, sizeof(buf), stdin)) {
        input.append(buf, n);
    }

    return fuzzOne(input.data(), input.size());
}
#endif
