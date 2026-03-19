// AFL++ / stdin fuzzing harness for the Cypher parser.
// Reads arbitrary input from stdin and feeds it to CypherParser::parse().

#include "CypherParser.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <string>

#include "CypherAST.h"
#include "procedures/ProcedureManager.h"

#include "CompilerException.h"
#include "FatalException.h"
#include "TuringException.h"

// Persistent-mode globals: allocated once, reused across inputs.
static std::unique_ptr<db::ProcedureManager> g_procedures;

static void initOnce() {
    if (!g_procedures) {
        g_procedures = db::ProcedureManager::create();
        g_procedures->init();
    }
}

static int fuzzOne(const char* data, size_t size) {
    // Limit input size to avoid OOM on huge generated inputs.
    if (size > 64 * 1024) {
        return 0;
    }

    std::string input(data, size);

    try {
        db::CypherAST ast(g_procedures.get(), input);
        db::CypherParser parser(&ast);
        parser.parse(input);
    } catch (const FatalException&) {
        // Internal logic error — let it crash so AFL reports it.
        throw;
    } catch (const db::CompilerException&) {
        // Expected for malformed queries.
    } catch (const TuringException&) {
        // Expected for various error conditions.
    } catch (const std::exception&) {
        // Catch anything else that isn't a crash.
    }

    return 0;
}

#if defined(__AFL_COMPILER) && defined(__AFL_HAVE_MANUAL_CONTROL)
// AFL++ persistent mode (afl-clang-fast++ only).
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
// Standalone mode: read from stdin.
int main(int argc, char** argv) {
    (void)argc;
    (void)argv;

    initOnce();

    // Read all of stdin.
    std::string input;
    char buf[4096];
    while (size_t n = fread(buf, 1, sizeof(buf), stdin)) {
        input.append(buf, n);
    }

    return fuzzOne(input.data(), input.size());
}
#endif
