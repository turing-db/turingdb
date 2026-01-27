#include <iostream>

#include <spdlog/spdlog.h>

#include "QueryTestRunner.h"

namespace {

void printUsage() {
    std::cout << "query_test_suite_cli --list | --run <name> | --run-all\n";
}

std::string escapeJson(std::string_view input) {
    std::string out;
    out.reserve(input.size());
    for (char ch : input) {
        switch (ch) {
        case '\\': out += "\\\\"; break;
        case '\"': out += "\\\""; break;
        case '\n': out += "\\n"; break;
        case '\r': out += "\\r"; break;
        case '\t': out += "\\t"; break;
        default: out.push_back(ch); break;
        }
    }
    return out;
}

}

int main(int argc, char** argv) {
    using turing::test::QueryTestRunner;
    using turing::test::QueryTestSpec;
    using turing::test::QueryTestResult;

    spdlog::set_level(spdlog::level::off);

    if (argc < 2) {
        printUsage();
        return 1;
    }

    const std::string command = argv[1];
    const fs::Path testsDir {QUERY_TEST_SUITE_DIR};
    const auto tests = QueryTestRunner::loadTestsFromDir(testsDir);

    if (command == "--list") {
        std::cout << "[";
        bool first = true;
        for (const auto& test : tests) {
            if (!first) {
                std::cout << ",";
            }
            first = false;
            std::cout << "{\"name\":\"" << escapeJson(test.name) << "\""
                      << ",\"query\":\"" << escapeJson(test.query) << "\""
                      << ",\"writeRequired\":" << (test.writeRequired ? "true" : "false")
                      << ",\"disabledReason\":\"" << escapeJson(test.disabledReason) << "\""
                      << ",\"tags\":[";
            for (size_t i = 0; i < test.tags.size(); ++i) {
                if (i > 0) {
                    std::cout << ",";
                }
                std::cout << "\"" << escapeJson(test.tags[i]) << "\"";
            }
            std::cout << "]"
                      << ",\"enabled\":" << (test.enabled ? "true" : "false")
                      << "}";
        }
        std::cout << "]\n";
        return 0;
    }

    QueryTestRunner runner;

    if (command == "--run" && argc >= 3) {
        const std::string name = argv[2];
        for (const auto& test : tests) {
            if (test.name != name) {
                continue;
            }
            const fs::Path outDir = fs::Path {"query_test_suite_cli"} / test.name;
            const QueryTestResult result = runner.runTest(test, outDir);
            std::cout << "{\"name\":\"" << escapeJson(result.name) << "\""
                      << ",\"planOutput\":\"" << escapeJson(result.planOutput) << "\""
                      << ",\"resultOutput\":\"" << escapeJson(result.resultOutput) << "\""
                      << ",\"planMatched\":" << (result.planMatched ? "true" : "false")
                      << ",\"resultMatched\":" << (result.resultMatched ? "true" : "false")
                      << "}\n";
            return 0;
        }
        std::cout << "{\"error\":\"Unknown test name\"}\n";
        return 1;
    }

    if (command == "--run-all") {
        std::cout << "[";
        bool first = true;
        for (const auto& test : tests) {
            if (!test.enabled) {
                continue;
            }
            const fs::Path outDir = fs::Path {"query_test_suite_cli"} / test.name;
            const QueryTestResult result = runner.runTest(test, outDir);
            if (!first) {
                std::cout << ",";
            }
            first = false;
            std::cout << "{\"name\":\"" << escapeJson(result.name) << "\""
                      << ",\"planOutput\":\"" << escapeJson(result.planOutput) << "\""
                      << ",\"resultOutput\":\"" << escapeJson(result.resultOutput) << "\""
                      << ",\"planMatched\":" << (result.planMatched ? "true" : "false")
                      << ",\"resultMatched\":" << (result.resultMatched ? "true" : "false")
                      << "}";
        }
        std::cout << "]\n";
        return 0;
    }

    printUsage();
    return 1;
}
