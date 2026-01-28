#include <sstream>

#include <argparse.hpp>
#include <spdlog/fmt/fmt.h>
#include <spdlog/spdlog.h>

#include "QueryTestRunner.h"

using namespace turing::test;

namespace {

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

std::string argParserUsage(const argparse::ArgumentParser& parser) {
    std::ostringstream out;
    out << parser;
    return out.str();
}

std::string serializeTest(const QueryTestSpec& test) {
    std::string out = fmt::format(
        "{{\"name\":\"{}\",\"query\":\"{}\",\"writeRequired\":{},\"disabledReason\":\"{}\",\"tags\":[",
        escapeJson(test.name),
        escapeJson(test.query),
        test.writeRequired ? "true" : "false",
        escapeJson(test.disabledReason));
    for (size_t i = 0; i < test.tags.size(); ++i) {
        if (i > 0) {
            out += ",";
        }
        out += fmt::format("\"{}\"", escapeJson(test.tags[i]));
    }
    out += fmt::format("],\"enabled\":{}}}", test.enabled ? "true" : "false");
    return out;
}

std::string serializeResult(const QueryTestResult& result) {
    return fmt::format(
        "{{\"name\":\"{}\",\"planOutput\":\"{}\",\"resultOutput\":\"{}\",\"planMatched\":{},\"resultMatched\":{},\"timeUs\":{}}}",
        escapeJson(result.name),
        escapeJson(result.planOutput),
        escapeJson(result.resultOutput),
        result.planMatched ? "true" : "false",
        result.resultMatched ? "true" : "false",
        result.timeUs);
}

}

int main(int argc, char** argv) {
    spdlog::set_level(spdlog::level::off);

    argparse::ArgumentParser program("turingdb-test-cli");
    program.add_argument("--list")
        .help("List tests as JSON")
        .default_value(false)
        .implicit_value(true);
    program.add_argument("--run")
        .help("Run a single test by name")
        .metavar("name")
        .nargs(1);
    program.add_argument("--run-all")
        .help("Run all enabled tests")
        .default_value(false)
        .implicit_value(true);

    try {
        program.parse_args(argc, argv);
    } catch (const std::exception& e) {
        fmt::print(stderr, "{}\n", e.what());
        fmt::println("{}", argParserUsage(program));
        return 1;
    }

    const bool doList = program.get<bool>("--list");
    const bool doRunAll = program.get<bool>("--run-all");
    const bool doRun = program.is_used("--run");

    if ((doList ? 1 : 0) + (doRun ? 1 : 0) + (doRunAll ? 1 : 0) != 1) {
        fmt::println("{}", argParserUsage(program));
        return 1;
    }

    const fs::Path testsDir {QUERY_TEST_SUITE_DIR};
    std::vector<QueryTestSpec> tests;

    try {
        QueryTestRunner::loadTestsFromDir(tests, testsDir);
    } catch (const std::exception& e) {
        fmt::println("{{\"error\":\"{}\"}}", e.what());
    }

    if (doList) {
        fmt::print("[");
        bool first = true;
        for (const auto& test : tests) {
            if (!first) {
                fmt::print(",");
            }
            first = false;
            fmt::print("{}", serializeTest(test));
        }
        fmt::println("]");
        return 0;
    }

    QueryTestRunner runner;

    if (doRun) {
        const std::string name = program.get<std::string>("--run");
        for (const auto& test : tests) {
            if (test.name != name) {
                continue;
            }
            const fs::Path outDir = fs::Path {"query_test_suite_cli"} / test.name;
            const QueryTestResult result = runner.runTest(test, outDir);
            fmt::println("{}", serializeResult(result));
            return 0;
        }
        fmt::println("{}", "{\"error\":\"Unknown test name\"}");
        return 1;
    }

    fmt::print("[");
    bool first = true;
    for (const auto& test : tests) {
        if (!test.enabled) {
            continue;
        }
        const fs::Path outDir = fs::Path {"query_test_suite_cli"} / test.name;
        const QueryTestResult result = runner.runTest(test, outDir);
        if (!first) {
            fmt::print(",");
        }
        first = false;
        fmt::print("{}", serializeResult(result));
    }
    fmt::println("]");
    return 0;
}
