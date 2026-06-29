#include <sstream>

#include <argparse.hpp>
#include <spdlog/fmt/fmt.h>
#include <spdlog/spdlog.h>

#include "QueryTestRunner.h"
#include "RemoteQueryTestRunner.h"

using namespace turing::test;

namespace {

// The CLI prints JSON directly to stdout, so control characters must be
// escaped here instead of relying on downstream shells or loggers.
std::string escapeJson(std::string_view input) {
    std::string out;
    out.reserve(input.size());
    for (char ch : input) {
        const auto byte = static_cast<unsigned char>(ch);
        switch (ch) {
            case '\\':
                out += "\\\\";
                break;
            case '\"':
                out += "\\\"";
                break;
            case '\b':
                out += "\\b";
                break;
            case '\f':
                out += "\\f";
                break;
            case '\n':
                out += "\\n";
                break;
            case '\r':
                out += "\\r";
                break;
            case '\t':
                out += "\\t";
                break;
            default:
                if (byte < 0x20) {
                    out += fmt::format("\\u{:04x}", static_cast<unsigned int>(byte));
                } else {
                    out.push_back(ch);
                }
                break;
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
        "{{\"name\":\"{}\",\"query\":\"{}\",\"writeRequired\":{},"
        "\"disabledReason\":\"{}\","
        "\"remoteEnabled\":{},\"remoteDisabledReason\":\"{}\",\"tags\":[",
        escapeJson(test._name), escapeJson(test._query),
        test._writeRequired ? "true" : "false", escapeJson(test._disabledReason),
        test._remoteEnabled ? "true" : "false",
        escapeJson(test._remoteDisabledReason));
    for (size_t i = 0; i < test._tags.size(); ++i) {
        if (i > 0) {
            out += ",";
        }
        out += fmt::format("\"{}\"", escapeJson(test._tags[i]));
    }
    out += fmt::format("],\"enabled\":{}}}", test._enabled ? "true" : "false");
    return out;
}

// Remote mode keeps the output transport-focused; local runs include the
// additional JSON expectation fields used by the in-process suite.
std::string serializeResult(const QueryTestResult& result, bool includeJsonFields = true) {
    if (!includeJsonFields) {
        return fmt::format(
            "{{\"name\":\"{}\",\"planOutput\":\"{}\","
            "\"resultOutput\":\"{}\","
            "\"planMatched\":{},\"resultMatched\":{},"
            "\"timeUs\":{}}}",
            escapeJson(result._name),
            escapeJson(result._planOutput),
            escapeJson(result._resultOutput),
            result._planMatched ? "true" : "false",
            result._resultMatched ? "true" : "false",
            result._timeUs);
    }

    return fmt::format(
        "{{\"name\":\"{}\",\"planOutput\":\"{}\","
        "\"resultOutput\":\"{}\",\"resultJsonOutput\":\"{}\","
        "\"resultJsonError\":\"{}\","
        "\"planMatched\":{},\"resultMatched\":{},"
        "\"resultJsonMatched\":{},\"resultJsonValid\":{},"
        "\"timeUs\":{}}}",
        escapeJson(result._name), escapeJson(result._planOutput),
        escapeJson(result._resultOutput), escapeJson(result._resultJsonOutput),
        escapeJson(result._resultJsonError),
        result._planMatched ? "true" : "false",
        result._resultMatched ? "true" : "false",
        result._resultJsonMatched ? "true" : "false",
        result._resultJsonValid ? "true" : "false", result._timeUs);
}

} // namespace

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
    program.add_argument("--run-remote")
        .help("Run a single test by name through the remote protocol")
        .metavar("name")
        .nargs(1);
    program.add_argument("--run-all")
        .help("Run all enabled tests")
        .default_value(false)
        .implicit_value(true);
    program.add_argument("--run-all-remote")
        .help("Run all enabled tests through the remote protocol")
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
    const bool doRunAllRemote = program.get<bool>("--run-all-remote");
    const bool doRun = program.is_used("--run");
    const bool doRunRemote = program.is_used("--run-remote");

    if ((doList ? 1 : 0) + (doRun ? 1 : 0) + (doRunRemote ? 1 : 0) + (doRunAll ? 1 : 0) + (doRunAllRemote ? 1 : 0) != 1) {
        fmt::println("{}", argParserUsage(program));
        return 1;
    }

    const fs::Path testsDir(QUERY_TEST_SUITE_DIR);
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
    RemoteQueryTestRunner remoteRunner;

    if (doRun) {
        const std::string name = program.get<std::string>("--run");
        for (const auto& test : tests) {
            if (test._name != name) {
                continue;
            }
            const fs::Path outDir = fs::Path {"query_test_suite_cli"} / test._name;
            const QueryTestResult result = runner.runTest(test, outDir);
            fmt::println("{}", serializeResult(result));
            return 0;
        }
        fmt::println("{}", "{\"error\":\"Unknown test name\"}");
        return 1;
    }

    if (doRunRemote) {
        const std::string name = program.get<std::string>("--run-remote");
        for (const auto& test : tests) {
            if (test._name != name) {
                continue;
            }
            if (!test._remoteEnabled) {
                fmt::println(stderr, "Remote test disabled for '{}': {}", name,
                             test._remoteDisabledReason);
                return 1;
            }
            const fs::Path outDir =
                fs::Path {"query_test_suite_cli_remote"} / test._name;
            const QueryTestResult result = remoteRunner.runTest(test, outDir);
            fmt::println("{}", serializeResult(result, false));
            return 0;
        }
        fmt::println("{}", "{\"error\":\"Unknown test name\"}");
        return 1;
    }

    fmt::print("[");
    bool first = true;
    for (const auto& test : tests) {
        if (!test._enabled || (doRunAllRemote && !test._remoteEnabled)) {
            continue;
        }
        QueryTestResult result;
        try {
            result =
                doRunAllRemote
                    ? remoteRunner.runTest(
                          test, fs::Path {"query_test_suite_cli_remote"} / test._name)
                    : runner.runTest(test,
                                     fs::Path {"query_test_suite_cli"} / test._name);
        } catch (const std::exception& e) {
            // A single failing test — e.g. an unsupported column type over the
            // remote protocol — must not abort the whole run. Record it as a
            // failed result and keep going with the remaining tests.
            result._name = test._name;
            result._resultOutput = fmt::format("ERROR: {}", e.what());
        }

        if (!first) {
            fmt::print(",");
        }
        first = false;
        fmt::print("{}", serializeResult(result, !doRunAllRemote));
    }
    fmt::println("]");
    return 0;
}
