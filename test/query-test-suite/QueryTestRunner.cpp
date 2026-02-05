#include "QueryTestRunner.h"

#include <algorithm>
#include <chrono>
#include <optional>
#include <sstream>

#include <nlohmann/json.hpp>
#include <spdlog/fmt/bundled/format.h>

#include "File.h"
#include "ID.h"
#include "TuringDB.h"
#include "TuringTestEnv.h"
#include "SystemManager.h"
#include "SimpleGraph.h"
#include "CompilerException.h"
#include "PlanGraphDebug.h"
#include "Graph.h"
#include "columns/AllowedKinds.h"
#include "columns/ColumnOperatorDispatcher.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"
#include "procedures/ProcedureBlueprintMap.h"
#include "PlanGraphGenerator.h"
#include "PlanGraph.h"
#include "CypherAnalyzer.h"
#include "CypherParser.h"
#include "CypherAST.h"
#include "columns/ColumnDispatcher.h"
#include "dataframe/Dataframe.h"
#include "metadata/PropertyType.h"
#include "QueryStatus.h"

namespace db {
class CommitBuilder;
class Change;
}

namespace turing::test {

using json = nlohmann::json;

namespace {

void rtrim(std::string& trimmed, std::string_view value) {
    trimmed.clear();
    size_t end = value.size();

    while (end > 0) {
        const char c = value[end - 1];
        if (c != ' ' && c != '\t' && c != '\r') {
            break;
        }
        --end;
    }

    trimmed = std::string(value.substr(0, end));
}

void trimTrailingEmptyLines(std::string& trimmed, std::vector<std::string>& lines) {
    trimmed.clear();

    while (!lines.empty() && lines.back().empty()) {
        lines.pop_back();
    }

    std::ostringstream out;
    for (size_t i = 0; i < lines.size(); ++i) {
        if (i > 0) {
            out << '\n';
        }
        out << lines[i];
    }

    trimmed = out.str();
}

[[maybe_unused]] std::string valueToString(const std::string& value) {
    return value;
}

[[maybe_unused]] std::string valueToString(const std::string_view& value) {
    return std::string(value);
}

[[maybe_unused]] std::string valueToString(const db::ValueType& value) {
    return std::string(db::ValueTypeName::value(value));
}

// [[maybe_unused]] std::string valueToString(const db::ValueType& value) {
    // return std::string(db::ValueTypeName::value(value));
// }

template <IntegralType T, int tag>
[[maybe_unused]] std::string valueToString(const db::ID<T, tag> value) {
    return std::to_string(value.getValue());
}

[[maybe_unused]] std::string valueToString(const ChangeID value) {
    return std::to_string(value.get());
}

[[maybe_unused]] std::string valueToString(const db::CustomBool& value) {
    return value ? "true" : "false";
}

[[maybe_unused]] std::string valueToString(const db::CommitBuilder* value) {
    return value ? "commit_builder_ptr" : "null";
}

[[maybe_unused]] std::string valueToString(const db::Change* value) {
    return value ? "change_ptr" : "null";
}

template <typename T>
[[maybe_unused]] std::string valueToString(const T& value) {
    return fmt::format("{}", value);
}

template <typename T>
[[maybe_unused]] std::string valueToString(const std::optional<T>& value) {
    if (!value.has_value()) {
        return "null";
    }

    return valueToString(*value);
}

struct Stringify {
    std::string& string;
    size_t row;

    template <typename T>
    void operator()(const ColumnVector<T>* typed) {
        const T& value = typed->at(row);
        string = valueToString(value);
    }

    template <typename T>
    void operator()(const ColumnConst<T>* typed) {
        bioassert(row == 0, "Attempted to output row {} of ColumnConst.", row);
        const T& value = typed->getRaw();
        string = valueToString(value);
    }
};

std::string columnValueToString(const db::Column* column, size_t row) {
    std::string string;
    Stringify stringify(string, row);
    using Types = OutputtedTypes;
    ColumnSingleDispatcher<Types::Allowed, Stringify, Types::Excluded>::dispatch(column, stringify);
    return string;
}

void escapeCsv(std::string& escaped, std::string_view value) {
    escaped.clear();

    bool needsQuotes = false;
    escaped.reserve(value.size());

    for (char ch : value) {
        if (ch == '"') {
            escaped.push_back('"');
            escaped.push_back('"');
            needsQuotes = true;
            continue;
        }

        if (ch == ',' || ch == '\n' || ch == '\r') {
            needsQuotes = true;
        }
        escaped.push_back(ch);
    }

    if (!needsQuotes) {
        escaped = value;
    } else {
        escaped = "\"" + escaped + "\"";
    }
}

std::string formatStatusError(db::QueryStatus::Status status, std::string_view message) {
    auto statusName = std::string(db::QueryStatusDescription::value(status));
    for (auto& ch : statusName) {
        if (ch == '_') {
            ch = ' ';
        }
    }
    if (message.empty()) {
        return statusName;
    }
    return fmt::format("{}\n{}", statusName, message);
}

void generatePlanGraph(std::string_view query,
                       db::GraphView view,
                       std::ostream& out) {
    auto procedures = db::ProcedureBlueprintMap::create();

    db::CypherAST ast(*procedures, query);
    db::CypherParser parser(&ast);
    db::CypherAnalyzer analyzer(&ast, view);
    db::PlanGraphGenerator planGen(ast, view);

    try {
        parser.parse(query);
    } catch (const db::CompilerException& e) {
        fmt::println(out, "PARSE ERROR");
        fmt::println(out, "{}", e.what());
        return;
    }

    try {
        analyzer.analyze();
    } catch (const db::CompilerException& e) {
        fmt::println(out, "ANALYZE ERROR");
        fmt::println(out, "{}", e.what());
        return;
    }

    try {
        planGen.generate(ast.queries().front());
    } catch (const db::CompilerException& e) {
        fmt::println(out, "PLAN ERROR");
        fmt::println(out, "{}", e.what());
        return;
    }

    const db::PlanGraph& planGraph = planGen.getPlanGraph();
    db::PlanGraphDebug::dumpMermaidContent(out, view, planGraph);
}

} // namespace

void QueryTestRunner::loadTestsFromDir(std::vector<QueryTestSpec>& specs, const fs::Path& dir) {
    specs.clear();

    auto filesOpt = dir.listDir();
    if (!filesOpt) {
        throw FatalException(fmt::format("Failed to list directory: {}", filesOpt.error().fmtMessage()));
    }

    std::vector<fs::Path> files = *filesOpt;
    std::sort(files.begin(), files.end(), [](const fs::Path& a, const fs::Path& b) {
        return a.get() < b.get();
    });

    std::string content;

    for (const auto& path : files) {
        if (!path.get().ends_with(".json")) {
            continue;
        }

        std::string filename = path.get();
        const auto lastSlash = filename.find_last_of("/\\");

        if (lastSlash != std::string::npos) {
            filename = filename.substr(lastSlash + 1);
        }

        if (filename.size() > 5 && filename.ends_with(".json")) {
            filename = filename.substr(0, filename.size() - 5);
        }

        std::replace(filename.begin(), filename.end(), '/', '-');

        readFile(content, path);
        json doc = json::parse(content, nullptr, true, true);

        QueryTestSpec spec;
        spec.name = filename;
        spec.graphName = doc.value("graph", spec.graphName);
        spec.query = doc.value("query", "");
        spec.enabled = doc.value("enabled", true);
        spec.writeRequired = doc.value("write-required", false);
        spec.disabledReason = doc.value("disabled-reason", "");

        if (doc.contains("tags") && doc["tags"].is_array()) {
            for (const auto& tag : doc["tags"]) {
                if (tag.is_string()) {
                    spec.tags.push_back(tag.get<std::string>());
                }
            }
        }

        if (doc.contains("expect")) {
            const auto& expect = doc["expect"];
            spec.expectPlan = expect.value("plan", "");
            spec.expectResult = expect.value("result", "");
        }

        specs.push_back(std::move(spec));
    }
}

QueryTestResult QueryTestRunner::runTest(const QueryTestSpec& spec, const fs::Path& outDir) {
    QueryTestResult result;
    result.name = spec.name;

    auto env = turing::test::TuringTestEnv::create(outDir);
    db::Graph* graph = env->getSystemManager().createGraph(spec.graphName);
    db::SimpleGraph::createSimpleGraph(graph);
    db::TuringDB* db = &env->getDB();

    std::stringstream planOut;
    {
        const db::Transaction tx = graph->openTransaction();
        const db::GraphView view = tx.viewGraph();
        generatePlanGraph(spec.query, view, planOut);
    }

    std::vector<std::vector<std::string>> rows;
    std::vector<std::string> columnNames;

    auto callback = [&](const db::Dataframe* df) {
        if (!df) {
            return;
        }
        if (columnNames.empty()) {
            for (auto* col : df->cols()) {
                columnNames.emplace_back(col->getName());
            }
        }
        const size_t rowCount = df->getRowCount();
        std::vector<std::string> values;

        for (size_t row = 0; row < rowCount; ++row) {
            values.clear();
            values.reserve(df->cols().size());

            for (auto* col : df->cols()) {
                values.push_back(columnValueToString(col->getColumn(), row));
            }

            rows.push_back(std::move(values));
        }
    };

    db::ChangeID changeID = db::ChangeID::head();

    if (spec.writeRequired) {
        db->query("CHANGE NEW", spec.graphName, &env->getMem(), [&](const db::Dataframe* df) {
                      NamedColumn* col = df->getColumn(ColumnTag {0});
                      bioassert(col, "Column not found");
                      auto& c = *static_cast<ColumnVector<ChangeID>*>(col->getColumn());
                      bioassert(c.size() == 1, "Expected 1 change");
                      changeID = c[0]; }, db::CommitHash::head(), db::ChangeID::head());
    }

    const auto queryStart = std::chrono::steady_clock::now();
    const db::QueryStatus status = db->query(spec.query,
                                             spec.graphName,
                                             &env->getMem(),
                                             callback,
                                             db::CommitHash::head(),
                                             changeID);
    const auto queryEnd = std::chrono::steady_clock::now();
    result.timeUs = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::microseconds>(queryEnd - queryStart).count());

    std::stringstream resultOut;
    std::string escaped;
    if (!status.isOk()) {
        resultOut << formatStatusError(status.getStatus(), status.getError());
    } else {
        for (size_t i = 0; i < columnNames.size(); ++i) {
            if (i > 0) {
                resultOut << ",";
            }

            escapeCsv(escaped, columnNames[i]);
            resultOut << escaped;
        }
        for (const auto& row : rows) {
            resultOut << "\n";
            for (size_t col = 0; col < row.size(); ++col) {
                if (col > 0) {
                    resultOut << ",";
                }

                escapeCsv(escaped, row[col]);
                resultOut << escaped;
            }
        }
    }

    if (spec.writeRequired) {
        db->query("CHANGE SUBMIT", spec.graphName, &env->getMem(), [](const db::Dataframe*) {}, db::CommitHash::head(), changeID);
    }

    normalizeOutput(result.planOutput, planOut.str());
    normalizeOutput(result.resultOutput, resultOut.str());

    std::string expected;
    normalizeOutput(expected, spec.expectPlan);
    result.planMatched = expected == result.planOutput;
    normalizeOutput(expected, spec.expectResult);
    result.resultMatched = expected == result.resultOutput;

    return result;
}

void QueryTestRunner::normalizeOutput(std::string& normalized, std::string_view output) {
    normalized.clear();
    normalized.reserve(output.size());

    for (char ch : output) {
        if (ch != '\r') {
            normalized.push_back(ch);
        }
    }

    std::vector<std::string> lines;
    std::stringstream stream(normalized);
    std::string line;

    while (std::getline(stream, line, '\n')) {
        rtrim(normalized, line);
        lines.push_back(std::move(normalized));
    }

    trimTrailingEmptyLines(normalized, lines);
}

void QueryTestRunner::readFile(std::string& content, const fs::Path& path) {
    content.clear();

    const auto file = fs::File::open(path);
    if (!file) {
        throw FatalException(fmt::format("Failed to open file '{}': {}",
                                         path.get(),
                                         file.error().fmtMessage()));
    }

    const size_t fileSize = file->getInfo()._size;
    content.resize(fileSize);

    if (!file->read(content.data(), fileSize)) {
        throw FatalException(fmt::format("Failed to read file '{}': {}",
                                         path.get(),
                                         file.error().fmtMessage()));
    }
}

}
