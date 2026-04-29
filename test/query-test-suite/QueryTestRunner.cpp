#include "QueryTestRunner.h"

#include <algorithm>
#include <optional>
#include <sstream>

#include <nlohmann/json.hpp>
#include <spdlog/fmt/bundled/format.h>

#include <range/v3/view/drop.hpp>

#include "CompilerException.h"
#include "CypherAST.h"
#include "CypherAnalyzer.h"
#include "CypherParser.h"
#include "File.h"
#include "Graph.h"
#include "ID.h"
#include "ListElementView.h"
#include "ListView.h"
#include "PlanGraph.h"
#include "PlanGraphDebug.h"
#include "QueryConfig.h"
#include "PlanGraphGenerator.h"
#include "PlanOptimizer.h"
#include "QueryCallbacks.h"
#include "QueryStatus.h"
#include "SimpleGraph.h"
#include "SystemManager.h"
#include "TuringDB.h"
#include "TuringTestEnv.h"
#include "columns/AllowedKinds.h"
#include "columns/ColumnOperatorDispatcher.h"
#include "dataframe/Dataframe.h"
#include "metadata/PropertyType.h"
#include "JsonEncoder.h"
#include "procedures/ProcedureManager.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"

namespace rg = ranges;
namespace rv = ranges::views;

namespace db {

class CommitBuilder;
class Change;

} // namespace db

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

void trimTrailingEmptyLines(std::string& trimmed,
                            std::vector<std::string>& lines) {
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

[[maybe_unused]] std::string valueToString(const Path& value) {
    std::string result;

    if (value.empty()) {
        return  "";
    }

    const auto reversed = value | std::views::reverse;
    size_t i = 0;
    for (auto val : reversed) {
        if (i % 2 == 0) {
            // NodeID
            result += fmt::format("({})", val.getValue());
        } else {
            // EdgeID
            result += fmt::format("-[{}]->", val.getValue());
        }
        ++i;
    }
    return result;
}

[[maybe_unused]] std::string valueToString(const std::string_view& value) {
    return std::string(value);
}

[[maybe_unused]] std::string valueToString(const db::ValueType& value) {
    return std::string(db::ValueTypeName::value(value));
}

template <IntegralType T, int tag>
[[maybe_unused]] std::string valueToString(const db::ID<T, tag> value) {
    return std::to_string(value.getValue());
}

template <int I>
[[maybe_unused]] std::string valueToString(const TemplateCommitHash<I>& value) {
    return std::to_string(value.get());
}

[[maybe_unused]] std::string valueToString(const db::CommitBuilder* value) {
    return value ? "commit_builder_ptr" : "null";
}

[[maybe_unused]] std::string valueToString(const db::Change* value) {
    return value ? "change_ptr" : "null";
}

[[maybe_unused]] std::string valueToString(const PropertyNull) {
    return "null";
}

[[maybe_unused]] std::string valueToString(const std::span<const float>& value) {
    std::string result = "[";
    if (value.size() > 0) {
        result += std::to_string(value[0]);
        for (size_t i = 1; i < value.size(); i++) {
            result += ",";
            result += std::to_string(value[i]);
        }
    }
    result += "]";
    return result;
}

template <typename T>
[[maybe_unused]] std::string valueToString(const T& value) {
    if constexpr (std::same_as<T, db::CustomBool>) {
        return value ? "true" : "false";
    } else {
        return fmt::format("{}", value);
    }
}

template <typename T>
[[maybe_unused]] std::string valueToString(const std::optional<T>& value) {
    if (!value.has_value()) {
        return "null";
    }

    return valueToString(*value);
}

[[maybe_unused]] std::string valueToString(const EntityList& value) {
    std::string s = "[";
    size_t i = 0;

    for (const auto& entry : value) {
        if (i++ > 0) {
            s += ", ";
        }

        if (entry._type == EntityType::Node) {
            s += fmt::format("({})", entry._id.getValue());
        } else {
            s += fmt::format("[{}]", entry._id.getValue());
        }
    }

    s += "]";

    return s;
}

// Forward declare so ListElementView overload sees this: an element may be a ListView itself
[[maybe_unused]] std::string valueToString(ListView view);

[[maybe_unused]] std::string valueToString(const ListElementView ele) {
    const auto writeTyped = []<typename T>(const ListElementView ele) -> std::string {
        const T typed = ele.getAs<T>();
        return valueToString(typed);
    };

    const ListBufferTypeTag tag = ele.getTag();
    ListTagDispatcher writer {._tag = tag};

    return writer.execute(writeTyped, ele);
}

[[maybe_unused]] std::string valueToString(const ListView view) {
    if (view.empty()) {
        return "[]";
    }

    std::string out;

    out += '[';

    const ListElementView fst = view.front();
    out += valueToString(fst);

    for (const ListElementView ele : view.elements() | rv::drop(1)) {
        out += ", ";
        out += valueToString(ele);
    }

    out += ']';

    return out;
}

struct Stringify {
    std::string& _string;
    size_t _row {0};

    template <typename T>
    void operator()(const ColumnVector<T>* typed) {
        const T& value = typed->at(_row);
        _string = valueToString(value);
    }

    template <typename T>
    void operator()(const ColumnConst<T>* typed) {
        const T& value = typed->at(_row);
        _string = valueToString(value);
    }
};

std::string columnValueToString(const db::Column* column, size_t row) {
    std::string string;
    Stringify stringify(string, row);

    using Types = OutputtedTypes;
    using Dispatcher = ColumnSingleDispatcher<Types::Allowed, Stringify, Types::Excluded>;

    Dispatcher::dispatch(column, stringify);

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

std::string formatStatusError(db::QueryStatus::Status status,
                              std::string_view message) {
    std::string statusName(db::QueryStatusDescription::value(status));

    for (char& ch : statusName) {
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
                       std::ostream& out,
                       db::PlanGenConfig* planGenConfig) {
    auto procedures = db::ProcedureManager::create();
    procedures->init();

    db::CypherAST ast(procedures.get(), query);
    db::CypherParser parser(&ast);
    db::CypherAnalyzer analyzer(&ast, view);
    db::PlanGraphGenerator planGen(planGenConfig, ast, view);

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

    db::PlanGraph& planGraph = planGen.getPlanGraph();

    db::LocalMemory mem;
    db::PlanOptimizer planOpt(&mem, &planGraph, view, &ast);
    planOpt.optimize();

    db::PlanGraphDebug::dumpMermaidContent(out, view, planGraph);
}

struct StringStreamWriter {
    std::string* _output {nullptr};

    void write(std::string_view content) { _output->append(content); }
    void write(char c) { _output->push_back(c); }
};

bool validateResultJson(std::string& error, std::string_view jsonStr) {
    json doc;
    try {
        doc = json::parse(jsonStr);
    } catch (const json::parse_error& e) {
        error = fmt::format("Invalid JSON: {}", e.what());
        return false;
    }

    if (!doc.is_object()) {
        error = "Root is not an object";
        return false;
    }

    if (doc.contains("error")) {
        if (!doc["error"].is_string()) {
            error = "\"error\" is not a string";
            return false;
        }

        if (!doc.contains("error_details") || !doc["error_details"].is_string()) {
            error = "\"error_details\" missing or not a string";
            return false;
        }
        return true;
    }

    if (!doc.contains("header") || !doc["header"].is_object()) {
        error = "\"header\" missing or not an object";
        return false;
    }

    const auto& header = doc["header"];

    if (!header.contains("column_names") || !header["column_names"].is_array()) {
        error = "\"column_names\" missing or not an array";
        return false;
    }

    if (!header.contains("column_types") || !header["column_types"].is_array()) {
        error = "\"column_types\" missing or not an array";
        return false;
    }

    const size_t numCols = header["column_names"].size();

    if (header["column_types"].size() != numCols) {
        error = fmt::format("column_types size ({}) != column_names size ({})",
                            header["column_types"].size(), numCols);
        return false;
    }

    if (!doc.contains("data") || !doc["data"].is_array()) {
        error = "\"data\" missing or not an array";
        return false;
    }

    const auto& data = doc["data"];

    for (size_t ci = 0; ci < data.size(); ++ci) {
        const auto& chunk = data[ci];

        if (!chunk.is_array()) {
            error = fmt::format("data[{}] is not an array", ci);
            return false;
        }

        if (chunk.size() != numCols) {
            error = fmt::format("data[{}] has {} columns, expected {}", ci,
                                chunk.size(), numCols);
            return false;
        }

        size_t expectedRows = 0;

        for (size_t col = 0; col < numCols; ++col) {
            if (!chunk[col].is_array()) {
                error = fmt::format("data[{}][{}] is not an array", ci, col);
                return false;
            }

            if (col == 0) {
                expectedRows = chunk[col].size();
            } else if (chunk[col].size() != expectedRows) {
                error = fmt::format("data[{}][{}] has {} rows, expected {} "
                                    "(from column 0)",
                                    ci, col, chunk[col].size(), expectedRows);
                return false;
            }
        }
    }

    return true;
}

}

void QueryTestRunner::loadTestsFromDir(std::vector<QueryTestSpec>& specs,
                                       const fs::Path& dir) {
    specs.clear();

    auto filesOpt = dir.listDir();
    if (!filesOpt) {
        throw FatalException(fmt::format("Failed to list directory: {}",
                                         filesOpt.error().fmtMessage()));
    }

    std::vector<fs::Path> files = *filesOpt;
    std::sort(
        files.begin(), files.end(),
        [](const fs::Path& a, const fs::Path& b) { return a.get() < b.get(); });

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
        spec._name = filename;
        spec._graphName = doc.value("graph", spec._graphName);
        spec._query = doc.value("query", "");
        spec._enabled = doc.value("enabled", true);
        spec._writeRequired = doc.value("write-required", false);
        spec._disabledReason = doc.value("disabled-reason", "");

        if (doc.contains("tags") && doc["tags"].is_array()) {
            for (const auto& tag : doc["tags"]) {
                if (tag.is_string()) {
                    spec._tags.push_back(tag.get<std::string>());
                }
            }
        }

        if (doc.contains("expect")) {
            const auto& expect = doc["expect"];
            spec._expectPlan = expect.value("plan", "");
            spec._expectResult = expect.value("result", "");
            spec._expectResultJson = expect.value("resultJson", "");
        }

        specs.push_back(std::move(spec));
    }
}

QueryTestResult QueryTestRunner::runTest(const QueryTestSpec& spec,
                                         const fs::Path& outDir) {
    QueryTestResult result;
    result._name = spec._name;

    auto env = turing::test::TuringTestEnv::create(outDir);
    db::Graph* graph = env->getSystemManager().createGraph(spec._graphName);
    db::SimpleGraph::createSimpleGraph(graph);
    db::TuringDB* db = &env->getDB();

    db::QueryConfig queryConfig;
    const bool forceVHJ = std::find(spec._tags.begin(), spec._tags.end(), "value-hash-join") != spec._tags.end();
    if (forceVHJ) {
        queryConfig.getPlanGenConfig().setForceValueHashJoin(true);
    }

    std::stringstream planOut;
    {
        const db::Transaction tx = graph->openTransaction();
        const db::GraphView view = tx.viewGraph();
        generatePlanGraph(spec._query, view, planOut, &queryConfig.getPlanGenConfig());
    }

    std::vector<std::vector<std::string>> rows;
    std::vector<std::string> columnNames;

    std::string jsonOutput;
    StringStreamWriter jsonWriter(&jsonOutput);
    db::JsonEncoder<StringStreamWriter> jsonEncoder(jsonWriter);
    bool jsonErrorEncoded = false;

    db::QueryCallbacks queryCallbacks;

    queryCallbacks.setOnBegin([&] { jsonEncoder.start(); });

    queryCallbacks.setOnOutputHeader([&](const db::Dataframe* df) {
        bioassert(df != nullptr, "Dataframe is null");

        // Write the header in the JSON output
        jsonEncoder.writeDataframeHeader(*df);

        // Write the header in the CSV output
        for (auto* col : df->cols()) {
            columnNames.emplace_back(col->getName());
        }
    });

    std::vector<std::string> values;

    queryCallbacks.setOnOutputData([&](const db::Dataframe* df) {
        bioassert(df != nullptr, "Dataframe is null");

        // Write the data in the JSON output
        jsonEncoder.writeDataframe(*df);

        // Write the data in the CSV output
        values.clear();
        const size_t rowCount = df->getLogicalRowCount();

        for (size_t row = 0; row < rowCount; ++row) {
            values.clear();
            values.reserve(df->cols().size());

            for (auto* col : df->cols()) {
                values.push_back(columnValueToString(col->getColumn(), row));
            }

            rows.push_back(std::move(values));
        }
    });

    queryCallbacks.setOnError([&](const db::QueryStatus& qs) {
        jsonEncoder.encodeError(qs.getStatus(), qs.getError());
        jsonErrorEncoded = true;
    });

    db::ChangeID changeID = db::ChangeID::head();

    if (spec._writeRequired) {
        db::QueryCallbacks changeNewCallbacks;
        changeNewCallbacks.setOnOutputData([&](const db::Dataframe* df) {
            NamedColumn* col = df->getColumn(ColumnTag {0});
            bioassert(col, "Column not found");

            auto& c = *static_cast<ColumnVector<ChangeID>*>(col->getColumn());
            bioassert(c.size() == 1, "Expected 1 change");

            changeID = c[0];
        });

        const db::QueryState changeNewState(spec._graphName, &env->getMem(), &queryConfig, &changeNewCallbacks);
        db->query("CHANGE NEW", changeNewState);
    }

    const db::QueryState queryState(spec._graphName, &env->getMem(), &queryConfig, &queryCallbacks, db::CommitHash::head(), changeID);
    const auto queryStart = Clock::now();
    const db::QueryStatus status = db->query(spec._query, queryState);
    const auto queryEnd = Clock::now();

    if (!status.isOk() && !jsonErrorEncoded) {
        jsonEncoder.encodeError(status.getStatus(), status.getError());
    }

    jsonEncoder.finish();
    result._timeUs = static_cast<uint64_t>(duration<Microseconds>(queryStart, queryEnd));

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

    if (spec._writeRequired) {
        db::QueryCallbacks submitCallbacks;
        const db::QueryState submitState(spec._graphName, &env->getMem(), &queryConfig, &submitCallbacks, db::CommitHash::head(), changeID);
        db->query("CHANGE SUBMIT", submitState);
    }

    normalizeOutput(result._planOutput, planOut.str());
    normalizeOutput(result._resultOutput, resultOut.str());
    result._resultJsonOutput = jsonOutput;

    std::string expected;

    normalizeOutput(expected, spec._expectPlan);
    result._planMatched = expected == result._planOutput;

    normalizeOutput(expected, spec._expectResult);
    result._resultMatched = expected == result._resultOutput;

    result._resultJsonValid = validateResultJson(result._resultJsonError, result._resultJsonOutput);
    result._resultJsonMatched = spec._expectResultJson == result._resultJsonOutput;

    return result;
}

void QueryTestRunner::normalizeOutput(std::string& normalized,
                                      std::string_view output) {
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
        throw FatalException(fmt::format("Failed to open file '{}': {}", path.get(),
                                         file.error().fmtMessage()));
    }

    const size_t fileSize = file->getInfo()._size;
    content.resize(fileSize);

    if (!file->read(content.data(), fileSize)) {
        throw FatalException(fmt::format("Failed to read file '{}': {}", path.get(),
                                         file.error().fmtMessage()));
    }
}

}
