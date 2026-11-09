#include "QueryTestRunner.h"

#include <algorithm>
#include <sstream>

#include <nlohmann/json.hpp>
#include <spdlog/fmt/bundled/format.h>

#include "CypherLexer.h"
#include "FatalException.h"
#include "File.h"

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

        if (doc.contains("setup")) {
            const json& setup = doc["setup"];
            const bool isStatementList = setup.is_array()
                                         && std::all_of(setup.begin(), setup.end(), [](const json& statement) { return statement.is_string(); });
            if (!isStatementList) {
                throw FatalException(fmt::format("The setup of test {} must be a list of statements", filename));
            }

            for (const json& statement : setup) {
                spec._setup.push_back(statement.get<std::string>());
            }
        }

        if (doc.contains("tags") && doc["tags"].is_array()) {
            for (const auto& tag : doc["tags"]) {
                if (tag.is_string()) {
                    spec._tags.push_back(tag.get<std::string>());
                }
            }
        }

        if (doc.contains("expect")) {
            const auto& expect = doc["expect"];
            spec._expectResult = expect.value("result", "");
            spec._expectMlir = expect.value("mlir", "");
        }

        specs.push_back(std::move(spec));
    }
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

void QueryTestRunner::splitStatements(std::vector<std::string>& statements, std::string_view script) {
    statements.clear();

    db::CypherLexer lexer(script);
    db::CypherToken token;

    bool inStatement = false;
    size_t statementBegin = 0;
    size_t statementEnd = 0;

    do {
        lexer.lex(token);

        const bool endsStatement = token._kind == db::CypherTokenKind::SEMI_COLON
                                   || token._kind == db::CypherTokenKind::PROG_END;

        if (token._kind == db::CypherTokenKind::LEXER_ERROR) {
            throw FatalException(fmt::format("Cannot split the script into statements at line {}: {}",
                                             token._location._beginLine,
                                             token._text));
        } else if (!endsStatement) {
            if (!inStatement) {
                statementBegin = token._location._beginOffset;
                inStatement = true;
            }

            statementEnd = token._location._endOffset;
        } else if (inStatement) {
            statements.emplace_back(script.substr(statementBegin, statementEnd - statementBegin));
            inStatement = false;
        }
    } while (token._kind != db::CypherTokenKind::PROG_END);
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
