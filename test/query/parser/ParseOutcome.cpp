#include "ParseOutcome.h"

#include <exception>

#include "CypherAST.h"
#include "CypherParser.h"
#include "ParserException.h"

using namespace db;

namespace {

constexpr std::string_view REPORT_TITLE = "-------* Cypher parser\n";

}

void turing::test::renderParseError(std::string_view query, std::string& report) {
    report.clear();

    CypherAST ast(nullptr, query);
    CypherParser parser(&ast);

    try {
        parser.parse(query);
    } catch (const ParserException& error) {
        std::string_view message = error.what();
        if (message.starts_with(REPORT_TITLE)) {
            message.remove_prefix(REPORT_TITLE.size());
        }

        report = message;
    }
}

bool turing::test::isWellFormedParse(std::string_view query, std::string& problem) {
    problem.clear();

    CypherAST ast(nullptr, query);
    CypherParser parser(&ast);

    try {
        parser.parse(query);
    } catch (const ParserException& error) {
        const std::string_view report = error.what();
        const size_t messageLine = report.rfind("\n-------* ");

        const bool titled = report.starts_with(REPORT_TITLE);
        const bool hasMessage = messageLine != std::string_view::npos && messageLine + 10 < report.size();
        if (!titled || !hasMessage) {
            problem = "malformed report: ";
            problem += report;
        }
    } catch (const std::exception& error) {
        problem = "unexpected exception: ";
        problem += error.what();
    }

    return problem.empty();
}
