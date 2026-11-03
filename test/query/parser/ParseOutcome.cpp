#include "ParseOutcome.h"

#include <stdint.h>
#include <exception>

#include "CypherAST.h"
#include "CypherParser.h"
#include "EntityPattern.h"
#include "ExplainRequest.h"
#include "NodePattern.h"
#include "ParserException.h"
#include "QueryCommand.h"
#include "SourceManager.h"
#include "expr/Expr.h"

using namespace db;

namespace {

constexpr std::string_view REPORT_TITLE = "-------* Cypher parser\n";

std::string_view getQueryKindName(QueryCommand::Kind kind) {
    switch (kind) {
        case QueryCommand::Kind::SINGLE_PART_QUERY:
            return "SINGLE_PART_QUERY";
        break;
        case QueryCommand::Kind::LOAD_GRAPH_QUERY:
            return "LOAD_GRAPH_QUERY";
        break;
        case QueryCommand::Kind::LOAD_JSONL_QUERY:
            return "LOAD_JSONL_QUERY";
        break;
        case QueryCommand::Kind::CHANGE_QUERY:
            return "CHANGE_QUERY";
        break;
        case QueryCommand::Kind::COMMIT_QUERY:
            return "COMMIT_QUERY";
        break;
        case QueryCommand::Kind::LIST_GRAPH_QUERY:
            return "LIST_GRAPH_QUERY";
        break;
        case QueryCommand::Kind::LIST_AVAILABLE_GRAPHS_QUERY:
            return "LIST_AVAILABLE_GRAPHS_QUERY";
        break;
        case QueryCommand::Kind::CREATE_GRAPH_QUERY:
            return "CREATE_GRAPH_QUERY";
        break;
        case QueryCommand::Kind::LOAD_GML_QUERY:
            return "LOAD_GML_QUERY";
        break;
        case QueryCommand::Kind::LOAD_PARQUET_QUERY:
            return "LOAD_PARQUET_QUERY";
        break;
        case QueryCommand::Kind::S3_CONNECT_QUERY:
            return "S3_CONNECT_QUERY";
        break;
        case QueryCommand::Kind::S3_TRANSFER_QUERY:
            return "S3_TRANSFER_QUERY";
        break;
        case QueryCommand::Kind::SHOW_PROCEDURES_QUERY:
            return "SHOW_PROCEDURES_QUERY";
        break;
        case QueryCommand::Kind::CREATE_VECTOR_INDEX_QUERY:
            return "CREATE_VECTOR_INDEX_QUERY";
        break;
        case QueryCommand::Kind::LOAD_VECTOR_QUERY:
            return "LOAD_VECTOR_QUERY";
        break;
        case QueryCommand::Kind::LOAD_EMBEDDING_QUERY:
            return "LOAD_EMBEDDING_QUERY";
        break;
        case QueryCommand::Kind::DELETE_VECTOR_INDEX_QUERY:
            return "DELETE_VECTOR_INDEX_QUERY";
        break;
        case QueryCommand::Kind::SHOW_VECTOR_INDEXES_QUERY:
            return "SHOW_VECTOR_INDEXES_QUERY";
        break;
        case QueryCommand::Kind::LOAD_COMMIT_QUERY:
            return "LOAD_COMMIT_QUERY";
        break;
        case QueryCommand::Kind::INSTALL_EXTENSION_QUERY:
            return "INSTALL_EXTENSION_QUERY";
        break;
        case QueryCommand::Kind::SHOW_EXTENSIONS_QUERY:
            return "SHOW_EXTENSIONS_QUERY";
        break;
        case QueryCommand::Kind::CREATE_NODE_PROPERTY_INDEX_QUERY:
            return "CREATE_NODE_PROPERTY_INDEX_QUERY";
        break;
        case QueryCommand::Kind::CREATE_EDGE_PROPERTY_INDEX_QUERY:
            return "CREATE_EDGE_PROPERTY_INDEX_QUERY";
        break;
        case QueryCommand::Kind::DROP_INDEX_QUERY:
            return "DROP_INDEX_QUERY";
        break;
        case QueryCommand::Kind::MERGE_DATAPARTS_QUERY:
            return "MERGE_DATAPARTS_QUERY";
        break;
        case QueryCommand::Kind::UNION_QUERY:
            return "UNION_QUERY";
        break;
        case QueryCommand::Kind::CONDITIONAL_QUERY:
            return "CONDITIONAL_QUERY";
        break;
    }

    return "UNKNOWN_QUERY";
}

std::string_view getExprKindName(Expr::Kind kind) {
    switch (kind) {
        case Expr::Kind::BINARY:
            return "BINARY";
        break;
        case Expr::Kind::LOGICAL:
            return "LOGICAL";
        break;
        case Expr::Kind::UNARY:
            return "UNARY";
        break;
        case Expr::Kind::STRING:
            return "STRING";
        break;
        case Expr::Kind::ENTITY_TYPES:
            return "ENTITY_TYPES";
        break;
        case Expr::Kind::PROPERTY:
            return "PROPERTY";
        break;
        case Expr::Kind::PROPERTY_LOOKUP:
            return "PROPERTY_LOOKUP";
        break;
        case Expr::Kind::SYMBOL:
            return "SYMBOL";
        break;
        case Expr::Kind::LITERAL:
            return "LITERAL";
        break;
        case Expr::Kind::FUNCTION_INVOCATION:
            return "FUNCTION_INVOCATION";
        break;
        case Expr::Kind::INDEX:
            return "INDEX";
        break;
        case Expr::Kind::LIST:
            return "LIST";
        break;
        case Expr::Kind::LIST_SLICE:
            return "LIST_SLICE";
        break;
        case Expr::Kind::LIST_COMPREHENSION:
            return "LIST_COMPREHENSION";
        break;
        case Expr::Kind::PATTERN_COMPREHENSION:
            return "PATTERN_COMPREHENSION";
        break;
        case Expr::Kind::LIST_PREDICATE:
            return "LIST_PREDICATE";
        break;
        case Expr::Kind::REDUCE:
            return "REDUCE";
        break;
        case Expr::Kind::ALL_REDUCE:
            return "ALL_REDUCE";
        break;
        case Expr::Kind::MAP_PROJECTION:
            return "MAP_PROJECTION";
        break;
        case Expr::Kind::CASE:
            return "CASE";
        break;
        case Expr::Kind::EXISTS:
            return "EXISTS";
        break;
        case Expr::Kind::COUNT_SUBQUERY:
            return "COUNT_SUBQUERY";
        break;
        case Expr::Kind::COLLECT_SUBQUERY:
            return "COLLECT_SUBQUERY";
        break;
        case Expr::Kind::PARAMETER:
            return "PARAMETER";
        break;
        case Expr::Kind::_SIZE:
        break;
    }

    return "UNKNOWN_EXPR";
}

void appendSpannedText(const SourceManager* sourceManager, uintptr_t object, std::string& outcome) {
    if (!sourceManager->getLocation(object)) {
        outcome += " (no location)\n";
        return;
    }

    outcome += " `";
    for (const char character : sourceManager->getStringRepr(object)) {
        if (character == '\n') {
            outcome += "\\n";
        } else {
            outcome += character;
        }
    }

    outcome += "`\n";
}

void appendExplainRequest(const ExplainRequest* explain, std::string& outcome) {
    static constexpr ExplainStage STAGES[] = {
        ExplainStage::AST,
        ExplainStage::VDG,
        ExplainStage::CODEGEN,
        ExplainStage::PASSES,
        ExplainStage::DB,
        ExplainStage::NL,
    };

    outcome += "explain";
    for (const ExplainStage stage : STAGES) {
        if (explain->isRequested(stage)) {
            outcome += " ";
            outcome += ExplainRequest::getStageName(stage);
        }
    }

    if (explain->hasNamedPasses()) {
        outcome += " with named passes";
    }

    outcome += "\n";
}

}

void turing::test::renderParseOutcome(std::string_view query, std::string& outcome) {
    outcome.clear();

    CypherAST ast(nullptr, query);
    CypherParser parser(&ast);

    try {
        parser.parse(query);
    } catch (const ParserException& error) {
        std::string_view report = error.what();
        if (report.starts_with(REPORT_TITLE)) {
            report.remove_prefix(REPORT_TITLE.size());
        }

        outcome = report;
        return;
    }

    for (const QueryCommand* command : ast.queries()) {
        outcome += getQueryKindName(command->getKind());
        outcome += "\n";
    }

    if (const ExplainRequest* explain = ast.getExplainRequest()) {
        appendExplainRequest(explain, outcome);
    }

    const SourceManager* sourceManager = ast.getSourceManager();

    for (const Expr* expr : ast.getExpressions()) {
        outcome += getExprKindName(expr->getKind());
        appendSpannedText(sourceManager, reinterpret_cast<uintptr_t>(expr), outcome);
    }

    for (const EntityPattern* entity : ast.getEntityPatterns()) {
        outcome += dynamic_cast<const NodePattern*>(entity) ? "node" : "edge";
        appendSpannedText(sourceManager, reinterpret_cast<uintptr_t>(entity), outcome);
    }

    if (outcome.ends_with('\n')) {
        outcome.pop_back();
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
