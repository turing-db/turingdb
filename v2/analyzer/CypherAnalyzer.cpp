#include "CypherAnalyzer.h"

#include <spdlog/fmt/bundled/core.h>

#include "CypherAST.h"
#include "DiagnosticsManager.h"
#include "ReadStmtAnalyzer.h"
#include "WriteStmtAnalyzer.h"
#include "ExprAnalyzer.h"
#include "QueryCommand.h"
#include "SinglePartQuery.h"
#include "LoadGraphQuery.h"
#include "CreateGraphQuery.h"
#include "LoadGMLQuery.h"
#include "LoadNeo4jQuery.h"
#include "Projection.h"
#include "expr/Expr.h"
#include "stmt/StmtContainer.h"
#include "stmt/ReturnStmt.h"
#include "stmt/OrderBy.h"
#include "stmt/OrderByItem.h"
#include "stmt/Skip.h"
#include "stmt/CallStmt.h"
#include "stmt/Limit.h"

#include "AnalyzeException.h"

using namespace db::v2;

CypherAnalyzer::CypherAnalyzer(CypherAST* ast, GraphView graphView)
    : _ast(ast),
    _graphView(graphView),
    _graphMetadata(graphView.metadata()),
    _exprAnalyzer(std::make_unique<ExprAnalyzer>(_ast, _graphView)),
    _readAnalyzer(std::make_unique<ReadStmtAnalyzer>(_ast, _graphView)),
    _writeAnalyzer(std::make_unique<WriteStmtAnalyzer>(_ast, _graphView))
{
    _readAnalyzer->setExprAnalyzer(_exprAnalyzer.get());
    _writeAnalyzer->setExprAnalyzer(_exprAnalyzer.get());
}

CypherAnalyzer::~CypherAnalyzer() {
}

void CypherAnalyzer::analyze() {
    for (const QueryCommand* query : _ast->queries()) {
        DeclContext* ctxt = query->getDeclContext();

        _exprAnalyzer->setDeclContext(ctxt);
        _readAnalyzer->setDeclContext(ctxt);
        _writeAnalyzer->setDeclContext(ctxt);

        switch (query->getKind()) {
            case QueryCommand::Kind::SINGLE_PART_QUERY:
                analyze(static_cast<const SinglePartQuery*>(query));
            break;

            case QueryCommand::Kind::LOAD_GRAPH_QUERY:
                analyze(static_cast<const LoadGraphQuery*>(query));
            break;

            case QueryCommand::Kind::LOAD_GML_QUERY:
                analyze(static_cast<const LoadGMLQuery*>(query));
            break;

            case QueryCommand::Kind::CREATE_GRAPH_QUERY:
                analyze(static_cast<const CreateGraphQuery*>(query));
            break;

            // Nothing to analyze
            case QueryCommand::Kind::CHANGE_QUERY:
            case QueryCommand::Kind::LIST_GRAPH_QUERY:
            break;

            default:
                 throwError("Unsupported query type", query);
            break;
        }
    }
}

void CypherAnalyzer::analyze(const SinglePartQuery* query) {
    const StmtContainer* readStmts = query->getReadStmts();
    const StmtContainer* updateStmts = query->getUpdateStmts();
    const ReturnStmt* returnStmt = query->getReturnStmt();

    bool returnMandatory = updateStmts == nullptr;

    // Generate read statements (optional)
    if (readStmts) {
        for (const Stmt* stmt : readStmts->stmts()) {
            if (stmt->getKind() == Stmt::Kind::CALL) {
                if (static_cast<const CallStmt*>(stmt)->isStandaloneCall()) {
                    returnMandatory = false;
                }
            }
            _readAnalyzer->analyze(stmt);
        }
    }

    // Generate update statements (optional)
    if (updateStmts) {
        for (const Stmt* stmt : updateStmts->stmts()) {
            _writeAnalyzer->analyze(stmt);
        }
    }

    if (!returnStmt && returnMandatory) {
        // Return statement is mandatory if there are no update statements
        throwError("Return statement is missing", query);
    }

    // Generate return statement
    if (returnStmt) {
        analyze(returnStmt);
    }
}

void CypherAnalyzer::analyze(const ReturnStmt* returnSt) {
    Projection* projection = returnSt->getProjection();

    if (projection->isDistinct()) {
        throwError("DISTINCT not supported", returnSt);
    }

    if (projection->hasOrderBy()) {
        analyze(projection->getOrderBy());
    }

    if (projection->hasSkip()) {
        analyze(projection->getSkip());
    }

    if (projection->hasLimit()) {
        analyze(projection->getLimit());
    }

    // Check if the projection contains aggregate expressions and grouping keys
    bool isAggregate = false;
    bool hasGroupingKeys = false;

    for (Expr* item : projection->items()) {
        _exprAnalyzer->analyzeRootExpr(item);
        isAggregate |= item->isAggregate();
        hasGroupingKeys |= !item->isAggregate();
    }

    if (isAggregate) {
        projection->setAggregate();
        projection->setHasGroupingKeys(hasGroupingKeys);
    }
}

void CypherAnalyzer::analyze(OrderBy* orderBySt) {
    for (OrderByItem* item : orderBySt->getItems()) {
        Expr* expr = item->getExpr();
        _exprAnalyzer->analyzeRootExpr(expr);

        if (expr->isAggregate()) {
            throwError("Aggregate expressions in ORDER BY are not supported yet", orderBySt);
        }
    }
}

void CypherAnalyzer::analyze(Skip* skipSt) {
    Expr* expr = skipSt->getExpr();
    _exprAnalyzer->analyzeRootExpr(expr);

    if (expr->getType() != EvaluatedType::Integer) {
        throwError("SKIP expression must be an integer", skipSt);
    }

    if (expr->isDynamic() || expr->isAggregate()) {
        throwError("SKIP expression must be a value that can be evaluated at compile time", skipSt);
    }
}

void CypherAnalyzer::analyze(Limit* limitSt) {
    Expr* expr = limitSt->getExpr();
    _exprAnalyzer->analyzeRootExpr(expr);

    if (expr->getType() != EvaluatedType::Integer) {
        throwError("LIMIT expression must be an integer", limitSt);
    }

    if (expr->isDynamic() || expr->isAggregate()) {
        throwError("LIMIT expression must be a value that can be evaluated at compile time", limitSt);
    }
}

void CypherAnalyzer::analyze(const LoadGraphQuery* loadGraph) {
    const std::string_view graphName = loadGraph->getGraphName();
    if (graphName.empty()) {
        throwError("LOAD GRAPH should not have an empty graph name");
    }

    // Check that the graph name is only [A-Z0-9_]+
    for (char c : graphName) {
        if (!(isalnum(c) || c == '_')) [[unlikely]] {
            throwError(fmt::format("Graph name must only contain alphanumeric characters or '_': character '{}' not allowed.", c), loadGraph);
        }
    }
}

void CypherAnalyzer::analyze(const CreateGraphQuery* createGraph) {
    std::string_view graphName = createGraph->getGraphName();
    if (graphName.empty()) {
        throwError("CREATE GRAPH should not have an empty graph name");
        // Check that the graph name is only [A-Z0-9_]+
        for (char c : graphName) {
            if (!(isalnum(c) || c == '_')) [[unlikely]] {
                throwError(fmt::format("Graph name must only contain alphanumeric characters or '_': "
                                       "character '{}' not allowed.",
                                       c),
                           createGraph);
            }
        }
    }
}

void CypherAnalyzer::analyze(const LoadNeo4jQuery* loadNeo4j) {
    std::string_view graphName = loadNeo4j->getGraphName();
    if (graphName.empty()) {
        graphName = loadNeo4j->getPath().basename();
    }

    // Check that the graph name is only [A-Z0-9_]+
    for (char c : graphName) {
        if (!(isalnum(c) || c == '_')) [[unlikely]] {
            throwError(fmt::format("Graph name must only contain alphanumeric characters or '_': "
                                   "character '{}' not allowed.",
                                   c),
                       loadNeo4j);
        }
    }
}

void CypherAnalyzer::analyze(const LoadGMLQuery* loadGML) {
    std::string_view graphName = loadGML->getGraphName();
    if (graphName.empty()) {
        graphName = loadGML->getFilePath().basename();
    }

    // Check that the graph name is only [A-Z0-9_]+
    for (char c : graphName) {
        if (!(isalnum(c) || c == '_')) [[unlikely]] {
            throwError(fmt::format("Graph name must only contain alphanumeric characters or '_': "
                                   "character '{}' not allowed.",
                                   c),
                       loadGML);
        }
    }
}

void CypherAnalyzer::throwError(std::string_view msg, const void* obj) const {
    throw AnalyzeException(_ast->getDiagnosticsManager()->createErrorString(msg, obj));
}
