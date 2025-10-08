#include "CypherAnalyzer.h"

#include <spdlog/fmt/bundled/core.h>

#include "AnalyzeException.h"

#include "AnalyzerVariables.h"
#include "CypherAST.h"
#include "QueryCommand.h"
#include "ReadStmtAnalyzer.h"
#include "ExprAnalyzer.h"
#include "WriteStmtAnalyzer.h"
#include "SinglePartQuery.h"
#include "stmt/MatchStmt.h"
#include "stmt/CreateStmt.h"
#include "stmt/ReturnStmt.h"
#include "stmt/StmtContainer.h"
#include "Projection.h"
#include "WhereClause.h"
#include "Pattern.h"
#include "PatternElement.h"
#include "Symbol.h"
#include "NodePattern.h"
#include "EdgePattern.h"
#include "decl/PatternData.h"
#include "expr/Literal.h"
#include "decl/VarDecl.h"
#include "decl/DeclContext.h"
#include "QualifiedName.h"

using namespace db::v2;

CypherAnalyzer::CypherAnalyzer(CypherAST* ast, GraphView graphView)
    : _ast(ast),
    _graphView(graphView),
    _graphMetadata(graphView.metadata()),
    _variables(std::make_unique<AnalyzerVariables>(_ast)),
    _exprAnalyzer(std::make_unique<ExprAnalyzer>(_ast, _graphView)),
    _readAnalyzer(std::make_unique<ReadStmtAnalyzer>(_ast, _graphView)),
    _writeAnalyzer(std::make_unique<WriteStmtAnalyzer>(_ast, _graphView))
{
    _exprAnalyzer->setVariables(_variables.get());
    _readAnalyzer->setVariables(_variables.get());
    _readAnalyzer->setExprAnalyzer(_exprAnalyzer.get());
    _writeAnalyzer->setVariables(_variables.get());
    _writeAnalyzer->setExprAnalyzer(_exprAnalyzer.get());
}

CypherAnalyzer::~CypherAnalyzer() {
}

void CypherAnalyzer::analyze() {
    for (const QueryCommand* query : _ast->queries()) {
        _variables->setContext(query->getDeclContext());

        if (const SinglePartQuery* q = dynamic_cast<const SinglePartQuery*>(query)) {
            analyze(q);
        } else {
            throwError("Unsupported query type", query);
        }
    }
}

void CypherAnalyzer::analyze(const SinglePartQuery* query) {
    const StmtContainer* readStmts = query->getReadStmts();
    const StmtContainer* updateStmts = query->getUpdateStmts();
    const ReturnStmt* returnStmt = query->getReturnStmt();

    // Generate read statements (optional)
    if (readStmts) {
        for (const Stmt* stmt : readStmts->stmts()) {
            _readAnalyzer->analyze(stmt);
        }
    }

    // Generate update statements (optional)
    if (updateStmts) {
        for (const Stmt* stmt : updateStmts->stmts()) {
            _writeAnalyzer->analyze(stmt);
        }
    } else {
        if (!returnStmt) {
            // Return statement is mandatory if there are no update statements
            throwError("Return statement is missing", query);
        }
    }

    // Generate return statement
    if (returnStmt) {
        analyze(returnStmt);
    }
}

void CypherAnalyzer::analyze(const ReturnStmt* returnSt) {
    const Projection* projection = returnSt->getProjection();
    if (projection->isDistinct()) {
        throwError("DISTINCT not supported", returnSt);
    }

    if (projection->hasSkip()) {
        throwError("SKIP not supported", returnSt);
    }

    if (projection->hasLimit()) {
        throwError("LIMIT not supported", returnSt);
    }

    if (projection->isAll()) {
        return;
    }

    for (Expr* item : projection->items()) {
        _exprAnalyzer->analyze(item);
    }
}

void CypherAnalyzer::throwError(std::string_view msg, const void* obj) const {
    throw AnalyzeException(_ast->createErrorString(msg, obj));
}
