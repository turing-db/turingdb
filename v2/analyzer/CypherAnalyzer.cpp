#include "CypherAnalyzer.h"

#include <spdlog/fmt/bundled/core.h>

#include "AnalyzeException.h"

#include "CypherAST.h"
#include "DiagnosticsManager.h"
#include "QueryCommand.h"
#include "ReadStmtAnalyzer.h"
#include "ExprAnalyzer.h"
#include "WriteStmtAnalyzer.h"
#include "SinglePartQuery.h"
#include "expr/Expr.h"
#include "stmt/Limit.h"
#include "stmt/MatchStmt.h"
#include "stmt/CreateStmt.h"
#include "stmt/ReturnStmt.h"
#include "stmt/Skip.h"
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
        analyze(projection->getSkip());
    }

    if (projection->hasLimit()) {
        analyze(projection->getLimit());
    }

    if (projection->isAll()) {
        return;
    }

    for (Expr* item : projection->items()) {
        _exprAnalyzer->analyze(item);
    }
}

void CypherAnalyzer::analyze(Skip* skipSt) {
    Expr* expr = skipSt->getExpr();
    _exprAnalyzer->analyze(expr);
    
    if (expr->getType() != EvaluatedType::Integer) {
        throwError("SKIP expression must be an integer", skipSt);
    }
}

void CypherAnalyzer::analyze(Limit* limitSt) {
    Expr* expr = limitSt->getExpr();
    _exprAnalyzer->analyze(expr);

    if (expr->getType() != EvaluatedType::Integer) {
        throwError("LIMIT expression must be an integer", limitSt);
    }
}

void CypherAnalyzer::throwError(std::string_view msg, const void* obj) const {
    throw AnalyzeException(_ast->getDiagnosticsManager()->createErrorString(msg, obj));
}
