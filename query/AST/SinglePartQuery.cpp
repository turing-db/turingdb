#include "SinglePartQuery.h"

#include <algorithm>

#include "CypherAST.h"
#include "decl/DeclContext.h"
#include "stmt/StmtContainer.h"

using namespace db;

SinglePartQuery::SinglePartQuery(DeclContext* declContext)
    : QueryCommand(declContext)
{
}

SinglePartQuery::~SinglePartQuery() {
}

SinglePartQuery* SinglePartQuery::create(CypherAST* ast) {
    DeclContext* declContext = DeclContext::create(ast, nullptr);
    SinglePartQuery* query = new SinglePartQuery(declContext);
    ast->addQuery(query);
    return query;
}

void SinglePartQuery::addStmt(Stmt* stmt) {
    _stmts->add(stmt);
}

bool SinglePartQuery::writesToTheGraph() const {
    if (!_stmts) {
        return false;
    }

    const auto writes = [](const Stmt* stmt) {
        return Stmt::writesToTheGraph(stmt);
    };

    return std::ranges::any_of(_stmts->stmts(), writes);
}

bool SinglePartQuery::readsTheGraph() const {
    if (!_stmts) {
        return false;
    }

    const auto reads = [](const Stmt* stmt) {
        return Stmt::readsTheGraph(stmt);
    };

    return std::ranges::any_of(_stmts->stmts(), reads);
}
