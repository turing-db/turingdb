#include "StmtContainer.h"

#include "CypherAST.h"

using namespace db;

StmtContainer::StmtContainer()
{
}

StmtContainer::~StmtContainer() {
}

StmtContainer* StmtContainer::create(CypherAST* ast) {
    StmtContainer* container = new StmtContainer();
    ast->addStmtContainer(container);
    return container;
}

void StmtContainer::add(const Stmt* stmt) {
    _stmts.push_back(stmt);
}
