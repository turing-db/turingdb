#include "CallSubqueryStmt.h"

#include "CypherAST.h"
#include "SinglePartQuery.h"

using namespace db;

CallSubqueryStmt::CallSubqueryStmt(const Branches& branches)
    : _branches(branches)
{
}

CallSubqueryStmt::~CallSubqueryStmt() {
}

CallSubqueryStmt* CallSubqueryStmt::create(CypherAST* ast, const Branches& branches) {
    for (const Branch& branch : branches) {
        ast->nestQuery(branch._query);
    }

    CallSubqueryStmt* stmt = new CallSubqueryStmt(branches);
    ast->addStmt(stmt);

    return stmt;
}

size_t CallSubqueryStmt::getDedupedBranchCount() const {
    size_t deduped = 0;

    for (size_t index = 1; index < _branches.size(); index++) {
        if (!_branches[index]._all) {
            deduped = index + 1;
        }
    }

    return deduped;
}

bool CallSubqueryStmt::isReturning() const {
    return _branches.front()._query->getReturnStmt() != nullptr;
}

bool CallSubqueryStmt::writesToTheGraph() const {
    for (const Branch& branch : _branches) {
        if (branch._query->writesToTheGraph()) {
            return true;
        }
    }

    return false;
}

bool CallSubqueryStmt::readsTheGraph() const {
    for (const Branch& branch : _branches) {
        if (branch._query->readsTheGraph()) {
            return true;
        }
    }

    return false;
}

void CallSubqueryStmt::addImport(const Symbol* symbol) {
    for (Branch& branch : _branches) {
        branch._imports.push_back(symbol);
    }
}
