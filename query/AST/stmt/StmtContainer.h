#pragma once

#include <vector>

#include "Stmt.h"

namespace db {

class CypherAST;

class StmtContainer {
public:
    friend CypherAST;
    using Stmts = std::vector<Stmt*>;

    static StmtContainer* create(CypherAST* ast);

    void add(Stmt* stmt);

    const Stmts& stmts() const { return _stmts; }

private:
    Stmts _stmts;

    StmtContainer();
    ~StmtContainer();
};

}
