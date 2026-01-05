#pragma once

#include <vector>

#include "Stmt.h"

namespace db::v2 {

class CypherAST;

class StmtContainer {
public:
    friend CypherAST;
    using Stmts = std::vector<const Stmt*>;

    static StmtContainer* create(CypherAST* ast);

    void add(const Stmt* stmt);

    const Stmts& stmts() const { return _stmts; }

private:
    Stmts _stmts;

    StmtContainer();
    ~StmtContainer();
};

}
