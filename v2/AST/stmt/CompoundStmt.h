#pragma once

#include <vector>

#include "Stmt.h"

namespace db::v2 {

class CypherAST;

class CompoundStmt : public Stmt {
public:
    using Stmts = std::vector<const Stmt*>;

    static CompoundStmt* create(CypherAST* ast);

    void add(const Stmt* stmt) {
        _stmts.push_back(stmt);
    }

    const Stmts& stmts() const { return _stmts; }

private:
    Stmts _stmts;

    CompoundStmt();
    ~CompoundStmt() override;
};

}
