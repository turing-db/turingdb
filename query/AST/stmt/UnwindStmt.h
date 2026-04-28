#pragma once

#include "Stmt.h"

namespace db {

class CypherAST;
class Expr;
class Symbol;

class UnwindStmt final : public Stmt {
public:
    static UnwindStmt* create(CypherAST* ast, Expr* expr, Symbol* sym);

    Kind getKind() const final { return Kind::UNWIND; }

private:
    Expr* _pattern {nullptr};
    Symbol* _symbol {nullptr};

    explicit UnwindStmt(Expr* pattern, Symbol* sym)
        : _pattern(pattern),
        _symbol(sym)
    {
    }

    ~UnwindStmt() final;
};

}
