#pragma once

#include "Stmt.h"

namespace db {

class CypherAST;
class Expr;
class Symbol;

class UnwindStmt final : public Stmt {
public:
    static UnwindStmt* create(CypherAST* ast, Expr* arg, Symbol* sym);

    Kind getKind() const final { return Kind::UNWIND; }

    const Expr* arg() const { return _arg; }
    const Symbol* symbol() const { return _symbol; }

private:
    Expr* _arg {nullptr};
    Symbol* _symbol {nullptr};

    explicit UnwindStmt(Expr* arg, Symbol* sym)
        : _arg(arg),
        _symbol(sym)
    {
    }

    ~UnwindStmt() final;
};

}
