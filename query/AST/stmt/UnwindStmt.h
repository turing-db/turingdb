#pragma once

#include "Stmt.h"

namespace db {

class CypherAST;
class Expr;
class Symbol;
class VarDecl;

class UnwindStmt final : public Stmt {
public:
    static UnwindStmt* create(CypherAST* ast, Expr* arg, Symbol* sym);

    Kind getKind() const final { return Kind::UNWIND; }

    const Expr* arg() const { return _arg; }
    const Symbol* symbol() const { return _symbol; }
    const VarDecl* getDecl() const { return _decl; }

    void setDecl(const VarDecl* decl) { _decl = decl; }

private:
    Expr* _arg {nullptr};
    Symbol* _symbol {nullptr};
    const VarDecl* _decl {nullptr};

    explicit UnwindStmt(Expr* arg, Symbol* sym)
        : _arg(arg),
        _symbol(sym)
    {
    }

    ~UnwindStmt() final;
};

}
