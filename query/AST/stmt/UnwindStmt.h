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

    Expr* arg() const { return _arg; }
    const Symbol* symbol() const { return _symbol; }
    const VarDecl* getDecl() const { return _decl; }

    void setDecl(const VarDecl* decl) { _decl = decl; }

    /// Whether every row this UNWIND emits is known at plan time: the argument is a
    /// literal, and a list literal's items are literals too, at any depth. Anything else
    /// is evaluated per row, against the rows already in flight.
    bool unwindsLiteral() const;

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
