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

    bool isForeach() const { return _isForeach; }
    void setIsForeach(bool isForeach) { _isForeach = isForeach; }

    /// Whether every row this UNWIND emits is known at plan time: the argument is a
    /// literal, and a list or map literal's values are literals too, at any depth.
    /// Anything else is evaluated per row, against the rows already in flight, and so is
    /// a literal whose variable a pattern reads as an entity, which each row is checked
    /// to hold.
    bool unwindsLiteral() const;

private:
    Expr* _arg {nullptr};
    Symbol* _symbol {nullptr};
    const VarDecl* _decl {nullptr};
    bool _isForeach {false};

    explicit UnwindStmt(Expr* arg, Symbol* sym)
        : _arg(arg),
        _symbol(sym)
    {
    }

    ~UnwindStmt() final;
};

}
