#pragma once

#include "Expr.h"

namespace db {

class CypherAST;
class VarDecl;

class IndexExpr : public Expr {
public:
    static IndexExpr* create(CypherAST* ast, Expr* base, Expr* indexExpr);

    Expr* getBase() const { return _base; }
    Expr* getIndexExpr() const { return _indexExpr; }

    bool hasLiteralIndex() const { return _hasLiteralIndex; }
    size_t getLiteralIndex() const { return _literalIndex; }

    // The field of the loaded row this access reads, under the declaration the load
    // publishes its column with. Null when the access names no field of a load, which a
    // computed index does. Kept apart from the expression's own declaration, since an
    // alias on the item replaces that one.
    VarDecl* getCSVFieldDecl() const { return _csvFieldDecl; }

    void setLiteralIndex(size_t index) {
        _hasLiteralIndex = true;
        _literalIndex = index;
    }

    void setCSVFieldDecl(VarDecl* decl) { _csvFieldDecl = decl; }

private:
    Expr* _base {nullptr};
    Expr* _indexExpr {nullptr};
    VarDecl* _csvFieldDecl {nullptr};
    bool _hasLiteralIndex {false};
    size_t _literalIndex {0};

    IndexExpr(Expr* base, Expr* indexExpr);

    ~IndexExpr() override;
};

}
