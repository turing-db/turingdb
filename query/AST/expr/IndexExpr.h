#pragma once

#include "Expr.h"

namespace db {

class CypherAST;

class IndexExpr : public Expr {
public:
    static IndexExpr* create(CypherAST* ast, Expr* base, Expr* indexExpr);

    Expr* getBase() const { return _base; }
    Expr* getIndexExpr() const { return _indexExpr; }

    bool hasLiteralIndex() const { return _hasLiteralIndex; }
    size_t getLiteralIndex() const { return _literalIndex; }

    void setLiteralIndex(size_t index) {
        _hasLiteralIndex = true;
        _literalIndex = index;
    }

private:
    Expr* _base {nullptr};
    Expr* _indexExpr {nullptr};
    bool _hasLiteralIndex {false};
    size_t _literalIndex {0};

    IndexExpr(Expr* base, Expr* indexExpr);

    ~IndexExpr() override;
};

}
