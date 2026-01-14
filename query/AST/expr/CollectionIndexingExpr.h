#pragma once

#include "Expr.h"
#include <variant>

namespace db {

class CypherAST;
class Expr;

class CollectionIndexingExpr : public Expr {
public:
    struct ElementRange {
        Expr* _lowerBound {nullptr};
        Expr* _upperBound {nullptr};
    };

    struct SingleElement {
        Expr* _index {nullptr};
    };

    using IndexExpr = std::variant<std::monostate, ElementRange, SingleElement>;

    static CollectionIndexingExpr* create(CypherAST* ast);

    Expr* getLhsExpr() const { return _lhsExpr; }
    const IndexExpr& getIndexExpr() const { return _indexExpr; }

    void setLhsExpr(Expr* expr) { _lhsExpr = expr; }
    void setIndexExpr(const IndexExpr& expr) { _indexExpr = expr; }

private:
    Expr* _lhsExpr {nullptr};
    IndexExpr _indexExpr;

    explicit CollectionIndexingExpr()
        : Expr(Kind::LIST_INDEXING)
    {
    }

    ~CollectionIndexingExpr() override;
};

}
