#pragma once

#include <stdint.h>

#include "Expr.h"

namespace db {

class CypherAST;
class ListComprehensionExpr;

// A Cypher list predicate: `all(x IN xs WHERE p(x))`, and `any`, `none` and `single`
// over the same form. The comprehension binds the variable to each element and holds
// the predicate; the quantifier says how many elements it must hold for.
class ListPredicateExpr : public Expr {
public:
    enum class Quantifier : uint8_t {
        All,
        Any,
        None,
        Single,
    };

    Quantifier getQuantifier() const { return _quantifier; }
    ListComprehensionExpr* getComprehension() const { return _comprehension; }

    static ListPredicateExpr* create(CypherAST* ast, Quantifier quantifier, ListComprehensionExpr* comprehension);

private:
    Quantifier _quantifier {Quantifier::All};
    ListComprehensionExpr* _comprehension {nullptr};

    ListPredicateExpr(Quantifier quantifier, ListComprehensionExpr* comprehension);
    ~ListPredicateExpr() override;
};

}
