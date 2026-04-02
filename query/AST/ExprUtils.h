#pragma once

#include <vector>

#include "ID.h"
#include "Literal.h"
#include "decl/VarDecl.h"
#include "expr/Expr.h"
#include "expr/LiteralExpr.h"
#include "expr/Operators.h"
#include "expr/PropertyExpr.h"
#include "expr/SymbolExpr.h"

#include "BioAssert.h"

namespace db {

class ExprUtils {
public:
    ExprUtils() = delete;
    ~ExprUtils() = delete;

    /**
     * @brief Helper to collect from a binary chain of homogeneous operations, e.g.
     * a disjunction of equalities (x = 10 OR x = 20 OR x = 30)
     * @param Traits struct defining the required necessary types and values to perform
     * extraction. See implementation for examples.
     */
    template <typename Traits>
    static bool collectFromHomogeneousBinaryChain(const Expr* root,
                                                  typename Traits::ValidatorType var,
                                                  std::vector<typename Traits::ResultType>& result);

    // Used for extracting `n = 10 OR n = 15`, etc. to rewrite a ScanNodes with a
    // ConstScan
    struct NodeIDEqualsOR;

    // Used for extracting `n.age = 10 OR n.age = 20`, etc. to rewrite a Filter with a
    // IndexLookup
    struct PropertyEqualsOR;
};

/// Extract NodeIDs from a chain of n = ID0 OR n = ID1 OR ...
struct ExprUtils::NodeIDEqualsOR {
    using ValidatorType = const VarDecl*;

    // The operator used to connect each leaf (e.g. x = 10 *OR* x = 20)
    static constexpr BinaryOperator chainOp = BinaryOperator::Or;
    // The operator used in each leaf comparison (e.g. x *=* 10 OR x *=* 20)
    static constexpr BinaryOperator matchOp = BinaryOperator::Equal;

    // The Expr::Kind and corresponding type of the "anchor" operand (the one validated
    // against varDecl) The operator used in each leaf comparison
    // (e.g. *x* = 10 OR *x* = 20)
    static constexpr Expr::Kind anchorKind = Expr::Kind::SYMBOL;
    using AnchorExpr = SymbolExpr;

    // The Expr::Kind and concrete type of the "value" operand (the one we extract values
    // from) (e.g. x = *10* OR x = *20* )
    static constexpr Expr::Kind valueKind = Expr::Kind::LITERAL;
    using ValueExpr = LiteralExpr;

    // The type collected into the result vector
    using ResultType = NodeID;

    /// Ensures the var decl is the same for all leaf expressions
    static bool validateAnchor(const AnchorExpr* anchor, const VarDecl* varDecl) {
        return anchor->getDecl() == varDecl;
    }

    /// Extracts the value from each leaf expression
    static bool extractValue(const ValueExpr* valueExpr, ResultType& out) {
        const Literal* literal = valueExpr->getLiteral();
        if (literal->getKind() != Literal::Kind::INTEGER) {
            return false;
        }

        const auto* intLit = static_cast<const IntegerLiteral*>(literal);
        if (intLit->getValue() < 0) {
            return false;
        }

        out = NodeID(static_cast<uint64_t>(intLit->getValue()));
        return true;
    }
};

/// Extract literals from a chain of x.prop = LIT0 OR x.prop = LIT1 OR ...
struct ExprUtils::PropertyEqualsOR {
    using ValidatorType = const PropertyExpr*;

    // The operator used to connect each leaf (e.g. x = 10 *OR* x = 20)
    static constexpr BinaryOperator chainOp = BinaryOperator::Or;
    // The operator used in each leaf comparison (e.g. x *=* 10 OR x *=* 20)
    static constexpr BinaryOperator matchOp = BinaryOperator::Equal;

    // The Expr::Kind and corresponding type of the "anchor" operand (the one validated
    // against varDecl) The operator used in each leaf comparison
    // (e.g. *x* = 10 OR *x* = 20)
    static constexpr Expr::Kind anchorKind = Expr::Kind::PROPERTY;
    using AnchorExpr = PropertyExpr;

    // The Expr::Kind and concrete type of the "value" operand (the one we extract values
    // from) (e.g. x = *10* OR x = *20* )
    static constexpr Expr::Kind valueKind = Expr::Kind::LITERAL;
    using ValueExpr = LiteralExpr;

    // The type collected into the result vector
    using ResultType = const LiteralExpr*;

    /// Ensures the property expression is the same for all leaf expressions
    static bool validateAnchor(const AnchorExpr* propAnchor, ValidatorType propertyExpr) {
        const VarDecl* anchEntity = propAnchor->getEntityVarDecl();
        const VarDecl* newEntity = propertyExpr->getEntityVarDecl();
        bioassert(anchEntity && newEntity, "Invalid entity variables.");

        // Ensure both properties refer to the same variable
        const std::string_view anchName = anchEntity->getName();
        const std::string_view newName = newEntity->getName();
        if (anchName != newName) {
            return false;
        }

        // Ensure both properties refer to the same property
        const std::string_view anchProp = propAnchor->getPropName();
        const std::string_view newProp = propertyExpr->getPropName();

        const bool sameProperty = anchProp == newProp;

        return sameProperty;
    }

    /// Extracts the value from each leaf expression
    static bool extractValue(const ValueExpr* valueExpr, ResultType& out) {
        out = valueExpr;
        return true;
    }
};
}
