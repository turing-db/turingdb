#include "ExprUtils.h"

#include "Literal.h"
#include "expr/BinaryExpr.h"
#include "expr/Operators.h"

using namespace db;

template <typename Traits>
bool ExprUtils::collectFromHomogeneousBinaryChain(const Expr* root,
                                                  typename Traits::ValidatorType var,
                                                  std::vector<typename Traits::ResultType>& result) {
    using AnchorExpr = typename Traits::AnchorExpr;
    using ValueExpr = typename Traits::ValueExpr;

    if (root->getKind() != Expr::Kind::BINARY) {
        return false;
    }

    const auto* binExpr = static_cast<const BinaryExpr*>(root);
    const BinaryOperator op = binExpr->getOperator();

    // Recurse down the chain
    if (op == Traits::chainOp) {
        return collectFromHomogeneousBinaryChain<Traits>(binExpr->getLHS(), var, result)
            && collectFromHomogeneousBinaryChain<Traits>(binExpr->getRHS(), var, result);
    }

    if (op != Traits::matchOp) {
        return false;
    }

    // Match anchor and value operands in either order
    const Expr* lhs = binExpr->getLHS();
    const Expr* rhs = binExpr->getRHS();

    const AnchorExpr* anchorExpr = nullptr;
    const ValueExpr* valueExpr = nullptr;

    if (lhs->getKind() == Traits::anchorKind && rhs->getKind() == Traits::valueKind) {
        anchorExpr = static_cast<const AnchorExpr*>(lhs);
        valueExpr = static_cast<const ValueExpr*>(rhs);
    } else if (rhs->getKind() == Traits::anchorKind && lhs->getKind() == Traits::valueKind) {
        anchorExpr = static_cast<const AnchorExpr*>(rhs);
        valueExpr = static_cast<const ValueExpr*>(lhs);
    } else {
        return false;
    }

    if (!Traits::validateAnchor(anchorExpr, var)) {
        return false;
    }

    typename Traits::ResultType value;
    if (!Traits::extractValue(valueExpr, value)) {
        return false;
    }

    result.emplace_back(std::move(value));
    return true;
}

namespace db {
template bool ExprUtils::collectFromHomogeneousBinaryChain<ExprUtils::NodeIDEqualsOR>(const Expr *root, typename NodeIDEqualsOR::ValidatorType var, std::vector<typename NodeIDEqualsOR::ResultType>& result);

template bool ExprUtils::collectFromHomogeneousBinaryChain<ExprUtils::PropertyEqualsOR>(const Expr *root, typename PropertyEqualsOR::ValidatorType var, std::vector<typename PropertyEqualsOR::ResultType>& result);

}
