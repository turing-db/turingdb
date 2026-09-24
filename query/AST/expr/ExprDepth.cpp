#include "ExprDepth.h"

#include <algorithm>
#include <unordered_map>
#include <utility>
#include <vector>

#include "Expr.h"
#include "ExprChildren.h"
#include "Literal.h"
#include "LiteralExpr.h"

using namespace db;

namespace {

void collectNestedExprs(const Expr* expr, std::vector<const Expr*>& children) {
    if (ExprChildren::collect(expr, children)) {
        return;
    }

    children.clear();

    if (expr->getKind() != Expr::Kind::LITERAL) {
        return;
    }

    const Literal* literal = static_cast<const LiteralExpr*>(expr)->getLiteral();
    if (literal->getKind() != Literal::Kind::MAP) {
        return;
    }

    for (const auto& [key, value] : *static_cast<const MapLiteral*>(literal)) {
        children.push_back(value);
    }
}

}

const Expr* ExprDepth::findDeeperThan(std::span<Expr* const> expressions, size_t maxDepth) {
    std::unordered_map<const Expr*, size_t> depths;
    std::vector<std::pair<const Expr*, bool>> pending;
    std::vector<const Expr*> children;

    for (const Expr* root : expressions) {
        if (depths.contains(root)) {
            continue;
        }

        pending.emplace_back(root, false);

        while (!pending.empty()) {
            const auto [expr, childrenQueued] = pending.back();
            collectNestedExprs(expr, children);

            if (!childrenQueued) {
                pending.back().second = true;

                for (const Expr* child : children) {
                    if (!depths.contains(child)) {
                        pending.emplace_back(child, false);
                    }
                }

                continue;
            }

            pending.pop_back();

            size_t depth = 1;
            for (const Expr* child : children) {
                depth = std::max(depth, depths.at(child) + 1);
            }

            if (depth > maxDepth) {
                return expr;
            }

            depths.emplace(expr, depth);
        }
    }

    return nullptr;
}
