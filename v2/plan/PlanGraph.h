#pragma once

#include <memory>
#include <span>

#include "nodes/PlanGraphNode.h"

namespace db::v2 {

class VarDecl;
class Expr;
class ExprConstraint;
class WherePredicate;

class PlanGraph {
public:
    using PlanGraphNodesSpan = std::span<const std::unique_ptr<PlanGraphNode>>;
    using WherePredicatesSpan = std::span<const std::unique_ptr<WherePredicate>>;

    PlanGraph();
    ~PlanGraph();

    void getRoots(std::vector<PlanGraphNode*>& roots) const;

    PlanGraphNodesSpan nodes() const { return _nodes; }

    WherePredicatesSpan wherePredicates() const { return _wherePredicates; }

    WherePredicate* createWherePredicate(const Expr* expr);

    template <typename T, typename... Args>
    T* create(Args&&... args) {
        auto node = std::make_unique<T>(std::forward<Args>(args)...);
        auto* nodePtr = node.get();
        _nodes.emplace_back(std::move(node));

        return nodePtr;
    }

    template <typename T, typename... Args>
    T* newOut(PlanGraphNode* prev, Args&&... args) {
        auto next = create<T>(std::forward<Args>(args)...);
        prev->connectOut(next);

        return next;
    }

    template <typename T, typename... Args>
    T* insertBefore(PlanGraphNode* after, Args&&... args) {
        auto before = create<T>(std::forward<Args>(args)...);
        for (PlanGraphNode* input : after->inputs()) {
            input->connectOut(before);
        }

        after->clearInputs();
        before->connectOut(after);

        return before;
    }

private:
    friend class PlanGraphDebug;

    std::vector<std::unique_ptr<PlanGraphNode>> _nodes;
    std::vector<std::unique_ptr<WherePredicate>> _wherePredicates;
};

}
