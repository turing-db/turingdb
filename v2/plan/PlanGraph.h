#pragma once

#include <memory>

#include "nodes/PlanGraphNode.h"

namespace db::v2 {

class VarDecl;
class Expr;
class ExprConstraint;

class PlanGraph {
public:
    PlanGraph();
    ~PlanGraph();

    template <typename T, typename... Args>
    T* create(Args&&... args) {
        auto node = std::make_unique<T>(std::forward<Args>(args)...);
        auto* nodePtr = node.get();
        _nodes.emplace_back(std::move(node));
        return nodePtr;
    }

    void getRoots(std::vector<PlanGraphNode*>& roots) const;

private:
    friend class PlanGraphDebug;

    std::vector<std::unique_ptr<PlanGraphNode>> _nodes;
};

}
