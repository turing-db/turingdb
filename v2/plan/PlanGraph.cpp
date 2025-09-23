#include "PlanGraph.h"

#include "WherePredicate.h"

using namespace db::v2;

PlanGraph::PlanGraph() {
}

PlanGraph::~PlanGraph() {
}

void PlanGraph::getRoots(std::vector<PlanGraphNode*>& roots) const {
    for (const auto& node : _nodes) {
        if (node->isRoot()) {
            roots.emplace_back(node.get());
        }
    }
}

WherePredicate* PlanGraph::createWherePredicate(const Expr* expr) {
    auto pred = std::make_unique<WherePredicate>(expr);
    auto* predPtr = pred.get();

    _wherePredicates.emplace_back(std::move(pred));

    return predPtr;
}
