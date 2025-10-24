#include "PlanGraph.h"

#include "Predicate.h"

using namespace db::v2;

PlanGraph::PlanGraph()
{
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

Predicate* PlanGraph::createPredicate(const Expr* expr) {
    auto pred = std::make_unique<Predicate>(expr);
    auto* predPtr = pred.get();

    _predicates.emplace_back(std::move(pred));

    return predPtr;
}
