#include "PlanGraph.h"

#include "Predicate.h"

using namespace db;

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

Predicate* PlanGraph::createPredicate(Expr* expr) {
    auto pred = std::make_unique<Predicate>(expr);
    auto* predPtr = pred.get();

    _predicates.emplace_back(std::move(pred));

    return predPtr;
}

void PlanGraph::removeIsolatedNodes() {
    std::vector<std::unique_ptr<PlanGraphNode>> newNodes;

    for (auto& node : _nodes) {
        const PlanGraphOpcode opc = node->getOpcode();
        const bool disconnected = node->inputs().empty() && node->outputs().empty();
        const bool canBeStandalone = opc == PlanGraphOpcode::WRITE || opc == PlanGraphOpcode::PRODUCE_RESULTS;

        if (disconnected && !canBeStandalone) {
            continue;
        }

        newNodes.emplace_back(std::move(node));
    }

    _nodes = std::move(newNodes);
}
