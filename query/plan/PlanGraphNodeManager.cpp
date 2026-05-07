#include "PlanGraphNodeManager.h"

#include "nodes/PlanGraphNode.h"

using namespace db;

void PlanGraphNodeManager::removeIsolatedNodes() {
    std::vector<std::unique_ptr<PlanGraphNode>> newNodes;

    for (auto& node : _nodes) {
        const PlanGraphOpcode opc = node->getOpcode();

        const bool disconnected = node->inputs().empty() && node->outputs().empty();
        // Write statements with no return clause; e.g. CREATE (n:Person)
        const bool canBeStandalone = opc == PlanGraphOpcode::WRITE;

        if (disconnected && !canBeStandalone) {
            continue;
        }

        newNodes.emplace_back(std::move(node));
    }

    _nodes.swap(newNodes);
}
