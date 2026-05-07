#include "PlanGraphNodeManager.h"

#include <memory>
#include <vector>

#include "nodes/PlanGraphNode.h"

using namespace db;

namespace {

const auto isolated = [](const std::unique_ptr<PlanGraphNode>& node) {
    const bool disconnected = node->inputs().empty() && node->outputs().empty();

    const PlanGraphOpcode opc = node->getOpcode();
    const bool canBeStandalone = opc == PlanGraphOpcode::WRITE;

    return disconnected && !canBeStandalone;
};

}

void PlanGraphNodeManager::removeIsolatedNodes() {
    std::erase_if(_nodes, isolated);
}
