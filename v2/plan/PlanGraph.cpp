#include "PlanGraph.h"

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
