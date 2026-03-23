#pragma once

#include "PlanGraphNode.h"

#include <vector>

#include "ID.h"

namespace db {

class ConstScanNode : public PlanGraphNode {
public:
    explicit ConstScanNode(const std::vector<NodeID>& nodeIDs)
        : PlanGraphNode(PlanGraphOpcode::CONST_SCAN),
        _nodeIDs(nodeIDs)
    {
    }

    const std::vector<NodeID>& getNodeIDs() const { return _nodeIDs; }

private:
    std::vector<NodeID> _nodeIDs;
};

}
