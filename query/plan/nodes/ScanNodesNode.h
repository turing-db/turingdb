#pragma once

#include "PlanGraphNode.h"

namespace db {

class ScanNodesNode : public PlanGraphNode {
public:
    explicit ScanNodesNode(PlanGraphNodeID id)
        : PlanGraphNode(id, PlanGraphOpcode::SCAN_NODES)
    {
    }
};

}
