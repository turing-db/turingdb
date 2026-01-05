#pragma once

#include "PlanGraphNode.h"

namespace db {

class ScanNodesNode : public PlanGraphNode {
public:
    explicit ScanNodesNode()
        : PlanGraphNode(PlanGraphOpcode::SCAN_NODES)
    {
    }
};

}
