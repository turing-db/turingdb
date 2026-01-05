#pragma once

#include "PlanGraphNode.h"

namespace db::v2 {

class ScanNodesNode : public PlanGraphNode {
public:
    explicit ScanNodesNode()
        : PlanGraphNode(PlanGraphOpcode::SCAN_NODES)
    {
    }
};

}
