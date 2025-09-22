#pragma once

#include "nodes/PlanGraphNode.h"

namespace db::v2 {

class ProduceResultsNode : public PlanGraphNode {
public:
    ProduceResultsNode()
        : PlanGraphNode(PlanGraphOpcode::PRODUCE_RESULTS)
    {
    }
};

}
