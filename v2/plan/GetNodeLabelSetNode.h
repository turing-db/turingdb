#pragma once

#include "PlanGraph.h"

namespace db::v2 {

class GetNodeLabelSetNode : public PlanGraphNode {
public:
    GetNodeLabelSetNode()
        : PlanGraphNode(PlanGraphOpcode::GET_NODE_LABEL_SET)
    {
    }
};

}
