#pragma once

#include "nodes/PlanGraphNode.h"

namespace db {

class MergeDataPartsNode : public PlanGraphNode {
public:
    MergeDataPartsNode()
        : PlanGraphNode(PlanGraphOpcode::MERGE_DATAPARTS)
    {
    }
};

}
