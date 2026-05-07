#pragma once

#include "nodes/PlanGraphNode.h"

namespace db {

class MergeDataPartsNode : public PlanGraphNode {
public:
    explicit MergeDataPartsNode(PlanGraphNodeID id)
        : PlanGraphNode(id, PlanGraphOpcode::MERGE_DATAPARTS)
    {
    }
};

}
