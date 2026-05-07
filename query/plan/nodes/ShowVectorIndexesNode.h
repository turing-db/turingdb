#pragma once

#include "PlanGraphNode.h"

namespace db {

class ShowVectorIndexesNode : public PlanGraphNode {
public:
    explicit ShowVectorIndexesNode(PlanGraphNodeID id)
        : PlanGraphNode(id, PlanGraphOpcode::SHOW_VECTOR_INDEXES)
    {
    }
};

}
