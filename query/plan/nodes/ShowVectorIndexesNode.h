#pragma once

#include "PlanGraphNode.h"

namespace db {

class ShowVectorIndexesNode : public PlanGraphNode {
public:
    ShowVectorIndexesNode()
        : PlanGraphNode(PlanGraphOpcode::SHOW_VECTOR_INDEXES)
    {
    }
};

}
