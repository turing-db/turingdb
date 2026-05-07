#pragma once

#include "nodes/PlanGraphNode.h"

namespace db {

class ShowExtensionsNode : public PlanGraphNode {
public:
    explicit ShowExtensionsNode(PlanGraphNodeID id)
        : PlanGraphNode(id, PlanGraphOpcode::SHOW_EXTENSIONS)
    {
    }
};

}
