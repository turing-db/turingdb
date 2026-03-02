#pragma once

#include "nodes/PlanGraphNode.h"

namespace db {

class ShowExtensionsNode : public PlanGraphNode {
public:
    ShowExtensionsNode()
        : PlanGraphNode(PlanGraphOpcode::SHOW_EXTENSIONS)
    {
    }
};

}
