#pragma once

#include "PlanGraphNode.h"

namespace db {

class CreateGraphNode : public PlanGraphNode {
public:
    CreateGraphNode(PlanGraphNodeID id, std::string_view graphName)
        : PlanGraphNode(id, PlanGraphOpcode::CREATE_GRAPH),
        _graphName(graphName)
    {
    }

    std::string_view getGraphName() const { return _graphName; }

private:
    std::string_view _graphName;
};

}
