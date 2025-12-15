#pragma once

#include "PlanGraphNode.h"

namespace db::v2 {

class CreateGraphNode : public PlanGraphNode {
public:
    explicit CreateGraphNode(std::string_view graphName)
        : PlanGraphNode(PlanGraphOpcode::CREATE_GRAPH),
        _graphName(graphName)
    {
    }

    std::string_view getGraphName() const { return _graphName; }

private:
    std::string_view _graphName;
};

}
