#pragma once

#include "PlanGraphNode.h"

namespace db::v2 {

class CreateGraphNode : public PlanGraphNode {
public:
    explicit CreateGraphNode(const std::string& graphName)
        : PlanGraphNode(PlanGraphOpcode::CREATE_GRAPH),
        _graphName(graphName)
    {
    }

    const std::string& getGraphName() const { return _graphName; }

private:
    std::string _graphName;
};

}
