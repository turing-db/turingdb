#pragma once

#include <string_view>

#include "PlanGraphNode.h"

namespace db::v2 {

class LoadGraphNode : public PlanGraphNode {
public:
    LoadGraphNode(std::string_view graphName)
        : PlanGraphNode(PlanGraphOpcode::LOAD_GRAPH),
        _graphName(graphName)
    {
    }

    std::string_view getGraphName() const { return _graphName; }

private:
    std::string_view _graphName;
};

}
