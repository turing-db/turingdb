#pragma once

#include <string_view>

#include "PlanGraphNode.h"

namespace db::v2 {

class LoadGMLNode : public PlanGraphNode {
public:
    LoadGMLNode(std::string_view graphName)
        : PlanGraphNode(PlanGraphOpcode::LOAD_GRAPH),
        _graphName(graphName)
    {
    }

    std::string_view getGraphName() const { return _graphName; }

private:
    std::string_view _graphName;
};

}
