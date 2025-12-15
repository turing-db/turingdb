#pragma once

#include <string_view>

#include "PlanGraphNode.h"

namespace db::v2 {

class LoadGMLNode : public PlanGraphNode {
public:
    LoadGMLNode(std::string_view graphName,
                std::string_view filePath)
        : PlanGraphNode(PlanGraphOpcode::LOAD_GML),
        _graphName(graphName)
    {
    }

    std::string_view getGraphName() const { return _graphName; }

    std::string_view getFilePath() const { return _filePath; }

private:
    std::string_view _graphName;
    std::string_view _filePath;
};

}
