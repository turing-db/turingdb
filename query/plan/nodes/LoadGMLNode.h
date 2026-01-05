#pragma once

#include <string_view>

#include "PlanGraphNode.h"

#include "Path.h"

namespace db::v2 {

class LoadGMLNode : public PlanGraphNode {
public:
    LoadGMLNode(std::string_view graphName,
                const fs::Path& filePath)
        : PlanGraphNode(PlanGraphOpcode::LOAD_GML),
        _graphName(graphName),
        _filePath(filePath)
    {
    }

    std::string_view getGraphName() const { return _graphName; }

    const fs::Path& getFilePath() const { return _filePath; }

private:
    std::string_view _graphName;
    fs::Path _filePath;
};

}
