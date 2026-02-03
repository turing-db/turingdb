#pragma once

#include <string_view>

#include "Path.h"
#include "PlanGraphNode.h"

namespace db {

class LoadJsonlNode : public PlanGraphNode {
public:
    LoadJsonlNode(const fs::Path& path, std::string_view graphName)
        : PlanGraphNode(PlanGraphOpcode::LOAD_JSONL),
        _path(path),
        _graphName(graphName)
    {
    }

    const fs::Path& getFilePath() const { return _path; }
    std::string_view getGraphName() const { return _graphName; }

private:
    fs::Path _path;
    std::string_view _graphName;
};

}
