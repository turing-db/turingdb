#pragma once

#include <string_view>

#include "Path.h"
#include "PlanGraphNode.h"

namespace db::v2 {

class LoadNeo4jNode : public PlanGraphNode {
public:
    LoadNeo4jNode(const fs::Path& path, std::string_view graphName)
        : PlanGraphNode(PlanGraphOpcode::LOAD_NEO4J),
        _path(path),
        _graphName(graphName)
    {
    }

    const fs::Path& getPath() const { return _path; }
    std::string_view getGraphName() const { return _graphName; }

private:
    fs::Path _path;
    std::string_view _graphName;
};

}
