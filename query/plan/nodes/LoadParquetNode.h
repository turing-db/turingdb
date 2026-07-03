#pragma once

#include <string_view>
#include <utility>

#include "PlanGraphNode.h"

#include "Path.h"

namespace db {

class LoadParquetNode final : public PlanGraphNode {
public:
    LoadParquetNode(PlanGraphNodeID id, std::string_view graphName, fs::Path filePath)
        : PlanGraphNode(id, PlanGraphOpcode::LOAD_PARQUET),
        _graphName(graphName),
        _filePath(std::move(filePath))
    {
    }

    std::string_view getGraphName() const { return _graphName; }

    const fs::Path& getFilePath() const { return _filePath; }

private:
    std::string_view _graphName;
    fs::Path _filePath;
};

}
