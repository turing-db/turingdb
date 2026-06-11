#pragma once

#include <string_view>
#include <unordered_map>

#include "Path.h"
#include "PlanGraphNode.h"

namespace db {

class LoadJsonlNode : public PlanGraphNode {
public:
    LoadJsonlNode(PlanGraphNodeID id,
                  const fs::Path& path,
                  std::string_view graphName,
                  std::unordered_map<std::string_view, size_t> embeddingSpecs)
        : PlanGraphNode(id, PlanGraphOpcode::LOAD_JSONL),
        _path(path),
        _graphName(graphName),
        _embeddingSpecs(std::move(embeddingSpecs))
    {
    }

    const fs::Path& getFilePath() const { return _path; }
    std::string_view getGraphName() const { return _graphName; }
    const std::unordered_map<std::string_view, size_t>& getEmbeddingSpecs() const {
        return _embeddingSpecs;
    }

private:
    fs::Path _path;
    std::string_view _graphName;
    std::unordered_map<std::string_view, size_t> _embeddingSpecs;
};

}
