#pragma once

#include <string_view>

#include "Path.h"
#include "PlanGraphNode.h"

#include "EmbeddingsSpec.h"

namespace db {

class LoadJsonlNode : public PlanGraphNode {
public:
    LoadJsonlNode(PlanGraphNodeID id,
                  const fs::Path& path,
                  std::string_view graphName,
                  EmbeddingsSpec embeddingSpecs)
        : PlanGraphNode(id, PlanGraphOpcode::LOAD_JSONL),
        _path(path),
        _graphName(graphName),
        _embeddingSpecs(std::move(embeddingSpecs))
    {
    }

    const fs::Path& getFilePath() const { return _path; }
    std::string_view getGraphName() const { return _graphName; }
    const EmbeddingsSpec& getEmbeddingSpecs() const { return _embeddingSpecs; }

private:
    fs::Path _path;
    std::string_view _graphName;
    EmbeddingsSpec _embeddingSpecs;
};

}
