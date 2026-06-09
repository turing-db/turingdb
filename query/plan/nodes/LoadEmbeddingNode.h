#pragma once

#include "PlanGraphNode.h"

#include <string_view>

namespace db {

class LoadEmbeddingNode : public PlanGraphNode {
public:
    LoadEmbeddingNode(PlanGraphNodeID id, std::string_view filePath, std::string_view propertyName)
        : PlanGraphNode(id, PlanGraphOpcode::LOAD_EMBEDDING),
        _filePath(filePath),
        _propertyName(propertyName)
    {
    }

    std::string_view getFilePath() const { return _filePath; }
    std::string_view getPropertyName() const { return _propertyName; }

private:
    std::string_view _filePath;
    std::string_view _propertyName;
};

}
