#pragma once

#include "PlanGraphNode.h"

#include <string_view>

namespace db {

class LoadVectorNode : public PlanGraphNode {
public:
    LoadVectorNode(std::string_view filePath, std::string_view indexName)
        : PlanGraphNode(PlanGraphOpcode::LOAD_VECTOR),
        _filePath(filePath),
        _indexName(indexName)
    {
    }

    std::string_view getFilePath() const { return _filePath; }
    std::string_view getIndexName() const { return _indexName; }

private:
    std::string_view _filePath;
    std::string_view _indexName;
};

}
