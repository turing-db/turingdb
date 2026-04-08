#pragma once

#include <string_view>

#include "PlanGraphNode.h"

namespace db {
    
class DropIndexNode final : public PlanGraphNode {
public:
    explicit DropIndexNode(std::string_view indexName)
        : PlanGraphNode(PlanGraphOpcode::DROP_INDEX),
        _indexName(indexName)
    {
    }

    std::string_view indexName() const { return _indexName; }

private:
    std::string_view _indexName;
};

}
