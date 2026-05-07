#pragma once

#include <string_view>

#include "PlanGraphNode.h"

namespace db {
    
class DropIndexNode final : public PlanGraphNode {
public:
    DropIndexNode(PlanGraphNodeID id, std::string_view indexName)
        : PlanGraphNode(id, PlanGraphOpcode::DROP_INDEX),
        _indexName(indexName)
    {
    }

    std::string_view indexName() const { return _indexName; }

private:
    std::string_view _indexName;
};

}
