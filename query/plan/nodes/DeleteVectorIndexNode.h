#pragma once

#include "PlanGraphNode.h"

#include <string_view>

namespace db {

class DeleteVectorIndexNode : public PlanGraphNode {
public:
    DeleteVectorIndexNode(PlanGraphNodeID id, std::string_view indexName)
        : PlanGraphNode(id, PlanGraphOpcode::DELETE_VECTOR_INDEX),
        _indexName(indexName)
    {
    }

    std::string_view getIndexName() const { return _indexName; }

private:
    std::string_view _indexName;
};

}
