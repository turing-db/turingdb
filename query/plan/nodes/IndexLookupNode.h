#pragma once

#include "nodes/PlanGraphNode.h"

namespace db {

class Index;
class Column;

class IndexLookupNode : public PlanGraphNode {
public:
    explicit IndexLookupNode(Index* index, Column* query)
        : PlanGraphNode(PlanGraphOpcode::INDEX_LOOKUP),
        _index(index),
        _query(query)
    {
    }

private:
    Index* _index {nullptr};
    Column* _query {nullptr};
};

}
