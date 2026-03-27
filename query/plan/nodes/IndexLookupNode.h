#pragma once

#include "nodes/PlanGraphNode.h"

namespace db {

class Index;
class Column;

class IndexLookupNode : public PlanGraphNode {
public:
    explicit IndexLookupNode(const Index* index, const Column* query)
        : PlanGraphNode(PlanGraphOpcode::INDEX_LOOKUP),
        _index(index),
        _query(query)
    {
    }

private:
    const Index* _index {nullptr};
    const Column* _query {nullptr};
};

}
