#pragma once

#include "nodes/PlanGraphNode.h"

namespace db {

class Index;
class Column;

template <typename Q, typename R>
class IndexLookupNode : public PlanGraphNode {
public:
    explicit IndexLookupNode(const Index* index, const Column* query)
        : PlanGraphNode(PlanGraphOpcode::INDEX_LOOKUP),
        _index(index),
        _query(query)
    {
    }

    const Index* index() const { return _index; }

private:
    const Index* _index {nullptr};
    const Column* _query {nullptr};
};

}
