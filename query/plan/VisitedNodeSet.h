#pragma once

#include <vector>

#include "nodes/PlanGraphNode.h"

namespace db {

class VisitedNodeSet {
public:
    VisitedNodeSet() = default;

    explicit VisitedNodeSet(size_t size)
        : _visited(size, false)
    {
    }

    bool visited(size_t id) const { return _visited[id]; }

    bool visited(const PlanGraphNode* node) const { return visited(node->id()); }

    void visit(size_t id) { _visited[id] = true; }

    void visit(const PlanGraphNode* node) { visit(node->id()); }

    void reset(size_t size) {
        _visited.assign(size, false);
    }

    void reset() {
        const size_t size = _visited.size();
        reset(size);
    }

private:
    std::vector<bool> _visited;
};

}
