#pragma once

#include <span>
#include <range/v3/view/join.hpp>

#include "EdgeRecord.h"

namespace db {

class EdgeIndexer;
class NodeView;
class GraphView;
struct EdgeRecord;
struct NodeData;

class NodeEdgeView {
public:
    NodeEdgeView(const NodeEdgeView&) = default;
    NodeEdgeView(NodeEdgeView&&) = default;
    NodeEdgeView& operator=(const NodeEdgeView&) = default;
    NodeEdgeView& operator=(NodeEdgeView&&) = default;
    ~NodeEdgeView() = default;

    std::span<const std::span<const EdgeRecord>> outEdgeSpans() const {
        return _outEdgeSpans;
    }
    std::span<const std::span<const EdgeRecord>> inEdgeSpans() const {
        return _inEdgeSpans;
    }

    auto outEdges() const {
        return _outEdgeSpans | ranges::views::join;
    }

    auto inEdges() const {
        return _inEdgeSpans | ranges::views::join;
    }

    size_t getOutEdgeCount() const;
    size_t getInEdgeCount() const;

private:
    friend EdgeIndexer;
    friend NodeView;
    friend GraphView;

    std::vector<std::span<const EdgeRecord>> _outEdgeSpans;
    std::vector<std::span<const EdgeRecord>> _inEdgeSpans;
    size_t _outCount = 0;
    size_t _inCount = 0;

    NodeEdgeView() = default;

    void addOuts(std::span<const EdgeRecord> outs);
    void addIns(std::span<const EdgeRecord> ins);
};

}

