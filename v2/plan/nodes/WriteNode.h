#pragma once

#include <span>
#include <variant>

#include "PlanGraphNode.h"

namespace db::v2 {

class Expr;
class VarDecl;
class NodePatternData;
class EdgePatternData;

class WriteNode : public PlanGraphNode {
public:
    struct PendingNode {
        const NodePatternData* _data {nullptr};
        size_t _offset {0};
    };

    class EdgeNeighbour {
    public:
        EdgeNeighbour() = default;

        explicit EdgeNeighbour(const VarDecl* var)
            : _data(var)
        {
        }

        explicit EdgeNeighbour(size_t offset)
            : _data(offset)
        {
        }

        bool isPendingNodeOffset() const {
            return std::holds_alternative<size_t>(_data);
        }

        bool isInput() const {
            return std::holds_alternative<const VarDecl*>(_data);
        }

        const VarDecl* asInput() const {
            return std::get<const VarDecl*>(_data);
        }

        size_t asPendingNodeOffset() const {
            return std::get<size_t>(_data);
        }

    private:
        std::variant<const VarDecl*, size_t> _data;
    };

    struct PendingEdge {
        EdgeNeighbour _src;
        EdgeNeighbour _tgt;
        const EdgePatternData* _data {nullptr};
    };

    WriteNode()
        : PlanGraphNode(PlanGraphOpcode::WRITE)
    {
    }

    size_t addNode(const NodePatternData* data) {
        const size_t offset = _nodes.size();
        _nodes.emplace_back(data, offset);

        return offset;
    }

    size_t addEdge(const EdgeNeighbour& src,
                   const EdgePatternData* data,
                   const EdgeNeighbour& tgt) {
        const size_t offset = _edges.size();
        _edges.emplace_back(src, tgt, data);

        return offset;
    }

    const PendingNode& getPendingNode(size_t offset) const {
        return _nodes.at(offset);
    }

    std::span<const PendingNode> pendingNodes() const { return _nodes; }
    std::span<const PendingEdge> pendingEdges() const { return _edges; }

private:
    std::vector<PendingNode> _nodes;
    std::vector<PendingEdge> _edges;
};

}
