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
    struct NodeUpdate {
        std::string_view _propTypeName;
        const Expr* _propValueExpr {nullptr};
    };

    struct EdgeUpdate {
        std::string_view _propTypeName;
        const Expr* _propValueExpr {nullptr};
    };

    struct PendingNode {
        const NodePatternData* _data {nullptr};
        size_t _offset {0};
    };

    class EdgeNeighbour {
    public:
        EdgeNeighbour() = default;

        explicit EdgeNeighbour(const VarDecl* var)
            : _data(var) {
        }

        explicit EdgeNeighbour(size_t offset)
            : _data(offset) {
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

    size_t addNode(const VarDecl* decl, const NodePatternData* data) {
        const size_t offset = _newNodes.size();
        _newNodes.emplace_back(data, offset);
        _newNodeDeclMapping[decl] = offset;

        return offset;
    }

    size_t addEdge(const VarDecl* decl,
                   const EdgeNeighbour& src,
                   const EdgePatternData* data,
                   const EdgeNeighbour& tgt) {
        const size_t offset = _newEdges.size();
        _newEdges.emplace_back(src, tgt, data);
        _newEdgeDeclMapping[decl] = offset;

        return offset;
    }

    void deleteNode(const VarDecl* decl) {
        _toDeleteNodes.push_back(decl);
    }

    void deleteEdge(const VarDecl* decl) {
        _toDeleteEdges.push_back(decl);
    }

    void addNodeUpdate(std::string_view propTypeExpr, const Expr* expr) {
        _nodeUpdates.emplace_back(propTypeExpr, expr);
    }

    void addEdgeUpdate(std::string_view propTypeExpr, const Expr* expr) {
        _edgeUpdates.emplace_back(propTypeExpr, expr);
    }

    const PendingNode& getPendingNode(size_t offset) const {
        return _newNodes.at(offset);
    }

    const PendingNode& getPendingNode(const VarDecl* decl) const {
        return _newNodes.at(_newNodeDeclMapping.at(decl));
    }

    bool hasPendingNode(const VarDecl* decl) const {
        return _newNodeDeclMapping.contains(decl);
    }

    bool hasPendingEdge(const VarDecl* decl) const {
        return _newEdgeDeclMapping.contains(decl);
    }

    using PendingNodeSpan = std::span<const PendingNode>;
    using PendingEdgeSpan = std::span<const PendingEdge>;
    using ToDeleteNodeSpan = std::span<const VarDecl* const>;
    using ToDeleteEdgeSpan = std::span<const VarDecl* const>;
    using NodeUpdateSpan = std::span<const NodeUpdate>;
    using EdgeUpdateSpan = std::span<const EdgeUpdate>;

    PendingNodeSpan pendingNodes() const { return _newNodes; }
    PendingEdgeSpan pendingEdges() const { return _newEdges; }
    ToDeleteNodeSpan toDeleteNodes() const { return _toDeleteNodes; }
    ToDeleteEdgeSpan toDeleteEdges() const { return _toDeleteEdges; }
    NodeUpdateSpan nodeUpdates() const { return _nodeUpdates; }
    EdgeUpdateSpan edgeUpdates() const { return _edgeUpdates; }

private:
    std::unordered_map<const VarDecl*, size_t> _newNodeDeclMapping;
    std::unordered_map<const VarDecl*, size_t> _newEdgeDeclMapping;
    std::vector<PendingNode> _newNodes;
    std::vector<PendingEdge> _newEdges;
    std::vector<NodeUpdate> _nodeUpdates;
    std::vector<EdgeUpdate> _edgeUpdates;
    std::vector<const VarDecl*> _toDeleteNodes;
    std::vector<const VarDecl*> _toDeleteEdges;
};

}
