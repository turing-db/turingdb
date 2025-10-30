#pragma once

#include <span>

#include "PlanGraphNode.h"

namespace db::v2 {

class Expr;
class VarDecl;
class NodePatternData;
class EdgePatternData;

class WriteNode : public PlanGraphNode {
public:
    /// @brief Describes a node property update to be applied.
    struct NodeUpdate {
        const VarDecl* _decl {nullptr};
        std::string_view _propTypeName;
        const Expr* _propValueExpr {nullptr};
    };

    /// @brief Describes an edge property update to be applied.
    struct EdgeUpdate {
        const VarDecl* _decl {nullptr};
        std::string_view _propTypeName;
        const Expr* _propValueExpr {nullptr};
    };

    /// @brief Describes a pending node.
    struct PendingNode {
        /// @brief The node data, contains the label and property constraints
        const NodePatternData* _data {nullptr};
    };

    /// @brief Describes a pending edge.
    struct PendingEdge {
        const VarDecl* _src {nullptr};
        const VarDecl* _tgt {nullptr};

        /// @brief The edge data, contains the edge type and property constraints
        const EdgePatternData* _data {nullptr};
    };

    WriteNode()
        : PlanGraphNode(PlanGraphOpcode::WRITE)
    {
    }

    size_t addNode(const VarDecl* decl, const NodePatternData* data) {
        const size_t offset = _newNodes.size();
        _newNodes.emplace_back(data);
        _newNodeDeclMapping[decl] = offset;

        return offset;
    }

    size_t addEdge(const VarDecl* decl,
                   const EdgePatternData* data,
                   const VarDecl* src,
                   const VarDecl* tgt) {
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

    void addNodeUpdate(const VarDecl* decl,
                       std::string_view propTypeExpr,
                       const Expr* expr) {
        _nodeUpdates.emplace_back(decl, propTypeExpr, expr);
    }

    void addEdgeUpdate(const VarDecl* decl,
                       std::string_view propTypeExpr,
                       const Expr* expr) {
        _edgeUpdates.emplace_back(decl, propTypeExpr, expr);
    }

    const PendingNode& getPendingNode(size_t offset) const {
        return _newNodes.at(offset);
    }

    const PendingNode& getPendingNode(const VarDecl* decl) const {
        return _newNodes.at(_newNodeDeclMapping.at(decl));
    }

    size_t getPendingNodeOffset(const VarDecl* decl) const {
        return _newNodeDeclMapping.at(decl);
    }

    size_t getPendingEdgeOffset(const VarDecl* decl) const {
        return _newEdgeDeclMapping.at(decl);
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
    /// @brief Maps the VarDecl of a new node to its offset in the _newNodes vector
    std::unordered_map<const VarDecl*, size_t> _newNodeDeclMapping;

    /// @brief Maps the VarDecl of a new edge to its offset in the _newEdges vector
    std::unordered_map<const VarDecl*, size_t> _newEdgeDeclMapping;

    /// @brief Contains the node data describing the new nodes
    std::vector<PendingNode> _newNodes;

    /// @brief Contains the edge data + src/tgt info describing the new edges
    std::vector<PendingEdge> _newEdges;

    /// @brief Contains the node property updates to be applied
    std::vector<NodeUpdate> _nodeUpdates;

    /// @brief Contains the edge property updates to be applied
    std::vector<EdgeUpdate> _edgeUpdates;

    /// @brief Contains the VarDecls of nodes to be deleted
    std::vector<const VarDecl*> _toDeleteNodes;

    /// @brief Contains the VarDecls of edges to be deleted
    std::vector<const VarDecl*> _toDeleteEdges;
};

}
