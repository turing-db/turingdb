#pragma once

#include <set>
#include <string>
#include <variant>
#include <vector>

#include "DataPart.h"
#include "ID.h"
#include "metadata/PropertyType.h"

namespace db {

class CommitWriteBuffer {
public:
    using SupportedTypeVariant =
        std::variant<types::Int64::Primitive, types::UInt64::Primitive,
                     types::Double::Primitive, std::string,
                     types::Bool::Primitive>;

     struct UntypedProperty {
        std::string propertyName;
        SupportedTypeVariant value;
     };

     struct PendingNode {
         std::vector<std::string> labelNames;
         std::vector<UntypedProperty> properties;
     };

     using PendingNodeOffset = size_t;

     // A node: either exists in previous commit (materialised as NodeID),
     // or to be created in this commit (materialised as PendingNode)
     using ContingentNode = std::variant<NodeID, PendingNodeOffset>;
 private:
     struct PendingEdge {
         ContingentNode src;
         ContingentNode tgt;
         std::string edgeLabelTypeName;
         std::vector<UntypedProperty> properties;
    };

public:
    CommitWriteBuffer() = default;

    PendingNodeOffset nextPendingNodeOffset() { return _pendingNodes.size(); }

    void addPendingNode(std::vector<std::string>& labels,
                        std::vector<UntypedProperty>& properties);

    void addPendingEdge(ContingentNode src, ContingentNode tgt, std::string& edgeType,
                        std::vector<UntypedProperty>& edgeProperties);

    std::vector<PendingNode>& pendingNodes() { return _pendingNodes; }
    std::vector<PendingEdge>& pendingEdges() { return _pendingEdges; }

    const std::set<NodeID> deletedNodes() const { return _deletedNodes; }

    /**
     * @brief Adds NodeIDs contained in @param newDeletedNodes to the member @ref
     * _deletedNodes
     * @detail Calls std::vector::reserve for the additional space before calling
     * std::vector::insert
     */
    void addDeletedNodes(const std::vector<NodeID>& newDeletedNodes);
    
private:
    friend DataPartBuilder;

    // Nodes to be created when this commit commits
    std::vector<PendingNode> _pendingNodes;

    // Edges to be created when this commit commits
    std::vector<PendingEdge> _pendingEdges;
    
    // Nodes to be deleted when this commit commits
    std::set<NodeID> _deletedNodes;

    // Edges to be deleted when this commit commits
    std::set<EdgeID> _deletedEdges;
};

}
