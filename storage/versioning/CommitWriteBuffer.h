#pragma once

#include <set>
#include <string>
#include <variant>
#include <vector>

#include "DataPart.h"
#include "ID.h"
#include "metadata/PropertyType.h"

namespace db {

class CommitWriteBufferRebaser;
class MetadataBuilder;

class CommitWriteBuffer {

struct PendingEdge;

public:
    using SupportedTypeVariant =
        std::variant<types::Int64::Primitive, types::UInt64::Primitive,
                     types::Double::Primitive, std::string,
                     types::Bool::Primitive>;

     struct UntypedProperty {
        std::string propertyName;
        SupportedTypeVariant value;
     };

     using UntypedProperties = std::vector<UntypedProperty>;

     struct PendingNode {
         std::vector<std::string> labelNames;
         UntypedProperties properties;
     };

     using PendingNodeOffset = size_t;

     // A node: either exists in previous commit (materialised as NodeID),
     // or to be created in this commit (materialised as PendingNodeOffset)
     using ExistingOrPendingNode = std::variant<NodeID, PendingNodeOffset>;
     using PendingNodes = std::vector<PendingNode>;
     using PendingEdges = std::vector<PendingEdge>;

     CommitWriteBuffer() = default;

     PendingNodeOffset nextPendingNodeOffset() { return _pendingNodes.size(); }

     /**
      * @brief Adds a pending node to this WriteBuffer with the provided properties and
      * labels.
      * @warn Takes ownership and moves ownership of @param labels and @param properties
      * to this WriteBuffer
      */
     void addPendingNode(std::vector<std::string>&& labels, UntypedProperties&& properties);

     /**
      * @brief Adds a pending edge to this WriteBuffer with the provided properties and
      * labels.
      * @warn Takes ownership and moves ownership of @param edgeType and @param
      * edgeProperties to this WriteBuffer. Does not take ownership of @param src nor
      * @param tgt
      */
     void addPendingEdge(ExistingOrPendingNode src, ExistingOrPendingNode tgt, std::string&& edgeType,
                         UntypedProperties&& edgeProperties);

     /**
      * @brief Adds the pending nodes and edges to the provided datapart builder
      */
     void buildPending(DataPartBuilder& builder);

     PendingNodes& pendingNodes() { return _pendingNodes; }
     PendingEdges& pendingEdges() { return _pendingEdges; }

     const std::set<NodeID>& deletedNodes() const { return _deletedNodes; }

     bool empty() const {
         return _pendingNodes.empty() && _pendingEdges.empty() && _deletedEdges.empty()
             && _deletedEdges.empty();
    }

    /**
     * @brief Adds NodeIDs contained in @param newDeletedNodes to the member @ref
     * _deletedNodes
     * @detail Calls std::vector::reserve for the additional space before calling
     * std::vector::insert
     */
    void addDeletedNodes(const std::vector<NodeID>& newDeletedNodes);

private:
    friend DataPartBuilder;
    friend CommitWriteBufferRebaser;

    struct PendingEdge {
         ExistingOrPendingNode src;
         ExistingOrPendingNode tgt;
         std::string edgeLabelTypeName;
         UntypedProperties properties;
    };

    // Nodes to be created when this commit commits
    std::vector<PendingNode> _pendingNodes;

    // Edges to be created when this commit commits
    std::vector<PendingEdge> _pendingEdges;

    // Nodes to be deleted when this commit commits
    std::set<NodeID> _deletedNodes;

    // Edges to be deleted when this commit commits
    std::set<EdgeID> _deletedEdges;

    // Collection of methods to write the buffer to the provided datapart builder
    void buildPendingNodes(DataPartBuilder& builder, MetadataBuilder& metadataBuilder);
    void buildPendingEdges(DataPartBuilder& builder, MetadataBuilder& metadataBuilder);

    void buildPendingNode(DataPartBuilder& builder,
                          MetadataBuilder& metadataBuilder,
                          const PendingNode& node);
    void buildPendingEdge(DataPartBuilder& builder,
                          MetadataBuilder& metadataBuilder,
                          const PendingEdge& edge);

};

class CommitWriteBufferRebaser {
public:
    explicit CommitWriteBufferRebaser(CommitWriteBuffer& buffer, NodeID entryNextNodeID,
                                      EdgeID entryNextEdgeID, NodeID currentNextNodeID,
                                      EdgeID currentNextEdgeID)
        : _buffer(buffer),
          _entryNextNodeID(entryNextNodeID),
          _currentNextNodeID(currentNextNodeID),
          _entryNextEdgeID(entryNextEdgeID),
          _currentNextEdgeID(currentNextEdgeID)
        {
        }

    void rebaseIncidentNodeIDs();

private:
    CommitWriteBuffer& _buffer;

    NodeID _entryNextNodeID;
    NodeID _currentNextNodeID;

    EdgeID _entryNextEdgeID;
    EdgeID _currentNextEdgeID;
};

}
