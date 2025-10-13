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
    CommitWriteBuffer() = default;

    using SupportedTypeVariant =
        std::variant<types::Int64::Primitive, types::UInt64::Primitive,
                     types::Double::Primitive, std::string,
                     types::Bool::Primitive>;

     struct UntypedProperty {
        PropertyTypeID propertyID;
        SupportedTypeVariant value;
     };

     using UntypedProperties = std::vector<UntypedProperty>;

     struct PendingNode {
         LabelSetHandle labelsetHandle;
         UntypedProperties properties;
     };

     using PendingNodeOffset = size_t;

     // A node: either exists in previous commit (materialised as NodeID),
     // or to be created in this commit (materialised as PendingNodeOffset)
     using ExistingOrPendingNode = std::variant<NodeID, PendingNodeOffset>;
     using PendingNodes = std::vector<PendingNode>;
     using PendingEdges = std::vector<PendingEdge>;

     /**
      * @brief Adds a pending node to this WriteBuffer with empty properties and
      * labels.
      */
     PendingNode& newPendingNode();

     /**
      * @brief Adds a pending edge to this WriteBuffer with provided source and target
      * nodes and empty properties and labels.
      */
     PendingEdge& newPendingEdge(ExistingOrPendingNode src, ExistingOrPendingNode tgt);

     /**
      * @brief Adds the pending nodes and edges to the provided datapart builder
      */
     void buildPending(DataPartBuilder& builder);

     PendingNodes& pendingNodes() { return _pendingNodes; }
     PendingEdges& pendingEdges() { return _pendingEdges; }

     std::vector<NodeID>& deletedNodes() { return _deletedNodes; }
     std::vector<EdgeID>& deletedEdges() { return _deletedEdges; }

     bool empty() const {
         return _pendingNodes.empty() && _pendingEdges.empty() && _deletedEdges.empty()
             && _deletedEdges.empty();
     }

    /**
     * @brief Adds NodeIDs contained in @param newDeletedNodes to the member @ref
     * _deletedNodes
     */
    void addDeletedNodes(const std::vector<NodeID>& newDeletedNodes);

    /**
     * @brief Adds EdgeIDs contained in @param newDeletedEdges to the member @ref
     * _deletedEdges
     */
    void addDeletedEdges(const std::vector<EdgeID>& newDeletedEdges);

    void setFlushed() { _flushed = true; }
    void setUnflushed() { _flushed = false; }
    bool isFlushed() const { return _flushed; }

private:
    friend DataPartBuilder;
    friend CommitWriteBufferRebaser;

    struct PendingEdge {
         ExistingOrPendingNode src;
         ExistingOrPendingNode tgt;
         EdgeTypeID edgeType;
         UntypedProperties properties;
    };

    bool _flushed {false};

    // Nodes to be created when this commit commits
    std::vector<PendingNode> _pendingNodes;

    // Edges to be created when this commit commits
    std::vector<PendingEdge> _pendingEdges;

    // Nodes to be deleted when this commit commits
    std::vector<NodeID> _deletedNodes;

    // Edges to be deleted when this commit commits
    std::vector<EdgeID> _deletedEdges;

    // Collection of methods to write the buffer to the provided datapart builder
    void buildPendingNodes(DataPartBuilder& builder);
    void buildPendingEdges(DataPartBuilder& builder);

    void buildPendingNode(DataPartBuilder& builder, const PendingNode& node);
    void buildPendingEdge(DataPartBuilder& builder, const PendingEdge& edge);
};

class CommitWriteBufferRebaser {
public:
    explicit CommitWriteBufferRebaser(CommitWriteBuffer& buffer)
        : _buffer(&buffer)
        {
        }

    void rebaseIncidentNodeIDs(NodeID entryNextNodeID, NodeID currentNextNodeID);

private:
    CommitWriteBuffer* _buffer;
};

}
