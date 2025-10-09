#pragma once

#include <string>
#include <variant>
#include <vector>

#include "DataPart.h"
#include "ID.h"
#include "metadata/PropertyType.h"

namespace db {

class CommitWriteBufferRebaser;
class MetadataBuilder;
class CommitBuilder;
class DataPartModifier;
class CommitJournal;

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
     void buildFromBuffer(DataPartBuilder& builder);

     PendingNodes& pendingNodes() { return _pendingNodes; }
     PendingEdges& pendingEdges() { return _pendingEdges; }

     std::vector<NodeID>& deletedNodes() { return _deletedNodes; }
     std::vector<EdgeID>& deletedEdges() { return _deletedEdges; }

     std::span<NodeID> deletedNodesFromDataPart(size_t index) const {
         return _perDataPartDeletedNodes[index];
     }
     std::span<EdgeID> deletedEdgesFromDataPart(size_t index) const {
         return _perDataPartDeletedEdges[index];
     }

     bool empty() const {
         return _pendingNodes.empty() && _pendingEdges.empty() && _deletedNodes.empty()
             && _deletedEdges.empty();
    }

    /**
     * @brief Adds NodeIDs contained in @param newDeletedNodes to the member @ref
     * _deletedNodes
     * @detail Calls std::set::insert
     */
    void addDeletedNodes(const std::vector<NodeID>& newDeletedNodes);

    /**
     * @brief Adds NodeIDs contained in @param newDeletedNodes to the member @ref
     * _deletedNodes
     * @detail Calls std::set::insert
     */
    void addDeletedEdges(const std::vector<EdgeID>& newDeletedEdges);

    void prepare(CommitBuilder* commitBuilder);

    // bool deletesApplied() const { return _deletesApplied; }
    // void setApplied() { _deletesApplied = true; }

private:
    friend DataPartBuilder;
    friend CommitWriteBufferRebaser;
    friend DataPartModifier;
    friend CommitJournal;

    struct PendingEdge {
         ExistingOrPendingNode src;
         ExistingOrPendingNode tgt;
         EdgeTypeID edgeType;
         UntypedProperties properties;
    };

    // bool _deletesApplied {false};

    CommitBuilder* _commitBuilder {nullptr};

    // Nodes to be created when this commit commits
    std::vector<PendingNode> _pendingNodes;

    // Edges to be created when this commit commits
    std::vector<PendingEdge> _pendingEdges;

    // Nodes to be deleted when this commit commits
    std::vector<NodeID> _deletedNodes;

    // Edges to be deleted when this commit commits
    std::vector<EdgeID> _deletedEdges;

    std::vector<std::span<NodeID>> _perDataPartDeletedNodes;
    std::vector<std::span<EdgeID>> _perDataPartDeletedEdges;

    // Collection of methods to write the buffer to the provided datapart builder
    void buildPendingNodes(DataPartBuilder& builder);
    void buildPendingEdges(DataPartBuilder& builder);

    void buildPendingNode(DataPartBuilder& builder, const PendingNode& node);
    void buildPendingEdge(DataPartBuilder& builder, const PendingEdge& edge);

    /**
    * @breif Performs the following operations to @ref _deletedNodes and @ref _deletedEdges:
    * 1. Sort
    * 2. Remove duplicates
    * @detail Uses @ref std::ranges::sort i.e. quicksort
    */
    void sortDeletions();

    /**
     * @brief Using @ref _commitBuilder->commitData as the state of the graph, checks for edges which are
     * incident to a node with an ID which appears in @ref _deletedNodes
     * vector. EdgeIDs which are found to be incident are appended to @ref _deletedEdges.
     * @warn Assumes that both @ref _deletedNodes and @ref _deletedEdges are sorted.
     */
    void detectHangingEdges();

    /**
     * @brief Using @ref _commitBuilder->commitData as the state of the graph, fills @ref
     * _perDataPartDeletedNodes and @ref _perDataPartDeletedEdges at index `i` with the
     * subspan of nodes to be deleted which are contained in the DataPart at index `i` of
     * @ref _commitBuilder->commitData->allDataparts.
     * @warn Assumes that both @ref _deletedNodes and @ref _deletedEdges are sorted.
     */
    void fillPerDataPartDeletions();
};

class CommitWriteBufferRebaser {
public:
    explicit CommitWriteBufferRebaser(CommitWriteBuffer& buffer,
                                      NodeID entryNextNodeID,
                                      EdgeID entryNextEdgeID,
                                      NodeID currentNextNodeID,
                                      EdgeID currentNextEdgeID)
        : _buffer(&buffer),
          _entryNextNodeID(entryNextNodeID),
          _currentNextNodeID(currentNextNodeID),
          _entryNextEdgeID(entryNextEdgeID),
          _currentNextEdgeID(currentNextEdgeID)
        {
        }

    void rebaseIncidentNodeIDs();

private:
    CommitWriteBuffer* _buffer;

    NodeID _entryNextNodeID;
    NodeID _currentNextNodeID;

    EdgeID _entryNextEdgeID;
    EdgeID _currentNextEdgeID;
};

}
