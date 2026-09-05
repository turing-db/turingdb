#pragma once

#include <unordered_set>
#include <variant>
#include <vector>

#include "ArcManager.h"

#include "Commit.h"
#include "datapart/DataPart.h"
#include "writers/DataPartBuilder.h"

#include "indexes/Index.h"
#include "metadata/PropertyType.h"

#include "views/GraphView.h"

#include "ID.h"

namespace db {

class CommitWriteBufferRebaser;
class EntityIDRebaser;
class MetadataBuilder;
class CommitJournal;
class MetadataRebaser;
class Tombstones;
struct ConflictCheckSets;

class CommitWriteBuffer {

public:
    struct UntypedProperty;
    struct PendingNode;
    struct PendingEdge;
    struct NodeUpdate;
    struct EdgeUpdate;

    using SupportedTypeVariant = std::variant<
                     types::Int64::Primitive,
                     types::UInt64::Primitive,
                     types::Double::Primitive,
                     std::string, /// Needs to be owning to outlive the query
                     types::Bool::Primitive,
                     types::Embedding::OwningPrimitive
     >;
     using UntypedProperties = std::vector<UntypedProperty>;
     using PendingNodeOffset = size_t;
     /// A node: either exists in previous commit (materialised as NodeID),
     /// or to be created in this commit (materialised as PendingNodeOffset)
     using ExistingOrPendingNode = std::variant<NodeID, PendingNodeOffset>;
     using PendingNodes = std::vector<PendingNode>;
     using PendingEdges = std::vector<PendingEdge>;
     using DeletedNodes = std::unordered_set<NodeID>;
     using DeletedEdges = std::unordered_set<EdgeID>;
     /// Offsets into @ref _pendingNodes / @ref _pendingEdges of writes this commit drops
     using DeletedPendingEntities = std::unordered_set<size_t>;
     using UpdatedNodes = std::vector<NodeUpdate>;
     using UpdatedEdges = std::vector<EdgeUpdate>;
     using PendingIndexes = std::vector<WeakArc<Index>>;
     using DroppedIndexes = std::vector<WeakArc<Index>>;

     explicit CommitWriteBuffer(CommitJournal& journal, GraphView view);

     struct UntypedProperty {
         PropertyTypeID propertyID;
         SupportedTypeVariant value;
     };

     struct PendingNode {
         LabelSetHandle labelsetHandle;
         UntypedProperties properties;
     };

     struct PendingEdge {
         ExistingOrPendingNode src;
         ExistingOrPendingNode tgt;
         EdgeTypeID edgeType;
         UntypedProperties properties;
     };

     struct NodeUpdate {
         NodeID _idToUpdate;
         UntypedProperty _updatedValue;
     };

     struct EdgeUpdate {
         EdgeID _idToUpdate;
         UntypedProperty _updatedValue;
     };

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
      * @brief Adds the pending nodes and edges to the provided datapart builder, as well
      * as registering all newly created nodes/edges in the associated @ref WriteSet of
      * @ref _journal
      */
     void buildPending(DataPartBuilder& builder, Tombstones& tombstones);

     void applyUpdates(DataPartBuilder& builder);

     size_t numPendingNodes() const { return _pendingNodes.size(); }
     size_t numPendingEdges() const { return _pendingEdges.size(); }

     /**
      * @brief Populates the provided @param tombstones and @ref _journal with the deleted
      * node and edge IDs contained in @ref _deletedNodes and @ref _deletedEdges.
      */
     void applyDeletions(Tombstones& tombstones);

     const PendingNodes& pendingNodes() const { return _pendingNodes; }
     const PendingEdges& pendingEdges() const { return _pendingEdges; }

     const DeletedNodes& deletedNodes() const { return _deletedNodes; }
     const DeletedEdges& deletedEdges() const { return _deletedEdges; }

     const UpdatedNodes& updatedNodes() const { return _updatedNodes; }
     const UpdatedEdges& updatedEdges() const { return _updatedEdges; }

     bool empty() const {
         return _pendingNodes.empty() && _pendingEdges.empty() && _deletedNodes.empty()
             && _deletedEdges.empty();
     }

     bool containsCreates() const {
         return !_pendingNodes.empty() || !_pendingEdges.empty();
     }

     bool containsDeletes() const {
         return !_deletedNodes.empty() || !_deletedEdges.empty();
     }

     bool containsUpdates() const {
         return !_updatedNodes.empty() || !_updatedEdges.empty();
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

    /**
     * @brief Adds a single NodeID contained in @param newDeletedNode to the member @ref
     * _deletedNodes
     */
    void addDeletedNode(const NodeID& newDeletedNode);

    /**
     * @brief Marks a node this commit creates as deleted again. Its slot is kept, so the
     * offsets a pending edge names stay valid; the node is built into the datapart and
     * tombstoned rather than written.
     */
    void addDeletedPendingNode(PendingNodeOffset offset);

    void addDeletedPendingEdge(size_t offset);

    const DeletedPendingEntities& deletedPendingNodes() const { return _deletedPendingNodes; }
    const DeletedPendingEntities& deletedPendingEdges() const { return _deletedPendingEdges; }

    /**
     * @brief Marks every pending edge incident to a node this commit deletes - one it
     * created or one already committed - as deleted too, the pending counterpart of
     * @ref addHangingEdges.
     */
    void addHangingPendingEdges();

    /**
     * @brief Whether any pending edge this commit has not deleted is incident to
     * @param node, which a DELETE without DETACH must refuse.
     */
    bool hasPendingEdgesOn(const ExistingOrPendingNode& node) const;

    void addNodeUpdate(NodeID id, UntypedProperty& updatedProperty);
    void addEdgeUpdate(EdgeID id, UntypedProperty& updatedProperty);

    /**
     * @brief Adds a single EdgeID contained in @param newDeletedEdge to the member @ref
     * _deletedEdges
     */
    void addDeletedEdge(const EdgeID& newDeletedEdge);

    void addHangingEdges(const GraphView& view);

    void addPendingIndex(const WeakArc<Index>& index);
    void addDroppedIndex(const WeakArc<Index>& index);

    void setFlushed() { _flushed = true; }
    void setUnflushed() { _flushed = false; }
    bool isFlushed() const { return _flushed; }

    PendingNode& getPendingNode(size_t idx) { return _pendingNodes.at(idx); }
    PendingEdge& getPendingEdge(size_t idx) { return _pendingEdges.at(idx); }

    const PendingIndexes& pendingIndexes() const { return _pendingIndexes; }
    const DroppedIndexes& droppedIndexes() const { return _droppedIndexes; }

private:
    friend DataPartBuilder;
    friend CommitWriteBufferRebaser;
    friend MetadataRebaser;

    bool _flushed {false};

    CommitJournal& _journal;

    // NOTE: This view is NOT updated by CommitWriteBuffer::rebase
    GraphView _view;

    // Nodes to be created when this commit commits
    PendingNodes _pendingNodes;

    // Edges to be created when this commit commits
    PendingEdges _pendingEdges;

    // Nodes to be deleted when this commit commits
    DeletedNodes _deletedNodes;

    // Edges to be deleted when this commit commits
    DeletedEdges _deletedEdges;

    // Creations of this commit that it deletes again before committing
    DeletedPendingEntities _deletedPendingNodes;
    DeletedPendingEntities _deletedPendingEdges;

    UpdatedNodes _updatedNodes;
    UpdatedEdges _updatedEdges;

    PendingIndexes _pendingIndexes;
    DroppedIndexes _droppedIndexes;

    PendingNodes& pendingNodes() { return _pendingNodes; }
    PendingEdges& pendingEdges() { return _pendingEdges; }

    // Collection of methods to write the buffer to the provided datapart builder
    void buildPendingNodes(DataPartBuilder& builder, Tombstones& tombstones);
    void buildPendingEdges(DataPartBuilder& builder, Tombstones& tombstones);

    NodeID buildPendingNode(DataPartBuilder& builder, const PendingNode& node, bool deleted);
    void addPendingNodeProperties(DataPartBuilder& builder, const PendingNode& node);

    EdgeID buildPendingEdge(DataPartBuilder& builder, const PendingEdge& edge, bool deleted);

    bool touchesDeletedNode(const PendingEdge& edge) const;

    void applyNodeUpdates(DataPartBuilder& builder);
    void applyEdgeUpdates(DataPartBuilder& builder);

    /// Updates a property of an edge which is already committed
    void applyExistingEdgeUpdate(DataPartBuilder& builder,
                                 const EdgeRecord& record,
                                 const CommitWriteBuffer::UntypedProperty& prop);

    /// Updates a property of an edge which is not yet committed
    void applyPendingEdgeUpdate(DataPartBuilder& builder,
                                EdgeID edgeID,
                                const CommitWriteBuffer::UntypedProperty& prop);
};

class CommitWriteBufferRebaser {
public:
    explicit CommitWriteBufferRebaser(EntityIDRebaser* idRebaser, CommitWriteBuffer& buffer)
        : _idRebaser(idRebaser),
        _buffer(&buffer)
    {
    }

    void rebase();

    void rebaseIndexes(Commit::CommitSpan commitsSinceBranch);

private:
    EntityIDRebaser* _idRebaser {nullptr};
    CommitWriteBuffer* _buffer {nullptr};
};

}
