#pragma once

#include <unordered_set>
#include <variant>
#include <vector>

#include "DataPart.h"
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
    CommitWriteBuffer(CommitJournal& journal);

    using SupportedTypeVariant =
        std::variant<types::Int64::Primitive, types::UInt64::Primitive,
                     types::Double::Primitive, std::string,
                     types::Bool::Primitive>;

     struct UntypedProperty {
        PropertyTypeID propertyID;
        SupportedTypeVariant value;
     };

     using UntypedProperties = std::vector<UntypedProperty>;
     using PendingNodeOffset = size_t;
     using ExistingOrPendingNode = std::variant<NodeID, PendingNodeOffset>;

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

     // A node: either exists in previous commit (materialised as NodeID),
     // or to be created in this commit (materialised as PendingNodeOffset)
     using PendingNodes = std::vector<PendingNode>;
     using PendingEdges = std::vector<PendingEdge>;
     using DeletedNodes = std::unordered_set<NodeID>;
     using DeletedEdges = std::unordered_set<EdgeID>;

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
     void buildPending(DataPartBuilder& builder);

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

     bool empty() const {
         return _pendingNodes.empty() && _pendingEdges.empty() && _deletedEdges.empty()
             && _deletedEdges.empty();
     }

     bool containsCreates() const {
         return !_pendingNodes.empty() || !_pendingEdges.empty();
     }

     bool containsDeletes() const {
         return !_deletedNodes.empty() || !_deletedEdges.empty();
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
     * @brief Adds a single EdgeID contained in @param newDeletedEdge to the member @ref
     * _deletedEdges
     */
    void addDeletedEdge(const EdgeID& newDeletedEdge);

    void addHangingEdges(const GraphView& view);

    void setFlushed() { _flushed = true; }
    void setUnflushed() { _flushed = false; }
    bool isFlushed() const { return _flushed; }

    PendingNode& getPendingNode(size_t idx) { return _pendingNodes.at(idx); }
    PendingEdge& getPendingEdge(size_t idx) { return _pendingEdges.at(idx); }

private:
    friend DataPartBuilder;
    friend CommitWriteBufferRebaser;
    friend MetadataRebaser;

    bool _flushed {false};

    CommitJournal& _journal;

    // Nodes to be created when this commit commits
    std::vector<PendingNode> _pendingNodes;

    // Edges to be created when this commit commits
    std::vector<PendingEdge> _pendingEdges;

    // Nodes to be deleted when this commit commits
    std::unordered_set<NodeID> _deletedNodes;

    // Edges to be deleted when this commit commits
    std::unordered_set<EdgeID> _deletedEdges;

    PendingNodes& pendingNodes() { return _pendingNodes; }
    PendingEdges& pendingEdges() { return _pendingEdges; }

    // Collection of methods to write the buffer to the provided datapart builder
    void buildPendingNodes(DataPartBuilder& builder);
    void buildPendingEdges(DataPartBuilder& builder);

    void buildPendingNode(DataPartBuilder& builder, const PendingNode& node);
    void buildPendingEdge(DataPartBuilder& builder, const PendingEdge& edge);
};

class CommitWriteBufferRebaser {
public:
    explicit CommitWriteBufferRebaser(EntityIDRebaser* idRebaser, CommitWriteBuffer& buffer)
        : _idRebaser(idRebaser),
        _buffer(&buffer)
    {
    }

    void rebase();

private:
    EntityIDRebaser* _idRebaser {nullptr};
    CommitWriteBuffer* _buffer {nullptr};
};

}
