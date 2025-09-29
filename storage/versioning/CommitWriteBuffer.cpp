#include "CommitWriteBuffer.h"

#include <utility>
#include <variant>
#include <vector>

#include "ID.h"
#include "versioning/MetadataRebaser.h"
#include "writers/DataPartBuilder.h"
#include "writers/MetadataBuilder.h"

using namespace db;

namespace {
// Functor for helping get the ValueType from an UntypedProperty
struct ValueTypeFromProperty {
    ValueType operator()([[maybe_unused]] types::Int64::Primitive propValue) const {
        return types::Int64::_valueType;
    }
    ValueType operator()([[maybe_unused]] types::UInt64::Primitive propValue) const {
        return types::UInt64::_valueType;
    }
    ValueType operator()([[maybe_unused]] types::Double::Primitive propValue) const {
        return types::Double::_valueType;
    }
    ValueType operator()([[maybe_unused]] const std::string& propValue) const {
        return types::String::_valueType;
    }
    ValueType operator()([[maybe_unused]] types::Bool::Primitive propValue) const {
        return types::Bool::_valueType;
    }
};
}


void CommitWriteBuffer::addPendingNode(LabelSetHandle lsh,
                                       std::vector<UntypedProperty>&& properties) {
    _pendingNodes.emplace_back(lsh, std::move(properties));
}

void CommitWriteBuffer::addPendingEdge(ExistingOrPendingNode src, ExistingOrPendingNode tgt,
                                       std::string&& edgeType,
                                       std::vector<UntypedProperty>&& edgeProperties) {
    _pendingEdges.emplace_back(src, tgt, std::move(edgeType), std::move(edgeProperties));
}

void CommitWriteBuffer::addDeletedNodes(const std::vector<NodeID>& newDeletedNodes) {
    _deletedNodes.insert(newDeletedNodes.begin(), newDeletedNodes.end());
}

void CommitWriteBuffer::buildPendingNode(DataPartBuilder& builder,
                                         [[maybe_unused]] MetadataBuilder& metadataBuilder,
                                         const PendingNode& node) {
    const NodeID nodeID = builder.addNode(node.labelsetHandle);

    // Adding node properties
    for (const auto& [id, value] : node.properties) {
        // Get the value type from the untyped property
        // const ValueType propValueType = std::visit(ValueTypeFromProperty {}, value);
        // Get the existing ID, or create a new property and get that ID
        // const PropertyTypeID propID =
            // metadataBuilder.getOrCreatePropertyType(prop.propertyID, propValueType)._id;

        // Add this property to this node
        std::visit(
            [&](auto&& val) {
                using T = std::decay_t<decltype(val)>;

                if constexpr (std::is_same_v<T, types::Int64::Primitive>) {
                    builder.addNodeProperty<types::Int64>(nodeID, id, val);
                } else if constexpr (std::is_same_v<T, types::UInt64::Primitive>) {
                    builder.addNodeProperty<types::UInt64>(nodeID, id, val);
                } else if constexpr (std::is_same_v<T, types::Double::Primitive>) {
                    builder.addNodeProperty<types::Double>(nodeID, id, val);
                } else if constexpr (std::is_same_v<T, std::string>) {
                    builder.addNodeProperty<types::String>(
                        nodeID, id, std::forward<decltype(val)>(val));
                } else if constexpr (std::is_same_v<T, types::Bool::Primitive>) {
                    builder.addNodeProperty<types::Bool>(nodeID, id, val);
                }
            },
            value);
    }
}

void CommitWriteBuffer::buildPendingNodes(DataPartBuilder& builder,
                                          MetadataBuilder& metadataBuilder) {
    for (const auto& node : pendingNodes()) {
        buildPendingNode(builder, metadataBuilder, node);
    }
}

void CommitWriteBuffer::buildPendingEdge(DataPartBuilder& builder,
                                         MetadataBuilder& metadataBuilder,
                                         const PendingEdge& edge) {
    // If this edge has source or target which is a node in a previous datapart, check
    // if it has been deleted. NOTE: Deletes currently not implemented
    if (const NodeID* srcID = std::get_if<NodeID>(&edge.src)) {
        if (deletedNodes().contains(*srcID)) {
            return;
        }
    }
    if (const NodeID* tgtID = std::get_if<NodeID>(&edge.tgt)) {
        if (deletedNodes().contains(*tgtID)) {
            return;
        }
    }
    // Otherwise: source and target are either non-deleted existing nodes, or nodes
    // created in this commit
    // WARN: PendingNodes have their IDs computed based on their offset in the
    // PendingNodes vector, and the firstNodeID of the datapart. This is correct if
    // PendingNodes are added to the datapart builder in the order that they appear in the
    // PendingNodes vector.
    const NodeID srcID =
        std::holds_alternative<NodeID>(edge.src)
            ? std::get<NodeID>(edge.src)
            : NodeID {std::get<CommitWriteBuffer::PendingNodeOffset>(edge.src)}
                  + builder.firstNodeID();

    const NodeID tgtID =
        std::holds_alternative<NodeID>(edge.tgt)
            ? std::get<NodeID>(edge.tgt)
            : NodeID { std::get<CommitWriteBuffer::PendingNodeOffset>(edge.tgt) } + builder.firstNodeID();

    const std::string& edgeTypeName = edge.edgeLabelTypeName;
    const EdgeTypeID edgeTypeID = metadataBuilder.getOrCreateEdgeType(edgeTypeName);

    const EdgeRecord newEdgeRecord = builder.addEdge(edgeTypeID, srcID, tgtID);

    const EdgeID newEdgeID = newEdgeRecord._edgeID;

    for (const auto& [id, value] : edge.properties) {
        // Get the value type from the untyped property
        // const ValueType propValueType = std::visit(ValueTypeFromProperty {}, value);

        // Get the existing ID, or create a new property and get that ID
        // const PropertyTypeID propID =
            // metadataBuilder.getOrCreatePropertyType(prop.propertyID, propValueType)._id;

        // Add this property to this edge in this builder
        std::visit(
            [&](const auto& val) {
                using T = std::decay_t<decltype(val)>;
                if constexpr (std::is_same_v<T, types::Int64::Primitive>) {
                    builder.addEdgeProperty<types::Int64>(
                        {newEdgeID, srcID, tgtID, edgeTypeID}, id, val);
                } else if constexpr (std::is_same_v<T, types::UInt64::Primitive>) {
                    builder.addEdgeProperty<types::UInt64>(
                        {newEdgeID, srcID, tgtID, edgeTypeID}, id, val);
                } else if constexpr (std::is_same_v<T, types::Double::Primitive>) {
                    builder.addEdgeProperty<types::Double>(
                        {newEdgeID, srcID, tgtID, edgeTypeID}, id, val);
                } else if constexpr (std::is_same_v<T, std::string>) {
                    builder.addEdgeProperty<types::String>(
                        {newEdgeID, srcID, tgtID, edgeTypeID}, id, val);
                } else if constexpr (std::is_same_v<T, types::Bool::Primitive>) {
                    builder.addEdgeProperty<types::Bool>(
                        {newEdgeID, srcID, tgtID, edgeTypeID}, id, val);
                }
            },
            value);
    }
}

void CommitWriteBuffer::buildPendingEdges(DataPartBuilder& builder,
                                          MetadataBuilder& metadataBuilder) {
    for (const PendingEdge& edge : pendingEdges()) {
        buildPendingEdge(builder, metadataBuilder, edge);
    }
}

void CommitWriteBuffer::buildPending(DataPartBuilder& builder) {
    MetadataBuilder& metadataBuilder = builder.getMetadata();
    std::unordered_map<CommitWriteBuffer::PendingNodeOffset, NodeID> tempIDMap;

    buildPendingNodes(builder, metadataBuilder);
    buildPendingEdges(builder, metadataBuilder);
}

void CommitWriteBufferRebaser::rebaseIncidentNodeIDs() {
    // If a @ref Change makes commits locally, it will create nodes according to what it
    // thinks the next NodeID should be. This view is determined by the next node ID at
    // the time the change branched from main. In subsequent commits on the Change, it is
    // possible that there have been edges created using NodeID injection, e.g.
    // @code
    // CREATE (n:NEWNODE) <- Node created in this Change
    // COMMIT             <- NEWNODE is written, assume it is given ID 5
    // CREATE (n @ 5)-[e:NEWEDGE]-(m:NEWNODE) <- Edge created in this change, from node 5
    // @endcode
    // There may have been other Changes which have submitted to main in the time between
    // this Change branching and submitting. Thus, when this Change comes to submit, there
    // may have been a node with ID 5 created on main. If this Change commits
    // without rebasing, NEWEDGE will be between the node with ID 5 that was created in
    // the other change, and submitted to main, and not the "local" node with ID 5 as it
    // should. Hence, we need to compare:
    // 1. the nextNodeID on main when this Change was created
    // 2. the nextNodeID on main when this Change attempts to submit
    // If they are different, there have been nodes created on main between Change
    // creation and submission. We therefore need to readjust any edges which reference a
    // NodeID which is greater than the nextNodeID at time of Change creation, as these
    // are edges between locally created nodes.
    const auto rebaseNodeID = [&](NodeID wbID) {
        if (wbID >= _entryNextNodeID) {
            return wbID + _currentNextNodeID - _entryNextNodeID;
        }
        return wbID;

    };

    for (CommitWriteBuffer::PendingEdge& edge : _buffer.pendingEdges()) {
        // We only care about edges that refer to NodeIDs
        if (NodeID* oldSrcID = std::get_if<NodeID>(&edge.src)) {
            edge.src = NodeID {rebaseNodeID(*oldSrcID)};
        }
        if (NodeID* oldTgtID = std::get_if<NodeID>(&edge.tgt)) {
            edge.tgt = NodeID {rebaseNodeID(*oldTgtID)};
        }
    }
}
