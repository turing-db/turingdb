#include "DataPartModifier.h"

#include <cstdint>
#include <memory>
#include <range/v3/view/enumerate.hpp>
#include <range/v3/view/iota.hpp>

#include "ArcManager.h"
#include "DataPart.h"
#include "EdgeContainer.h"
#include "ID.h"
#include "TuringException.h"
#include "metadata/PropertyType.h"
#include "properties/PropertyContainer.h"
#include "versioning/EdgeContainerModifier.h"
#include "versioning/NodeContainerModifier.h"
#include "NodeContainer.h"
#include "writers/DataPartBuilder.h"

using namespace db;

namespace rg = ranges;
namespace rv = rg::views;

namespace {
    // Enumerate from starting value @ref start (inclusive)
    template <typename Rng>
    auto enumerate_from(std::ptrdiff_t start, Rng&& rng) {
        return rv::zip(rv::iota(start), std::forward<Rng>(rng));
    }
}

void DataPartModifier::applyDeletions() {
    if (_nodesToDelete.empty() && _edgesToDelete.empty()) {
        return;
    }

    prepare();

    deleteNodes();
    deleteEdges();
    
    // TODO: Update PropertyManager
    // TODO: Update StringIndex}

    assembleNewPart();
}

void DataPartModifier::prepare() {
    // 1. Create builder
    _builder = DataPartBuilder::createEmptyBuilder();

    // 2. Work out which edges need to be deleted as a result of one of their nodes
    // being deleted, add these to @ref _edgesToDelete
    detectHangingEdges();

    // 3. Calculate NodeID mapping function

    // The only nodes whose ID changes are those which have an ID greater than a
    // deleted node, as whenever a node is deleted, all subsequent nodes need to have
    // their ID reduced by 1 to fill the gap left by the deleted node. Thus, the new
    // ID of a node, x, is equal to the number of deleted nodes which have an ID
    // smaller than x.
    _nodeIDMapping = [this](NodeID x) {
        if (_nodesToDelete.contains(x)) {
            throw TuringException(fmt::format(
                "Node {} is being deleted. Should not to attempt to get a new ID.", x));
        }
        auto smallerDeletedNodesIt = std::ranges::lower_bound(_nodesToDelete, x);
        size_t numSmallerDeletedNodes =
            std::distance(_nodesToDelete.cbegin(), smallerDeletedNodesIt);
        return x - numSmallerDeletedNodes;
    };

    // 4. Calculate EdgeID mapping function

    // Same logic as nodes above
    _edgeIDMapping = [this](EdgeID x) {
        if (_edgesToDelete.contains(x)) {
            throw TuringException(fmt::format(
                "Edge {} is being deleted. Should not to attempt to get a new ID.", x));
        }
        auto smallerDeletedEdgesIt = std::ranges::lower_bound(_edgesToDelete, x);
        size_t numSmallerDeletedEdges =
            std::distance(_edgesToDelete.cbegin(), smallerDeletedEdgesIt);
        return x - numSmallerDeletedEdges;
    };
}

void DataPartModifier::addNodeProperties(const PropertyTypeID& propID, const PropertyContainer& cont) {
    switch (cont.getValueType()) {

        case ValueType::Int64: {
            auto& casted = cont.cast<types::Int64>();
            for (const auto& [id, value] : casted.zipped()) {
                // ID in original datapart: check if deleted
                NodeID oldID(id.getValue());
                if (!_nodesToDelete.contains(oldID)) {
                    // Get the new ID of the node
                    NodeID newID(_nodeIDMapping(oldID));
                    _builder->addNodeProperty<types::Int64>(newID, propID, value);
                }
            }
        break;
        }

        case ValueType::UInt64: {
            auto& casted = cont.cast<types::UInt64>();
            for (const auto& [id, value] : casted.zipped()) {
                // ID in original datapart: check if deleted
                NodeID oldID(id.getValue());
                if (!_nodesToDelete.contains(oldID)) {
                    // Get the new ID of the node
                    NodeID newID(_nodeIDMapping(oldID));
                    _builder->addNodeProperty<types::UInt64>(newID, propID, value);
                }
            }
        break;
        }

        case ValueType::Double: {
            auto& casted = cont.cast<types::Double>();
            for (const auto& [id, value] : casted.zipped()) {
                // ID in original datapart: check if deleted
                NodeID oldID(id.getValue());
                if (!_nodesToDelete.contains(oldID)) {
                    // Get the new ID of the node
                    NodeID newID(_nodeIDMapping(oldID));
                    _builder->addNodeProperty<types::Double>(newID, propID, value);
                }
            }
        break;
        }

        case ValueType::String: {
            auto& casted = cont.cast<types::String>();
            for (const auto& [id, value] : casted.zipped()) {
                // ID in original datapart: check if deleted
                NodeID oldID(id.getValue());
                if (!_nodesToDelete.contains(oldID)) {
                    // Get the new ID of the node
                    NodeID newID(_nodeIDMapping(oldID));
                    _builder->addNodeProperty<types::String>(newID, propID, value);
                }
            }
        }

        case ValueType::Bool: {
            auto& casted = cont.cast<types::Bool>();
            for (const auto& [id, value] : casted.zipped()) {
                // ID in original datapart: check if deleted
                NodeID oldID(id.getValue());
                if (!_nodesToDelete.contains(oldID)) {
                    // Get the new ID of the node
                    NodeID newID(_nodeIDMapping(oldID));
                    _builder->addNodeProperty<types::Bool>(newID, propID, value);
                }
            }
        break;
        }

        default: {
            throw TuringException(fmt::format("Invalid node property container type: {}",
                                              ValueTypeName::value(cont.getValueType())));
        }
    }
}

void DataPartModifier::addEdgeProperties(const PropertyTypeID& propID,
                                         const PropertyContainer& cont) {
    switch (cont.getValueType()) {

        case ValueType::Int64: {
            auto& casted = cont.cast<types::Int64>();
            for (const auto& [id, value] : casted.zipped()) {

                // ID in the old datapart: what we need to check is deleted
                EdgeID oldID(id.getValue());
                if (!_edgesToDelete.contains(oldID)) {
                    // The edges' new ID in the newdatapart
                    EdgeID newID(_edgeIDMapping(oldID));

                    // Get the associated edgeRecord that was populated in @ref reconstruct
                    EdgeRecord& record = _builder->_edges.at(newID.getValue());
                     
                    // Add property using newly constructed record
                    _builder->addEdgeProperty<types::Int64>(record, propID, value);
                }
            }
        break;
        }

        case ValueType::UInt64: {
            auto& casted = cont.cast<types::UInt64>();
            for (const auto& [id, value] : casted.zipped()) {

                // ID in the old datapart: what we need to check is deleted
                EdgeID oldID(id.getValue());
                if (!_edgesToDelete.contains(oldID)) {
                    // The edges' new ID in the newdatapart
                    EdgeID newID(_edgeIDMapping(oldID));

                    // Get the associated edgeRecord that was populated in @ref reconstruct
                    EdgeRecord& record = _builder->_edges.at(newID.getValue());
                     
                    // Add property using newly constructed record
                    _builder->addEdgeProperty<types::UInt64>(record, propID, value);
                }
            }
        break;
        }

        case ValueType::Double: {
            auto& casted = cont.cast<types::Double>();
            for (const auto& [id, value] : casted.zipped()) {

                // ID in the old datapart: what we need to check is deleted
                EdgeID oldID(id.getValue());
                if (!_edgesToDelete.contains(oldID)) {
                    // The edges' new ID in the newdatapart
                    EdgeID newID(_edgeIDMapping(oldID));

                    // Get the associated edgeRecord that was populated in @ref reconstruct
                    EdgeRecord& record = _builder->_edges.at(newID.getValue());
                     
                    // Add property using newly constructed record
                    _builder->addEdgeProperty<types::Double>(record, propID, value);
                }
            }
        break;
        }

        case ValueType::String: {
            auto& casted = cont.cast<types::String>();
            for (const auto& [id, value] : casted.zipped()) {

                // ID in the old datapart: what we need to check is deleted
                EdgeID oldID(id.getValue());
                if (!_edgesToDelete.contains(oldID)) {
                    // The edges' new ID in the new datapart
                    EdgeID newID(_edgeIDMapping(oldID));

                    // Get the associated edgeRecord that was populated in @ref reconstruct
                    EdgeRecord& record = _builder->_edges.at(newID.getValue());
                     
                    // Add property using newly constructed record
                    _builder->addEdgeProperty<types::String>(record, propID, value);
                }
            }
        break;
        }

        case ValueType::Bool: {
            auto& casted = cont.cast<types::Bool>();
            for (const auto& [id, value] : casted.zipped()) {

                // ID in the old datapart: what we need to check is deleted
                EdgeID oldID(id.getValue());
                if (!_edgesToDelete.contains(oldID)) {
                    // The edges' new ID in the newdatapart
                    EdgeID newID(_edgeIDMapping(oldID));

                    // Get the associated edgeRecord that was populated in @ref reconstruct
                    EdgeRecord& record = _builder->_edges.at(newID.getValue());
                     
                    // Add property using newly constructed record
                    _builder->addEdgeProperty<types::Bool>(record, propID, value);
                }
            }
        break;
        }

        default: {
            throw TuringException("Invalid edge property container type");
        }
    }
}

void DataPartModifier::reconstruct() {
    NodeID oldFirstNodeID = _oldDP->getFirstNodeID();
    EdgeID oldFirstEdgeID = _oldDP->getFirstEdgeID();

    // We are reconstructing the new datapart in the same ID space
    _builder->_firstNodeID = _builder->_nextNodeID = oldFirstNodeID;
    _builder->_firstEdgeID = _builder->_nextEdgeID = oldFirstEdgeID;

    _builder->_nodeProperties = std::make_unique<PropertyManager>();
    _builder->_edgeProperties = std::make_unique<PropertyManager>();

    const std::vector<NodeRecord>& oldNodeRecords = _oldDP->nodes().records();
    // DataPartBuilder only requires providing OUT edges
    const std::span<const EdgeRecord>& oldOutEdgeRecords = _oldDP->edges().getOuts();

    // Nodes
    for (const auto& [nodeID, nodeRecord] :
         enumerate_from(oldFirstNodeID.getValue(), oldNodeRecords)) {
             if (!_nodesToDelete.contains(nodeID)) {
                 _builder->addNode(nodeRecord._labelset);
             }
    }

    // Edges
    for (const auto& [edgeID, edgeRecord] :
         enumerate_from(oldFirstEdgeID.getValue(), oldOutEdgeRecords)) {
        if (!_edgesToDelete.contains(edgeID)) {
            EdgeTypeID typeID = edgeRecord._edgeTypeID;
            NodeID src = _nodeIDMapping(edgeRecord._nodeID);
            NodeID tgt = _nodeIDMapping(edgeRecord._otherID);
            _builder->addEdge(typeID, src, tgt);
        }
    }

    // Node & Edge properties are registered in @ref DataPart::load
}

void DataPartModifier::getCoreNodeLabelSets() {
    std::vector<LabelSetHandle>& coreNodeLabelSets = _builder->_coreNodeLabelSets;

    auto& oldNodeContainer = _oldDP->nodes();
    NodeID oldFirstNodeID = oldNodeContainer.getFirstNodeID();
    size_t oldNodeCount = oldNodeContainer.size();

    // NOTE: May not be necessary if we do not want to error on an attempt to delete a non-existing node
    // Bounds check to ensure all nodes that are to be deleted are in this DP
    NodeID smallestNodeToDelete = *_nodesToDelete.cbegin();
    NodeID largestNodeToDelete = *_nodesToDelete.crbegin();
    if (smallestNodeToDelete < oldFirstNodeID) {
        throw TuringException(fmt::format("Node with ID {} is not in this datapart; "
                                          "smallest ID in this datapart is {}",
                                          smallestNodeToDelete, oldFirstNodeID));
    }
    if (largestNodeToDelete > oldFirstNodeID + oldNodeCount - 1) {
        throw TuringException(fmt::format("Node with ID {} is not in this datapart; "
                                          "largest ID in this datapart is {}",
                                          largestNodeToDelete,
                                          oldFirstNodeID + oldNodeCount - 1));
    }

    // If entire NodeContainer was deleted, exit early
    // NOTE: This requires the above bounds check to be correct
    if (oldNodeCount == _nodesToDelete.size()) {
        coreNodeLabelSets.clear(); // No nodes: all deleted
        return;
    }

    // Find the first NodeID of the new datapart
    _builder->_firstNodeID = UINT64_MAX;
    // Find the largest ID of the first contiguous block of NodeIDs that will be deleted
    auto largestContiguouslyDeleted = _nodesToDelete.begin();

    // If the first NodeID of the old container is not deleted, then the first NodeID
    // remains the same in the new container
    if (*largestContiguouslyDeleted != oldFirstNodeID) {
        _builder->_firstNodeID = oldFirstNodeID;
    } else { // Otherwise, keep incrementing the new first NodeID until we find an
        while (*++largestContiguouslyDeleted == ++_builder->_firstNodeID)
            ;
    }
}

void DataPartModifier::detectHangingEdges() {
    const auto& oldEdgeContainer = _oldDP->edges();
    const uint64_t oldFirstEdgeID = oldEdgeContainer.getFirstEdgeID().getValue();

    // Get [edgeId, edgeRecord] pairs starting from the first edgeID, check the record to
    // see if the source or target node of the edge is to be deleted. If it is, this edge
    // also needs to be deleted.

    // In edges
    for (const auto& [edgeID, edgeRecord] :
         enumerate_from(oldFirstEdgeID, oldEdgeContainer.getIns())) {

        if (_nodesToDelete.contains(edgeRecord._nodeID)) {
            _edgesToDelete.emplace(edgeID);
        }
        if (_nodesToDelete.contains(edgeRecord._otherID)) {
            _edgesToDelete.emplace(edgeID);
        }
    }

    // Out edges
    for (const auto& [edgeID, edgeRecord] :
         enumerate_from(oldFirstEdgeID, oldEdgeContainer.getOuts())) {

        if (_nodesToDelete.contains(edgeRecord._nodeID)) {
            _edgesToDelete.emplace(edgeID);
        }
        if (_nodesToDelete.contains(edgeRecord._otherID)) {
            _edgesToDelete.emplace(edgeID);
        }
    }
}

void DataPartModifier::deleteNodes() {
    // NodeContainerModifier::deleteNodes(_oldDP->nodes(), *_newNodeCont, _nodesToDelete);
}

void DataPartModifier::deleteEdges() {
    // EdgeContainerModifier::deleteEdges(_oldDP->edges(), *_newEdgeCont, _edgesToDelete);
}

void DataPartModifier::assembleNewPart() {
    // _newDP->_nodes = std::move(_newNodeCont);
    // _newDP->_edges = std::move(_newEdgeCont);
}
