#include "DataPartModifier.h"

#include <memory>
#include <range/v3/view/enumerate.hpp>
#include <range/v3/view/iota.hpp>

#include "ArcManager.h"
#include "DataPart.h"
#include "EdgeContainer.h"
#include "ID.h"
#include "TuringException.h"
#include "versioning/EdgeContainerModifier.h"
#include "versioning/NodeContainerModifier.h"
#include "NodeContainer.h"
#include "writers/DataPartBuilder.h"
#include "EnumerateFrom.h"

using namespace db;

void DataPartModifier::prepare() {
    // 1. Calculate NodeID mapping function

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

    // 2. Calculate EdgeID mapping function

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

void DataPartModifier::fillBuilder() {
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
         EnumerateFrom(oldFirstNodeID.getValue(), oldNodeRecords)) {
             if (!_nodesToDelete.contains(nodeID)) {
                 _builder->addNode(nodeRecord._labelset);
             }
    }

    // Edges
    for (const auto& [edgeID, edgeRecord] :
         EnumerateFrom(oldFirstEdgeID.getValue(), oldOutEdgeRecords)) {
        // Get the original IDs of the source and target nodes
        NodeID src = edgeRecord._nodeID;
        NodeID tgt = edgeRecord._otherID;
        // If this edge is not deleted, and neither its source nor target is deleted, include it
        if (!_edgesToDelete.contains(edgeID) && !_nodesToDelete.contains(src)
            && !_nodesToDelete.contains(tgt)) {
            EdgeTypeID typeID = edgeRecord._edgeTypeID;
            // Update source and target nodes to their new ID
            NodeID newSrc = _nodeIDMapping(edgeRecord._nodeID);
            NodeID newTgt = _nodeIDMapping(edgeRecord._otherID);
            _builder->addEdge(typeID, newSrc, newTgt);
        }
    }

    // Node & Edge properties are registered in @ref DataPart::load
}
