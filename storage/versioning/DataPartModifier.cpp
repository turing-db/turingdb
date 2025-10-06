#include "DataPartModifier.h"

#include "EdgeContainer.h"
#include "EnumerateFrom.h"
#include "NodeContainer.h"
#include "metadata/PropertyType.h"
#include "CommitBuilder.h"
#include "range/v3/view/enumerate.hpp"

using namespace db;

void DataPartModifier::applyModifications(size_t index) {
    _dpIndex = index;

    _nodesToDelete = _writeBuffer.deletedNodesFromDataPart(_dpIndex);
    _edgesToDelete = _writeBuffer.deletedEdgesFromDataPart(_dpIndex);

    // If we are deleting the entire datapart, return an empty builder
    if (_oldDP->getNodeCount() == _nodesToDelete.size()
        && _oldDP->getEdgeCount() == _edgesToDelete.size()) {
        return;
    }

    // The first node/edge ID differs if some block of IDs starting with the first is
    // deleted
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

    // Add nodes which were not deleted
    for (const auto& [nodeID, nodeRecord] :
         EnumerateFrom(oldFirstNodeID.getValue(), oldNodeRecords)) {
        if (!std::ranges::binary_search(_nodesToDelete, NodeID{nodeID})) {
            _builder->addNode(nodeRecord._labelset);
        }
    }

    EdgeIDToRecordMap oldIdsToNewRecords;
    // Add edges which were not deleted, remap their source and target if needed
    for (const EdgeRecord& edgeRecord : oldOutEdgeRecords) {
        // Get the original IDs of the source and target nodes
        NodeID src = edgeRecord._nodeID;
        NodeID tgt = edgeRecord._otherID;
        EdgeID edgeID = edgeRecord._edgeID;

        if (!std::ranges::binary_search(_edgesToDelete, edgeID)
            && !std::ranges::binary_search(_nodesToDelete, src)
            && !std::ranges::binary_search(_nodesToDelete, tgt)) {
            EdgeTypeID typeID = edgeRecord._edgeTypeID;
            // Update source and target nodes to their new ID
            NodeID newSrc = nodeIDMapping(src);
            NodeID newTgt = nodeIDMapping(tgt);
            oldIdsToNewRecords[edgeID] = _builder->addEdge(typeID, newSrc, newTgt);
        }
    }

    const PropertyManager& nodePropManager = _oldDP->nodeProperties();
    copyNodeProps<types::Int64>(nodePropManager._int64s);
    copyNodeProps<types::UInt64>(nodePropManager._uint64s);
    copyNodeProps<types::Double>(nodePropManager._doubles);
    copyNodeProps<types::String>(nodePropManager._strings);
    copyNodeProps<types::Bool>(nodePropManager._bools);

    const PropertyManager& edgePropManager = _oldDP->edgeProperties();
    copyEdgeProps<types::Int64>(edgePropManager._int64s, oldIdsToNewRecords);
    copyEdgeProps<types::UInt64>(edgePropManager._uint64s, oldIdsToNewRecords);
    copyEdgeProps<types::Double>(edgePropManager._doubles, oldIdsToNewRecords);
    copyEdgeProps<types::String>(edgePropManager._strings, oldIdsToNewRecords);
    copyEdgeProps<types::Bool>(edgePropManager._bools, oldIdsToNewRecords);
}

DataPartModifier::DataPartIndex DataPartModifier::getDataPartIndex(NodeID node) {
    auto it = _nodeToDPMap.find(node);
    if (it != _nodeToDPMap.end()) {
        return it->second;
    }

    DataPartSpan dataparts = _writeBuffer._commitBuilder->commitData().allDataparts();

    // TODO:: Binary search-esque
    for (const auto& [index, part] : rv::enumerate(dataparts)) {
        if (part->hasNode(node)) {
            _nodeToDPMap[node] = index;
            return index;
        }
    }
    panic("Node {} does not exist in dataparts.", node);
}
