#include "DataPartModifier.h"

#include <bits/ranges_algo.h>

#include "EdgeContainer.h"
#include "NodeContainer.h"
#include "EnumerateFrom.h"

using namespace db;

void DataPartModifier::applyModifications() {
    // The only nodes (edges) whose ID changes are those which have an ID greater than a
    // deleted node (edge), as whenever a node (edge) is deleted, all subsequent nodes
    // (edges) need to have their ID reduced by 1 to fill the gap left by the deleted
    // node (edge). Thus, the new ID of a node (edge), x, is equal to the number of
    // deleted nodes (edges) in this datapart which have an ID smaller than x.
    const auto nodeIDMapping = [&](NodeID x) {
        if (std::ranges::binary_search(_nodesToDelete, x)) [[unlikely]] {
            std::string err =
                fmt::format("Attempted to get mapped ID of deleted node: {}.", x);
            throw TuringException(std::move(err));
        }
        // lower_bound O(logn) (binary search); distance is O(1)
        auto smallerNodesIt = std::ranges::lower_bound(_nodesToDelete, x);
        size_t numSmallerNodes = std::distance(_nodesToDelete.begin(), smallerNodesIt);
        return x - numSmallerNodes;
    };
    const auto edgeIDMapping = [&](EdgeID x) {
        if (std::ranges::binary_search(_edgesToDelete, x)) [[unlikely]] {
            std::string err =
                fmt::format("Attempted to get mapped ID of deleted edge: {}.", x);
            throw TuringException(std::move(err));
        }
        // lower_bound O(logn) (binary search); distance is O(1)
        auto smallerEdgesIt = std::ranges::lower_bound(_edgesToDelete, x);
        size_t numSmallerEdges = std::distance(_edgesToDelete.begin(), smallerEdgesIt);
        return x - numSmallerEdges;
    };

    // The first node/edge ID differs if some block of IDs starting with the first is
    // deleted
    NodeID oldFirstNodeID = _oldDP->getFirstNodeID();
    EdgeID oldFirstEdgeID = _oldDP->getFirstEdgeID();

    // We are reconstructing the new datapart in the same ID space
    _builder->_firstNodeID = _builder->_nextNodeID = nodeIDMapping(oldFirstNodeID);
    _builder->_firstEdgeID = _builder->_nextEdgeID = edgeIDMapping(oldFirstEdgeID);

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

    // Add edges which were not deleted, remap their source and target if needed
    for (const auto& [edgeID, edgeRecord] :
         EnumerateFrom(oldFirstEdgeID.getValue(), oldOutEdgeRecords)) {
        // Get the original IDs of the source and target nodes
        NodeID src = edgeRecord._nodeID;
        NodeID tgt = edgeRecord._otherID;

        if (!std::ranges::binary_search(_edgesToDelete, EdgeID {edgeID})
            && !std::ranges::binary_search(_nodesToDelete, NodeID {src})
            && !std::ranges::binary_search(_nodesToDelete, NodeID {tgt})) {
            EdgeTypeID typeID = edgeRecord._edgeTypeID;
            // Update source and target nodes to their new ID
            NodeID newSrc = nodeIDMapping(edgeRecord._nodeID);
            NodeID newTgt = nodeIDMapping(edgeRecord._otherID);
            _builder->addEdge(typeID, newSrc, newTgt);
        }
    }
}
