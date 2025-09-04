#include "DataPartModifier.h"

#include <cstdint>
#include <range/v3/view/enumerate.hpp>
#include <range/v3/view/iota.hpp>

#include "ArcManager.h"
#include "DataPart.h"
#include "EdgeContainer.h"
#include "versioning/NodeContainerModifier.h"
#include "NodeContainer.h"

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
    prepare();
    deleteNodes();
}

void DataPartModifier::prepare() {
    // 1. Work out which edges need to be deleted as a result of one of their nodes
    // being deleted
    detectHangingEdges();

    // 2. Calculate NodeID mapping function

    // The only nodes whose ID changes are those which have an ID greater than a
    // deleted node, as whenever a node is deleted, all subsequent nodes need to have
    // their ID reduced by 1 to fill the gap left by the deleted node. Thus, the new
    // ID of a node, x, is equal to the number of deleted nodes which have an ID
    // smaller than x.
    _nodeIDMapping = [this](NodeID x) {
        auto smallerDeletedNodesIt = std::ranges::lower_bound(_nodesToDelete, x);
        size_t numSmallerDeletedNodes =
            std::distance(smallerDeletedNodesIt, _nodesToDelete.cbegin());
        return x - numSmallerDeletedNodes;
    };

    // 3. Calculate EdgeID mapping function

    // Same logic as nodes above
    _edgeIDMapping = [this](EdgeID x) {
        auto smallerDeletedEdgesIt = std::ranges::lower_bound(_edgesToDelete, x);
        size_t numSmallerDeletedEdges =
            std::distance(smallerDeletedEdgesIt, _edgesToDelete.cbegin());
        return x - numSmallerDeletedEdges;
    };
}

void DataPartModifier::detectHangingEdges() {
    const auto& oldEdgeContainer = _oldDP->edges();
    const uint64_t oldFirstEdgeID = oldEdgeContainer.getFirstEdgeID().getValue();

    // Get [edgeId, edgeRecord] pairs starting from the first edgeID, check the record to
    // see if the source or target node of the edge is to be deleted. If it is, this edge
    // also needs to be deleted.
    for (const auto& [edgeID, edgeRecord] :
         enumerate_from(oldFirstEdgeID, oldEdgeContainer.getIns())) {

        if (_nodesToDelete.contains(edgeRecord._nodeID)) {
            _edgesToDelete.emplace(edgeID);
        }
        if (_nodesToDelete.contains(edgeRecord._otherID)) {
            _edgesToDelete.emplace(edgeID);
        }
    }
}

void DataPartModifier::deleteNodes() {
    _newDP->_nodes = NodeContainerModifier::deleteNodes(_oldDP->nodes(), _nodesToDelete);

    // TODO: Update EdgeContainer (delete edges where an incident node has been deleted)
    // TODO: Update PropertyManager
    // TODO: Update StringIndex
}
