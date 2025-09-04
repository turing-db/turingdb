#include "DataPartModifier.h"

#include "ArcManager.h"
#include "DataPart.h"
#include "versioning/NodeContainerModifier.h"

using namespace db;

void DataPartModifier::prepare() {
    // 1. Work out which edges need to be deleted

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
}

void DataPartModifier::deleteNodes() {
    // TODO: Work out NodeID mapping function

    _newDP->_nodes = NodeContainerModifier::deleteNodes(_oldDP->nodes(), _nodesToDelete);

    // TODO: Update EdgeContainer (delete edges where an incident node has been deleted)
    // TODO: Update PropertyManager
    // TODO: Update StringIndex
}
