#include "DataPartModifier.h"

#include "ArcManager.h"
#include "DataPart.h"
#include "versioning/NodeContainerModifier.h"

using namespace db;

void DataPartModifier::deleteNodes(const WeakArc<DataPart> oldDP, WeakArc<DataPart> newDP,
                                  const std::set<NodeID> toDelete) {
    // TODO: Work out NodeID mapping function

    // The only nodes whose ID changes are those which have an ID greater than a deleted
    // node, as whenever a node is deleted, all subsequent nodes need to have their ID
    // reduced by 1 to fill the gap left by the deleted node. Thus, the new ID of a node,
    // x, is equal to the number of deleted nodes which have an ID smaller than x.
    [[maybe_unused]] auto nodeIDMapping = [&toDelete](NodeID x) {
        auto smallerDeletedNodes = std::ranges::lower_bound(toDelete, x);
        size_t numSmallerDeletedNodes = std::distance(smallerDeletedNodes, toDelete.cbegin());
        return x - numSmallerDeletedNodes;
    };

    newDP->_nodes = NodeContainerModifier::deleteNodes(oldDP->nodes(), toDelete);

    // TODO: Update EdgeContainer (delete edges where an incident node has been deleted)
    // TODO: Update PropertyManager
    // TODO: Update StringIndex

    return;
}
