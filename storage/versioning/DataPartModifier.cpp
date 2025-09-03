#include "DataPartModifier.h"

#include "ArcManager.h"
#include "DataPart.h"
#include "versioning/NodeContainerModifier.h"

using namespace db;

void DataPartModifier::deleteNodes(const WeakArc<DataPart> oldDP, WeakArc<DataPart> newDP,
                                  const std::set<NodeID> toDelete) {
    // TODO: Work out NodeID mapping function
    newDP->_nodes = NodeContainerModifier::deleteNodes(oldDP->nodes(), toDelete);

    // TODO: Update EdgeContainer (delete edges where an incident node has been deleted)
    // TODO: Update PropertyManager
    // TODO: Update StringIndex

    return;
}
