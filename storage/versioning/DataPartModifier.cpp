#include "DataPartModifier.h"

#include "ArcManager.h"
#include "DataPart.h"
#include "TuringException.h"
#include "writers/DataPartBuilder.h"

using namespace db;

WeakArc<DataPart> DataPartModifier::deleteNode(const NodeID idToDelete, const WeakArc<DataPart> dp) {
    if (idToDelete < dp->getFirstNodeID() || idToDelete > dp->getFirstNodeID() + dp->getNodeCount()) {
        throw TuringException(
            fmt::format("Node {} is not in provided datapart", idToDelete));
    }


    return {};
}
