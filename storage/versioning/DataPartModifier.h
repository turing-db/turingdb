#pragma once

#include <set>

#include "ArcManager.h"
#include "DataPart.h"

namespace db {

class DataPartModifier {
public:
    /**
     * @brief Modifies @ref newDP in place to remove all nodes with NodeIDs specified in
     * @ref toDelete
     * @detail TODO: Also updates other datastructures in DataParts to handle the updated
     * IDs of the nodes as a result of reassigning NodeIDs after deletion
     */
    static void deleteNodes(const WeakArc<DataPart> oldDP, WeakArc<DataPart> newDP,
                     const std::set<NodeID> toDelete);
private:
};
}
