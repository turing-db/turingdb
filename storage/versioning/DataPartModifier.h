#pragma once

#include <set>

#include "ArcManager.h"
#include "DataPart.h"
#include "ID.h"
#include "TuringException.h"

namespace db {

class DataPartModifier {
public:
    DataPartModifier(const DataPartModifier&) = delete;
    DataPartModifier& operator=(const DataPartModifier&) = delete;
    DataPartModifier(DataPartModifier&&) = delete;
    DataPartModifier& operator=(DataPartModifier&&) = delete;

    ~DataPartModifier() = default;

    void applyDeletions();

    /**
     * @brief Modifies @ref newDP in place to remove all nodes with NodeIDs specified in
     * @ref toDelete
     * @detail TODO: Also updates other datastructures in DataParts to handle the updated
     * IDs of the nodes as a result of reassigning NodeIDs after deletion
     */
    void deleteNodes();

private:
    const WeakArc<DataPart> _oldDP {}; // nullptr
    WeakArc<DataPart> _newDP {};       // nullptr

    std::set<NodeID> _nodesToDelete;
    std::set<EdgeID> _edgesToDelete;

    std::function<NodeID(NodeID)> _nodeIDMapping {[](NodeID x) -> NodeID {
        throw TuringException("Attempted to use uninitialised _nodeIDMapping");
    }};

    std::function<EdgeID(EdgeID)> _edgeIDMapping {[](EdgeID x) -> EdgeID {
        throw TuringException("Attempted to use uninitialised _edgeIDMapping");
    }};

    void prepare();

    // Copy constructor WeakArc to the old datapart so the old referee still holds a
    // reference, but take ownership of WeakArc of the new datapart
    DataPartModifier(const WeakArc<DataPart> oldDP, WeakArc<DataPart> newDP,
                     std::set<NodeID> nodesToDelete, std::set<EdgeID> edgesToDelete)
        : _oldDP(oldDP),
          _newDP(std::move(newDP)),
          _nodesToDelete(nodesToDelete),
          _edgesToDelete(edgesToDelete)
      {

      }
};
}
