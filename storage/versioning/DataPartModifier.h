#pragma once

#include <set>

#include "ArcManager.h"
#include "DataPart.h"
#include "Graph.h"
#include "VersionController.h"
#include "ID.h"
#include "TuringException.h"
#include "EdgeContainer.h"

namespace db {

class DataPartModifier {
public:
    DataPartModifier(const DataPartModifier&) = delete;
    DataPartModifier& operator=(const DataPartModifier&) = delete;
    DataPartModifier(DataPartModifier&&) = delete;
    DataPartModifier& operator=(DataPartModifier&&) = delete;

    ~DataPartModifier() = default;

    // this is for the purpose of testing in a sample - will integrate into commit
    // modifier later
    [[nodiscard]] static WeakArc<DataPart> newDPinGraph(Graph* g) {
        return g->_versionController->createDataPart(0, 0);
    }

    void applyDeletions();

    // this is for the purpose of testing in a sample - will integrate into commit
    // modifier later
    [[nodiscard]] static std::unique_ptr<DataPartModifier> create(const WeakArc<DataPart> oldDP,
                                                  WeakArc<DataPart> newDP,
                                                  std::set<NodeID> nodesToDelete,
                                                  std::set<EdgeID> edgesToDelete) {
        // private constructor so make raw then make unique
        auto raw = new DataPartModifier {oldDP, newDP, nodesToDelete, edgesToDelete};
        return std::unique_ptr<DataPartModifier>(raw);
    }

private:
    const WeakArc<DataPart> _oldDP {}; // nullptr
    WeakArc<DataPart> _newDP {};       // nullptr

    std::unique_ptr<NodeContainer> _newNodeCont {};
    std::unique_ptr<EdgeContainer> _newEdgeCont {};

    std::set<NodeID> _nodesToDelete;
    std::set<EdgeID> _edgesToDelete;

    std::function<NodeID(NodeID)> _nodeIDMapping {[](NodeID x) -> NodeID {
        throw TuringException("Attempted to use uninitialised _nodeIDMapping.");
    }};

    std::function<EdgeID(EdgeID)> _edgeIDMapping {[](EdgeID x) -> EdgeID {
        throw TuringException("Attempted to use uninitialised _edgeIDMapping.");
    }};

    void prepare();

    /**
     * @brief Modifies @ref newDP in place to remove all nodes with NodeIDs specified in
     * @ref _nodesToDelete
     */
    void deleteNodes();

    /**
     * @brief Modifies @ref newDP in place to remove all nodes with NodeIDs specified in
     * @ref _edgesToDelete
     */
    void deleteEdges();

    void assembleNewPart();

    /**
     * @brief Identifies edges that must be deleted due to one of their incident nodes
     *        being deleted, and appends those EdgeIDs to @ref _edgesToDelete.
     */
    void detectHangingEdges();

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
