#pragma once

#include <set>

#include "ArcManager.h"
#include "DataPart.h"
#include "Graph.h"
#include "ID.h"
#include "TuringException.h"
#include "EdgeContainer.h"
#include "writers/DataPartBuilder.h"

namespace db {

class DataPartModifier {
public:
    DataPartModifier(const WeakArc<DataPart> oldDP, DataPartBuilder& builder,
                         std::set<NodeID> nodesToDelete, std::set<EdgeID> edgesToDelete)
        : _oldDP(oldDP),
        _builder(&builder),
        _nodesToDelete(nodesToDelete),
        _edgesToDelete(edgesToDelete)
    {
    }
    DataPartModifier(const DataPartModifier&) = delete;
    DataPartModifier& operator=(const DataPartModifier&) = delete;
    DataPartModifier(DataPartModifier&&) = delete;
    DataPartModifier& operator=(DataPartModifier&&) = delete;

    ~DataPartModifier() = default;

    void applyModifications() {
        prepare();
        fillBuilder();
    }

    DataPartBuilder* builder() { return _builder; }

private:
    const WeakArc<DataPart> _oldDP {}; // nullptr
    DataPartBuilder* _builder {nullptr};

    std::set<NodeID> _nodesToDelete;
    std::set<EdgeID> _edgesToDelete;

    std::function<NodeID(NodeID)> _nodeIDMapping {[](NodeID x) -> NodeID {
        throw TuringException("Attempted to use uninitialised _nodeIDMapping.");
    }};

    std::function<EdgeID(EdgeID)> _edgeIDMapping {[](EdgeID x) -> EdgeID {
        throw TuringException("Attempted to use uninitialised _edgeIDMapping.");
    }};

    void prepare();

    void fillBuilder();
    };
}
