#pragma once

#include <span>

#include "ArcManager.h"
#include "ID.h"
#include "writers/DataPartBuilder.h"

namespace db {

class DataPart;

class DataPartModifier {
public:
    DataPartModifier(const WeakArc<DataPart> oldDP, DataPartBuilder& builder,
                         std::span<NodeID> nodesToDelete, std::span<EdgeID> edgesToDelete)
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

    DataPartBuilder& builder() { return *_builder; }

    void applyModifications();

private:
    const WeakArc<DataPart> _oldDP;
    DataPartBuilder* _builder;

    std::span<NodeID> _nodesToDelete;
    std::span<EdgeID> _edgesToDelete;
};
    
}

