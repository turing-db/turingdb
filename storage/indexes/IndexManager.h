#pragma once

#include <memory>
#include <unordered_map>

#include "ArcManager.h"
#include "ID.h"
#include "Index.h"

#include "metadata/SupportedType.h"

namespace db {

class IndexManager {
public:
    // TODO: Ensure property type IDs are rebased
    using PropertyIndexMap = std::unordered_map<PropertyTypeID, const Index*>;

    // TODO: Ensure IDs are rebased
    using NodeIndexMap = std::unordered_map<LabelSetID, PropertyIndexMap>;
    using EdgeIndexMap = std::unordered_map<EdgeTypeID, PropertyIndexMap>;


    template <SupportedType P>
    WeakArc<Index> createNodeIndex(PropertyTypeID ptID,
                                   LabelSetID lblset = _unconstrainedLabels);

    template <SupportedType P>
    WeakArc<Index> createEdgeIndex(PropertyTypeID ptID,
                                   EdgeTypeID type = _unconstrainedType);

private:
    NodeIndexMap _nodeIndexes;
    EdgeIndexMap _edgeIndexes;

    std::unique_ptr<ArcManager<Index>> _indexes;

    // Use the invalid labelset/edge ID to denote an index on a node/edge with any
    // labels/type
    constexpr static LabelSetID _unconstrainedLabels = LabelSetID::max();
    constexpr static EdgeTypeID _unconstrainedType = EdgeTypeID::max();
};

}
