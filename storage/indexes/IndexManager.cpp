#include "IndexManager.h"

#include "ArcManager.h"
#include "indexes/PropertyHashIndex.h"

#include "metadata/SupportedType.h"

using namespace db;

template <SupportedType P>
WeakArc<Index> IndexManager::createNodeIndex(PropertyTypeID ptID, LabelSetID lblSet) {
    const WeakArc<Index> newIndex = _indexes->takeOwnership(new PropertyHashIndex<P, NodeID>);
    const Index* raw =newIndex.get();

    // Get the map for nodes of this label set, to register this index
    PropertyIndexMap thisLblSetMap = _nodeIndexes[lblSet];

    // Register this index, at this property type, at this label set
    thisLblSetMap.emplace(ptID, raw);

    return newIndex;
}

template <SupportedType P>
WeakArc<Index> IndexManager::createEdgeIndex(PropertyTypeID ptID, EdgeTypeID type) {
    const WeakArc<Index> newIndex = _indexes->takeOwnership(new PropertyHashIndex<P, EdgeID>);
    const Index* raw = newIndex.get();

    // Get the map for nodes of this label set, to register this index
    PropertyIndexMap thisLblSetMap = _edgeIndexes[type];

    // Register this index, at this property type, at this label set
    thisLblSetMap.emplace(ptID, raw);

    return newIndex;
}
