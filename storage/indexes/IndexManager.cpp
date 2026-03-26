#include "IndexManager.h"

#include <memory>

#include "ArcManager.h"
#include "indexes/PropertyHashIndex.h"

#include "metadata/SupportedType.h"

using namespace db;

IndexManager::IndexManager()
    : _indexes(std::make_unique<ArcManager<Index>>())
{
}

IndexManager::~IndexManager() {
    _indexes.reset();
}

template <SupportedType P>
WeakArc<Index> IndexManager::createNodeIndex(PropertyTypeID ptID, LabelSetID lblSet) {
    const WeakArc<Index> newIndex = _indexes->takeOwnership(new PropertyHashIndex<P, NodeID>(ptID));
    const Index* raw = newIndex.get();

    // Get the map for nodes of this label set, to register this index
    PropertyIndexMap thisLblSetMap = _nodeIndexes[lblSet];

    // Register this index, at this property type, at this label set
    thisLblSetMap.emplace(ptID, raw);

    return newIndex;
}

/*
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
*/

namespace db {
template WeakArc<Index> IndexManager::createNodeIndex<types::Int64>(PropertyTypeID ptID, LabelSetID lblset);
template WeakArc<Index> IndexManager::createNodeIndex<types::UInt64>(PropertyTypeID ptID, LabelSetID lblset);
template WeakArc<Index> IndexManager::createNodeIndex<types::Double>(PropertyTypeID ptID, LabelSetID lblset);
template WeakArc<Index> IndexManager::createNodeIndex<types::String>(PropertyTypeID ptID, LabelSetID lblset);
template WeakArc<Index> IndexManager::createNodeIndex<types::Bool>(PropertyTypeID ptID, LabelSetID lblset);
template WeakArc<Index> IndexManager::createNodeIndex<types::Embedding>(PropertyTypeID ptID, LabelSetID lblset);
}
