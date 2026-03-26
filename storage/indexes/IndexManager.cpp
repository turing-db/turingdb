#include "IndexManager.h"

#include <memory>
#include <string_view>

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
WeakArc<Index> IndexManager::createNodeIndex(std::string_view indexName,
                                             PropertyTypeID ptID,
                                             LabelSetID lblSet) {
    Index* raw = new PropertyHashIndex<P, NodeID>(indexName, ptID);
    const WeakArc<Index> newIndex = _indexes->takeOwnership(raw);

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
template WeakArc<Index> IndexManager::createNodeIndex<types::Int64>(std::string_view indexName, PropertyTypeID ptID, LabelSetID lblset);
template WeakArc<Index> IndexManager::createNodeIndex<types::UInt64>(std::string_view indexName, PropertyTypeID ptID, LabelSetID lblset);
template WeakArc<Index> IndexManager::createNodeIndex<types::Double>(std::string_view indexName, PropertyTypeID ptID, LabelSetID lblset);
template WeakArc<Index> IndexManager::createNodeIndex<types::String>(std::string_view indexName, PropertyTypeID ptID, LabelSetID lblset);
template WeakArc<Index> IndexManager::createNodeIndex<types::Bool>(std::string_view indexName, PropertyTypeID ptID, LabelSetID lblset);
template WeakArc<Index> IndexManager::createNodeIndex<types::Embedding>(std::string_view indexName, PropertyTypeID ptID, LabelSetID lblset);
}
