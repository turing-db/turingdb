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
WeakArc<Index> IndexManager::createNodeIndex(std::string_view indexName,
                                             PropertyTypeID ptID) {
    bioassert(_indexes, "Null index manager.");
    Index* raw = new PropertyHashIndex<P, NodeID>(indexName, ptID);
    const WeakArc<Index> newIndex = _indexes->takeOwnership(raw);

    _nodeIndexes.push_back(newIndex);

    return newIndex;
}

template <SupportedType P>
WeakArc<Index> IndexManager::createEdgeIndex(std::string_view indexName,
                                             PropertyTypeID ptID) {
    bioassert(_indexes, "Null index manager.");
    Index* raw = new PropertyHashIndex<P, EdgeID>(indexName, ptID);
    const WeakArc<Index> newIndex = _indexes->takeOwnership(raw);

    _edgeIndexes.push_back(newIndex);

    return newIndex;
}

namespace db {
template WeakArc<Index> IndexManager::createNodeIndex<types::Int64>(std::string_view indexName, PropertyTypeID ptID);
template WeakArc<Index> IndexManager::createNodeIndex<types::UInt64>(std::string_view indexName, PropertyTypeID ptID);
template WeakArc<Index> IndexManager::createNodeIndex<types::Double>(std::string_view indexName, PropertyTypeID ptID);
template WeakArc<Index> IndexManager::createNodeIndex<types::String>(std::string_view indexName, PropertyTypeID ptID);
template WeakArc<Index> IndexManager::createNodeIndex<types::Bool>(std::string_view indexName, PropertyTypeID ptID);
template WeakArc<Index> IndexManager::createNodeIndex<types::Embedding>(std::string_view indexName, PropertyTypeID ptID);

template WeakArc<Index> IndexManager::createEdgeIndex<types::Int64>(std::string_view indexName, PropertyTypeID ptID);
template WeakArc<Index> IndexManager::createEdgeIndex<types::UInt64>(std::string_view indexName, PropertyTypeID ptID);
template WeakArc<Index> IndexManager::createEdgeIndex<types::Double>(std::string_view indexName, PropertyTypeID ptID);
template WeakArc<Index> IndexManager::createEdgeIndex<types::String>(std::string_view indexName, PropertyTypeID ptID);
template WeakArc<Index> IndexManager::createEdgeIndex<types::Bool>(std::string_view indexName, PropertyTypeID ptID);
template WeakArc<Index> IndexManager::createEdgeIndex<types::Embedding>(std::string_view indexName, PropertyTypeID ptID);
}
