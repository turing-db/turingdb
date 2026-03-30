#include "IndexManager.h"

#include <memory>
#include <string_view>

#include "ArcManager.h"
#include "indexes/PropertyHashIndex.h"

#include "metadata/SupportedType.h"

#include "versioning/VersionControlException.h"
#include "FatalException.h"

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

    const bool alreadyIndexed = thisLblSetMap.find(ptID) != end(thisLblSetMap);
    if (alreadyIndexed) {
        throw VersionControlException(fmt::format(
            "Failed to create index '{}'; PropertyType {} is already indexed.", indexName,
            ptID.getValue()));
    }

    // Register this index, at this property type, at this label set
    const auto& [_, success] = thisLblSetMap.emplace(ptID, raw);
    if (!success) {
        throw FatalException("Failed to create index.");
    }

    return newIndex;
}

template <SupportedType P>
WeakArc<Index> IndexManager::createEdgeIndex(std::string_view indexName,
                                             PropertyTypeID ptID,
                                             EdgeTypeID edgeType) {
    Index* raw = new PropertyHashIndex<P, EdgeID>(indexName, ptID);
    const WeakArc<Index> newIndex = _indexes->takeOwnership(raw);

    PropertyIndexMap thisTypeMap = _edgeIndexes[edgeType];

    const bool alreadyIndexed = thisTypeMap.find(ptID) != end(thisTypeMap);
    if (alreadyIndexed) {
        throw VersionControlException(fmt::format(
            "Failed to create index '{}'; PropertyType {} is already indexed.", indexName,
            ptID.getValue()));
    }

    const auto& [_, success] = thisTypeMap.emplace(ptID, raw);
    if (!success) {
        throw FatalException("Failed to create index.");
    }

    return newIndex;
}

namespace db {
template WeakArc<Index> IndexManager::createNodeIndex<types::Int64>(std::string_view indexName, PropertyTypeID ptID, LabelSetID lblset);
template WeakArc<Index> IndexManager::createNodeIndex<types::UInt64>(std::string_view indexName, PropertyTypeID ptID, LabelSetID lblset);
template WeakArc<Index> IndexManager::createNodeIndex<types::Double>(std::string_view indexName, PropertyTypeID ptID, LabelSetID lblset);
template WeakArc<Index> IndexManager::createNodeIndex<types::String>(std::string_view indexName, PropertyTypeID ptID, LabelSetID lblset);
template WeakArc<Index> IndexManager::createNodeIndex<types::Bool>(std::string_view indexName, PropertyTypeID ptID, LabelSetID lblset);
template WeakArc<Index> IndexManager::createNodeIndex<types::Embedding>(std::string_view indexName, PropertyTypeID ptID, LabelSetID lblset);

template WeakArc<Index> IndexManager::createEdgeIndex<types::Int64>(std::string_view indexName, PropertyTypeID ptID, EdgeTypeID edgeType);
template WeakArc<Index> IndexManager::createEdgeIndex<types::UInt64>(std::string_view indexName, PropertyTypeID ptID, EdgeTypeID edgeType);
template WeakArc<Index> IndexManager::createEdgeIndex<types::Double>(std::string_view indexName, PropertyTypeID ptID, EdgeTypeID edgeType);
template WeakArc<Index> IndexManager::createEdgeIndex<types::String>(std::string_view indexName, PropertyTypeID ptID, EdgeTypeID edgeType);
template WeakArc<Index> IndexManager::createEdgeIndex<types::Bool>(std::string_view indexName, PropertyTypeID ptID, EdgeTypeID edgeType);
template WeakArc<Index> IndexManager::createEdgeIndex<types::Embedding>(std::string_view indexName, PropertyTypeID ptID, EdgeTypeID edgeType);
}
