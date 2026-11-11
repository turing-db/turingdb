#include "DataPartBuilder.h"

#include <spdlog/fmt/fmt.h>

#include "Graph.h"
#include "ID.h"
#include "datapart/DataPart.h"
#include "metadata/LabelSetHandle.h"
#include "metadata/PropertyType.h"
#include "properties/PropertyManager.h"
#include "reader/GraphReader.h"
#include "versioning/EmbeddingDimensionChecker.h"
#include "versioning/VersionControlException.h"
#include "writers/MetadataBuilder.h"

using namespace db;

DataPartBuilder::~DataPartBuilder() = default;

std::unique_ptr<DataPartBuilder> DataPartBuilder::prepare(MetadataBuilder& metadata,
                                                          const GraphView& view,
                                                          size_t partIndex) {
    const GraphReader reader = view.read();

    return create(metadata,
                  view,
                  reader.getTotalNodesAllocated(),
                  reader.getTotalEdgesAllocated(),
                  partIndex);
}

std::unique_ptr<DataPartBuilder> DataPartBuilder::prepareMerge(MetadataBuilder& metadata,
                                                               const GraphView& view) {
    return create(metadata, view, 0, 0, 0);
}

std::unique_ptr<DataPartBuilder> DataPartBuilder::create(MetadataBuilder& metadata,
                                                         const GraphView& view,
                                                         const size_t nodeCount,
                                                         const size_t edgeCount,
                                                         size_t partIndex) {
    auto* ptr = new DataPartBuilder();

    ptr->_metadata = &metadata;
    ptr->_view = view;
    ptr->_firstNodeID = nodeCount;
    ptr->_firstEdgeID = edgeCount;
    ptr->_nextNodeID = ptr->_firstNodeID;
    ptr->_nextEdgeID = ptr->_firstEdgeID;
    ptr->_nodeProperties = std::make_unique<PropertyManager>();
    ptr->_edgeProperties = std::make_unique<PropertyManager>();
    ptr->_partIndex = partIndex;

    return std::unique_ptr<DataPartBuilder> {ptr};
}

NodeID DataPartBuilder::addNode(const LabelSetHandle& labelset) {
    if (!labelset.isStored()) {
        const LabelSet toBeStored = LabelSet::fromIntegers(labelset.integers());
        LabelSetHandle stored = _metadata->getOrCreateLabelSet(toBeStored);
        _coreNodeLabelSets.emplace_back(stored);
    } else {
        _coreNodeLabelSets.emplace_back(labelset);
    }

    return _nextNodeID++;
}

NodeID DataPartBuilder::addNode(const LabelSet& labelset) {
    LabelSetHandle ref = _metadata->getOrCreateLabelSet(labelset);
    _coreNodeLabelSets.emplace_back(ref);

    return _nextNodeID++;
}

template <SupportedType T>
void DataPartBuilder::addNodeProperty(NodeID nodeID,
                                      PropertyTypeID ptID,
                                      std::optional<typename T::Primitive>&& value) {
    throwIfUnknownNode(nodeID);

    if (!_nodeProperties->hasPropertyType(ptID)) {
        _nodeProperties->registerPropertyType<T>(ptID);
    }

    if (nodeID < _firstNodeID) {
        _patchNodeLabelSets.emplace(nodeID, LabelSetHandle {});
    }
    _nodeProperties->add<T>(ptID, nodeID.getValue(), std::move(value));
}

template <SupportedType T>
void DataPartBuilder::addEdgeProperty(const EdgeRecord& edge,
                                      PropertyTypeID ptID,
                                      std::optional<typename T::Primitive>&& value,
                                      LabelSetHandle srcLblSet/*={}*/) {
    throwIfUnknownEdge(edge);

    // If the property does not exist in this DP, create it
    if (!_edgeProperties->hasPropertyType(ptID)) {
        _edgeProperties->registerPropertyType<T>(ptID);
    }
    // If the edge being assigned a property existed before this DP, it is a patch
    if (edge._edgeID < _firstEdgeID) {
        _patchedEdges.emplace(edge._edgeID, edge);
    }
    // If the src node of the edge being assigned existed before this DP, it is a patch
    // NOTE: If this node is patch, @param srcLblSet will be default-invalid-initialised,
    // this gets updated in @ref DataPart::load to its actual value.
    if (edge._nodeID < _firstNodeID) {
        _patchNodeLabelSets.emplace(edge._nodeID, srcLblSet);
    }
    _edgeProperties->add<T>(ptID, edge._edgeID.getValue(), std::move(value));
}

template <SupportedType T, TypedInternalID I>
bool DataPartBuilder::hasProperty(I id, PropertyTypeID pid) {
    constexpr bool isNode = std::is_same_v<I, NodeID>;
    const PropertyManager* propertyManager =
        isNode ? _nodeProperties.get() : _edgeProperties.get();
    const EntitiesByProperty& nullsWithoutContainer =
        isNode ? _nodeNullsWithoutContainer : _edgeNullsWithoutContainer;

    const auto nulls = nullsWithoutContainer.find(pid);
    const bool nullWithoutContainer = nulls != nullsWithoutContainer.end() && nulls->second.contains(id.getValue());
    if (nullWithoutContainer) {
        return true;
    }

    const auto maybeProp = propertyManager->tryGetWithNull<T>(pid, id.getValue());
    const bool explicitNull = !maybeProp.has_value();

    // Explicit null still means that this property has been registered: return true
    return explicitNull or maybeProp.value() != nullptr;
}

const EdgeRecord& DataPartBuilder::addEdge(EdgeTypeID typeID, NodeID srcID, NodeID tgtID) {
    throwIfUnknownNode(srcID);
    throwIfUnknownNode(tgtID);

    auto& edge = _edges.emplace_back();
    edge._edgeID = _nextEdgeID;
    edge._nodeID = srcID;
    edge._otherID = tgtID;
    edge._edgeTypeID = typeID;

    if (edge._nodeID < _firstNodeID) {
        _nodeHasPatchEdges.emplace(edge._nodeID);
        _patchNodeLabelSets.emplace(edge._nodeID, LabelSetHandle {});
        _outPatchEdgeCount += 1;
    }

    if (edge._otherID < _firstNodeID) {
        _nodeHasPatchEdges.emplace(edge._otherID);
        _patchNodeLabelSets.emplace(edge._otherID, LabelSetHandle {});
        _inPatchEdgeCount += 1;
    }

    ++_nextEdgeID;
    return edge;
}

void DataPartBuilder::throwIfUnknownNode(NodeID nodeID) const {
    if (nodeID >= _nextNodeID) {
        throw VersionControlException(fmt::format("Node {} does not exist", nodeID.getValue()));
    }
}

void DataPartBuilder::throwIfUnknownEdge(EdgeID edgeID) const {
    if (edgeID >= _nextEdgeID) {
        throw VersionControlException(fmt::format("Edge {} does not exist", edgeID.getValue()));
    }
}

void DataPartBuilder::throwIfUnknownEdge(const EdgeRecord& edge) const {
    throwIfUnknownEdge(edge._edgeID);
    throwIfUnknownNode(edge._nodeID);
}

template <>
void DataPartBuilder::addNodeProperty<types::Embedding>(NodeID nodeID,
                                                        PropertyTypeID ptID,
                                                        std::optional<types::Embedding::Primitive>&& value) {
    throwIfUnknownNode(nodeID);

    if (!registerEmbeddingWrite(*_nodeProperties, ptID, value)) {
        _nodeNullsWithoutContainer[ptID].insert(nodeID.getValue());
        return;
    }

    if (nodeID < _firstNodeID) {
        _patchNodeLabelSets.emplace(nodeID, LabelSetHandle {});
    }
    _nodeProperties->add<types::Embedding>(ptID, nodeID.getValue(), value);
}

template <>
void DataPartBuilder::addEdgeProperty<types::Embedding>(const EdgeRecord& edge,
                                                        PropertyTypeID ptID,
                                                        std::optional<types::Embedding::Primitive>&& value,
                                                        LabelSetHandle srcLblSet/*={}*/) {
    throwIfUnknownEdge(edge);

    if (!registerEmbeddingWrite(*_edgeProperties, ptID, value)) {
        _edgeNullsWithoutContainer[ptID].insert(edge._edgeID.getValue());
        return;
    }

    if (edge._edgeID < _firstEdgeID) {
        _patchedEdges.emplace(edge._edgeID, edge);
        _patchNodeLabelSets.emplace(edge._nodeID, srcLblSet);
    }
    _edgeProperties->add<types::Embedding>(ptID, edge._edgeID.getValue(), value);
}

template <SupportedType T>
requires std::same_as<T, types::List> || std::same_as<T, types::Map>
void DataPartBuilder::addNodeProperty(NodeID nodeID,
                                      PropertyTypeID ptID,
                                      std::optional<typename T::OwningPrimitive>&& value) {
    throwIfUnknownNode(nodeID);

    if (!_nodeProperties->hasPropertyType(ptID)) {
        _nodeProperties->registerPropertyType<T>(ptID);
    }

    if (nodeID < _firstNodeID) {
        _patchNodeLabelSets.emplace(nodeID, LabelSetHandle {});
    }

    if (!value.has_value()) {
        _nodeProperties->add<T>(ptID, nodeID.getValue(), std::optional<typename T::Primitive> {});
        return;
    }

    _nodeProperties->add<T>(ptID, nodeID.getValue(), *value);
}

template <SupportedType T>
requires std::same_as<T, types::List> || std::same_as<T, types::Map>
void DataPartBuilder::addEdgeProperty(const EdgeRecord& edge,
                                      PropertyTypeID ptID,
                                      std::optional<typename T::OwningPrimitive>&& value,
                                      LabelSetHandle srcLblSet/*={}*/) {
    throwIfUnknownEdge(edge);

    if (!_edgeProperties->hasPropertyType(ptID)) {
        _edgeProperties->registerPropertyType<T>(ptID);
    }

    if (edge._edgeID < _firstEdgeID) {
        _patchedEdges.emplace(edge._edgeID, edge);
    }

    if (edge._nodeID < _firstNodeID) {
        _patchNodeLabelSets.emplace(edge._nodeID, srcLblSet);
    }

    if (!value.has_value()) {
        _edgeProperties->add<T>(ptID, edge._edgeID.getValue(), std::optional<typename T::Primitive> {});
        return;
    }

    _edgeProperties->add<T>(ptID, edge._edgeID.getValue(), *value);
}

template void DataPartBuilder::addNodeProperty<types::List>(NodeID,
                                                            PropertyTypeID,
                                                            std::optional<types::List::OwningPrimitive>&&);
template void DataPartBuilder::addEdgeProperty<types::List>(const EdgeRecord&,
                                                            PropertyTypeID,
                                                            std::optional<types::List::OwningPrimitive>&&,
                                                            LabelSetHandle);
template void DataPartBuilder::addNodeProperty<types::Map>(NodeID,
                                                           PropertyTypeID,
                                                           std::optional<types::Map::OwningPrimitive>&&);
template void DataPartBuilder::addEdgeProperty<types::Map>(const EdgeRecord&,
                                                           PropertyTypeID,
                                                           std::optional<types::Map::OwningPrimitive>&&,
                                                           LabelSetHandle);

std::optional<size_t> DataPartBuilder::findEmbeddingDimension(PropertyTypeID ptID) const {
    const GraphReader reader = _view.read();

    return _nodeProperties->findEmbeddingDimension(ptID)
        .or_else([&] { return _edgeProperties->findEmbeddingDimension(ptID); })
        .or_else([&] { return reader.findEmbeddingDimension(ptID); });
}

bool DataPartBuilder::registerEmbeddingWrite(PropertyManager& properties,
                                             PropertyTypeID ptID,
                                             const std::optional<types::Embedding::Primitive>& value) {
    const std::optional<size_t> heldDimension = findEmbeddingDimension(ptID);
    const std::optional<size_t> dimension = value.has_value() ? value->size() : heldDimension;

    if (!dimension.has_value()) {
        return false;
    } else if (heldDimension.has_value() && *heldDimension != *dimension) {
        const std::optional<std::string_view> name = _metadata->findPropertyTypeName(ptID);
        EmbeddingDimensionChecker::throwDimensionError(name.value_or(""), *heldDimension, *dimension);
    }

    if (!properties.hasPropertyType(ptID)) {
        properties.registerEmbeddingPropertyType(ptID, *dimension);
    }

    return true;
}

template bool DataPartBuilder::hasProperty<types::Embedding>(NodeID id, PropertyTypeID pid);
template bool DataPartBuilder::hasProperty<types::Embedding>(EdgeID id, PropertyTypeID pid);

#define INSTANTIATE(PType)                                                   \
    template void DataPartBuilder::addNodeProperty<PType>(NodeID,            \
                                                          PropertyTypeID,    \
                                                          std::optional<PType::Primitive>&&); \
    template void DataPartBuilder::addEdgeProperty<PType>(const EdgeRecord&, \
                                                          PropertyTypeID,    \
                                                          std::optional<PType::Primitive>&&,  \
                                                          LabelSetHandle);   \
    template bool DataPartBuilder::hasProperty<PType>(NodeID id, PropertyTypeID pid);             \
    template bool DataPartBuilder::hasProperty<PType>(EdgeID id, PropertyTypeID pid);             \

INSTANTIATE(types::Int64);
INSTANTIATE(types::UInt64);
INSTANTIATE(types::Double);
INSTANTIATE(types::String);
INSTANTIATE(types::Bool);
INSTANTIATE(types::List);
INSTANTIATE(types::DateTime);
INSTANTIATE(types::Duration);
INSTANTIATE(types::Point);
INSTANTIATE(types::Map);
