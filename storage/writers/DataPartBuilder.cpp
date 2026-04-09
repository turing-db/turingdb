#include "DataPartBuilder.h"

#include "Graph.h"
#include "ID.h"
#include "metadata/LabelSetHandle.h"
#include "metadata/PropertyType.h"
#include "properties/PropertyManager.h"
#include "writers/MetadataBuilder.h"

using namespace db;

DataPartBuilder::~DataPartBuilder() = default;

std::unique_ptr<DataPartBuilder> DataPartBuilder::prepare(MetadataBuilder& metadata,
                                                          const size_t nodeCount,
                                                          const size_t edgeCount,
                                                          size_t partIndex) {
    auto* ptr = new DataPartBuilder();

    ptr->_metadata = &metadata;
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
    PropertyManager const* propertyManager {nullptr};

    if constexpr (std::is_same_v<I, NodeID>) {
        propertyManager = _nodeProperties.get();
    } else {
        propertyManager = _edgeProperties.get();
    }

    return propertyManager->tryGet<T>(pid, id.getValue());
}

const EdgeRecord& DataPartBuilder::addEdge(EdgeTypeID typeID, NodeID srcID, NodeID tgtID) {
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

template <>
void DataPartBuilder::addNodeProperty<types::Embedding>(NodeID nodeID,
                                                        PropertyTypeID ptID,
                                                        std::optional<types::Embedding::Primitive>&& value) {
    if (!_nodeProperties->hasPropertyType(ptID)) {
        bioassert(value.has_value(), "Null embedding on register.");
        _nodeProperties->registerEmbeddingPropertyType(ptID, value->size());
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
    if (!_edgeProperties->hasPropertyType(ptID)) {
        bioassert(value.has_value(), "Null embedding on register.");
        _edgeProperties->registerEmbeddingPropertyType(ptID, value->size());
    }
    if (edge._edgeID < _firstEdgeID) {
        _patchedEdges.emplace(edge._edgeID, edge);
        _patchNodeLabelSets.emplace(edge._nodeID, srcLblSet);
    }
    _edgeProperties->add<types::Embedding>(ptID, edge._edgeID.getValue(), value);
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
