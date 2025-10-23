#include "DataPartBuilder.h"

#include "Graph.h"
#include "ID.h"
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
                                      T::Primitive value) {
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
                                      T::Primitive value) {
    if (!_edgeProperties->hasPropertyType(ptID)) {
        _edgeProperties->registerPropertyType<T>(ptID);
    }
    if (edge._edgeID < _firstEdgeID) {
        _patchedEdges.emplace(edge._edgeID, &edge);
        _patchNodeLabelSets.emplace(edge._nodeID, LabelSetHandle {});
    }
    _edgeProperties->add<T>(ptID, edge._edgeID.getValue(), std::move(value));
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

#define INSTANTIATE(PType)                                                   \
    template void DataPartBuilder::addNodeProperty<PType>(NodeID,            \
                                                          PropertyTypeID,    \
                                                          PType::Primitive); \
    template void DataPartBuilder::addEdgeProperty<PType>(const EdgeRecord&, \
                                                          PropertyTypeID,    \
                                                          PType::Primitive);

INSTANTIATE(types::Int64);
INSTANTIATE(types::UInt64);
INSTANTIATE(types::Double);
INSTANTIATE(types::String);
INSTANTIATE(types::Bool);
