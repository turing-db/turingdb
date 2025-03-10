#include "DataPartBuilder.h"

#include <range/v3/view/enumerate.hpp>

#include "Graph.h"
#include "GraphMetadata.h"
#include "views/GraphView.h"
#include "reader/GraphReader.h"
#include "properties/PropertyManager.h"

using namespace db;

DataPartBuilder::~DataPartBuilder() = default;

std::unique_ptr<DataPartBuilder> DataPartBuilder::prepare(Graph& graph,
                                                          const GraphView& view) {
    const auto reader = view.read();
    return DataPartBuilder::prepare(graph, view, reader.getNodeCount(), reader.getEdgeCount());
}

std::unique_ptr<DataPartBuilder> DataPartBuilder::prepare(Graph& graph,
                                                          const GraphView& view,
                                                          EntityID firstNodeID,
                                                          EntityID firstEdgeID) {
    auto* ptr = new DataPartBuilder();

    ptr->_view = view;
    ptr->_graph = &graph;
    ptr->_firstNodeID = firstNodeID;
    ptr->_firstEdgeID = firstEdgeID;
    ptr->_nextNodeID = firstNodeID;
    ptr->_nextEdgeID = firstEdgeID;
    ptr->_nodeProperties = std::make_unique<PropertyManager>(graph.getMetadata());
    ptr->_edgeProperties = std::make_unique<PropertyManager>(graph.getMetadata());

    return std::unique_ptr<DataPartBuilder> {ptr};
}

EntityID DataPartBuilder::addNode(const LabelSetID& labelset) {
    _coreNodeLabelSets.emplace_back(labelset);

    return _nextNodeID++;
}

EntityID DataPartBuilder::addNode(const LabelSet& labelset) {
    const LabelSetID id = _graph->getMetadata()->labelsets().getOrCreate(labelset);
    _coreNodeLabelSets.emplace_back(id);

    return _nextNodeID++;
}

template <SupportedType T>
void DataPartBuilder::addNodeProperty(EntityID nodeID,
                                      PropertyTypeID ptID,
                                      T::Primitive value) {
    if (!_nodeProperties->hasPropertyType(ptID)) {
        _nodeProperties->registerPropertyType<T>(ptID);
    }
    if (nodeID < _firstNodeID) {
        _patchNodeLabelSets.emplace(nodeID, LabelSetID {});
    }
    _nodeProperties->add<T>(ptID, nodeID, std::move(value));
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
        _patchNodeLabelSets.emplace(edge._nodeID, LabelSetID {});
    }
    _edgeProperties->add<T>(ptID, edge._edgeID, std::move(value));
}

const EdgeRecord& DataPartBuilder::addEdge(EdgeTypeID typeID, EntityID srcID, EntityID tgtID) {
    auto& edge = _edges.emplace_back();
    edge._edgeID = _nextEdgeID;
    edge._nodeID = srcID;
    edge._otherID = tgtID;
    edge._edgeTypeID = typeID;


    if (edge._nodeID < _firstNodeID) {
        _nodeHasPatchEdges.emplace(edge._nodeID);
        _patchNodeLabelSets.emplace(edge._nodeID, LabelSetID {});
        _outPatchEdgeCount += 1;
    }

    if (edge._otherID < _firstNodeID) {
        _nodeHasPatchEdges.emplace(edge._otherID);
        _patchNodeLabelSets.emplace(edge._otherID, LabelSetID {});
        _inPatchEdgeCount += 1;
    }

    ++_nextEdgeID;
    return edge;
}

#define INSTANTIATE(PType)                                                   \
    template void DataPartBuilder::addNodeProperty<PType>(EntityID,          \
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
