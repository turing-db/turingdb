#include "DataPartBuilder.h"

#include <range/v3/view/enumerate.hpp>

#include "Graph.h"
#include "GraphMetadata.h"
#include "DataPartManager.h"
#include "views/GraphView.h"
#include "properties/PropertyManager.h"

using namespace db;

DataPartBuilder::DataPartBuilder(EntityID firstNodeID,
                                 EntityID firstEdgeID,
                                 Graph* graph)
    : _firstNodeID(firstNodeID),
      _firstEdgeID(firstEdgeID),
      _nextNodeID(firstNodeID),
      _nextEdgeID(firstEdgeID),
      _graph(graph),
      _nodeProperties(new PropertyManager(graph->getMetadata())),
      _edgeProperties(new PropertyManager(graph->getMetadata()))
{
}

DataPartBuilder::~DataPartBuilder() = default;

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

void DataPartBuilder::commit(JobSystem& jobSystem) {
    const size_t nodes = nodeCount();
    const size_t edges = edgeCount();

    const auto [firstNodeID, firstEdgeID] = _graph->allocIDRange(nodes, edges);
    std::unique_ptr<DataPart> part = std::make_unique<DataPart>(firstNodeID, firstEdgeID);

    part->load(_graph->view(), jobSystem, *this);
    _graph->_parts->store(std::move(part));
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
