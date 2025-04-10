#include "GraphReader.h"

#include "DataPart.h"
#include "NodeContainer.h"
#include "EdgeContainer.h"
#include "indexers/EdgeIndexer.h"
#include "views/EdgeView.h"
#include "properties/PropertyManager.h"

using namespace db;

size_t GraphReader::getNodeCount() const {
    size_t count = 0;
    for (const auto& part : _view.dataparts()) {
        count += part->nodes().getAll()._count;
    }
    return count;
}

size_t GraphReader::getEdgeCount() const {
    size_t count = 0;
    for (const auto& part : _view.dataparts()) {
        count += part->edges().getOuts().size();
    }
    return count;
}

LabelSetHandle GraphReader::getNodeLabelSet(NodeID nodeID) const {
    for (const auto& part : _view.dataparts()) {
        if (part->hasNode(nodeID)) {
            return part->nodes().getNodeLabelSet(nodeID);
        }
    }
    return LabelSetHandle {};
}

const EdgeRecord* GraphReader::getEdge(EdgeID edgeID) const {
    for (const auto& part : _view.dataparts()) {
        if (part->hasEdge(edgeID)) {
            return &part->edges().get(edgeID);
        }
    }
    return nullptr;
}

size_t GraphReader::getNodeCountMatchingLabelset(const LabelSetHandle& labelset) const {
    size_t count = 0;

    auto it = matchLabelSets(labelset);
    for (; it.isValid(); it.next()) {
        for (const auto& part : _view.dataparts()) {
            const auto& nodes = part->nodes();
            const LabelSetHandle& current = it.get();
            if (!nodes.hasLabelSet(current)) {
                continue;
            }

            count += part->nodes().getRange(current)._count;
        }
    }

    return count;
}

size_t GraphReader::getDatapartCount() const {
    return _view.dataparts().size();
}

size_t GraphReader::getNodePropertyCount(PropertyTypeID ptID) const {
    size_t count = 0;
    for (const auto& part : _view.dataparts()) {
        const auto& props = part->nodeProperties();
        if (props.hasPropertyType(ptID)) {
            count += props.count(ptID);
        }
    }
    return count;
}

size_t GraphReader::getNodePropertyCount(size_t datapartIndex,
                                         PropertyTypeID ptID) const {
    const auto& props = _view.dataparts()[datapartIndex]->nodeProperties();
    if (props.hasPropertyType(ptID)) {
        return props.count(ptID);
    }
    return 0;
}

NodeID GraphReader::getFinalNodeID(size_t partIndex, NodeID tmpID) const {
    // TODO Update with a new Unique-Internal ID System.
    // This implementation does not work if multiple nodes
    // have the same temporary ID
    if (partIndex >= _view.dataparts().size()) {
        return NodeID {};
    }

    const auto& part = _view.dataparts()[partIndex];
    auto it = part->_tmpToFinalNodeIDs.find(tmpID);
    if (it == part->_tmpToFinalNodeIDs.end()) {
        return NodeID {};
    }

    return it->second;
}


const GraphMetadata& GraphReader::getMetadata() const {
    return _view.metadata();
}

GetOutEdgesRange GraphReader::getOutEdges(const ColumnNodeIDs* inputNodeIDs) const {
    return {_view, inputNodeIDs};
}

GetInEdgesRange GraphReader::getInEdges(const ColumnNodeIDs* inputNodeIDs) const {
    return {_view, inputNodeIDs};
}

ScanEdgesRange GraphReader::scanOutEdges() const {
    return {_view};
}

ScanNodesRange GraphReader::scanNodes() const {
    return {_view};
}

ScanNodesByLabelRange GraphReader::scanNodesByLabel(const LabelSetHandle& labelset) const {
    return {_view, labelset};
}

ScanOutEdgesByLabelRange GraphReader::scanOutEdgesByLabel(const LabelSetHandle& labelset) const {
    return {_view, labelset};
}

ScanInEdgesByLabelRange GraphReader::scanInEdgesByLabel(const LabelSetHandle& labelset) const {
    return {_view, labelset};
}

GetNodeViewsRange GraphReader::getNodeViews(const ColumnNodeIDs* inputNodeIDs) const {
    return {_view, inputNodeIDs};
}

NodeView GraphReader::getNodeView(NodeID id) const {
    NodeView view;
    PartIterator partIt(_view);
    LabelSetHandle labelset;

    // Find definition of the node
    for (; partIt.isValid(); partIt.next()) {
        const auto* part = partIt.get();
        const NodeContainer& nodes = part->nodes();

        labelset = nodes.getNodeLabelSet(id);
        if (labelset.isValid()) {
            view._labelset = labelset;
            view._nodeID = id;
            break;
        }
    }

    if (!view.isValid()) {
        return view;
    }

    // Once we found the labelset of a node,
    // it means we found the definition of the node
    // We can start gathering properties and edges
    for (; partIt.isValid(); partIt.next()) {
        const auto* part = partIt.get();
        const EdgeIndexer& edgeIndexer = part->edgeIndexer();
        const PropertyManager& nodeProperties = part->nodeProperties();

        nodeProperties.fillEntityPropertyView(id.getValue(), labelset, view._props);
        edgeIndexer.fillEntityEdgeView(id, view._edges);
    }

    return view;
}

EdgeView GraphReader::getEdgeView(EdgeID id) const {
    EdgeView view;
    PartIterator partIt(_view);
    LabelSetHandle labelset;
    const EdgeRecord* edge {nullptr};

    // Find definition of the edge
    for (; partIt.isValid(); partIt.next()) {
        const auto* part = partIt.get();
        const EdgeContainer& edges = part->edges();
        edge = edges.tryGet(id);

        if (edge) {
            labelset = getNodeLabelSet(edge->_nodeID);
            view._edgeID = edge->_edgeID;
            view._srcID = edge->_nodeID;
            view._tgtID = edge->_otherID;
            view._typeID = edge->_edgeTypeID;
            break;
        }
    }

    if (!view.isValid()) {
        return view;
    }

    // Once we found the edge, we can start gathering its properties
    for (; partIt.isValid(); partIt.next()) {
        const auto* part = partIt.get();
        const PropertyManager& edgeProperties = part->edgeProperties();

        edgeProperties.fillEntityPropertyView(id.getValue(), labelset, view._props);
    }

    return view;
}

EdgeTypeID GraphReader::getEdgeTypeID(EdgeID edgeID) const {
    for (const auto& part : _view.dataparts()) {
        const auto* edge = part->edges().tryGet(edgeID);
        if (edge) {
            return edge->_edgeTypeID;
        }
    }
    return {};
}

MatchLabelSetIterator GraphReader::matchLabelSets(const LabelSetHandle& labelSet) const {
    return MatchLabelSetIterator(_view, labelSet);
}

bool GraphReader::nodeHasProperty(PropertyTypeID ptID, NodeID nodeID) const {
    for (const auto& part : _view.dataparts()) {
        if (part->nodeProperties().has(ptID, nodeID.getValue())) {
            return true;
        }
    }
    return false;
}

void GraphReader::getGraphProperties(ColumnVector<PropertyTypeID>* ids, ColumnVector<std::string>* name, ColumnVector<std::string>* type) const {
    const PropertyTypeMap& propTypeMap = getMetadata().propTypes();
    const std::unordered_map<std::string, size_t>& offsetMap = propTypeMap._offsetMap;

    for (const auto& entry : offsetMap) {
        name->emplace_back(entry.first);
        const auto propType = propTypeMap.get(entry.first);
        ids->emplace_back(propType._id);
        type->emplace_back(PropertyValueTypeDescription::value(propType._valueType));
    };
}

void GraphReader::getGraphLabels(ColumnVector<LabelID>* ids, ColumnVector<std::string>* name) const {
    const LabelMap& labelMap = getMetadata().labels();
    const std::unordered_map<LabelID, std::string_view>& idMap = labelMap._idMap;


    for (const auto& entry : idMap) {
        ids->emplace_back(entry.first);
        name->emplace_back(entry.second);
    };
}
void GraphReader::getGraphEdgeTypes(ColumnVector<EdgeTypeID>* ids, ColumnVector<std::string>* name) const {
    const EdgeTypeMap& edgeTypeMap = getMetadata().edgeTypes();
    const std::unordered_map<EdgeTypeID, std::string_view>& idMap = edgeTypeMap._idMap;


    for (const auto& entry : idMap) {
        ids->emplace_back(entry.first);
        name->emplace_back(entry.second);
    };
}

template <SupportedType T>
const T::Primitive* GraphReader::tryGetNodeProperty(PropertyTypeID ptID, NodeID nodeID) const {
    for (const auto& part : _view.dataparts()) {
        const auto* p = part->nodeProperties().tryGet<T>(ptID, nodeID.getValue());
        if (p) {
            return p;
        }
    }

    return nullptr;
}


template const types::UInt64::Primitive* GraphReader::tryGetNodeProperty<types::UInt64>(PropertyTypeID ptID, NodeID nodeID) const;
template const types::Int64::Primitive* GraphReader::tryGetNodeProperty<types::Int64>(PropertyTypeID ptID, NodeID nodeID) const;
template const types::Double::Primitive* GraphReader::tryGetNodeProperty<types::Double>(PropertyTypeID ptID, NodeID nodeID) const;
template const types::String::Primitive* GraphReader::tryGetNodeProperty<types::String>(PropertyTypeID ptID, NodeID nodeID) const;
template const types::Bool::Primitive* GraphReader::tryGetNodeProperty<types::Bool>(PropertyTypeID ptID, NodeID nodeID) const;
