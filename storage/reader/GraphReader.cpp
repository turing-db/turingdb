#include "GraphReader.h"

#include "DataPart.h"
#include "NodeContainer.h"
#include "EdgeContainer.h"
#include "indexers/EdgeIndexer.h"
#include "views/EdgeView.h"
#include "properties/PropertyManager.h"
#include "labels/LabelSet.h"

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

LabelSetID GraphReader::getNodeLabelSetID(EntityID nodeID) const {
    for (const auto& part : _view.dataparts()) {
        if (part->hasNode(nodeID)) {
            return part->nodes().getNodeLabelSet(nodeID);
        }
    }
    return LabelSetID {};
}

const LabelSet* GraphReader::getNodeLabelSet(EntityID nodeID) const {
    const LabelSetID labelSetID = getNodeLabelSetID(nodeID);
    if (!labelSetID.isValid()) {
        return nullptr;
    }

    return &_view.metadata().labelsets().getValue(labelSetID);
}

const EdgeRecord* GraphReader::getEdge(EntityID edgeID) const {
    for (const auto& part : _view.dataparts()) {
        if (part->hasEdge(edgeID)) {
            return &part->edges().get(edgeID);
        }
    }
    return nullptr;
}

size_t GraphReader::getNodeCountMatchingLabelset(const LabelSet& labelset) const {
    size_t count = 0;

    auto it = matchLabelSetIDs(&labelset);
    for (; it.isValid(); it.next()) {
        for (const auto& part : _view.dataparts()) {
            const auto& nodes = part->nodes();
            const LabelSetID id = it.get();
            if (!nodes.hasLabelSet(id)) {
                continue;
            }

            count += part->nodes().getRange(id)._count;
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

EntityID GraphReader::getFinalNodeID(EntityID tmpID) const {
    // TODO Update with a new Unique-Internal ID System.
    // This implementation does not work if multiple nodes
    // have the same temporary ID
    for (const auto& part : _view.dataparts()) {
        const auto& map = part->_tmpToFinalNodeIDs;
        auto it = map.find(tmpID);
        if (it != map.end()) {
            return it->second;
        }
    }

    return EntityID {};
}


const GraphMetadata& GraphReader::getMetadata() const {
    return _view.metadata();
}

GraphMetadata& GraphReader::getMetadata() {
    return _view.metadata();
}

GetOutEdgesRange GraphReader::getOutEdges(const ColumnIDs* inputNodeIDs) const {
    return {_view, inputNodeIDs};
}

GetInEdgesRange GraphReader::getInEdges(const ColumnIDs* inputNodeIDs) const {
    return {_view, inputNodeIDs};
}

ScanEdgesRange GraphReader::scanOutEdges() const {
    return {_view};
}

ScanNodesRange GraphReader::scanNodes() const {
    return {_view};
}

ScanNodesByLabelRange GraphReader::scanNodesByLabel(const LabelSet* labelset) const {
    return {_view, labelset};
}

ScanOutEdgesByLabelRange GraphReader::scanOutEdgesByLabel(const LabelSet* labelset) const {
    return {_view, labelset};
}

ScanInEdgesByLabelRange GraphReader::scanInEdgesByLabel(const LabelSet* labelset) const {
    return {_view, labelset};
}

GetNodeViewsRange GraphReader::getNodeViews(const ColumnIDs* inputNodeIDs) const {
    return {_view, inputNodeIDs};
}

NodeView GraphReader::getNodeView(EntityID id) const {
    NodeView view;
    PartIterator partIt(_view);
    LabelSetID labelsetID;

    // Find definition of the node
    for (; partIt.isValid(); partIt.next()) {
        const auto* part = partIt.get();
        const NodeContainer& nodes = part->nodes();

        labelsetID = nodes.getNodeLabelSet(id);
        if (labelsetID.isValid()) {
            view._labelset = labelsetID;
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

        nodeProperties.fillEntityPropertyView(id, labelsetID, view._props);
        edgeIndexer.fillEntityEdgeView(id, view._edges);
    }

    return view;
}

EdgeView GraphReader::getEdgeView(EntityID id) const {
    EdgeView view;
    PartIterator partIt(_view);
    LabelSetID labelsetID;
    const EdgeRecord* edge {nullptr};

    // Find definition of the edge
    for (; partIt.isValid(); partIt.next()) {
        const auto* part = partIt.get();
        const EdgeContainer& edges = part->edges();
        edge = edges.tryGet(id);

        if (edge) {
            labelsetID = getNodeLabelSetID(edge->_nodeID);
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

        edgeProperties.fillEntityPropertyView(id, labelsetID, view._props);
    }

    return view;
}

EdgeTypeID GraphReader::getEdgeTypeID(EntityID edgeID) const {
    for (const auto& part : _view.dataparts()) {
        const auto* edge = part->edges().tryGet(edgeID);
        if (edge) {
            return edge->_edgeTypeID;
        }
    }
    return {};
}

MatchLabelSetIterator GraphReader::matchLabelSetIDs(const LabelSet* labelSet) const {
    return MatchLabelSetIterator(_view, labelSet);
}

bool GraphReader::nodeHasProperty(PropertyTypeID ptID, EntityID nodeID) const {
    for (const auto& part : _view.dataparts()) {
        if (part->nodeProperties().has(ptID, nodeID)) {
            return true;
        }
    }
    return false;
}


template <SupportedType T>
const T::Primitive* GraphReader::tryGetNodeProperty(PropertyTypeID ptID, EntityID nodeID) const {
    for (const auto& part : _view.dataparts()) {
        const auto* p = part->nodeProperties().tryGet<T>(ptID, nodeID);
        if (p) {
            return p;
        }
    }

    return nullptr;
}

PropertyType GraphReader::getPropertyType(const std::string& name) const {
    const auto& propTypes = _view.metadata().propTypes();
    return propTypes.get(name);
}


template const types::UInt64::Primitive* GraphReader::tryGetNodeProperty<types::UInt64>(PropertyTypeID ptID, EntityID nodeID) const;
template const types::Int64::Primitive* GraphReader::tryGetNodeProperty<types::Int64>(PropertyTypeID ptID, EntityID nodeID) const;
template const types::Double::Primitive* GraphReader::tryGetNodeProperty<types::Double>(PropertyTypeID ptID, EntityID nodeID) const;
template const types::String::Primitive* GraphReader::tryGetNodeProperty<types::String>(PropertyTypeID ptID, EntityID nodeID) const;
template const types::Bool::Primitive* GraphReader::tryGetNodeProperty<types::Bool>(PropertyTypeID ptID, EntityID nodeID) const;
