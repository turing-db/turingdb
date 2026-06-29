#include "GraphReader.h"

#include <algorithm>

#include "datapart/DataPart.h"
#include "ID.h"
#include "datapart/NodeContainer.h"
#include "datapart/EdgeContainer.h"
#include "indexers/EdgeIndexer.h"
#include "indexers/LabelSetIndexer.h"
#include "views/EdgeView.h"
#include "properties/PropertyManager.h"

#include "BioAssert.h"

using namespace db;

size_t GraphReader::getTotalNodesAllocated() const {
    if (_view.dataparts().empty()) {
        return 0;
    }

    const auto& lastPart = _view.dataparts().back();
    return lastPart->getFirstNodeID().getValue() + lastPart->getNodeContainerSize();
}

size_t GraphReader::getTotalEdgesAllocated() const {
    if (_view.dataparts().empty()) {
        return 0;
    }

    const auto& lastPart = _view.dataparts().back();
    return lastPart->getFirstEdgeID().getValue() + lastPart->getEdgeContainerSize();
}

size_t GraphReader::getNodeCount() const {
    const size_t totalCount = getTotalNodesAllocated();
    const size_t deletedCount = _view.tombstones().nodeTombstones().size();

    bioassert(deletedCount <= totalCount, "invalid deleted count");

    return totalCount - deletedCount;
}

size_t GraphReader::getEdgeCount() const {
    const size_t totalCount = getTotalEdgesAllocated();
    const size_t deletedCount = _view.tombstones().edgeTombstones().size();

    bioassert(deletedCount <= totalCount, "invalid deleted count");

    return totalCount - deletedCount;
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
    // Iterate each data part exactly once, summing the node counts of that
    // part's own labelsets that match `labelset`. Every node is counted once,
    // in the part that owns it.
    size_t count = 0;

    auto filteredNodeCount = [](NodeRange range, const Tombstones* tombstones) {
        size_t count = range._count;
        if (tombstones->hasNodes()) {
            for (const auto id : range) {
                if (tombstones->containsNode(id)) {
                    count -= 1;
                }
            }
        }

        return count;
    };

    const auto* tombstones = &_view.tombstones();
    for (const auto& part : _view.dataparts()) {
        const NodeContainer& nodes = part->nodes();
        const LabelSetIndexer<NodeRange>& indexer = nodes.getLabelSetIndexer();
        for (auto it = indexer.matchIterate(labelset); it.isValid(); it.next()) {
            count += filteredNodeCount(it.getValue(), tombstones);
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

size_t GraphReader::getEdgePropertyCount(PropertyTypeID ptID) const {
    size_t count = 0;
    for (const auto& part : _view.dataparts()) {
        const auto& props = part->edgeProperties();
        if (props.hasPropertyType(ptID)) {
            count += props.count(ptID);
        }
    }
    return count;
}

size_t GraphReader::getEdgePropertyCount(size_t datapartIndex,
                                         PropertyTypeID ptID) const {
    const auto& props = _view.dataparts()[datapartIndex]->edgeProperties();
    if (props.hasPropertyType(ptID)) {
        return props.count(ptID);
    }
    return 0;
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

GetEdgesRange GraphReader::getEdges(const ColumnNodeIDs* inputNodeIDs) const {
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
    for (; partIt.isNotEnd(); partIt.next()) {
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
    for (; partIt.isNotEnd(); partIt.next()) {
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
    const EdgeRecord* edge = nullptr;

    // Find definition of the edge
    for (; partIt.isNotEnd(); partIt.next()) {
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
    for (; partIt.isNotEnd(); partIt.next()) {
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

bool GraphReader::graphHasNode(NodeID nodeID) const {
    const bool exists = nodeID < getTotalNodesAllocated();
    const bool isDeleted = _view.tombstones().contains(nodeID);
    return exists && !isDeleted;
}

bool GraphReader::graphHasEdge(EdgeID edgeID) const {
    const bool exists = edgeID < getTotalEdgesAllocated();
    const bool isDeleted = _view.tombstones().contains(edgeID);
    return exists && !isDeleted;
}

bool GraphReader::nodeIsDeleted(NodeID nodeID) const {
    return _view.tombstones().containsNode(nodeID);
}

bool GraphReader::edgeIsDeleted(EdgeID edgeID) const {
    return _view.tombstones().containsEdge(edgeID);
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

template <SupportedType T>
const T::Primitive* GraphReader::tryGetEdgeProperty(PropertyTypeID ptID, EdgeID edgeID) const {
    for (const auto& part : _view.dataparts()) {
        const auto* p = part->edgeProperties().tryGet<T>(ptID, edgeID.getValue());
        if (p) {
            return p;
        }
    }

    return nullptr;
}

bool GraphReader::isNodeProperty(PropertyTypeID ptID) const {
    const DataPartSpan& parts = _view.dataparts();

    const auto hasProperty = [ptID](const WeakArc<DataPart>& part) {
        const PropertyManager& nodeProps = part->nodeProperties();
        return nodeProps.hasPropertyType(ptID);
    };

    const bool isNodeProperty = std::ranges::any_of(parts, hasProperty);

    return isNodeProperty;
}

bool GraphReader::isEdgeProperty(PropertyTypeID ptID) const {
    const DataPartSpan& parts = _view.dataparts();

    const auto hasProperty = [ptID](const WeakArc<DataPart>& part) {
        const PropertyManager& edgeProps = part->edgeProperties();
        return edgeProps.hasPropertyType(ptID);
    };

    const bool isEdgeProperty = std::ranges::any_of(parts, hasProperty);

    return isEdgeProperty;
}

template const types::UInt64::Primitive* GraphReader::tryGetNodeProperty<types::UInt64>(PropertyTypeID ptID, NodeID nodeID) const;
template const types::Int64::Primitive* GraphReader::tryGetNodeProperty<types::Int64>(PropertyTypeID ptID, NodeID nodeID) const;
template const types::Double::Primitive* GraphReader::tryGetNodeProperty<types::Double>(PropertyTypeID ptID, NodeID nodeID) const;
template const types::String::Primitive* GraphReader::tryGetNodeProperty<types::String>(PropertyTypeID ptID, NodeID nodeID) const;
template const types::Bool::Primitive* GraphReader::tryGetNodeProperty<types::Bool>(PropertyTypeID ptID, NodeID nodeID) const;
template const types::Embedding::Primitive* GraphReader::tryGetNodeProperty<types::Embedding>(PropertyTypeID ptID, NodeID nodeID) const;

template const types::UInt64::Primitive* GraphReader::tryGetEdgeProperty<types::UInt64>(PropertyTypeID ptID, EdgeID edgeID) const;
template const types::Int64::Primitive* GraphReader::tryGetEdgeProperty<types::Int64>(PropertyTypeID ptID, EdgeID edgeID) const;
template const types::Double::Primitive* GraphReader::tryGetEdgeProperty<types::Double>(PropertyTypeID ptID, EdgeID edgeID) const;
template const types::String::Primitive* GraphReader::tryGetEdgeProperty<types::String>(PropertyTypeID ptID, EdgeID edgeID) const;
template const types::Bool::Primitive* GraphReader::tryGetEdgeProperty<types::Bool>(PropertyTypeID ptID, EdgeID edgeID) const;
template const types::Embedding::Primitive* GraphReader::tryGetEdgeProperty<types::Embedding>(PropertyTypeID ptID, EdgeID edgeID) const;
