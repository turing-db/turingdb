#include "GraphReader.h"

#include <algorithm>
#include <optional>
#include <span>
#include <unordered_set>

#include <range/v3/view/reverse.hpp>

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
namespace rv = ranges::views;

namespace {

template <typename VisitRange>
void visitMatchingNodeRanges(DataPartSpan parts, const LabelSetHandle& labelset, VisitRange visitRange) {
    for (const auto& part : parts) {
        const LabelSetIndexer<NodeRange>& indexer = part->nodes().getLabelSetIndexer();
        for (auto labelsetIt = indexer.matchIterate(labelset); labelsetIt.isValid(); labelsetIt.next()) {
            visitRange(labelsetIt.getValue());
        }
    }
}

size_t countIndexedEdges(const LabelSetIndexer<EdgeIndexer::EdgeSpans>& indexer, const LabelSetHandle& labelset) {
    size_t count = 0;
    for (auto labelsetIt = indexer.matchIterate(labelset); labelsetIt.isValid(); labelsetIt.next()) {
        for (const EdgeIndexer::EdgeSpan& span : labelsetIt.getValue()) {
            count += span.size();
        }
    }

    return count;
}

// The matched entries of one part that stand for a live node: those whose node is neither
// deleted nor held by an entry of a newer part. The excluded entries are found from the
// smaller side: each matched entry checked against the exclusions, or each exclusion
// checked against the part's entries and the labels.
size_t countLiveEntries(const GraphReader& reader,
                        const PropertyManager& properties,
                        PropertyTypeID propertyTypeID,
                        const LabelSetHandle& labelset,
                        const std::unordered_set<EntityID>& overridden) {
    const LabelSetPropertyIndexer* indexer = properties.tryGetIndexer(propertyTypeID);
    bioassert(indexer, "A part holding a property container has no label set indexer for it");

    size_t matchedCount = 0;
    for (auto labelsetIt = indexer->matchIterate(labelset); labelsetIt.isValid(); labelsetIt.next()) {
        for (const PropertyRange& range : labelsetIt.getValue()) {
            matchedCount += range._count;
        }
    }

    const Tombstones& tombstones = reader.getView().tombstones();
    const size_t exclusionCount = tombstones.numNodes() + overridden.size();
    if (exclusionCount == 0) {
        return matchedCount;
    }

    if (matchedCount <= exclusionCount) {
        const std::span<const EntityID> entityIDs = properties.ids(propertyTypeID);

        size_t liveCount = 0;
        for (auto labelsetIt = indexer->matchIterate(labelset); labelsetIt.isValid(); labelsetIt.next()) {
            for (const PropertyRange& range : labelsetIt.getValue()) {
                for (const EntityID entityID : entityIDs.subspan(range._offset, range._count)) {
                    const bool deleted = tombstones.containsNode(NodeID {entityID.getValue()});
                    if (!deleted && !overridden.contains(entityID)) {
                        liveCount++;
                    }
                }
            }
        }

        return liveCount;
    }

    const PropertyContainer& container = *properties.find(propertyTypeID)->second;
    const auto holdsMatchedEntry = [&reader, &container, &labelset](NodeID nodeID) {
        if (!container.has(EntityID {nodeID.getValue()})) {
            return false;
        }

        const LabelSetHandle nodeLabels = reader.getNodeLabelSet(nodeID);
        return nodeLabels.isValid() && nodeLabels.hasAtLeastLabels(labelset);
    };

    size_t excludedCount = 0;
    for (const NodeID nodeID : tombstones.nodeTombstones()) {
        if (holdsMatchedEntry(nodeID)) {
            excludedCount++;
        }
    }

    for (const EntityID entityID : overridden) {
        const NodeID nodeID {entityID.getValue()};
        if (!tombstones.containsNode(nodeID) && holdsMatchedEntry(nodeID)) {
            excludedCount++;
        }
    }

    return matchedCount - excludedCount;
}

}

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
    const DataPartSpan parts = _view.dataparts();

    size_t count = 0;
    visitMatchingNodeRanges(parts, labelset, [&count](NodeRange range) {
        count += range._count;
    });

    const Tombstones& tombstones = _view.tombstones();
    if (!tombstones.hasNodes()) {
        return count;
    }

    // The deleted nodes are found from the smaller side: each matched node checked against
    // the deletions, or each deletion checked against the labels.
    size_t deletedCount = 0;
    if (count <= tombstones.numNodes()) {
        visitMatchingNodeRanges(parts, labelset, [&deletedCount, &tombstones](NodeRange range) {
            for (const NodeID nodeID : range) {
                if (tombstones.containsNode(nodeID)) {
                    deletedCount++;
                }
            }
        });
    } else {
        for (const NodeID nodeID : tombstones.nodeTombstones()) {
            const LabelSetHandle nodeLabels = getNodeLabelSet(nodeID);
            if (nodeLabels.isValid() && nodeLabels.hasAtLeastLabels(labelset)) {
                deletedCount++;
            }
        }
    }

    return count - deletedCount;
}

size_t GraphReader::getOutEdgeCountMatchingLabelset(const LabelSetHandle& labelset) const {
    size_t count = 0;
    for (const auto& part : _view.dataparts()) {
        const EdgeIndexer& indexer = part->edgeIndexer();
        count += countIndexedEdges(indexer.getOutsByLabelSet(), labelset);
    }

    return count;
}

size_t GraphReader::getInEdgeCountMatchingLabelset(const LabelSetHandle& labelset) const {
    size_t count = 0;
    for (const auto& part : _view.dataparts()) {
        const EdgeIndexer& indexer = part->edgeIndexer();
        count += countIndexedEdges(indexer.getInsByLabelSet(), labelset);
    }

    return count;
}

size_t GraphReader::getNodeCountWithProperty(const LabelSetHandle& labelset,
                                             PropertyTypeID propertyTypeID) const {
    const DataPartSpan parts = _view.dataparts();

    size_t oldestHolder = parts.size();
    for (size_t partIndex = 0; partIndex < parts.size(); partIndex++) {
        if (parts[partIndex]->nodeProperties().hasPropertyType(propertyTypeID)) {
            oldestHolder = partIndex;
            break;
        }
    }

    // The entities a newer part holds an entry for, a value or an explicit null. That entry
    // stands for the node, so an older part's entry for it no longer counts.
    std::unordered_set<EntityID> overridden;

    size_t count = 0;
    for (size_t newerParts = parts.size(); newerParts > oldestHolder; newerParts--) {
        const size_t partIndex = newerParts - 1;
        const PropertyManager& properties = parts[partIndex]->nodeProperties();
        if (!properties.hasPropertyType(propertyTypeID)) {
            continue;
        }

        count += countLiveEntries(*this, properties, propertyTypeID, labelset, overridden);

        if (partIndex > oldestHolder) {
            const PropertyContainer& container = *properties.find(propertyTypeID)->second;
            const PropertyContainer::IDs& valueIDs = container.ids();
            const PropertyContainer::IDs& nullIDs = container.nullIds();
            overridden.insert(valueIDs.begin(), valueIDs.end());
            overridden.insert(nullIDs.begin(), nullIDs.end());
        }
    }

    return count;
}

size_t GraphReader::getDatapartCount() const {
    return _view.dataparts().size();
}

GetNodeLabelSetRange GraphReader::getNodeLabelSets(const ColumnNodeIDs* inputNodeIDs) const {
    return {_view, inputNodeIDs};
}

GetEdgeTypesRange GraphReader::getEdgeTypes(const ColumnEdgeIDs* inputEdgeIDs) const {
    return {_view, inputEdgeIDs};
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

ScanOutEdgesBySourceLabelRange GraphReader::scanOutEdgesBySourceLabel(const LabelSetHandle& labelset) const {
    return {_view, labelset};
}

ScanInEdgesByTargetLabelRange GraphReader::scanInEdgesByTargetLabel(const LabelSetHandle& labelset) const {
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
    const EntityID entityID {nodeID.getValue()};

    for (const auto& part : rv::reverse(_view.dataparts())) {
        const PropertyManager& properties = part->nodeProperties();
        const auto containerIt = properties.find(ptID);

        if (containerIt == properties.end()) {
            continue;
        }

        const PropertyContainer& container = *containerIt->second;

        if (container.hasEntry(entityID)) {
            return container.has(entityID);
        }
    }

    return false;
}

bool GraphReader::graphHasNode(NodeID nodeID) const {
    const bool exists = nodeID < getTotalNodesAllocated();
    const bool isDeleted = _view.isDeleted(nodeID);
    return exists && !isDeleted;
}

bool GraphReader::graphHasEdge(EdgeID edgeID) const {
    const bool exists = edgeID < getTotalEdgesAllocated();
    const bool isDeleted = _view.isDeleted(edgeID);
    return exists && !isDeleted;
}

bool GraphReader::nodeIsDeleted(NodeID nodeID) const {
    return _view.isDeleted(nodeID);
}

bool GraphReader::edgeIsDeleted(EdgeID edgeID) const {
    return _view.isDeleted(edgeID);
}

template <SupportedType T>
std::optional<const typename T::Primitive*> GraphReader::tryGetNodeProperty(PropertyTypeID ptID, NodeID nodeID) const {
    const DataPartSpan& parts = _view.dataparts();

    // newest to oldest => most recent value wins
    for (const auto& part : rv::reverse(parts)) {
        const auto& p = part->nodeProperties().tryGetWithNull<T>(ptID, nodeID.getValue());
        if (!p.has_value()) {
            return std::nullopt;
        }
        const typename T::Primitive* value = *p;
        if (value) {
            return value;
        }
    }

    return nullptr;
}

template <SupportedType T>
std::optional<const typename T::Primitive*> GraphReader::tryGetEdgeProperty(PropertyTypeID ptID, EdgeID edgeID) const {
    const DataPartSpan& parts = _view.dataparts();

    // newest to oldest => most recent value wins
    for (const auto& part : rv::reverse(parts)) {
        const auto& p = part->edgeProperties().tryGetWithNull<T>(ptID, edgeID.getValue());
        if (!p.has_value()) {
            return std::nullopt;
        }
        const typename T::Primitive* value = *p;
        if (value) {
            return value;
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

template std::optional<const types::UInt64::Primitive*> GraphReader::tryGetNodeProperty<types::UInt64>(PropertyTypeID ptID, NodeID nodeID) const;
template std::optional<const types::Int64::Primitive*> GraphReader::tryGetNodeProperty<types::Int64>(PropertyTypeID ptID, NodeID nodeID) const;
template std::optional<const types::Double::Primitive*> GraphReader::tryGetNodeProperty<types::Double>(PropertyTypeID ptID, NodeID nodeID) const;
template std::optional<const types::String::Primitive*> GraphReader::tryGetNodeProperty<types::String>(PropertyTypeID ptID, NodeID nodeID) const;
template std::optional<const types::Bool::Primitive*> GraphReader::tryGetNodeProperty<types::Bool>(PropertyTypeID ptID, NodeID nodeID) const;
template std::optional<const types::Embedding::Primitive*> GraphReader::tryGetNodeProperty<types::Embedding>(PropertyTypeID ptID, NodeID nodeID) const;
template std::optional<const types::List::Primitive*> GraphReader::tryGetNodeProperty<types::List>(PropertyTypeID ptID, NodeID nodeID) const;
template std::optional<const types::DateTime::Primitive*> GraphReader::tryGetNodeProperty<types::DateTime>(PropertyTypeID ptID, NodeID nodeID) const;
template std::optional<const types::Duration::Primitive*> GraphReader::tryGetNodeProperty<types::Duration>(PropertyTypeID ptID, NodeID nodeID) const;
template std::optional<const types::Map::Primitive*> GraphReader::tryGetNodeProperty<types::Map>(PropertyTypeID ptID, NodeID nodeID) const;

template std::optional<const types::UInt64::Primitive*> GraphReader::tryGetEdgeProperty<types::UInt64>(PropertyTypeID ptID, EdgeID edgeID) const;
template std::optional<const types::Int64::Primitive*> GraphReader::tryGetEdgeProperty<types::Int64>(PropertyTypeID ptID, EdgeID edgeID) const;
template std::optional<const types::Double::Primitive*> GraphReader::tryGetEdgeProperty<types::Double>(PropertyTypeID ptID, EdgeID edgeID) const;
template std::optional<const types::String::Primitive*> GraphReader::tryGetEdgeProperty<types::String>(PropertyTypeID ptID, EdgeID edgeID) const;
template std::optional<const types::Bool::Primitive*> GraphReader::tryGetEdgeProperty<types::Bool>(PropertyTypeID ptID, EdgeID edgeID) const;
template std::optional<const types::Embedding::Primitive*> GraphReader::tryGetEdgeProperty<types::Embedding>(PropertyTypeID ptID, EdgeID edgeID) const;
template std::optional<const types::List::Primitive*> GraphReader::tryGetEdgeProperty<types::List>(PropertyTypeID ptID, EdgeID edgeID) const;
template std::optional<const types::DateTime::Primitive*> GraphReader::tryGetEdgeProperty<types::DateTime>(PropertyTypeID ptID, EdgeID edgeID) const;
template std::optional<const types::Duration::Primitive*> GraphReader::tryGetEdgeProperty<types::Duration>(PropertyTypeID ptID, EdgeID edgeID) const;
template std::optional<const types::Map::Primitive*> GraphReader::tryGetEdgeProperty<types::Map>(PropertyTypeID ptID, EdgeID edgeID) const;
