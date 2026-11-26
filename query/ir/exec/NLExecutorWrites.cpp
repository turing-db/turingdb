#include "NLExecutor.h"

#include <stdint.h>
#include <algorithm>
#include <span>
#include <string>
#include <string_view>
#include <type_traits>

#include "ID.h"
#include "iterators/GetInEdgesIterator.h"
#include "iterators/GetOutEdgesIterator.h"
#include "columns/ColumnVector.h"
#include "CellOrder.h"
#include "metadata/PropertyType.h"

#include "versioning/CommitWriteBuffer.h"
#include "views/GraphView.h"

#include "NLProgram.h"
#include "NLRuntimeProperty.h"
#include "NLMergeExecutor.h"
#include "NLEntityProperties.h"
#include "NLWriteProperties.h"
#include "NLWrittenValues.h"
#include "NLPendingRows.h"

#include "IRException.h"
#include "BioAssert.h"

using namespace db;

namespace {

// The write buffer knows a node this change wrote by its offset, so a row naming one is
// resolved back to it; every other row names a node the graph holds.
CommitWriteBuffer::ExistingOrPendingNode resolveNode(const ColumnNodeIDs* column,
                                                     size_t row,
                                                     const PendingRows& pendingRows,
                                                     size_t firstPendingNodeID) {
    const NodeID nodeID = (*column)[row];
    if (pendingRows.has(row, nodeID.getValue())) {
        return CommitWriteBuffer::PendingNodeOffset(nodeID.getValue() - firstPendingNodeID);
    } else {
        return nodeID;
    }
}

// The write buffer builds one column per property of a pending entity, so a second entry
// for the same property would be a second value for the same cell: ON CREATE writes over
// the value the pattern wrote a moment earlier rather than beside it.
void setPendingProperty(CommitWriteBuffer::UntypedProperties& properties,
                        const CommitWriteBuffer::UntypedProperty& property) {
    for (CommitWriteBuffer::UntypedProperty& held : properties) {
        if (held.propertyID == property.propertyID) {
            held.value = property.value;
            return;
        }
    }

    properties.push_back(property);
}

template <TypedInternalID IDT>
bool setWritesRow(const ColumnVector<CustomBool>* rows, IDT entity, size_t row) {
    return writeTouchesRow(rows, row) && entity.isValid();
}

template <typename IDColumn>
void markWrittenRows(const IDColumn* entities, const ColumnVector<CustomBool>* rows, ColumnVector<CustomBool>& written) {
    const auto& raw = entities->getRaw();
    written.resize(raw.size());

    for (size_t row = 0; row < raw.size(); row++) {
        written[row] = setWritesRow(rows, raw[row], row);
    }
}

// Stages the values a SET writes in @param buffer and answers the property it writes them
// to: invalid where it writes nothing
template <typename SetData>
PropertyTypeID stageSetProperty(MetadataBuilder* metadataBuilder,
                                SetData* setData,
                                size_t rowCount,
                                CommitWriteBuffer::UntypedProperties& buffer) {
    NLRuntimeProperty* runtimeProperty = setData->getRuntimeProperty();
    const PropertyTypeID propertyTypeID = setData->getPropertyTypeID();
    const Column* values = setData->getValue();

    if (!runtimeProperty) {
        extractColumnProperties(values, rowCount, propertyTypeID, buffer);
        return propertyTypeID;
    } else if (setData->writesNull()) {
        const bool staged = runtimeProperty->stageNulls(metadataBuilder, rowCount, buffer);
        return staged ? runtimeProperty->getType()._id : PropertyTypeID {};
    }

    ColumnVector<CustomBool>* written = setData->getWrittenRows();
    markWrittenRows(setData->getInput(), setData->getRows(), *written);

    const bool staged = runtimeProperty->stage(metadataBuilder, values, rowCount, written, buffer);

    return staged ? runtimeProperty->getType()._id : PropertyTypeID {};
}

// False for a chunk of nulls held in cells reaching a property with no type, which writes
// it nothing
bool stageCreatedProperty(MetadataBuilder* metadataBuilder,
                          const NLCreatedProperty& property,
                          size_t rowCount,
                          CommitWriteBuffer::UntypedProperties& buffer) {
    if (property._runtimeProperty) {
        return property._runtimeProperty->stage(metadataBuilder, property._values, rowCount, nullptr, buffer);
    }

    extractColumnProperties(property._values, rowCount, property._propertyTypeID, buffer);

    return true;
}

bool isSameMap(const WrittenMap& lhs, const WrittenMap& rhs) {
    const bool holdsAnotherKind = lhs._tag != rhs._tag || lhs._map.has_value() != rhs._map.has_value();

    if (holdsAnotherKind) {
        return false;
    } else if (!lhs._map) {
        return true;
    }

    const std::span<const MapEntryView> lhsEntries = lhs._map->entries();
    const std::span<const MapEntryView> rhsEntries = rhs._map->entries();

    return lhsEntries.data() == rhsEntries.data() && lhsEntries.size() == rhsEntries.size();
}

template <typename IDs, typename Visit>
void visitWrittenMapRows(const std::vector<WrittenMap>& maps,
                         const ColumnVector<CustomBool>* rows,
                         const IDs& raw,
                         size_t rowCount,
                         Visit visit) {
    const WrittenMap* previousMap = nullptr;

    for (size_t row = 0; row < rowCount; row++) {
        if (!setWritesRow(rows, raw[row], row)) {
            continue;
        }

        const bool holdsAnotherMap = !previousMap || !isSameMap(*previousMap, maps[row]);
        visit(row, holdsAnotherMap);

        previousMap = &maps[row];
    }
}

template <typename IDColumn>
void runSetProperties(NLExecutionContext* context, NLFunctionData* data) {
    constexpr bool WRITES_NODES = std::is_same_v<IDColumn, ColumnNodeIDs>;

    NLSetPropertiesData<IDColumn>* setData = static_cast<NLSetPropertiesData<IDColumn>*>(data);
    CommitWriteBuffer* writeBuffer = context->getWriteBuffer();
    bioassert(writeBuffer, "A properties write requires an active write transaction");

    const IDColumn* entities = setData->getInput();
    const size_t rowCount = entities->size();

    std::vector<WrittenMap>& maps = setData->getMaps();
    if (setData->writesNull()) {
        maps.assign(rowCount, WrittenMap {std::nullopt, CellTag::Null});
    } else {
        extractWrittenMaps(setData->getValue(), rowCount, maps);
    }

    const GraphView* view = context->getView();
    MetadataBuilder* metadataBuilder = context->getMetadataBuilder();
    const ColumnVector<CustomBool>* rows = setData->getRows();
    const bool replaces = setData->getReplaces();
    const auto& raw = entities->getRaw();

    std::vector<MapKeyPropertyType>& keyTypes = setData->getKeyTypes();
    std::vector<MapKeyCreatedType> createdTypes;

    forgetUntypedMapKeys(keyTypes);
    visitWrittenMapRows(maps, rows, raw, rowCount, [&](size_t row, bool holdsAnotherMap) {
        if (holdsAnotherMap) {
            widenCreatedMapKeyTypes(maps[row], view, metadataBuilder, keyTypes, createdTypes);
        }
    });

    for (const MapKeyCreatedType& created : createdTypes) {
        metadataBuilder->getOrCreatePropertyType(created._key, created._type);
    }

    CommitWriteBuffer::UntypedProperties& mapProperties = setData->getMapProperties();
    std::vector<StagedRange>& mapRanges = setData->getMapRanges();

    mapProperties.clear();
    mapRanges.assign(rowCount, StagedRange {});

    StagedRange stagedRange;

    forgetUntypedMapKeys(keyTypes);
    visitWrittenMapRows(maps, rows, raw, rowCount, [&](size_t row, bool holdsAnotherMap) {
        if (holdsAnotherMap) {
            const size_t begin = mapProperties.size();
            stageMapProperties(maps[row], view, metadataBuilder, keyTypes, mapProperties);

            stagedRange = StagedRange {begin, mapProperties.size()};
        }

        mapRanges[row] = stagedRange;
    });

    const size_t firstPendingID = WRITES_NODES ? committedNodeCount(view) : committedEdgeCount(view);
    const size_t pendingCount = WRITES_NODES ? writeBuffer->numPendingNodes() : writeBuffer->numPendingEdges();
    const PendingRows pendingRows(setData->getPending(), setData->isAllPending(), firstPendingID, pendingCount);
    NLWrittenValues& written = context->getWrittenValues();

    std::optional<NLEntityProperties> heldProperties;
    if (replaces) {
        heldProperties.emplace(context, nullptr);
    }

    CommitWriteBuffer::UntypedProperties& removals = setData->getRemovals();
    std::vector<PropertyTypeID>& held = setData->getHeld();
    std::vector<ValueType>& heldTypes = setData->getHeldTypes();

    for (size_t row = 0; row < rowCount; row++) {
        if (!setWritesRow(rows, raw[row], row)) {
            continue;
        }

        const StagedRange& range = mapRanges[row];
        const std::span<CommitWriteBuffer::UntypedProperty> writes {mapProperties.begin() + range._begin,
                                                                    mapProperties.begin() + range._end};

        removals.clear();
        if (replaces) {
            heldProperties->readIDs(raw[row], held);
            stageRemovals(held, writes, view, metadataBuilder, heldTypes, removals);
        }

        const uint64_t id = raw[row].getValue();

        if (pendingRows.has(row, id)) {
            const size_t offset = id - firstPendingID;

            if constexpr (WRITES_NODES) {
                CommitWriteBuffer::PendingNode& node = writeBuffer->getPendingNode(offset);
                const auto writeProperty = [&](const CommitWriteBuffer::UntypedProperty& property) {
                    setPendingProperty(node.properties, property);
                    written.addPendingNodeUpdate(offset, property.propertyID);
                };

                std::ranges::for_each(writes, writeProperty);
                std::ranges::for_each(removals, writeProperty);
            } else {
                CommitWriteBuffer::PendingEdge& edge = writeBuffer->getPendingEdge(offset);
                const auto writeProperty = [&](const CommitWriteBuffer::UntypedProperty& property) {
                    setPendingProperty(edge.properties, property);
                };

                std::ranges::for_each(writes, writeProperty);
                std::ranges::for_each(removals, writeProperty);
            }
        } else {
            const auto writeProperty = [&](CommitWriteBuffer::UntypedProperty& property) {
                if constexpr (WRITES_NODES) {
                    writeBuffer->addNodeUpdate(raw[row], property);
                } else {
                    writeBuffer->addEdgeUpdate(raw[row], property);
                }
            };

            std::ranges::for_each(writes, writeProperty);
            std::ranges::for_each(removals, writeProperty);
        }
    }
}

// A node an OPTIONAL MATCH did not match is an invalid ID, and no edge can hang off one.
// The whole column is read before anything is staged, so a CREATE naming a null endpoint
// writes none of its edges rather than leaving the commit one it cannot resolve. A pending
// endpoint is an offset into this change's new nodes rather than an ID, so only an existing
// one can be null.
void throwIfAnyNodeIsNull(const ColumnNodeIDs* column, bool isPending, std::string_view error) {
    if (isPending) {
        return;
    }

    for (const NodeID nodeID : column->getRaw()) {
        if (!nodeID.isValid()) {
            throw IRException(std::string(error));
        }
    }
}

void throwIfNodesHaveEdges(const GraphView& view, const ColumnNodeIDs* nodes) {
    const GetOutEdgesRange outEdges(view, nodes);
    for (const EdgeRecord& record : outEdges) {
        if (!view.isDeleted(record._edgeID)) {
            throw IRException("Cannot delete a node with relationships; use DETACH DELETE");
        }
    }

    const GetInEdgesRange inEdges(view, nodes);
    for (const EdgeRecord& record : inEdges) {
        if (!view.isDeleted(record._edgeID)) {
            throw IRException("Cannot delete a node with relationships; use DETACH DELETE");
        }
    }
}

}

void NLExecutor::runCreateNode(NLExecutionContext* context, NLFunctionData* data) {
    NLCreateNodeData* createData = static_cast<NLCreateNodeData*>(data);
    CommitWriteBuffer* writeBuffer = context->getWriteBuffer();
    bioassert(writeBuffer, "nl.create_node requires an active write transaction");

    const size_t rowCount = createData->getRowCount();
    const LabelSetHandle labelsetHandle = createData->getLabelSetHandle();

    MetadataBuilder* metadataBuilder = context->getMetadataBuilder();

    const std::vector<NLCreateNodeData::Property>& properties = createData->properties();
    std::vector<CommitWriteBuffer::UntypedProperties> extracted;
    extracted.reserve(properties.size());

    for (const NLCreateNodeData::Property& property : properties) {
        CommitWriteBuffer::UntypedProperties& values = extracted.emplace_back();

        if (!stageCreatedProperty(metadataBuilder, property, rowCount, values)) {
            extracted.pop_back();
        }
    }

    // Must extract this value before adding in the loop
    const size_t numPendingNodes = writeBuffer->numPendingNodes();

    for (size_t row = 0; row < rowCount; row++) {
        CommitWriteBuffer::PendingNode& node = writeBuffer->newPendingNode();
        node.labelsetHandle = labelsetHandle;

        for (const CommitWriteBuffer::UntypedProperties& values : extracted) {
            node.properties.push_back(values[row]);
        }
    }

    const GraphView* view = context->getView();
    const NodeID nextNodeID = committedNodeCount(view) + numPendingNodes;

    ColumnNodeIDs* result = createData->getResult();
    result->resize(rowCount);
    auto& raw = result->getRaw();
    for (size_t row = 0; row < rowCount; row++) {
        raw[row] = NodeID(nextNodeID + row);
    }
}

void NLExecutor::runMerge(NLExecutionContext* context, NLFunctionData* data) {
    NLMergeExecutor merge(context, static_cast<NLMergeData*>(data));
    merge.run();
}

void NLExecutor::runCreateEdge(NLExecutionContext* context, NLFunctionData* data) {
    NLCreateEdgeData* createData = static_cast<NLCreateEdgeData*>(data);
    CommitWriteBuffer* writeBuffer = context->getWriteBuffer();
    bioassert(writeBuffer, "nl.create_edge requires an active write transaction");

    const ColumnNodeIDs* src = createData->getSrc();
    const ColumnNodeIDs* tgt = createData->getTgt();
    const bool srcIsPending = createData->isSrcPending();
    const bool tgtIsPending = createData->isTgtPending();
    const size_t rowCount = src->size();
    const EdgeTypeID edgeTypeID = createData->getEdgeTypeID();

    const ColumnVector<CustomBool>* srcPending = createData->getSrcPendingMask();
    const ColumnVector<CustomBool>* tgtPending = createData->getTgtPendingMask();

    throwIfAnyNodeIsNull(src, srcIsPending, "Cannot create an edge from a null node");
    throwIfAnyNodeIsNull(tgt, tgtIsPending, "Cannot create an edge to a null node");

    const GraphView* view = context->getView();
    const size_t firstPendingNodeID = committedNodeCount(view);
    const size_t numPendingNodes = writeBuffer->numPendingNodes();

    const PendingRows srcRows(srcPending, srcIsPending, firstPendingNodeID, numPendingNodes);
    const PendingRows tgtRows(tgtPending, tgtIsPending, firstPendingNodeID, numPendingNodes);

    MetadataBuilder* metadataBuilder = context->getMetadataBuilder();

    const std::vector<NLCreateEdgeData::Property>& properties = createData->properties();
    std::vector<CommitWriteBuffer::UntypedProperties> extracted;
    extracted.reserve(properties.size());

    for (const NLCreateEdgeData::Property& property : properties) {
        CommitWriteBuffer::UntypedProperties& values = extracted.emplace_back();

        if (!stageCreatedProperty(metadataBuilder, property, rowCount, values)) {
            extracted.pop_back();
        }
    }

    // Must extract this value before adding in the loop
    const size_t numPendingEdges = writeBuffer->numPendingEdges();

    for (size_t row = 0; row < rowCount; row++) {
        const CommitWriteBuffer::ExistingOrPendingNode srcNode = resolveNode(src, row, srcRows, firstPendingNodeID);
        const CommitWriteBuffer::ExistingOrPendingNode tgtNode = resolveNode(tgt, row, tgtRows, firstPendingNodeID);

        CommitWriteBuffer::PendingEdge& edge = writeBuffer->newPendingEdge(srcNode, tgtNode);
        edge.edgeType = edgeTypeID;

        for (const CommitWriteBuffer::UntypedProperties& values : extracted) {
            edge.properties.push_back(values[row]);
        }
    }

    const EdgeID nextEdgeID = committedEdgeCount(view) + numPendingEdges;

    ColumnEdgeIDs* result = createData->getResult();
    result->resize(rowCount);
    for (size_t row = 0; row < rowCount; row++) {
        (*result)[row] = EdgeID(nextEdgeID + row);
    }
}

void NLExecutor::runSetNodeProperty(NLExecutionContext* context, NLFunctionData* data) {
    NLSetNodePropertyData* setData = static_cast<NLSetNodePropertyData*>(data);
    CommitWriteBuffer* writeBuffer = context->getWriteBuffer();
    bioassert(writeBuffer, "nl.set_node_property requires an active write transaction");

    const ColumnNodeIDs* nodes = setData->getInput();
    const size_t rowCount = nodes->size();

    CommitWriteBuffer::UntypedProperties propsBuffer;
    const PropertyTypeID propID = stageSetProperty(context->getMetadataBuilder(), setData, rowCount, propsBuffer);
    if (!propID.isValid()) {
        return;
    }

    const ColumnVector<CustomBool>* pending = setData->getPending();
    const bool allPending = setData->isAllPending();
    const ColumnVector<CustomBool>* rows = setData->getRows();

    const size_t firstPendingNodeID = committedNodeCount(context->getView());
    const PendingRows pendingRows(pending, allPending, firstPendingNodeID, writeBuffer->numPendingNodes());
    NLWrittenValues& written = context->getWrittenValues();

    // A node an OPTIONAL MATCH did not match is an invalid ID, which Cypher writes nothing
    // for: staging it would have the commit look the ID up among the nodes this change
    // wrote, where it is not.
    const auto& raw = nodes->getRaw();
    for (size_t row = 0; row < rowCount; row++) {
        if (!setWritesRow(rows, raw[row], row)) {
            continue;
        }

        if (pendingRows.has(row, raw[row].getValue())) {
            const size_t offset = raw[row].getValue() - firstPendingNodeID;
            CommitWriteBuffer::PendingNode& node = writeBuffer->getPendingNode(offset);
            setPendingProperty(node.properties, propsBuffer[row]);
            written.addPendingNodeUpdate(offset, propID);
        } else {
            writeBuffer->addNodeUpdate(raw[row], propsBuffer[row]);
        }
    }
}

void NLExecutor::runSetEdgeProperty(NLExecutionContext* context, NLFunctionData* data) {
    NLSetEdgePropertyData* setData = static_cast<NLSetEdgePropertyData*>(data);
    CommitWriteBuffer* writeBuffer = context->getWriteBuffer();
    bioassert(writeBuffer, "nl.set_edge_property requires an active write transaction");

    const ColumnEdgeIDs* edges = setData->getInput();
    const size_t rowCount = edges->size();

    CommitWriteBuffer::UntypedProperties propsBuffer;
    const PropertyTypeID propID = stageSetProperty(context->getMetadataBuilder(), setData, rowCount, propsBuffer);
    if (!propID.isValid()) {
        return;
    }

    const ColumnVector<CustomBool>* pending = setData->getPending();
    const bool allPending = setData->isAllPending();
    const ColumnVector<CustomBool>* rows = setData->getRows();

    const size_t firstPendingEdgeID = committedEdgeCount(context->getView());
    const PendingRows pendingRows(pending, allPending, firstPendingEdgeID, writeBuffer->numPendingEdges());

    const auto& raw = edges->getRaw();
    for (size_t row = 0; row < rowCount; row++) {
        if (!setWritesRow(rows, raw[row], row)) {
            continue;
        }

        if (pendingRows.has(row, raw[row].getValue())) {
            CommitWriteBuffer::PendingEdge& edge =
                writeBuffer->getPendingEdge(raw[row].getValue() - firstPendingEdgeID);
            setPendingProperty(edge.properties, propsBuffer[row]);
        } else {
            writeBuffer->addEdgeUpdate(raw[row], propsBuffer[row]);
        }
    }
}

void NLExecutor::runSetNodeProperties(NLExecutionContext* context, NLFunctionData* data) {
    runSetProperties<ColumnNodeIDs>(context, data);
}

void NLExecutor::runSetEdgeProperties(NLExecutionContext* context, NLFunctionData* data) {
    runSetProperties<ColumnEdgeIDs>(context, data);
}

void NLExecutor::runDeleteNode(NLExecutionContext* context, NLFunctionData* data) {
    NLDeleteNodeData* deleteData = static_cast<NLDeleteNodeData*>(data);
    CommitWriteBuffer* writeBuffer = context->getWriteBuffer();
    bioassert(writeBuffer, "nl.delete_node requires an active write transaction");

    const ColumnNodeIDs* nodes = deleteData->getInput();
    const GraphView* view = context->getView();
    const ColumnVector<CustomBool>* pending = deleteData->getPending();
    const bool allPending = deleteData->isAllPending();
    const bool detaching = deleteData->isDetaching();

    ColumnNodeIDs* committed = deleteData->getCommitted();
    committed->clear();

    const size_t firstPendingNodeID = committedNodeCount(view);
    const PendingRows pendingRows(pending, allPending, firstPendingNodeID, writeBuffer->numPendingNodes());

    const auto& raw = nodes->getRaw();

    // A node an OPTIONAL MATCH did not match is an invalid ID, which Cypher deletes nothing
    // for. Staging it would tombstone an ID the graph never held, and the node count reads
    // the tombstones off what it allocated: it would report one node too few.
    for (size_t row = 0; row < raw.size(); row++) {
        if (!raw[row].isValid()) {
            continue;
        }

        const CommitWriteBuffer::ExistingOrPendingNode node =
            resolveNode(nodes, row, pendingRows, firstPendingNodeID);

        // A relationship this query wrote counts as much as one the graph holds, whether
        // it hangs off a node the query wrote or off one already committed
        if (!detaching && writeBuffer->hasPendingEdgesOn(node)) {
            throw IRException("Cannot delete a node with relationships; use DETACH DELETE");
        }

        if (std::holds_alternative<CommitWriteBuffer::PendingNodeOffset>(node)) {
            writeBuffer->addDeletedPendingNode(std::get<CommitWriteBuffer::PendingNodeOffset>(node));
        } else {
            committed->push_back(raw[row]);
        }
    }

    if (!detaching) {
        throwIfNodesHaveEdges(*view, committed);
    }

    writeBuffer->addDeletedNodes(committed->getRaw());

    if (!detaching) {
        return;
    }

    writeBuffer->addHangingEdges(*view);
    writeBuffer->addHangingPendingEdges();
}

void NLExecutor::runDeleteEdge(NLExecutionContext* context, NLFunctionData* data) {
    NLDeleteEdgeData* deleteData = static_cast<NLDeleteEdgeData*>(data);
    CommitWriteBuffer* writeBuffer = context->getWriteBuffer();
    bioassert(writeBuffer, "nl.delete_edge requires an active write transaction");

    const ColumnEdgeIDs* edges = deleteData->getInput();
    const ColumnVector<CustomBool>* pending = deleteData->getPending();
    const bool allPending = deleteData->isAllPending();

    ColumnEdgeIDs* committed = deleteData->getCommitted();
    committed->clear();

    const size_t firstPendingEdgeID = committedEdgeCount(context->getView());
    const PendingRows pendingRows(pending, allPending, firstPendingEdgeID, writeBuffer->numPendingEdges());

    const auto& raw = edges->getRaw();
    for (size_t row = 0; row < raw.size(); row++) {
        if (!raw[row].isValid()) {
            continue;
        }

        if (pendingRows.has(row, raw[row].getValue())) {
            writeBuffer->addDeletedPendingEdge(raw[row].getValue() - firstPendingEdgeID);
        } else {
            committed->push_back(raw[row]);
        }
    }

    writeBuffer->addDeletedEdges(committed->getRaw());
}
