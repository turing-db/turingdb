#include "NLExecutor.h"

#include <algorithm>
#include <concepts>
#include <span>
#include <string>
#include <type_traits>

#include "NumberOrder.h"
#include "EntityIDOfNumber.h"
#include "TypeUtils.h"
#include "ID.h"
#include "iterators/ScanEdgesByEndpointLabelsChunkWriter.h"
#include "iterators/ScanEdgesByTypeIterator.h"
#include "iterators/ScanEdgesIterator.h"
#include "iterators/ScanInEdgesByTargetLabelIterator.h"
#include "iterators/ScanOutEdgesBySourceLabelIterator.h"
#include "iterators/ScanNodesIterator.h"
#include "iterators/ScanNodesByLabelIterator.h"
#include "columns/ColumnOptVector.h"
#include "columns/ColumnVector.h"
#include "columns/ColumnConst.h"
#include "CellOrder.h"
#include "metadata/PropertyType.h"
#include "columns/ColumnOperand.h"

#include "versioning/CommitWriteBuffer.h"
#include "versioning/ScannedEntities.h"
#include "views/GraphView.h"

#include "NLProgram.h"
#include "NLRuntimeProperty.h"
#include "NLPendingEdges.h"
#include "NLPendingNodes.h"
#include "NLPropertyValueScan.h"
#include "NLBoundConstant.h"
#include "NLLoopSteps.h"

#include "IRException.h"
#include "BioAssert.h"

using namespace db;

namespace {

template <SupportedType T>
typename T::Primitive propertyScanLiteralAs(const NLScanByPropertyValueLoopData::Literal& literal) {
    if constexpr (std::same_as<T, types::String>) {
        return std::get<std::string>(literal);
    } else {
        return std::get<typename T::Primitive>(literal);
    }
}

template <SupportedType T>
bool readBoundValueAs(const Column* bound, NLScanByPropertyValueLoopData::Literal& given) {
    std::optional<typename T::Primitive> value;
    const bool readsAConstant = readBoundConstant(bound, value);
    bioassert(readsAConstant, "A bound value holds one value for the run");

    if (!value) {
        return false;
    }

    if constexpr (std::same_as<T, types::String>) {
        given = std::string(*value);
    } else {
        given = *value;
    }

    return true;
}

// The value bound to a property-value scan, read as the literal a query spells. False where
// it holds null, which no stored value equals.
bool readBoundPropertyValue(const Column* bound, ValueType boundType, NLScanByPropertyValueLoopData::Literal& given) {
    switch (boundType) {
        case ValueType::Int64:
            return readBoundValueAs<types::Int64>(bound, given);
        break;
        case ValueType::Double:
            return readBoundValueAs<types::Double>(bound, given);
        break;
        case ValueType::Bool:
            return readBoundValueAs<types::Bool>(bound, given);
        break;
        case ValueType::String:
            return readBoundValueAs<types::String>(bound, given);
        break;
        default:
            return false;
        break;
    }
}

// Appends to @param listed the nodes or edges a scan produces that the bound IDs of a const
// scan name: the entities they hold, and the numbers they hold, each naming the entity whose
// ID it equals
template <TypedInternalID IDType>
void appendBoundIDs(std::span<const Column* const> boundIDs,
                    const ScannedEntities<IDType>& scanned,
                    std::vector<IDType>& listed) {
    const auto appendEntity = [&scanned, &listed](IDType entity) {
        if (scanned.contains(entity)) {
            listed.push_back(entity);
        }
    };

    const auto appendNumberedEntity = [&appendEntity]<Number T>(T number) {
        IDType entity;
        if (entityIDOfNumber(number, entity)) {
            appendEntity(entity);
        }
    };

    const auto appendListedEntities = [&appendNumberedEntity](const ListView& list) {
        for (const CellView element : list) {
            switch (element.getTag()) {
                case CellTag::Int:
                    appendNumberedEntity(element.getAs<types::Int64::Primitive>());
                break;
                case CellTag::UInt:
                    appendNumberedEntity(element.getAs<types::UInt64::Primitive>());
                break;
                case CellTag::Double:
                    appendNumberedEntity(element.getAs<types::Double::Primitive>());
                break;
                default:
                break;
            }
        }
    };

    for (const Column* bound : boundIDs) {
        std::optional<types::Int64::Primitive> integer;
        std::optional<ListView> list;

        if (bound->getKind() == ColumnConst<IDType>::staticKind()) {
            appendEntity(static_cast<const ColumnConst<IDType>*>(bound)->getRaw());
        } else if (readBoundConstant(bound, integer)) {
            if (integer) {
                appendNumberedEntity(*integer);
            }
        } else if (readBoundConstant(bound, list)) {
            if (list) {
                appendListedEntities(*list);
            }
        } else {
            bioassert(false, "Bound IDs hold one entity, one integer or one list for the run");
        }
    }
}

template <TypedInternalID IDType>
std::optional<IDType> namedEntity(IDType entity) {
    return entity;
}

template <TypedInternalID IDType, std::integral Integer>
std::optional<IDType> namedEntity(Integer id) {
    IDType entity;
    if (!entityIDOfNumber(id, entity)) {
        return std::nullopt;
    }

    return entity;
}

template <TypedInternalID IDType>
std::optional<IDType> namedEntity(CellView cell) {
    constexpr CellTag ENTITY_TAG = std::same_as<IDType, NodeID> ? CellTag::NodeID : CellTag::EdgeID;
    const CellTag tag = cell.getTag();

    if (tag == CellTag::Int) {
        return namedEntity<IDType>(cell.getAs<types::Int64::Primitive>());
    } else if (tag == CellTag::UInt) {
        return namedEntity<IDType>(cell.getAs<types::UInt64::Primitive>());
    } else if (tag == ENTITY_TAG) {
        return cell.getAs<IDType>();
    }

    return std::nullopt;
}

template <TypedInternalID IDType, TypeConcepts::OptionalType Cell>
std::optional<IDType> namedEntity(const Cell& cell) {
    if (!cell.has_value()) {
        return std::nullopt;
    }

    return namedEntity<IDType>(*cell);
}

template <typename Cell>
void fetchNamedNodes(const Column* ids, const NLExecutionContext* context, ColumnVector<size_t>* indices, ColumnNodeIDs* nodes) {
    const ColumnVectorOf<Cell>* cells = static_cast<const ColumnVectorOf<Cell>*>(ids);
    std::vector<size_t>& rows = indices->getRaw();
    std::vector<NodeID>& fetched = nodes->getRaw();

    const ScannedNodes scanned(*context->getView(), context->getWriteBuffer(), context->getFirstQueryNode());

    const size_t rowCount = cells->size();
    for (size_t row = 0; row < rowCount; row++) {
        const std::optional<NodeID> node = namedEntity<NodeID>(operandRow(cells, row));

        if (node.has_value() && scanned.contains(*node)) {
            rows.push_back(row);
            fetched.push_back(*node);
        }
    }
}

// The rows an edge comes out as: once from its source, and with @param undirected once more
// from its target, as a hop walking either way reaches it from each of its ends
void appendEdgeRows(const EdgeRecord& edge, bool undirected, std::vector<EdgeRecord>& rows) {
    rows.push_back(edge);

    if (undirected) {
        rows.push_back(EdgeRecord {._edgeID = edge._edgeID,
                                   ._nodeID = edge._otherID,
                                   ._otherID = edge._nodeID,
                                   ._edgeTypeID = edge._edgeTypeID});
    }
}

void writeEdgeRows(std::span<const EdgeRecord> rows,
                   ColumnNodeIDs* sources,
                   ColumnEdgeIDs* edgeIDs,
                   ColumnEdgeTypes* edgeTypes,
                   ColumnNodeIDs* targets) {
    const size_t rowCount = rows.size();

    sources->getRaw().resize(rowCount);
    edgeIDs->getRaw().resize(rowCount);
    edgeTypes->getRaw().resize(rowCount);
    targets->getRaw().resize(rowCount);

    std::ranges::transform(rows, sources->getRaw().begin(), &EdgeRecord::_nodeID);
    std::ranges::transform(rows, edgeIDs->getRaw().begin(), &EdgeRecord::_edgeID);
    std::ranges::transform(rows, edgeTypes->getRaw().begin(), &EdgeRecord::_edgeTypeID);
    std::ranges::transform(rows, targets->getRaw().begin(), &EdgeRecord::_otherID);
}

template <typename Cell>
void fetchNamedEdges(const NLExecutionContext* context, NLFetchEdgesData* fetch) {
    const ColumnVectorOf<Cell>* cells = static_cast<const ColumnVectorOf<Cell>*>(fetch->getIDs());
    std::vector<size_t>& indices = fetch->getIndices()->getRaw();
    const bool undirected = fetch->isUndirected();

    const ScannedEdges scanned(*context->getView(), context->getWriteBuffer(), context->getFirstQueryEdge());

    std::vector<EdgeRecord>& rows = fetch->getRows();
    rows.clear();

    const size_t rowCount = cells->size();
    for (size_t row = 0; row < rowCount; row++) {
        const std::optional<EdgeID> edge = namedEntity<EdgeID>(operandRow(cells, row));
        if (!edge.has_value() || !scanned.contains(*edge)) {
            continue;
        }

        EdgeRecord record;
        scanned.readEdge(*edge, record);

        const size_t firstRow = rows.size();
        appendEdgeRows(record, undirected, rows);
        indices.insert(indices.end(), rows.size() - firstRow, row);
    }

    writeEdgeRows(rows, fetch->getSources(), fetch->getEdgeIDs(), fetch->getEdgeTypes(), fetch->getTargets());
}

template <TypedInternalID IDType, typename Cell>
constexpr NLFetchFunction<IDType> fetchNamedEntities() {
    if constexpr (std::same_as<IDType, NodeID>) {
        return &fetchNamedNodes<Cell>;
    } else {
        return &fetchNamedEdges<Cell>;
    }
}

// Execute a scan loop: the rows the graph holds, then the ones this change has written,
// which the pending step reads once the committed one is drained. @param rows is the chunk
// a step fills that measures it - the node chunk of a node scan, the source chunk of an
// edge scan.
template <typename ChunkWriterType, typename PendingScanType>
void runScanLoopSteps(NLExecutionContext* context,
                      ChunkWriterType* chunkWriter,
                      PendingScanType* pendingScan,
                      const ColumnNodeIDs* rows,
                      const NLStmtContainer* loopBody,
                      const NLLimitState* limit) {
    const auto hasStep = [&]() {
        return chunkWriter->isValid() || pendingScan->isValid();
    };

    const auto runIteration = [&]() {
        if (chunkWriter->isValid()) {
            chunkWriter->fill(context->getChunkSize());
        } else {
            pendingScan->fill(context->getChunkSize());
        }

        if (rows->empty()) {
            return;
        }

        runBody(context, loopBody);
    };

    if (limit) {
        while (hasStep() && limit->getRemaining() > 0) {
            runIteration();
        }
    } else {
        while (hasStep()) {
            runIteration();
        }
    }
}

// The searched value as the write buffer holds one, so a scan can compare it against what
// a pending node was written with. A string the scan borrows is copied: the buffer's own
// values are owning.
template <typename T>
CommitWriteBuffer::SupportedTypeVariant pendingValueOf(typename T::Primitive value) {
    if constexpr (std::is_same_v<T, types::String>) {
        return std::optional<std::string> {std::string(value)};
    } else {
        return std::optional<typename T::Primitive> {value};
    }
}

}

void NLExecutor::runScanNodesLoop(NLExecutionContext* context, NLFunctionData* data) {
    NLScanLoopData* loopData = static_cast<NLScanLoopData*>(data);
    const NLStmtContainer* loopBody = loopData->getStmts();
    ColumnNodeIDs* nodeIDs = loopData->getNodeIDs();

    // A null limit leaves the loop unbounded; otherwise it stops once the budget
    // is spent. nl.limit_update inside runBody mutates remaining, so the next
    // test breaks here, and an enclosing loop carrying the same handle breaks on
    // its next test too - unwinding the whole nest. LIMIT 0 fails the guard on
    // entry, so nothing is scanned. The limit is fixed for the whole loop, so
    // the null check is hoisted out of the per-iteration condition.
    const NLLimitState* limit = loopData->getLimit();

    ScanNodesChunkWriter chunkWriter(*context->getView());
    chunkWriter.setNodeIDs(nodeIDs);

    NLPendingNodeScan pendingNodes(context, nodeIDs);

    runScanLoopSteps(context, &chunkWriter, &pendingNodes, nodeIDs, loopBody, limit);
}

void NLExecutor::runScanNodesByLabelLoop(NLExecutionContext* context, NLFunctionData* data) {
    NLScanByLabelLoopData* loopData = static_cast<NLScanByLabelLoopData*>(data);

    // A requested label was absent from the schema, so no node carries the full
    // conjunction: the scan matches nothing and the loop body never runs.
    if (!loopData->isMatchable()) {
        return;
    }

    const NLStmtContainer* loopBody = loopData->getStmts();
    ColumnNodeIDs* nodeIDs = loopData->getNodeIDs();

    // A null limit leaves the loop unbounded, exactly as in runScanNodesLoop.
    const NLLimitState* limit = loopData->getLimit();

    // The LabelSetHandle borrows the loop data's owned LabelSet, which lives for
    // the whole program, so the handle stays valid for every fill below.
    const GraphView& view = *context->getView();
    const LabelSetHandle labelset(loopData->getLabelSet());

    ScanNodesByLabelChunkWriter chunkWriter(view, labelset);
    chunkWriter.setNodeIDs(nodeIDs);

    NLPendingNodeScan pendingNodes(context, nodeIDs);
    pendingNodes.setLabelSet(loopData->getLabelSet());

    runScanLoopSteps(context, &chunkWriter, &pendingNodes, nodeIDs, loopBody, limit);
}

template <SupportedType T>
void NLExecutor::runScanNodesByPropertyValueLoopAs(NLExecutionContext* context,
                                                   NLScanByPropertyValueLoopData* loopData,
                                                   const NLScanByPropertyValueLoopData::Literal& literal) {
    const NLStmtContainer* loopBody = loopData->getStmts();
    ColumnNodeIDs* nodeIDs = loopData->getNodeIDs();
    const NLLimitState* limit = loopData->getLimit();

    // The label set is owned by the loop data, which lives for the whole program, and the
    // literal outlives the loop, so a string view of the one and a handle on the other stay
    // valid for every fill below.
    const typename T::Primitive value = propertyScanLiteralAs<T>(literal);
    const LabelSetHandle labelset = loopData->isByLabel() ? LabelSetHandle(loopData->getLabelSet()) : LabelSetHandle();

    NLPropertyValueScan<T> graphNodes(context, nodeIDs, loopData->getPropertyTypeID(), value, labelset);

    NLPendingNodeScan pendingNodes(context, nodeIDs);
    pendingNodes.setProperty(loopData->getPropertyTypeID(), pendingValueOf<T>(value));

    if (loopData->isByLabel()) {
        pendingNodes.setLabelSet(loopData->getLabelSet());
    }

    runScanLoopSteps(context, &graphNodes, &pendingNodes, nodeIDs, loopBody, limit);
}

void NLExecutor::runScanNodesByPropertyValueLoop(NLExecutionContext* context, NLFunctionData* data) {
    NLScanByPropertyValueLoopData* loopData = static_cast<NLScanByPropertyValueLoopData*>(data);
    if (!loopData->isMatchable()) {
        return;
    }

    const ValueType valueType = loopData->getValueType();

    NLScanByPropertyValueLoopData::Literal boundLiteral;
    const Column* bound = loopData->getBoundValue();
    if (bound) {
        NLScanByPropertyValueLoopData::Literal given;
        const bool readsBound = readBoundPropertyValue(bound, loopData->getBoundType(), given);
        if (!readsBound || !NLScanByPropertyValueLoopData::convertToStored(valueType, given, boundLiteral)) {
            return;
        }
    }

    const NLScanByPropertyValueLoopData::Literal& literal = bound ? boundLiteral : loopData->getLiteral();

    switch (valueType) {
        case ValueType::Int64:
            runScanNodesByPropertyValueLoopAs<types::Int64>(context, loopData, literal);
        break;
        case ValueType::UInt64:
            runScanNodesByPropertyValueLoopAs<types::UInt64>(context, loopData, literal);
        break;
        case ValueType::Double:
            runScanNodesByPropertyValueLoopAs<types::Double>(context, loopData, literal);
        break;
        case ValueType::Bool:
            runScanNodesByPropertyValueLoopAs<types::Bool>(context, loopData, literal);
        break;
        case ValueType::String:
            runScanNodesByPropertyValueLoopAs<types::String>(context, loopData, literal);
        break;
        default:
            throw IRException("scan_nodes_by_property_value cannot scan this property type");
        break;
    }
}

void NLExecutor::runConstScanNodesLoop(NLExecutionContext* context, NLFunctionData* data) {
    NLConstScanLoopData* loopData = static_cast<NLConstScanLoopData*>(data);
    const NLStmtContainer* loopBody = loopData->getStmts();
    ColumnNodeIDs* nodeIDs = loopData->getNodeIDs();
    const size_t chunkSize = context->getChunkSize();
    const GraphView* view = context->getView();

    const ScannedNodes scanned(*view, context->getWriteBuffer(), context->getFirstQueryNode());

    std::vector<NodeID>& listed = loopData->getListedNodeIDs();
    listed.clear();
    std::ranges::copy_if(loopData->getConstNodeIDs(), std::back_inserter(listed), [&scanned](NodeID node) {
        return scanned.contains(node);
    });

    if (loopData->hasBoundIDs()) {
        appendBoundIDs(loopData->getBoundIDs(), scanned, listed);

        std::ranges::sort(listed);
        listed.erase(std::unique(listed.begin(), listed.end()), listed.end());
    }

    const std::span<const NodeID> constNodeIDs = listed;
    const size_t totalCount = constNodeIDs.size();

    // A null limit leaves the loop unbounded, exactly as in runScanNodesLoop.
    const NLLimitState* limit = loopData->getLimit();

    // Emit the fixed node ID list one chunk at a time: each step copies the next
    // slice into the loop's node chunk and runs the body over it. The cursor is
    // local to this call, so a const scan nested in a cross product restarts from
    // the first ID on every outer step, the same way runScanNodesLoop opens a
    // fresh chunk writer each call.
    size_t cursor = 0;

    const auto runIteration = [&]() {
        const size_t remaining = totalCount - cursor;
        const size_t rows = std::min(chunkSize, remaining);

        std::vector<NodeID>& raw = nodeIDs->getRaw();
        raw.assign(constNodeIDs.begin() + cursor, constNodeIDs.begin() + cursor + rows);

        cursor += rows;

        // The IDs were checked when the loop started, before any DELETE of this query ran
        if (view->hasDeletedNodes()) {
            std::erase_if(raw, [view](NodeID node) { return view->isDeleted(node); });
        }

        if (raw.empty()) {
            return;
        }

        runBody(context, loopBody);
    };

    if (limit) {
        while (cursor < totalCount && limit->getRemaining() > 0) {
            runIteration();
        }
    } else {
        while (cursor < totalCount) {
            runIteration();
        }
    }
}

void NLExecutor::runConstScanEdgesLoop(NLExecutionContext* context, NLFunctionData* data) {
    NLConstScanEdgesLoopData* loopData = static_cast<NLConstScanEdgesLoopData*>(data);
    const NLStmtContainer* loopBody = loopData->getStmts();
    const size_t chunkSize = context->getChunkSize();
    const GraphView* view = context->getView();

    ColumnNodeIDs* sources = loopData->getSources();
    ColumnEdgeIDs* edgeIDs = loopData->getEdgeIDs();
    ColumnEdgeTypes* edgeTypes = loopData->getEdgeTypes();
    ColumnNodeIDs* targets = loopData->getTargets();

    const ScannedEdges scanned(*view, context->getWriteBuffer(), context->getFirstQueryEdge());

    std::vector<EdgeID>& listed = loopData->getListedEdgeIDs();
    listed.clear();
    std::ranges::copy_if(loopData->getConstEdgeIDs(), std::back_inserter(listed), [&scanned](EdgeID edge) {
        return scanned.contains(edge);
    });

    const std::span<const Column* const> boundIDs = loopData->getBoundIDs();
    if (!boundIDs.empty()) {
        appendBoundIDs(boundIDs, scanned, listed);

        std::ranges::sort(listed);
        listed.erase(std::unique(listed.begin(), listed.end()), listed.end());
    }

    std::vector<EdgeRecord>& rows = loopData->getListedRows();
    rows.clear();

    for (const EdgeID edge : listed) {
        EdgeRecord record;
        scanned.readEdge(edge, record);
        appendEdgeRows(record, loopData->isUndirected(), rows);
    }

    const size_t totalCount = rows.size();
    const NLLimitState* limit = loopData->getLimit();
    size_t cursor = 0;
    std::vector<EdgeRecord> kept;

    const auto runIteration = [&]() {
        const size_t count = std::min(chunkSize, totalCount - cursor);
        std::span<const EdgeRecord> slice = std::span<const EdgeRecord>(rows).subspan(cursor, count);
        cursor += count;

        // The edges were checked when the loop started, before any DELETE of this query ran
        if (view->hasDeletedEdges()) {
            kept.clear();
            std::ranges::copy_if(slice, std::back_inserter(kept), [view](const EdgeRecord& row) {
                return !view->isDeleted(row._edgeID);
            });
            slice = kept;
        }

        if (slice.empty()) {
            return;
        }

        writeEdgeRows(slice, sources, edgeIDs, edgeTypes, targets);
        runBody(context, loopBody);
    };

    if (limit) {
        while (cursor < totalCount && limit->getRemaining() > 0) {
            runIteration();
        }
    } else {
        while (cursor < totalCount) {
            runIteration();
        }
    }
}

void NLExecutor::runScanEdgesLoop(NLExecutionContext* context, NLFunctionData* data) {
    NLScanEdgesLoopData* loopData = static_cast<NLScanEdgesLoopData*>(data);
    const NLStmtContainer* loopBody = loopData->getStmts();
    ColumnNodeIDs* sources = loopData->getSources();

    // A null limit leaves the loop unbounded; otherwise it stops once the budget
    // is spent, exactly as in runScanNodesLoop.
    const NLLimitState* limit = loopData->getLimit();

    ScanEdgesChunkWriter chunkWriter(*context->getView());
    chunkWriter.setSrcIDs(sources);
    chunkWriter.setEdgeIDs(loopData->getEdgeIDs());
    chunkWriter.setEdgeTypes(loopData->getEdgeTypes());
    chunkWriter.setTgtIDs(loopData->getTargets());

    NLPendingEdgeScan pendingEdges(context, loopData);

    // The four columns are row-aligned, so the source column measures the step.
    runScanLoopSteps(context, &chunkWriter, &pendingEdges, sources, loopBody, limit);
}

void NLExecutor::runScanEdgesByTypeLoop(NLExecutionContext* context, NLFunctionData* data) {
    NLScanEdgesByTypeLoopData* loopData = static_cast<NLScanEdgesByTypeLoopData*>(data);

    // No type the pattern named is in the schema, so no edge can match and the loop body never runs.
    if (!loopData->isMatchable()) {
        return;
    }

    const NLStmtContainer* loopBody = loopData->getStmts();
    ColumnNodeIDs* sources = loopData->getSources();

    const NLLimitState* limit = loopData->getLimit();

    ScanEdgesByTypeChunkWriter chunkWriter(*context->getView(), loopData->getRequestedTypes());
    chunkWriter.setSrcIDs(sources);
    chunkWriter.setEdgeIDs(loopData->getEdgeIDs());
    chunkWriter.setEdgeTypes(loopData->getEdgeTypes());
    chunkWriter.setTgtIDs(loopData->getTargets());

    NLPendingEdgeScan pendingEdges(context, loopData);
    pendingEdges.setEdgeTypes(loopData->getRequestedTypes());

    runScanLoopSteps(context, &chunkWriter, &pendingEdges, sources, loopBody, limit);
}

void NLExecutor::runScanEdgesBySourceLabelLoop(NLExecutionContext* context, NLFunctionData* data) {
    NLScanEdgesByLabelLoopData* loopData = static_cast<NLScanEdgesByLabelLoopData*>(data);

    // A requested label was absent from the schema, so no node carries the full
    // conjunction: the scan matches no edge and the loop body never runs.
    if (!loopData->isMatchable()) {
        return;
    }

    const NLStmtContainer* loopBody = loopData->getStmts();
    ColumnNodeIDs* sources = loopData->getSources();

    const NLLimitState* limit = loopData->getLimit();

    // The LabelSetHandle borrows the loop data's owned LabelSet, which lives for the
    // whole program, so the handle stays valid for every fill below.
    const LabelSetHandle labelset(loopData->getLabelSet());

    ScanOutEdgesBySourceLabelChunkWriter chunkWriter(*context->getView(), labelset);
    chunkWriter.setSrcIDs(sources);
    chunkWriter.setEdgeIDs(loopData->getEdgeIDs());
    chunkWriter.setEdgeTypes(loopData->getEdgeTypes());
    chunkWriter.setTgtIDs(loopData->getTargets());

    NLPendingEdgeScan pendingEdges(context, loopData);
    pendingEdges.setSourceLabelSet(loopData->getLabelSet());

    runScanLoopSteps(context, &chunkWriter, &pendingEdges, sources, loopBody, limit);
}

void NLExecutor::runScanEdgesByTargetLabelLoop(NLExecutionContext* context, NLFunctionData* data) {
    NLScanEdgesByLabelLoopData* loopData = static_cast<NLScanEdgesByLabelLoopData*>(data);

    if (!loopData->isMatchable()) {
        return;
    }

    const NLStmtContainer* loopBody = loopData->getStmts();
    ColumnNodeIDs* sources = loopData->getSources();

    const NLLimitState* limit = loopData->getLimit();

    const LabelSetHandle labelset(loopData->getLabelSet());

    // The writer names its two node columns after the walk rather than after the edge:
    // the labelled end it hangs each edge off goes to setSrcIDs, and here that end is the
    // edge's target. The chunks hold the edge as the graph stores it, the way
    // nl.get_in_edges fills them, so the two are crossed here.
    ScanInEdgesByTargetLabelChunkWriter chunkWriter(*context->getView(), labelset);
    chunkWriter.setSrcIDs(loopData->getTargets());
    chunkWriter.setEdgeIDs(loopData->getEdgeIDs());
    chunkWriter.setEdgeTypes(loopData->getEdgeTypes());
    chunkWriter.setTgtIDs(sources);

    NLPendingEdgeScan pendingEdges(context, loopData);
    pendingEdges.setTargetLabelSet(loopData->getLabelSet());

    runScanLoopSteps(context, &chunkWriter, &pendingEdges, sources, loopBody, limit);
}

void NLExecutor::runScanEdgesByEndpointLabelsFromSourceLoop(NLExecutionContext* context, NLFunctionData* data) {
    NLScanEdgesByEndpointLabelsLoopData* loopData = static_cast<NLScanEdgesByEndpointLabelsLoopData*>(data);

    if (!loopData->isMatchable()) {
        return;
    }

    const NLStmtContainer* loopBody = loopData->getStmts();
    ColumnNodeIDs* sources = loopData->getSources();

    const NLLimitState* limit = loopData->getLimit();

    const LabelSet& sourceLabelSet = loopData->getLabelSet();
    const LabelSet& targetLabelSet = loopData->getOtherEndLabelSet();
    const LabelSetHandle sourceLabels(sourceLabelSet);
    const LabelSetHandle targetLabels(targetLabelSet);

    ScanOutEdgesByBothLabelsChunkWriter chunkWriter(*context->getView(), sourceLabels, targetLabels);
    chunkWriter.setSrcIDs(sources);
    chunkWriter.setEdgeIDs(loopData->getEdgeIDs());
    chunkWriter.setEdgeTypes(loopData->getEdgeTypes());
    chunkWriter.setTgtIDs(loopData->getTargets());

    NLPendingEdgeScan pendingEdges(context, loopData);
    pendingEdges.setSourceLabelSet(sourceLabelSet);
    pendingEdges.setTargetLabelSet(targetLabelSet);

    runScanLoopSteps(context, &chunkWriter, &pendingEdges, sources, loopBody, limit);
}

void NLExecutor::runScanEdgesByEndpointLabelsFromTargetLoop(NLExecutionContext* context, NLFunctionData* data) {
    NLScanEdgesByEndpointLabelsLoopData* loopData = static_cast<NLScanEdgesByEndpointLabelsLoopData*>(data);

    if (!loopData->isMatchable()) {
        return;
    }

    const NLStmtContainer* loopBody = loopData->getStmts();
    ColumnNodeIDs* sources = loopData->getSources();

    const NLLimitState* limit = loopData->getLimit();

    const LabelSet& targetLabelSet = loopData->getLabelSet();
    const LabelSet& sourceLabelSet = loopData->getOtherEndLabelSet();
    const LabelSetHandle targetLabels(targetLabelSet);
    const LabelSetHandle sourceLabels(sourceLabelSet);

    // The writer fills the indexed end through setSrcIDs, which here is the edge's target,
    // so the two node columns cross as in runScanEdgesByTargetLabelLoop.
    ScanInEdgesByBothLabelsChunkWriter chunkWriter(*context->getView(), targetLabels, sourceLabels);
    chunkWriter.setSrcIDs(loopData->getTargets());
    chunkWriter.setEdgeIDs(loopData->getEdgeIDs());
    chunkWriter.setEdgeTypes(loopData->getEdgeTypes());
    chunkWriter.setTgtIDs(sources);

    NLPendingEdgeScan pendingEdges(context, loopData);
    pendingEdges.setTargetLabelSet(targetLabelSet);
    pendingEdges.setSourceLabelSet(sourceLabelSet);

    runScanLoopSteps(context, &chunkWriter, &pendingEdges, sources, loopBody, limit);
}

void NLExecutor::runFetchNodes(NLExecutionContext* context, NLFunctionData* data) {
    NLFetchNodesData* fetch = static_cast<NLFetchNodesData*>(data);

    ColumnVector<size_t>* indices = fetch->getIndices();
    ColumnNodeIDs* nodes = fetch->getNodes();

    indices->getRaw().clear();
    nodes->getRaw().clear();

    fetch->getFetch()(fetch->getIDs(), context, indices, nodes);

    for (const NLFilterData::FilterColumn& column : fetch->getColumns()) {
        column._gather(column._input, indices, column._output);
    }
}

void NLExecutor::runFetchEdges(NLExecutionContext* context, NLFunctionData* data) {
    NLFetchEdgesData* fetch = static_cast<NLFetchEdgesData*>(data);

    ColumnVector<size_t>* indices = fetch->getIndices();
    indices->getRaw().clear();

    fetch->getFetch()(context, fetch);

    for (const NLFilterData::FilterColumn& column : fetch->getColumns()) {
        column._gather(column._input, indices, column._output);
    }
}

template <TypedInternalID IDType>
NLFetchFunction<IDType> NLExecutor::selectEntityFetchFunction() {
    return fetchNamedEntities<IDType, IDType>();
}

template <TypedInternalID IDType>
NLFetchFunction<IDType> NLExecutor::selectIntegerFetchFunction(NLChunkKind kind, bool nullable) {
    switch (kind) {
        case NLChunkKind::Int64:
            return nullable ? fetchNamedEntities<IDType, std::optional<types::Int64::Primitive>>()
                            : fetchNamedEntities<IDType, types::Int64::Primitive>();
        break;

        case NLChunkKind::UInt64:
            return nullable ? fetchNamedEntities<IDType, std::optional<types::UInt64::Primitive>>()
                            : fetchNamedEntities<IDType, types::UInt64::Primitive>();
        break;

        default:
            throw IRException("A fetch reads node or edge IDs out of entities, integers or tagged cells");
        break;
    }
}

template <TypedInternalID IDType>
NLFetchFunction<IDType> NLExecutor::selectTaggedFetchFunction() {
    return fetchNamedEntities<IDType, CellView>();
}

template NLFetchFunction<NodeID> NLExecutor::selectEntityFetchFunction<NodeID>();
template NLFetchFunction<EdgeID> NLExecutor::selectEntityFetchFunction<EdgeID>();
template NLFetchFunction<NodeID> NLExecutor::selectIntegerFetchFunction<NodeID>(NLChunkKind kind, bool nullable);
template NLFetchFunction<EdgeID> NLExecutor::selectIntegerFetchFunction<EdgeID>(NLChunkKind kind, bool nullable);
template NLFetchFunction<NodeID> NLExecutor::selectTaggedFetchFunction<NodeID>();
template NLFetchFunction<EdgeID> NLExecutor::selectTaggedFetchFunction<EdgeID>();
