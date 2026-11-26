#include "NLExecutor.h"

#include <stdint.h>
#include <type_traits>

#include "ID.h"
#include "iterators/GetEdgeTypesIterator.h"
#include "iterators/GetNodeLabelSetIterator.h"
#include "iterators/GetPropertiesWithNullIterator.h"
#include "columns/ColumnOptVector.h"
#include "columns/ColumnVector.h"
#include "metadata/PropertyType.h"

#include "versioning/CommitWriteBuffer.h"
#include "views/GraphView.h"

#include "NLProgram.h"
#include "NLRuntimeProperty.h"
#include "NLWrittenValues.h"
#include "NLColumnKernels.h"
#include "NLPendingRows.h"

#include "BioAssert.h"

using namespace db;

namespace {

template <typename T>
std::optional<typename T::Primitive> readPendingProperty(NLWrittenValues& written,
                                                         NLValueStore& values,
                                                         const CommitWriteBuffer::UntypedProperties& properties,
                                                         PropertyTypeID propertyTypeID) {
    for (const CommitWriteBuffer::UntypedProperty& property : properties) {
        if (property.propertyID == propertyTypeID) {
            return written.read<T>(property.value, values);
        }
    }

    return std::nullopt;
}

// A pending row names its entity by the ID it will commit as, so the write buffer is
// indexed by what is left once the committed space is taken off
template <typename ID, typename T>
std::optional<typename T::Primitive> readPendingEntityProperty(CommitWriteBuffer* writeBuffer,
                                                               NLWrittenValues& written,
                                                               NLValueStore& values,
                                                               const GraphView* view,
                                                               uint64_t id,
                                                               PropertyTypeID propertyTypeID) {
    if constexpr (std::is_same_v<ID, NodeID>) {
        const size_t offset = id - committedNodeCount(view);
        return readPendingProperty<T>(written, values, writeBuffer->getPendingNode(offset).properties, propertyTypeID);
    } else {
        const size_t offset = id - committedEdgeCount(view);
        return readPendingProperty<T>(written, values, writeBuffer->getPendingEdge(offset).properties, propertyTypeID);
    }
}

}

// Read one property of the current input chunk into a nullable value column.
// The with-null writer emits one value per input row (null where the row lacks
// it), so no row is dropped and the value column lines up with the input chunk.
// The PropertyTypeID was resolved from the name during translation.
template <typename ID, typename T>
void NLExecutor::runPropertyFetch(NLExecutionContext* context, NLFunctionData* data) {
    NLPropertyFetchData* fetchData = static_cast<NLPropertyFetchData*>(data);

    const GraphView& view = *context->getView();
    const PropertyTypeID propertyTypeID = fetchData->getPropertyTypeID();
    const auto* inputIDs = static_cast<const ColumnVector<ID>*>(fetchData->getInput());
    auto* output = static_cast<ColumnOptVector<typename T::Primitive>*>(fetchData->getOutput());

    GetPropertiesWithNullChunkWriter<ID, T> writer(view, propertyTypeID, inputIDs);
    writer.setOutput(output);
    writer.fill(inputIDs->size());

    // The fetch above read the graph, which holds this change's writes nowhere: a row
    // whose entity the change wrote names it by the ID it will commit as, which the graph
    // holds nothing under, and a row it merely updated still reads the value from before.
    const ColumnVector<CustomBool>* pending = fetchData->getPending();
    const bool allPending = fetchData->isAllPending();
    CommitWriteBuffer* writeBuffer = context->getWriteBuffer();

    if (!writeBuffer) {
        bioassert(!pending && !allPending,
                  "a property fetch over written entities requires an active write transaction");
        return;
    }

    NLWrittenValues& written = context->getWrittenValues();
    written.indexUpdates(writeBuffer);

    NLValueStore& fetchedValues = fetchData->fetchedValues();
    fetchedValues.clear();

    const bool isNode = std::is_same_v<ID, NodeID>;
    const size_t committedCount = isNode ? committedNodeCount(&view) : committedEdgeCount(&view);
    const size_t pendingCount = isNode ? writeBuffer->numPendingNodes() : writeBuffer->numPendingEdges();
    const PendingRows pendingRows(pending, allPending, committedCount, pendingCount);

    if (!pending && !allPending && !written.hasUpdates() && pendingCount == 0) {
        return;
    }

    const auto& inputRaw = inputIDs->getRaw();
    const size_t rowCount = output->size();
    for (size_t row = 0; row < rowCount; row++) {
        if (pendingRows.has(row, inputRaw[row].getValue())) {
            output->setOptional(row,
                                readPendingEntityProperty<ID, T>(writeBuffer,
                                                                 written,
                                                                 fetchedValues,
                                                                 &view,
                                                                 inputRaw[row].getValue(),
                                                                 propertyTypeID));
            continue;
        }

        const CommitWriteBuffer::SupportedTypeVariant* update = written.findUpdate(inputRaw[row], propertyTypeID);
        if (update) {
            output->setOptional(row, written.read<T>(*update, fetchedValues));
        }
    }

    output->dropAllValidMask();
}

// The translator selects among these by the value type the property resolves
// to, on the node or edge side; only these (ID, T) pairs are available as
// handlers.
template void NLExecutor::runPropertyFetch<NodeID, types::Int64>(NLExecutionContext*, NLFunctionData*);
template void NLExecutor::runPropertyFetch<NodeID, types::UInt64>(NLExecutionContext*, NLFunctionData*);
template void NLExecutor::runPropertyFetch<NodeID, types::Double>(NLExecutionContext*, NLFunctionData*);
template void NLExecutor::runPropertyFetch<NodeID, types::Bool>(NLExecutionContext*, NLFunctionData*);
template void NLExecutor::runPropertyFetch<NodeID, types::String>(NLExecutionContext*, NLFunctionData*);
template void NLExecutor::runPropertyFetch<NodeID, types::Embedding>(NLExecutionContext*, NLFunctionData*);
template void NLExecutor::runPropertyFetch<NodeID, types::List>(NLExecutionContext*, NLFunctionData*);
template void NLExecutor::runPropertyFetch<NodeID, types::DateTime>(NLExecutionContext*, NLFunctionData*);
template void NLExecutor::runPropertyFetch<NodeID, types::Duration>(NLExecutionContext*, NLFunctionData*);
template void NLExecutor::runPropertyFetch<NodeID, types::Point>(NLExecutionContext*, NLFunctionData*);
template void NLExecutor::runPropertyFetch<NodeID, types::Map>(NLExecutionContext*, NLFunctionData*);
template void NLExecutor::runPropertyFetch<EdgeID, types::Int64>(NLExecutionContext*, NLFunctionData*);
template void NLExecutor::runPropertyFetch<EdgeID, types::UInt64>(NLExecutionContext*, NLFunctionData*);
template void NLExecutor::runPropertyFetch<EdgeID, types::Double>(NLExecutionContext*, NLFunctionData*);
template void NLExecutor::runPropertyFetch<EdgeID, types::Bool>(NLExecutionContext*, NLFunctionData*);
template void NLExecutor::runPropertyFetch<EdgeID, types::String>(NLExecutionContext*, NLFunctionData*);
template void NLExecutor::runPropertyFetch<EdgeID, types::Embedding>(NLExecutionContext*, NLFunctionData*);
template void NLExecutor::runPropertyFetch<EdgeID, types::List>(NLExecutionContext*, NLFunctionData*);
template void NLExecutor::runPropertyFetch<EdgeID, types::DateTime>(NLExecutionContext*, NLFunctionData*);
template void NLExecutor::runPropertyFetch<EdgeID, types::Duration>(NLExecutionContext*, NLFunctionData*);
template void NLExecutor::runPropertyFetch<EdgeID, types::Point>(NLExecutionContext*, NLFunctionData*);
template void NLExecutor::runPropertyFetch<EdgeID, types::Map>(NLExecutionContext*, NLFunctionData*);

void NLExecutor::runGetNodeLabelSet(NLExecutionContext* context, NLFunctionData* data) {
    NLGetNodeLabelSetData* fetchData = static_cast<NLGetNodeLabelSetData*>(data);

    const GraphView& view = *context->getView();
    const ColumnNodeIDs* input = fetchData->getInput();
    ColumnLabelSetIDs* output = fetchData->getOutput();

    GetNodeLabelSetChunkWriter writer(view, input);
    writer.setLabelSetIDs(output);
    writer.fill(input->size());

    // The graph holds no label set for a node this change wrote: the write buffer holds the
    // handle the CREATE spelled, under the ID the commit will know the set by.
    CommitWriteBuffer* writeBuffer = context->getWriteBuffer();
    if (!writeBuffer || writeBuffer->numPendingNodes() == 0) {
        return;
    }

    const size_t committedCount = committedNodeCount(&view);
    const PendingRows pendingRows(nullptr, false, committedCount, writeBuffer->numPendingNodes());

    auto& raw = output->getRaw();
    const auto& inputRaw = input->getRaw();
    for (size_t row = 0; row < raw.size(); row++) {
        const uint64_t nodeID = inputRaw[row].getValue();
        if (!pendingRows.has(row, nodeID)) {
            continue;
        }

        raw[row] = writeBuffer->getPendingNode(nodeID - committedCount).labelsetHandle.getID();
    }
}

void NLExecutor::runGetEdgeTypes(NLExecutionContext* context, NLFunctionData* data) {
    NLGetEdgeTypesData* fetchData = static_cast<NLGetEdgeTypesData*>(data);

    const GraphView& view = *context->getView();
    const ColumnEdgeIDs* input = fetchData->getInput();
    ColumnEdgeTypes* output = fetchData->getOutput();

    GetEdgeTypesChunkWriter writer(view, input);
    writer.setEdgeTypes(output);
    writer.fill(input->size());

    // The graph holds no type for an edge this change wrote: the write buffer holds the one
    // the CREATE spelled, under the ID the commit will know the edge by.
    CommitWriteBuffer* writeBuffer = context->getWriteBuffer();
    if (!writeBuffer || writeBuffer->numPendingEdges() == 0) {
        return;
    }

    const size_t committedCount = committedEdgeCount(&view);
    const PendingRows pendingRows(nullptr, false, committedCount, writeBuffer->numPendingEdges());

    auto& raw = output->getRaw();
    const auto& inputRaw = input->getRaw();
    for (size_t row = 0; row < raw.size(); row++) {
        const uint64_t edgeID = inputRaw[row].getValue();
        if (!pendingRows.has(row, edgeID)) {
            continue;
        }

        raw[row] = writeBuffer->getPendingEdge(edgeID - committedCount).edgeType;
    }
}

template <typename MaskColumn>
void NLExecutor::runCheckLabelConstraint(NLExecutionContext* context, NLFunctionData* data) {
    NLCheckLabelConstraintData* checkData = static_cast<NLCheckLabelConstraintData*>(data);

    const ColumnLabelSetIDs* input = checkData->getInput();
    MaskColumn* output = static_cast<MaskColumn*>(checkData->getOutput());

    const size_t rowCount = input->size();
    resetForRowWrites(output, rowCount);

    for (size_t rowIndex = 0; rowIndex < rowCount; rowIndex++) {
        const LabelSetID id = (*input)[rowIndex];

        if constexpr (std::is_same_v<MaskColumn, ColumnOptVector<CustomBool>>) {
            if (!id.isValid()) {
                output->setNull(rowIndex);
                continue;
            }
        }

        output->set(rowIndex, CustomBool {checkData->isMatching(id)});
    }
}

template void NLExecutor::runCheckLabelConstraint<ColumnVector<CustomBool>>(NLExecutionContext*, NLFunctionData*);
template void NLExecutor::runCheckLabelConstraint<ColumnOptVector<CustomBool>>(NLExecutionContext*, NLFunctionData*);

template <typename MaskColumn>
void NLExecutor::runCheckEdgeTypeConstraint(NLExecutionContext* context, NLFunctionData* data) {
    NLCheckEdgeTypeConstraintData* checkData = static_cast<NLCheckEdgeTypeConstraintData*>(data);

    const ColumnEdgeTypes* input = checkData->getInput();
    MaskColumn* output = static_cast<MaskColumn*>(checkData->getOutput());

    const size_t rowCount = input->size();
    resetForRowWrites(output, rowCount);

    for (size_t rowIndex = 0; rowIndex < rowCount; rowIndex++) {
        const EdgeTypeID id = (*input)[rowIndex];

        if constexpr (std::is_same_v<MaskColumn, ColumnOptVector<CustomBool>>) {
            if (!id.isValid()) {
                output->setNull(rowIndex);
                continue;
            }
        }

        output->set(rowIndex, CustomBool {checkData->isMatching(id)});
    }
}

template void NLExecutor::runCheckEdgeTypeConstraint<ColumnVector<CustomBool>>(NLExecutionContext*, NLFunctionData*);
template void NLExecutor::runCheckEdgeTypeConstraint<ColumnOptVector<CustomBool>>(NLExecutionContext*, NLFunctionData*);
