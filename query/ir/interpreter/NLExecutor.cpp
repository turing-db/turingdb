#include "NLExecutor.h"

#include <algorithm>
#include <type_traits>

#include "iterators/GetInEdgesIterator.h"
#include "iterators/GetOutEdgesIterator.h"
#include "iterators/GetPropertiesWithNullIterator.h"
#include "iterators/ScanNodesIterator.h"
#include "columns/ColumnOptVector.h"
#include "metadata/PropertyType.h"

#include "NLProgram.h"
#include "NLOutputSink.h"

#include "IRException.h"
#include "BioAssert.h"

using namespace db;

namespace {

// Execute a body of statements
void runBody(NLExecutionContext* context, const NLStmtContainer* body) {
    for (const NLFunctionDescriptor& descriptor : body->stmts()) {
        const auto func = descriptor.getFunction();
        NLFunctionData* funcData = descriptor.getData();
        func(context, funcData);
    }
}

// Gather rows of a carried column by applying indices
template <typename ElementType>
void gatherColumn(const Column* input,
                  const ColumnVector<size_t>* indices,
                  Column* output) {
    const ColumnVector<ElementType>* typedInput = static_cast<const ColumnVector<ElementType>*>(input);
    ColumnVector<ElementType>* typedOutput = static_cast<ColumnVector<ElementType>*>(output);

    typedOutput->clear();
    typedOutput->reserve(indices->size());
    const auto& indicesRaw = indices->getRaw();
    auto& typedInputRaw = typedInput->getRaw();

    for (const size_t index : indicesRaw) {
        typedOutput->push_back(typedInputRaw[index]);
    }
}

// Block-repeat: each input row is emitted `factor` times in a row, so an input
// of N rows becomes N*factor rows with input row i at output indices
// [i*factor, (i+1)*factor). This lays out an outer column of a cross product,
// where each outer row pairs with the whole inner chunk.
template <typename ElementType>
void blockRepeatColumn(const Column* input, size_t factor, Column* output) {
    const ColumnVector<ElementType>* typedInput = static_cast<const ColumnVector<ElementType>*>(input);
    ColumnVector<ElementType>* typedOutput = static_cast<ColumnVector<ElementType>*>(output);

    const auto& inputRaw = typedInput->getRaw();
    auto& outputRaw = typedOutput->getRaw();
    outputRaw.resize(inputRaw.size() * factor);

    auto outputIt = outputRaw.begin();
    for (const ElementType& value : inputRaw) {
        std::fill_n(outputIt, factor, value);
        outputIt += factor;
    }
}

// Tile: the whole input chunk is emitted `factor` times back to back, so an
// input of M rows becomes M*factor rows with input row j at output indices
// j, j+M, j+2M, ... This lays out an inner column of a cross product, where the
// inner chunk repeats once per outer row.
template <typename ElementType>
void tileColumn(const Column* input, size_t factor, Column* output) {
    const ColumnVector<ElementType>* typedInput = static_cast<const ColumnVector<ElementType>*>(input);
    ColumnVector<ElementType>* typedOutput = static_cast<ColumnVector<ElementType>*>(output);

    const auto& inputRaw = typedInput->getRaw();
    auto& outputRaw = typedOutput->getRaw();
    outputRaw.resize(inputRaw.size() * factor);

    auto outputIt = outputRaw.begin();
    for (size_t repeat = 0; repeat < factor; repeat++) {
        outputIt = std::copy(inputRaw.begin(), inputRaw.end(), outputIt);
    }
}

// Execute a get_out_edges/get_in_edges loop
template <typename ChunkWriterType>
void runEdgeLoopSteps(NLExecutionContext* context,
                      NLEdgeLoopData* loopData,
                      ChunkWriterType* chunkWriter,
                      ColumnNodeIDs* gatheredNodeIDs) {
    const NLStmtContainer* loopBody = loopData->getStmts();

    // Same early-exit as the scan loop: a null limit is unbounded, otherwise the
    // loop stops once the budget is spent, and the break unwinds any enclosing
    // loop carrying the same handle.
    const NLLimitState* limit = loopData->getLimit();

    while (chunkWriter->isValid() && (!limit || limit->remaining() > 0)) {
        chunkWriter->fill(context->getChunkSize());

        const ColumnVector<size_t>* indices = loopData->getIndices();
        if (indices->empty()) {
            continue;
        }

        // Gather either source or target if we are get_out or get_in (the source side)
        gatherColumn<NodeID>(loopData->getInput(), indices, gatheredNodeIDs);

        // Transform all the columns in the carried set according to indices
        for (const NLCarriedColumn& carriedColumn : loopData->carriedColumns()) {
            const auto gatherFunc = carriedColumn.getGatherFunc();
            gatherFunc(carriedColumn.getInput(), indices, carriedColumn.getOutput());
        }

        runBody(context, loopBody);
    }
}

}

NLExecutor::NLExecutor(const GraphView* view,
                             const NLProgram* prog,
                             NLOutputSink* sink)
    : _ctxt(view, sink, prog->getChunkSize()),
    _prog(prog)
{
}

NLExecutor::~NLExecutor() {
}

void NLExecutor::run() {
    runBody(&_ctxt, _prog->getStmts());
}

void NLExecutor::runScanNodesLoop(NLExecutionContext* context, NLFunctionData* data) {
    NLScanLoopData* loopData = static_cast<NLScanLoopData*>(data);
    const NLStmtContainer* loopBody = loopData->getStmts();
    ColumnNodeIDs* nodeIDs = loopData->getNodeIDs();
    const size_t chunkSize = context->getChunkSize();

    // A null limit leaves the loop unbounded; otherwise it stops once the budget
    // is spent. nl.limit_update inside runBody mutates remaining, so the next
    // test breaks here, and an enclosing loop carrying the same handle breaks on
    // its next test too - unwinding the whole nest. LIMIT 0 fails the guard on
    // entry, so nothing is scanned.
    const NLLimitState* limit = loopData->getLimit();

    ScanNodesChunkWriter chunkWriter(*context->getView());
    chunkWriter.setNodeIDs(nodeIDs);

    while (chunkWriter.isValid() && (!limit || limit->remaining() > 0)) {
        chunkWriter.fill(chunkSize);

        if (nodeIDs->empty()) {
            continue;
        }

        runBody(context, loopBody);
    }
}

void NLExecutor::runGetOutEdgesLoop(NLExecutionContext* context, NLFunctionData* data) {
    NLEdgeLoopData* loopData = static_cast<NLEdgeLoopData*>(data);
    const ColumnNodeIDs* inputNodeIDs = loopData->getInput();

    if (inputNodeIDs->empty()) {
        return;
    }

    GetOutEdgesChunkWriter chunkWriter(*context->getView(), inputNodeIDs);
    chunkWriter.setIndices(loopData->getIndices());
    chunkWriter.setEdgeIDs(loopData->getEdgeIDs());
    chunkWriter.setEdgeTypes(loopData->getEdgeTypes());
    chunkWriter.setTgtIDs(loopData->getTargets());

    runEdgeLoopSteps(context, loopData, &chunkWriter, loopData->getSources());
}

void NLExecutor::runGetInEdgesLoop(NLExecutionContext* context, NLFunctionData* data) {
    NLEdgeLoopData* loopData = static_cast<NLEdgeLoopData*>(data);
    const ColumnNodeIDs* inputNodeIDs = loopData->getInput();

    if (inputNodeIDs->empty()) {
        return;
    }

    GetInEdgesChunkWriter chunkWriter(*context->getView(), inputNodeIDs);
    chunkWriter.setIndices(loopData->getIndices());
    chunkWriter.setEdgeIDs(loopData->getEdgeIDs());
    chunkWriter.setEdgeTypes(loopData->getEdgeTypes());
    chunkWriter.setSrcIDs(loopData->getSources());

    runEdgeLoopSteps(context, loopData, &chunkWriter, loopData->getTargets());
}

void NLExecutor::runCrossProduct(NLExecutionContext* context, NLFunctionData* data) {
    NLCrossProductData* cross = static_cast<NLCrossProductData*>(data);

    const NLCrossProductData::Columns& outerColumns = cross->outerColumns();
    const NLCrossProductData::Columns& innerColumns = cross->innerColumns();
    bioassert(!outerColumns.empty() && !innerColumns.empty(),
              "nl.cross_product needs a column on each side to size the product");

    // N outer rows crossed with M inner rows: each outer column is
    // block-repeated x M and each inner column tiled x N, so every outer row
    // pairs with every inner row. The counts come from the first column of each
    // side; all columns of a side are row-aligned, so any one measures it.
    const size_t outerRowCount = outerColumns.front().getInput()->size();
    const size_t innerRowCount = innerColumns.front().getInput()->size();

    for (const NLCrossColumn& column : outerColumns) {
        const NLBroadcastFunction broadcast = column.getBroadcast();
        broadcast(column.getInput(), innerRowCount, column.getOutput());
    }

    for (const NLCrossColumn& column : innerColumns) {
        const NLBroadcastFunction broadcast = column.getBroadcast();
        broadcast(column.getInput(), outerRowCount, column.getOutput());
    }
}

void NLExecutor::runLimitInit(NLExecutionContext* context, NLFunctionData* data) {
    const NLLimitInitData* init = static_cast<NLLimitInitData*>(data);
    init->getState()->reset(init->getCount());
}

void NLExecutor::runLimitUpdate(NLExecutionContext* context, NLFunctionData* data) {
    const NLLimitUpdateData* update = static_cast<NLLimitUpdateData*>(data);
    update->getState()->update(update->getRows()->size());
}

void NLExecutor::runOutput(NLExecutionContext* context, NLFunctionData* data) {
    const NLOutputData* output = static_cast<NLOutputData*>(data);
    const auto& cols = output->outputs();
    bioassert(!cols.empty(), "nl.output requires at least one column");

    // With a limit, emit the prefix nl.limit_update sized this step (reading
    // emitThisStep, not remaining, so the decrement already done does not affect
    // the count); without one, emit the whole chunk. Either way no row is copied.
    const NLLimitState* limit = output->getLimit();
    const size_t rowCount = limit ? limit->emitThisStep() : cols.front()->size();

    context->getSink()->appendChunks(cols, rowCount);
}

NLGatherFunction NLExecutor::selectGatherFunction(NLChunkKind kind) {
    switch (kind) {
        case NLChunkKind::NodeID:
            return &gatherColumn<NodeID>;
        break;

        case NLChunkKind::EdgeID:
            return &gatherColumn<EdgeID>;
        break;

        case NLChunkKind::EdgeTypeID:
            return &gatherColumn<EdgeTypeID>;
        break;
    }

    bioassert(false, "Unknown NLChunkKind");
    return nullptr;
}

NLBroadcastFunction NLExecutor::selectBlockRepeatFunction(NLChunkKind kind) {
    switch (kind) {
        case NLChunkKind::NodeID:
            return &blockRepeatColumn<NodeID>;
        break;

        case NLChunkKind::EdgeID:
            return &blockRepeatColumn<EdgeID>;
        break;

        case NLChunkKind::EdgeTypeID:
            return &blockRepeatColumn<EdgeTypeID>;
        break;
    }

    bioassert(false, "Unknown NLChunkKind");
    return nullptr;
}

NLBroadcastFunction NLExecutor::selectTileFunction(NLChunkKind kind) {
    switch (kind) {
        case NLChunkKind::NodeID:
            return &tileColumn<NodeID>;
        break;

        case NLChunkKind::EdgeID:
            return &tileColumn<EdgeID>;
        break;

        case NLChunkKind::EdgeTypeID:
            return &tileColumn<EdgeTypeID>;
        break;
    }

    bioassert(false, "Unknown NLChunkKind");
    return nullptr;
}

// A nullable value chunk is a ColumnOptVector<Primitive> - that is,
// ColumnVector<std::optional<Primitive>> - so the same broadcast templates,
// instantiated on std::optional<Primitive>, carry value and null together.
NLBroadcastFunction NLExecutor::selectOptBlockRepeatFunction(ValueType valueType) {
    NLBroadcastFunction broadcast = nullptr;
    const auto select = [&]<SupportedType T>() {
        broadcast = &blockRepeatColumn<std::optional<typename T::Primitive>>;
    };
    ValueTypeDispatcher(valueType).execute(select);

    return broadcast;
}

NLBroadcastFunction NLExecutor::selectOptTileFunction(ValueType valueType) {
    NLBroadcastFunction broadcast = nullptr;
    const auto select = [&]<SupportedType T>() {
        broadcast = &tileColumn<std::optional<typename T::Primitive>>;
    };
    ValueTypeDispatcher(valueType).execute(select);

    return broadcast;
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
template void NLExecutor::runPropertyFetch<EdgeID, types::Int64>(NLExecutionContext*, NLFunctionData*);
template void NLExecutor::runPropertyFetch<EdgeID, types::UInt64>(NLExecutionContext*, NLFunctionData*);
template void NLExecutor::runPropertyFetch<EdgeID, types::Double>(NLExecutionContext*, NLFunctionData*);
template void NLExecutor::runPropertyFetch<EdgeID, types::Bool>(NLExecutionContext*, NLFunctionData*);
template void NLExecutor::runPropertyFetch<EdgeID, types::String>(NLExecutionContext*, NLFunctionData*);
template void NLExecutor::runPropertyFetch<EdgeID, types::Embedding>(NLExecutionContext*, NLFunctionData*);
