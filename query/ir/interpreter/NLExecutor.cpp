#include "NLExecutor.h"

#include <algorithm>
#include <string>
#include <string_view>
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

// Block-repeat: each input row is emitted `factor` times in a row, so input row
// i lands at output indices [i*factor, (i+1)*factor). This lays out an outer
// column of a cross product, where each outer row pairs with the whole inner
// chunk. The fill stops at `outputRowCount` rows (min(N*factor, remaining) under
// a limit), so the last block may be partial and later input rows are skipped.
template <typename ElementType>
void blockRepeatColumn(const Column* input, size_t factor, size_t outputRowCount, Column* output) {
    const ColumnVector<ElementType>* typedInput = static_cast<const ColumnVector<ElementType>*>(input);
    ColumnVector<ElementType>* typedOutput = static_cast<ColumnVector<ElementType>*>(output);

    const auto& inputRaw = typedInput->getRaw();
    auto& outputRaw = typedOutput->getRaw();
    outputRaw.resize(outputRowCount);

    auto outputIt = outputRaw.begin();
    size_t rowsLeft = outputRowCount;
    for (const ElementType& value : inputRaw) {
        if (rowsLeft == 0) {
            break;
        }

        const size_t count = std::min(factor, rowsLeft);
        std::fill_n(outputIt, count, value);
        outputIt += count;
        rowsLeft -= count;
    }
}

// Tile: the whole input chunk is emitted back to back, so input row j lands at
// output indices j, j+M, j+2M, ... This lays out an inner column of a cross
// product, where the inner chunk repeats once per outer row. The fill stops at
// `outputRowCount` rows (min(M*N, remaining) under a limit), which alone bounds
// the repeats, so the row count drives it rather than the `factor` (N) the outer
// side uses. The last tile may be partial.
template <typename ElementType>
void tileColumn(const Column* input, size_t factor, size_t outputRowCount, Column* output) {
    const ColumnVector<ElementType>* typedInput = static_cast<const ColumnVector<ElementType>*>(input);
    ColumnVector<ElementType>* typedOutput = static_cast<ColumnVector<ElementType>*>(output);

    const auto& inputRaw = typedInput->getRaw();
    auto& outputRaw = typedOutput->getRaw();
    outputRaw.resize(outputRowCount);

    const size_t tileLength = inputRaw.size();
    auto outputIt = outputRaw.begin();
    size_t rowsLeft = outputRowCount;
    while (rowsLeft > 0) {
        const size_t count = std::min(tileLength, rowsLeft);
        outputIt = std::copy(inputRaw.begin(), inputRaw.begin() + count, outputIt);
        rowsLeft -= count;
    }
}

// Range copy: rows [inputOffset, inputOffset + rowCount) of the input land at
// output indices [0, rowCount). This lifts a skip's surviving suffix to the front
// of a fresh chunk - nl.skip_truncate passes inputOffset = skipThisStep and
// rowCount = emitThisStep. std::copy of a contiguous range is lowered to memcpy.
template <typename ElementType>
void copyRangeColumn(const Column* input, size_t inputOffset, size_t rowCount, Column* output) {
    const ColumnVector<ElementType>* typedInput = static_cast<const ColumnVector<ElementType>*>(input);
    ColumnVector<ElementType>* typedOutput = static_cast<ColumnVector<ElementType>*>(output);

    const auto& inputRaw = typedInput->getRaw();
    auto& outputRaw = typedOutput->getRaw();
    outputRaw.resize(rowCount);

    const auto first = inputRaw.begin() + inputOffset;
    std::copy(first, first + rowCount, outputRaw.begin());
}

// Append every row of an input chunk onto the tail of a growing buffer of the
// same element type. nl.sort_collect calls this once per producing-loop step, so
// the buffer accumulates every row across all chunks, row-aligned with the other
// buffers of the same accumulator.
template <typename ElementType>
void appendColumn(const Column* input, Column* buffer) {
    const ColumnVector<ElementType>* typedInput = static_cast<const ColumnVector<ElementType>*>(input);
    ColumnVector<ElementType>* typedBuffer = static_cast<ColumnVector<ElementType>*>(buffer);

    const auto& inputRaw = typedInput->getRaw();
    auto& bufferRaw = typedBuffer->getRaw();

    bufferRaw.insert(bufferRaw.end(), inputRaw.begin(), inputRaw.end());
}

// 3-way compare two rows of a non-null orderable column (an ID column): negative
// if row a sorts before row b, positive if after, zero if they are equal.
template <typename ElementType>
int compareColumn(const Column* column, size_t a, size_t b) {
    const auto& raw = static_cast<const ColumnVector<ElementType>*>(column)->getRaw();
    const ElementType& valueA = raw[a];
    const ElementType& valueB = raw[b];

    if (valueA < valueB) {
        return -1;
    } else if (valueB < valueA) {
        return 1;
    }

    return 0;
}

// 3-way compare two rows of a nullable value column. A null sorts after every
// value, so an ascending order places nulls last (matching Cypher's ORDER BY),
// and two nulls tie; non-null values compare by their natural order.
template <typename Primitive>
int compareOptColumn(const Column* column, size_t a, size_t b) {
    const auto& raw = static_cast<const ColumnVector<std::optional<Primitive>>*>(column)->getRaw();
    const std::optional<Primitive>& valueA = raw[a];
    const std::optional<Primitive>& valueB = raw[b];

    const bool aNull = !valueA.has_value();
    const bool bNull = !valueB.has_value();
    if (aNull || bNull) {
        if (aNull && bNull) {
            return 0;
        }

        return aNull ? 1 : -1;
    }

    if (*valueA < *valueB) {
        return -1;
    } else if (*valueB < *valueA) {
        return 1;
    }

    return 0;
}

// Append the raw bytes of a present property value to a distinct row key. The key
// is a std::string used purely as a growable byte buffer - not text - so the value
// is appended verbatim: a trivially-copyable primitive copies its object bytes; a
// string copies a length prefix then its characters, so two rows never collide by
// concatenation (so "a"+"b" and "ab"+"" get distinct keys).
template <typename Primitive>
void distinctAppendValueBytes(std::string& key, const Primitive& value) {
    key.append(reinterpret_cast<const char*>(&value), sizeof(Primitive));
}

void distinctAppendValueBytes(std::string& key, std::string_view value) {
    const size_t length = value.size();
    key.append(reinterpret_cast<const char*>(&length), sizeof(length));
    key.append(value.data(), value.size());
}

// Serialize one row of an ID column (node/edge/edge-type IDs) into the row key -
// a std::string used as a byte buffer, not text - as the ID's underlying integer
// value, byte for byte.
template <typename ElementType>
void distinctKeyAppendColumn(const Column* column, size_t row, std::string& key) {
    const auto& raw = static_cast<const ColumnVector<ElementType>*>(column)->getRaw();
    const auto value = raw[row].getValue();
    distinctAppendValueBytes(key, value);
}

// Serialize one row of a nullable value column into the row key - a std::string
// used as a byte buffer, not text - as a tag byte telling null from present, then,
// when present, the value's bytes. A null serializes to the tag alone, so all
// nulls share a key and DISTINCT dedups them together, matching Cypher.
template <typename Primitive>
void distinctKeyAppendOptColumn(const Column* column, size_t row, std::string& key) {
    const auto& raw = static_cast<const ColumnVector<std::optional<Primitive>>*>(column)->getRaw();
    const std::optional<Primitive>& value = raw[row];

    if (!value.has_value()) {
        key.push_back('\0');
        return;
    }

    key.push_back('\1');
    distinctAppendValueBytes(key, *value);
}

// Count the present (non-null) values of a nullable value column - a
// ColumnVector<std::optional<Primitive>> - so Cypher count(x) charges only the
// rows in which x is not null.
template <typename OptType>
size_t countPresentColumn(const Column* column) {
    const auto& raw = static_cast<const ColumnVector<OptType>*>(column)->getRaw();
    return std::count_if(raw.begin(), raw.end(), [](const OptType& value) {
        return value.has_value();
    });
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
    // loop carrying the same handle. The limit is fixed for the whole loop, so
    // the null check is hoisted out of the per-iteration condition.
    const NLLimitState* limit = loopData->getLimit();

    const auto runIteration = [&]() {
        chunkWriter->fill(context->getChunkSize());

        const ColumnVector<size_t>* indices = loopData->getIndices();
        if (indices->empty()) {
            return;
        }

        // Gather either source or target if we are get_out or get_in (the source side)
        gatherColumn<NodeID>(loopData->getInput(), indices, gatheredNodeIDs);

        // Transform all the columns in the carried set according to indices
        for (const NLCarriedColumn& carriedColumn : loopData->carriedColumns()) {
            const auto gatherFunc = carriedColumn.getGatherFunc();
            gatherFunc(carriedColumn.getInput(), indices, carriedColumn.getOutput());
        }

        runBody(context, loopBody);
    };

    if (limit) {
        while (chunkWriter->isValid() && limit->getRemaining() > 0) {
            runIteration();
        }
    } else {
        while (chunkWriter->isValid()) {
            runIteration();
        }
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
    // entry, so nothing is scanned. The limit is fixed for the whole loop, so
    // the null check is hoisted out of the per-iteration condition.
    const NLLimitState* limit = loopData->getLimit();

    ScanNodesChunkWriter chunkWriter(*context->getView());
    chunkWriter.setNodeIDs(nodeIDs);

    const auto runIteration = [&]() {
        chunkWriter.fill(chunkSize);

        if (nodeIDs->empty()) {
            return;
        }

        runBody(context, loopBody);
    };

    if (limit) {
        while (chunkWriter.isValid() && limit->getRemaining() > 0) {
            runIteration();
        }
    } else {
        while (chunkWriter.isValid()) {
            runIteration();
        }
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

    const NLLimitState* limit = cross->getLimit();
    const size_t productRowCount = outerRowCount * innerRowCount;
    const size_t remaining = limit ? limit->getRemaining() : productRowCount;
    const size_t outputRowCount = std::min(productRowCount, remaining);

    for (const NLCrossColumn& column : outerColumns) {
        const NLBroadcastFunction broadcast = column.getBroadcast();
        broadcast(column.getInput(), innerRowCount, outputRowCount, column.getOutput());
    }

    for (const NLCrossColumn& column : innerColumns) {
        const NLBroadcastFunction broadcast = column.getBroadcast();
        broadcast(column.getInput(), outerRowCount, outputRowCount, column.getOutput());
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

void NLExecutor::runLimitTruncate(NLExecutionContext* context, NLFunctionData* data) {
    const NLLimitTruncateData* truncate = static_cast<NLLimitTruncateData*>(data);
    const size_t emitThisStep = truncate->getState()->getEmitThisStep();

    // Block-repeat with factor 1 emits each input row once and stops at
    // emitThisStep, so it copies exactly the prefix [0, emitThisStep) into the
    // fresh output chunk. Reads emitThisStep (set by the preceding
    // nl.limit_update); never mutates the counter.
    for (const NLCrossColumn& column : truncate->columns()) {
        const NLBroadcastFunction copyPrefix = column.getBroadcast();
        copyPrefix(column.getInput(), 1, emitThisStep, column.getOutput());
    }
}

void NLExecutor::runSkipInit(NLExecutionContext* context, NLFunctionData* data) {
    const NLSkipInitData* init = static_cast<NLSkipInitData*>(data);
    init->getState()->reset(init->getCount());
}

void NLExecutor::runSkipUpdate(NLExecutionContext* context, NLFunctionData* data) {
    const NLSkipUpdateData* update = static_cast<NLSkipUpdateData*>(data);
    update->getState()->update(update->getRows()->size());
}

void NLExecutor::runSkipTruncate(NLExecutionContext* context, NLFunctionData* data) {
    const NLSkipTruncateData* truncate = static_cast<NLSkipTruncateData*>(data);
    const NLSkipState* state = truncate->getState();

    // Copy the surviving suffix [skipThisStep, skipThisStep + emitThisStep) of each
    // column into the fresh front-aligned output chunk. Reads the offset and count
    // (set by the preceding nl.skip_update); never mutates the counter.
    const size_t offset = state->getSkipThisStep();
    const size_t rowCount = state->getEmitThisStep();
    for (const NLSkipColumn& column : truncate->columns()) {
        const NLCopyFunction copySuffix = column.getCopy();
        copySuffix(column.getInput(), offset, rowCount, column.getOutput());
    }
}

void NLExecutor::runOutput(NLExecutionContext* context, NLFunctionData* data) {
    const NLOutputData* output = static_cast<NLOutputData*>(data);
    const auto& cols = output->outputs();
    bioassert(!cols.empty(), "nl.output requires at least one column");

    // Compute the [offset, offset + rowCount) window to emit, copy-free:
    //  - skip (the folded terminal-SKIP form): emit the surviving suffix at offset
    //    getSkipThisStep() for getEmitThisStep() rows, both sized by the preceding
    //    nl.skip_update. Reading them, not remaining, so the decrement already done
    //    does not affect the window.
    //  - limit (the folded terminal-LIMIT form): emit the getEmitThisStep() prefix
    //    nl.limit_update sized this step, from offset zero.
    //  - neither: emit the whole chunk (already cut by a truncate when one governs
    //    it).
    // A folded output carries at most one of limit/skip, so the two never combine.
    const NLLimitState* limit = output->getLimit();
    const NLSkipState* skip = output->getSkip();

    size_t offset = 0;
    size_t rowCount = 0;
    if (skip) {
        offset = skip->getSkipThisStep();
        rowCount = skip->getEmitThisStep();
    } else if (limit) {
        rowCount = limit->getEmitThisStep();
    } else {
        rowCount = cols.front()->size();
    }

    context->getSink()->appendChunks(cols, offset, rowCount);
}

void NLExecutor::runSortReset(NLExecutionContext* context, NLFunctionData* data) {
    const NLSortResetData* reset = static_cast<NLSortResetData*>(data);
    reset->getState()->reset();
}

void NLExecutor::runSortCollect(NLExecutionContext* context, NLFunctionData* data) {
    const NLSortCollectData* collect = static_cast<NLSortCollectData*>(data);

    // Append this step's chunk of every column onto its buffer's tail; the
    // columns are taken together so the buffers stay row-aligned.
    for (const NLSortCollectData::Append& append : collect->appends()) {
        append._append(append._input, append._buffer);
    }

    // For a bounded (top-K) accumulator, drop all but the best k once the buffers
    // have grown past the bound, so memory stays O(k) rather than O(rows). A
    // no-op for an unbounded sort.
    collect->getState()->trimIfNeeded();
}

void NLExecutor::runSortLoop(NLExecutionContext* context, NLFunctionData* data) {
    NLSortLoopData* loopData = static_cast<NLSortLoopData*>(data);
    NLSortState* state = loopData->getState();

    // Sort the accumulated rows once: the permutation is the global row order the
    // emit chunks read in.
    state->sort();
    const std::vector<size_t>& permutation = state->permutation().getRaw();
    const size_t totalRows = permutation.size();

    const NLStmtContainer* loopBody = loopData->getStmts();
    const size_t chunkSize = context->getChunkSize();
    ColumnVector<size_t>* indices = loopData->getIndices();

    // Re-chunk the sorted rows: each step gathers the next chunkSize rows, in
    // permutation order, into the loop variables, then runs the body (nl.output).
    // The last chunk may be partial; an empty result runs the body zero times.
    for (size_t offset = 0; offset < totalRows; offset += chunkSize) {
        const size_t stepRows = std::min(chunkSize, totalRows - offset);

        std::vector<size_t>& indicesRaw = indices->getRaw();
        indicesRaw.assign(permutation.begin() + offset, permutation.begin() + offset + stepRows);

        for (const NLCarriedColumn& column : loopData->columns()) {
            const NLGatherFunction gather = column.getGatherFunc();
            gather(column.getInput(), indices, column.getOutput());
        }

        runBody(context, loopBody);
    }
}

void NLExecutor::runDistinctReset(NLExecutionContext* context, NLFunctionData* data) {
    const NLDistinctResetData* reset = static_cast<NLDistinctResetData*>(data);
    reset->getState()->reset();
}

void NLExecutor::runDistinctFilter(NLExecutionContext* context, NLFunctionData* data) {
    NLDistinctFilterData* filter = static_cast<NLDistinctFilterData*>(data);
    NLDistinctState* state = filter->getState();

    const std::vector<NLDistinctFilterData::FilterColumn>& columns = filter->columns();
    bioassert(!columns.empty(), "nl.distinct_filter needs at least one column");

    // Every column is row-aligned, so the first sizes this step's row set.
    const size_t rowCount = columns.front()._input->size();

    // Collect this step's surviving row indices: a row survives iff its key - the
    // concatenation of every column's serialized value at that row - is new to the
    // seen-set. insertIfNew both tests membership and records the new key, so a
    // duplicate later in the same chunk is caught by an earlier row of it too.
    ColumnVector<size_t>* indices = filter->getIndices();
    std::vector<size_t>& survivingRaw = indices->getRaw();
    survivingRaw.clear();

    std::string* key = filter->getKeyScratch();
    for (size_t row = 0; row < rowCount; row++) {
        key->clear();
        for (const NLDistinctFilterData::FilterColumn& column : columns) {
            column._keyAppend(column._input, row, *key);
        }

        if (state->insertIfNew(*key)) {
            survivingRaw.push_back(row);
        }
    }

    // Gather the survivors into the fresh output chunks, in first-seen order, so a
    // downstream consumer reads a genuinely deduped chunk.
    for (const NLDistinctFilterData::FilterColumn& column : columns) {
        column._gather(column._input, indices, column._output);
    }
}

void NLExecutor::runCountReset(NLExecutionContext* context, NLFunctionData* data) {
    const NLCountResetData* reset = static_cast<NLCountResetData*>(data);
    reset->getState()->reset();
}

void NLExecutor::runCountUpdate(NLExecutionContext* context, NLFunctionData* data) {
    const NLCountUpdateData* update = static_cast<NLCountUpdateData*>(data);

    // Charge this step's non-null rows: the handle returns all rows for an ID
    // chunk, the present values for a nullable value chunk.
    const NLCountFunction count = update->getCount();
    update->getState()->add(count(update->getRows()));
}

void NLExecutor::runCountResult(NLExecutionContext* context, NLFunctionData* data) {
    const NLCountResultData* result = static_cast<NLCountResultData*>(data);

    // The aggregate collapses every counted row to a single result row: write the
    // final tally into the output chunk as one unsigned i64 (the !nl.chunk<ui64>
    // count column). Runs after the producing loop, so the counter holds the whole
    // dataflow's count; nl.output emits this chunk at function scope.
    const size_t count = result->getState()->getCount();
    ColumnVector<uint64_t>* output = static_cast<ColumnVector<uint64_t>*>(result->getOutput());
    std::vector<uint64_t>& raw = output->getRaw();
    raw.assign(1, static_cast<uint64_t>(count));
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

NLCopyFunction NLExecutor::selectCopyFunction(NLChunkKind kind) {
    switch (kind) {
        case NLChunkKind::NodeID:
            return &copyRangeColumn<NodeID>;
        break;

        case NLChunkKind::EdgeID:
            return &copyRangeColumn<EdgeID>;
        break;

        case NLChunkKind::EdgeTypeID:
            return &copyRangeColumn<EdgeTypeID>;
        break;
    }

    bioassert(false, "Unknown NLChunkKind");
    return nullptr;
}

// A nullable value chunk gathers the same way an ID chunk does - copy the indexed
// rows - on the ColumnOptVector<Primitive> instantiation of the gather template.
NLGatherFunction NLExecutor::selectOptGatherFunction(ValueType valueType) {
    NLGatherFunction gather = nullptr;
    const auto select = [&]<SupportedType T>() {
        gather = &gatherColumn<std::optional<typename T::Primitive>>;
    };
    ValueTypeDispatcher(valueType).execute(select);

    return gather;
}

NLAppendFunction NLExecutor::selectAppendFunction(NLChunkKind kind) {
    switch (kind) {
        case NLChunkKind::NodeID:
            return &appendColumn<NodeID>;
        break;

        case NLChunkKind::EdgeID:
            return &appendColumn<EdgeID>;
        break;

        case NLChunkKind::EdgeTypeID:
            return &appendColumn<EdgeTypeID>;
        break;
    }

    bioassert(false, "Unknown NLChunkKind");
    return nullptr;
}

// A nullable value chunk is a ColumnOptVector<Primitive> - that is,
// ColumnVector<std::optional<Primitive>> - so the range copy template,
// instantiated on std::optional<Primitive>, carries value and null together.
NLCopyFunction NLExecutor::selectOptCopyFunction(ValueType valueType) {
    NLCopyFunction copy = nullptr;
    const auto select = [&]<SupportedType T>() {
        copy = &copyRangeColumn<std::optional<typename T::Primitive>>;
    };
    ValueTypeDispatcher(valueType).execute(select);

    return copy;
}

NLAppendFunction NLExecutor::selectOptAppendFunction(ValueType valueType) {
    NLAppendFunction append = nullptr;
    const auto select = [&]<SupportedType T>() {
        append = &appendColumn<std::optional<typename T::Primitive>>;
    };
    ValueTypeDispatcher(valueType).execute(select);

    return append;
}

NLKeyAppendFunction NLExecutor::selectKeyAppendFunction(NLChunkKind kind) {
    switch (kind) {
        case NLChunkKind::NodeID:
            return &distinctKeyAppendColumn<NodeID>;
        break;

        case NLChunkKind::EdgeID:
            return &distinctKeyAppendColumn<EdgeID>;
        break;

        case NLChunkKind::EdgeTypeID:
            return &distinctKeyAppendColumn<EdgeTypeID>;
        break;
    }

    bioassert(false, "Unknown NLChunkKind");
    return nullptr;
}

// Selected per column from its value type. A manual switch, not ValueTypeDispatcher,
// because an embedding has no byte identity to key on: dispatching would instantiate
// the serializer for std::span<const float> - a view, not owned bytes - which cannot
// be a DISTINCT key. The embedding case throws instead, so that instantiation is
// never named, the same shape as selectOptCompareFunction.
NLKeyAppendFunction NLExecutor::selectOptKeyAppendFunction(ValueType valueType) {
    switch (valueType) {
        case ValueType::Int64:
            return &distinctKeyAppendOptColumn<types::Int64::Primitive>;
        break;

        case ValueType::UInt64:
            return &distinctKeyAppendOptColumn<types::UInt64::Primitive>;
        break;

        case ValueType::Double:
            return &distinctKeyAppendOptColumn<types::Double::Primitive>;
        break;

        case ValueType::Bool:
            return &distinctKeyAppendOptColumn<types::Bool::Primitive>;
        break;

        case ValueType::String:
            return &distinctKeyAppendOptColumn<types::String::Primitive>;
        break;

        case ValueType::Embedding:
            throw IRException("cannot remove duplicates on an embedding column");
        break;

        case ValueType::Invalid:
        case ValueType::_SIZE:
            throw IRException("invalid distinct key value type");
        break;
    }

    bioassert(false, "Unhandled value type");
    return nullptr;
}

// An ID chunk (node/edge/edge-type IDs) has no null rows, so every row counts,
// regardless of kind. The count sibling of selectKeyAppendFunction, but kind is
// irrelevant here - the row count is just the column size.
size_t NLExecutor::countAllRows(const Column* column) {
    return column->size();
}

// Selected per column from its value type, so count(x) tallies only the rows in
// which x is not null. Every value type has a present/absent flag, so - unlike
// selectOptKeyAppendFunction - an embedding is fine: counting reads has_value(),
// never the value's bytes.
NLCountFunction NLExecutor::selectOptCountFunction(ValueType valueType) {
    NLCountFunction count = nullptr;
    const auto select = [&]<SupportedType T>() {
        count = &countPresentColumn<std::optional<typename T::Primitive>>;
    };
    ValueTypeDispatcher(valueType).execute(select);

    return count;
}

NLCompareFunction NLExecutor::selectCompareFunction(NLChunkKind kind) {
    switch (kind) {
        case NLChunkKind::NodeID:
            return &compareColumn<NodeID>;
        break;

        case NLChunkKind::EdgeID:
            return &compareColumn<EdgeID>;
        break;

        case NLChunkKind::EdgeTypeID:
            return &compareColumn<EdgeTypeID>;
        break;
    }

    bioassert(false, "Unknown NLChunkKind");
    return nullptr;
}

// Selected per key from its value type. A manual switch, not ValueTypeDispatcher,
// because an embedding has no order: dispatching would instantiate the comparator
// for std::span<const float>, which does not compile. The embedding case throws
// instead, so that instantiation is never named.
NLCompareFunction NLExecutor::selectOptCompareFunction(ValueType valueType) {
    switch (valueType) {
        case ValueType::Int64:
            return &compareOptColumn<types::Int64::Primitive>;
        break;

        case ValueType::UInt64:
            return &compareOptColumn<types::UInt64::Primitive>;
        break;

        case ValueType::Double:
            return &compareOptColumn<types::Double::Primitive>;
        break;

        case ValueType::Bool:
            return &compareOptColumn<types::Bool::Primitive>;
        break;

        case ValueType::String:
            return &compareOptColumn<types::String::Primitive>;
        break;

        case ValueType::Embedding:
            throw IRException("cannot sort by an embedding column");
        break;

        case ValueType::Invalid:
        case ValueType::_SIZE:
            throw IRException("invalid sort key value type");
        break;
    }

    bioassert(false, "Unhandled value type");
    return nullptr;
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
