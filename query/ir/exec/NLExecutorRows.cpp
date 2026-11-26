#include "NLExecutor.h"

#include <algorithm>
#include <numeric>
#include <span>
#include <string_view>

#include <spdlog/fmt/bundled/format.h>

#include "ID.h"
#include "columns/ColumnVector.h"
#include "columns/ColumnConst.h"
#include "QueryTypeName.h"
#include "list/PathTrie.h"
#include "metadata/PropertyType.h"

#include "NLProgram.h"
#include "NLRuntimeProperty.h"
#include "NLOutputSink.h"
#include "NLBoundConstant.h"
#include "NLColumnKernels.h"
#include "NLLoopSteps.h"

#include "IRException.h"
#include "BioAssert.h"

using namespace db;

namespace {

// The row count bound to a LIMIT or a SKIP, read when its loop starts: an integer, or a cell
// holding one
size_t readBoundRowCount(const Column* bound, std::string_view clause) {
    std::optional<types::Int64::Primitive> count;
    std::optional<CellView> cell;

    if (readBoundConstant(bound, cell)) {
        const CellTag tag = cell ? cell->getTag() : CellTag::Null;
        if (tag == CellTag::Int) {
            count = cell->getAs<types::Int64::Primitive>();
        } else if (tag == CellTag::UInt) {
            return static_cast<size_t>(cell->getAs<types::UInt64::Primitive>());
        } else if (tag != CellTag::Null) {
            throw IRException(fmt::format("{} requires a non-negative integer, got a {}", clause, queryTypeName(tag)));
        }
    } else {
        const bool readsAnInteger = readBoundConstant(bound, count);
        bioassert(readsAnInteger, "A bound row count holds one integer or one cell for the run");
    }

    if (!count) {
        throw IRException(fmt::format("{} requires a non-negative integer, got null", clause));
    } else if (*count < 0) {
        throw IRException(fmt::format("{} requires a non-negative integer, got {}", clause, *count));
    }

    return static_cast<size_t>(*count);
}

bool isMaskConstant(const Column* mask) {
    return mask->getKind() == ColumnConst<CustomBool>::staticKind();
}

}

void NLExecutor::runEachRowLoop(NLExecutionContext* context, NLFunctionData* data) {
    NLEachRowLoopData* loopData = static_cast<NLEachRowLoopData*>(data);
    const NLStmtContainer* loopBody = loopData->getStmts();
    const NLEachRowLoopData::Columns& columns = loopData->columns();
    ColumnVector<size_t>* indices = loopData->getIndices();
    std::vector<size_t>& indicesRaw = indices->getRaw();

    // A null limit leaves the loop unbounded, exactly as in runUnwindConstLoop.
    const NLLimitState* limit = loopData->getLimit();
    const auto budgetLeft = [&]() { return !limit || limit->getRemaining() > 0; };

    const size_t rowCount = columns.front().getInput()->size();
    indicesRaw.resize(1);

    for (size_t row = 0; row < rowCount && budgetLeft(); row++) {
        indicesRaw[0] = row;

        for (const NLCarriedColumn& column : columns) {
            column.getGatherFunc()(column.getInput(), indices, column.getOutput());
        }

        runBody(context, loopBody);
    }
}

void NLExecutor::runLimitInit(NLExecutionContext* context, NLFunctionData* data) {
    const NLLimitInitData* init = static_cast<NLLimitInitData*>(data);
    const Column* boundCount = init->getBoundCount();
    init->getState()->reset(boundCount ? readBoundRowCount(boundCount, "LIMIT") : init->getCount());
}

void NLExecutor::runLimitRemaining(NLExecutionContext* context, NLFunctionData* data) {
    const NLLimitRemainingData* remaining = static_cast<NLLimitRemainingData*>(data);
    remaining->getState()->reset(remaining->getSource()->getRemaining());
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
        copyPrefix(column.getInput(), 1, 0, emitThisStep, column.getOutput());
    }
}

void NLExecutor::runSkipInit(NLExecutionContext* context, NLFunctionData* data) {
    const NLSkipInitData* init = static_cast<NLSkipInitData*>(data);
    const Column* boundCount = init->getBoundCount();
    init->getState()->reset(boundCount ? readBoundRowCount(boundCount, "SKIP") : init->getCount());
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
    NLOutputData* output = static_cast<NLOutputData*>(data);
    const auto& cols = output->outputs();
    bioassert(!cols.empty(), "nl.output requires at least one column");

    // A path chunk reaches the sink as the lists its handles stand for, expanded into a
    // buffer of this emission alone: the sink reads them during the call and never again
    const std::vector<NLOutputData::PathExpansion>& expansions = output->pathExpansions();
    if (!expansions.empty()) {
        QueryListBuffer& listBuffer = output->pathListBuffer();
        listBuffer.clear();

        const PathTrie& trie = *output->getTrie();
        for (const NLOutputData::PathExpansion& expansion : expansions) {
            const std::vector<PathRef>& paths = expansion._paths->getRaw();
            std::vector<ListView>& lists = expansion._lists->getRaw();

            lists.resize(paths.size());
            for (size_t row = 0; row < paths.size(); row++) {
                lists[row] = trie.expandEdges(paths[row], listBuffer, false);
            }
        }
    }

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
    // Column that defines the cardinality of the result
    const Column* cardinality = output->getCardinality();

    size_t offset = 0;
    size_t rowCount = 0;
    if (skip) {
        offset = skip->getSkipThisStep();
        rowCount = skip->getEmitThisStep();
    } else if (limit) {
        rowCount = limit->getEmitThisStep();
    } else if (cardinality) {
        rowCount = cardinality->size();
    } else {
        // Logical row count, assumes the columns that carry rows share a dimension. A
        // constant is not one of them: it holds a single value standing for every row of
        // the step, so a step that kept no row would emit it as a row of its own
        for (const Column* column : output->rowCountColumns()) {
            rowCount = std::max(rowCount, column->size());
        }
    }

    context->getSink()->appendChunks(cols, offset, rowCount);
}

void NLExecutor::runSortReset(NLExecutionContext* context, NLFunctionData* data) {
    const NLSortResetData* reset = static_cast<NLSortResetData*>(data);
    NLSortState* state = reset->getState();

    if (const Column* boundTopK = reset->getBoundTopK()) {
        state->setTopK(readBoundRowCount(boundTopK, "LIMIT"));
    }

    state->reset();
}

void NLExecutor::runSortCollect(NLExecutionContext* context, NLFunctionData* data) {
    const NLSortCollectData* collect = static_cast<NLSortCollectData*>(data);

    // Append this step's chunk of every column onto its buffer's tail; the
    // columns are taken together so the buffers stay row-aligned.
    for (const NLSortCollectData::Append& append : collect->appends()) {
        appendCollectedChunk(append, collect->getState()->values());
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

    const NLLimitState* limit = loopData->getLimit();

    // Re-chunk the sorted rows: each step gathers the next chunkSize rows, in
    // permutation order, into the loop variables, then runs the body (nl.output).
    // The last chunk may be partial; an empty result runs the body zero times.
    const auto runIteration = [&](size_t offset) {
        const size_t stepRows = std::min(chunkSize, totalRows - offset);

        std::vector<size_t>& indicesRaw = indices->getRaw();
        indicesRaw.assign(permutation.begin() + offset, permutation.begin() + offset + stepRows);

        for (const NLCarriedColumn& column : loopData->columns()) {
            const NLGatherFunction gather = column.getGatherFunc();
            gather(column.getInput(), indices, column.getOutput());
        }

        runBody(context, loopBody);
    };

    if (limit) {
        for (size_t offset = 0; offset < totalRows && limit->getRemaining() > 0; offset += chunkSize) {
            runIteration(offset);
        }
    } else {
        for (size_t offset = 0; offset < totalRows; offset += chunkSize) {
            runIteration(offset);
        }
    }
}

void NLExecutor::runRowReset(NLExecutionContext* context, NLFunctionData* data) {
    const NLRowResetData* reset = static_cast<NLRowResetData*>(data);
    reset->getState()->reset();
}

void NLExecutor::runRowCollect(NLExecutionContext* context, NLFunctionData* data) {
    const NLRowCollectData* collect = static_cast<NLRowCollectData*>(data);

    for (const NLSortCollectData::Append& append : collect->appends()) {
        appendCollectedChunk(append, collect->getState()->values());
    }
}

void NLExecutor::runRowLoop(NLExecutionContext* context, NLFunctionData* data) {
    NLRowLoopData* loopData = static_cast<NLRowLoopData*>(data);
    const size_t totalRows = loopData->getState()->getRowCount();

    const NLStmtContainer* loopBody = loopData->getStmts();
    const size_t chunkSize = context->getChunkSize();
    ColumnVector<size_t>* indices = loopData->getIndices();

    const NLLimitState* limit = loopData->getLimit();

    const auto runIteration = [&](size_t offset) {
        const size_t stepRows = std::min(chunkSize, totalRows - offset);

        std::vector<size_t>& indicesRaw = indices->getRaw();
        indicesRaw.resize(stepRows);
        std::iota(indicesRaw.begin(), indicesRaw.end(), offset);

        for (const NLCarriedColumn& column : loopData->columns()) {
            const NLGatherFunction gather = column.getGatherFunc();
            gather(column.getInput(), indices, column.getOutput());
        }

        runBody(context, loopBody);
    };

    if (limit) {
        for (size_t offset = 0; offset < totalRows && limit->getRemaining() > 0; offset += chunkSize) {
            runIteration(offset);
        }
    } else {
        for (size_t offset = 0; offset < totalRows; offset += chunkSize) {
            runIteration(offset);
        }
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

    // A row survives iff its key - every column's value at that row - is new to the hash
    // table, which is when the row creates a group in it. A duplicate later in the same
    // chunk finds the group an earlier row of it created.
    NLGroupHashTable& hashTable = state->hashTable();
    hashTable.assign(filter->getKeyColumns(), rowCount);

    const std::vector<size_t>& newGroupRows = hashTable.getNewGroupRows();

    ColumnVector<size_t>* indices = filter->getIndices();
    std::vector<size_t>& survivingRaw = indices->getRaw();
    survivingRaw.assign(newGroupRows.begin(), newGroupRows.end());

    // Gather the survivors into the fresh output chunks, in first-seen order, so a
    // downstream consumer reads a genuinely deduped chunk.
    for (const NLDistinctFilterData::FilterColumn& column : columns) {
        column._gather(column._input, indices, column._output);
    }
}

void NLExecutor::runFilter(NLExecutionContext* context, NLFunctionData* data) {
    NLFilterData* filter = static_cast<NLFilterData*>(data);

    const std::vector<NLFilterData::FilterColumn>& columns = filter->columns();
    bioassert(!columns.empty(), "nl.filter needs at least one column");

    const Column* mask = filter->getMask();

    // Special case for a ColumnConst<Bool> mask where all rows are either kept or
    // filtered
    if (isMaskConstant(mask)) {
        const auto* constMask = static_cast<const ColumnConst<CustomBool>*>(mask);
        const bool passes = !constMask->empty() && constMask->getRaw()._boolean;

        for (const NLFilterData::FilterColumn& column : columns) {
            if (passes) {
                column._output->assign(column._input);
            } else {
                column._output->clear();
            }
        }
        return;
    }

    // General case: Otherwise apply the mask to each column
    ColumnVector<size_t>* indices = filter->getIndices();
    indices->getRaw().clear();
    filter->getSurvivors()(mask, indices);

    for (const NLFilterData::FilterColumn& column : columns) {
        column._gather(column._input, indices, column._output);
    }
}
