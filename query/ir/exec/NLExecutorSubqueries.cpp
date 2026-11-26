#include "NLExecutor.h"

#include <stdint.h>
#include <algorithm>
#include <numeric>
#include <span>

#include "columns/ColumnVector.h"
#include "metadata/PropertyType.h"

#include "NLProgram.h"
#include "NLRuntimeProperty.h"
#include "NLColumnKernels.h"
#include "NLLoopSteps.h"

#include "BioAssert.h"

using namespace db;

namespace {

// The step row an untagged mark, tally or collect answers for: the position its row chunk
// holds, or the single row of the step when it has none
uint64_t untaggedRow(const ColumnVector<uint64_t>* row) {
    return row ? row->getRaw().front() : 0;
}

}

void NLExecutor::runRowListsReset(NLExecutionContext* context, NLFunctionData* data) {
    NLRowListsResetData* reset = static_cast<NLRowListsResetData*>(data);

    // No cardinality chunk: nothing was in flight where the lists are read, which is the
    // single empty row Cypher starts from
    const Column* const cardinality = reset->getCardinality();
    const size_t rowCount = cardinality ? cardinality->size() : 1;

    reset->getState()->reset(rowCount);

    // The tag holds each row's position, so the body carries it the way it carries any
    // other column and the collect reads the row each value belongs to off it.
    std::vector<uint64_t>& tagRaw = reset->getTag()->getRaw();
    tagRaw.resize(rowCount);
    std::iota(tagRaw.begin(), tagRaw.end(), uint64_t {0});
}

void NLExecutor::runRowListsCollect(NLExecutionContext* context, NLFunctionData* data) {
    NLRowListsCollectData* collect = static_cast<NLRowListsCollectData*>(data);

    NLRowListsState* state = collect->getState();
    const Column* const value = collect->getValue();
    const NLListItemReadFunction valueRead = collect->getValueRead();
    NLStepBuffers* const memory = collect->getStepBuffers();
    memory->clear();

    const ColumnVector<uint64_t>* const tag = collect->getTag();

    if (!tag) {
        const uint64_t row = untaggedRow(collect->getRow());

        for (size_t match = 0; match < value->size(); match++) {
            state->stage(row, valueRead(value, match, memory));
        }

        return;
    }

    const std::vector<uint64_t>& tagRaw = tag->getRaw();

    bioassert(value->size() == tagRaw.size(),
              "Yielded value of a row lists body is not row-aligned with its row tags.");

    for (size_t match = 0; match < tagRaw.size(); match++) {
        state->stage(tagRaw[match], valueRead(value, match, memory));
    }
}

void NLExecutor::runRowListsResult(NLExecutionContext* context, NLFunctionData* data) {
    NLRowListsResultData* result = static_cast<NLRowListsResultData*>(data);

    NLStepBuffers* const memory = result->getStepBuffers();
    memory->clear();

    result->getState()->buildLists(memory->listBuffer(), result->getResult()->getRaw());
}

void NLExecutor::runOptionalReset(NLExecutionContext* context, NLFunctionData* data) {
    NLOptionalResetData* reset = static_cast<NLOptionalResetData*>(data);
    NLOptionalState* state = reset->getState();

    state->reset();

    // The tag holds each input row's position, so the pattern carries it the way it
    // carries any other column and the collect reads the rows it matched off it.
    std::vector<uint64_t>& tagRaw = reset->getTag()->getRaw();
    tagRaw.resize(state->getRowCount());
    std::iota(tagRaw.begin(), tagRaw.end(), uint64_t {0});
}

void NLExecutor::runOptionalCollect(NLExecutionContext* context, NLFunctionData* data) {
    NLOptionalCollectData* collect = static_cast<NLOptionalCollectData*>(data);
    NLOptionalState* state = collect->getState();

    for (const NLSortCollectData::Append& append : collect->appends()) {
        appendCollectedChunk(append, collect->getState()->values());
    }

    const ColumnVector<uint64_t>* tag = collect->getTag();
    if (!tag) {
        // No input row to tag: the step is the single empty row, which this step's rows
        // match - and a predicate that cut them all leaves nothing to match it.
        const std::vector<NLSortCollectData::Append>& appends = collect->appends();
        bioassert(!appends.empty(), "nl.optional_collect needs at least one column");

        if (appends.front()._input->size() > 0) {
            state->markMatched(0);
        }

        return;
    }

    for (const uint64_t row : tag->getRaw()) {
        state->markMatched(row);
    }
}

void NLExecutor::runOptionalDrainLoop(NLExecutionContext* context, NLFunctionData* data) {
    NLOptionalDrainLoopData* loopData = static_cast<NLOptionalDrainLoopData*>(data);
    NLOptionalState* state = loopData->getState();

    const NLStmtContainer* loopBody = loopData->getStmts();
    const size_t chunkSize = context->getChunkSize();
    ColumnVector<size_t>* indices = loopData->getIndices();
    std::vector<size_t>& indicesRaw = indices->getRaw();

    const NLLimitState* limit = loopData->getLimit();
    const auto budgetLeft = [&]() { return !limit || limit->getRemaining() > 0; };

    // The rows the pattern matched, re-chunked out of the buffers the collect grew. A
    // chunk never mixes them with the padded ones: the two are gathered from different
    // columns, so each step reads one or the other.
    const size_t matchedRows = state->getMatchedRowCount();
    for (size_t offset = 0; offset < matchedRows && budgetLeft(); offset += chunkSize) {
        const size_t stepRows = std::min(chunkSize, matchedRows - offset);

        indicesRaw.resize(stepRows);
        std::iota(indicesRaw.begin(), indicesRaw.end(), offset);

        for (const NLOptionalDrainLoopData::DrainColumn& column : loopData->columns()) {
            column._gather(column._buffer, indices, column._output);
        }

        runBody(context, loopBody);
    }

    // Then one row per input row nothing matched: its own values in the columns the
    // pattern joined onto, and null in the ones the pattern binds.
    const std::vector<size_t>& missedRaw = state->missedRows().getRaw();
    const size_t missedRows = missedRaw.size();
    for (size_t offset = 0; offset < missedRows && budgetLeft(); offset += chunkSize) {
        const size_t stepRows = std::min(chunkSize, missedRows - offset);

        indicesRaw.assign(missedRaw.begin() + offset, missedRaw.begin() + offset + stepRows);

        for (const NLOptionalDrainLoopData::DrainColumn& column : loopData->columns()) {
            if (column._input) {
                column._gather(column._input, indices, column._output);
            } else {
                column._fillNull(column._output, stepRows);
            }
        }

        runBody(context, loopBody);
    }
}

void NLExecutor::runSubqueryExpressionReset(NLExecutionContext* context, NLFunctionData* data) {
    NLSubqueryExpressionResetData* reset = static_cast<NLSubqueryExpressionResetData*>(data);
    NLSubqueryExpressionState* state = reset->getState();

    state->reset();

    // The tag holds each input row's position, so the body carries it the way it carries
    // any other column and the mark reads the rows it reached off it.
    std::vector<uint64_t>& tagRaw = reset->getTag()->getRaw();
    tagRaw.resize(state->getRowCount());
    std::iota(tagRaw.begin(), tagRaw.end(), uint64_t {0});
}

void NLExecutor::runExistsMark(NLExecutionContext* context, NLFunctionData* data) {
    NLExistsMarkData* mark = static_cast<NLExistsMarkData*>(data);
    NLExistsState* state = mark->getState();

    const ColumnVector<uint64_t>* tag = mark->getTag();
    if (!tag) {
        // Every row of this step answers for one input row, which they mark - and a
        // predicate that cut them all leaves nothing to mark it.
        const std::vector<const Column*>& columns = mark->columns();
        bioassert(!columns.empty(), "nl.exists_mark needs at least one column when it has no tag");

        if (columns.front()->size() > 0) {
            state->markMatched(untaggedRow(mark->getRow()));
        }

        return;
    }

    for (const uint64_t row : tag->getRaw()) {
        state->markMatched(row);
    }
}

void NLExecutor::runExistsResult(NLExecutionContext* context, NLFunctionData* data) {
    NLExistsResultData* result = static_cast<NLExistsResultData*>(data);
    const NLExistsState* state = result->getState();

    const std::vector<bool>& matched = state->matched();

    std::vector<CustomBool>& answer = result->getResult()->getRaw();
    answer.resize(matched.size());

    std::copy(matched.begin(), matched.end(), answer.begin());
}

void NLExecutor::runCountSubqueryTally(NLExecutionContext* context, NLFunctionData* data) {
    NLCountSubqueryTallyData* tally = static_cast<NLCountSubqueryTallyData*>(data);
    NLCountSubqueryState* state = tally->getState();

    const ColumnVector<uint64_t>* tag = tally->getTag();
    if (!tag) {
        const Column* rows = tally->getRows();
        state->addRows(untaggedRow(tally->getRow()), rows->size());

        return;
    }

    state->addTaggedRows(tag->getRaw());
}

void NLExecutor::runCountSubqueryResult(NLExecutionContext* context, NLFunctionData* data) {
    NLCountSubqueryResultData* result = static_cast<NLCountSubqueryResultData*>(data);
    const NLCountSubqueryState* state = result->getState();

    const std::vector<uint64_t>& counts = state->getCounts();

    std::vector<uint64_t>& answer = result->getResult()->getRaw();
    answer.resize(counts.size());

    std::copy(counts.begin(), counts.end(), answer.begin());
}
