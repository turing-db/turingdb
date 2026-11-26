#include "NLExecutor.h"

#include <stdint.h>
#include <algorithm>
#include <span>
#include <set>

#include "columns/ColumnOptVector.h"
#include "columns/ColumnVector.h"
#include "metadata/PropertyType.h"

#include "NLProgram.h"
#include "NLRuntimeProperty.h"
#include "NLColumnKernels.h"
#include "NLElementCursor.h"
#include "NLLoopSteps.h"

#include "BioAssert.h"

using namespace db;

namespace {

// The truth value a list predicate holds for a row, from how many of its elements held,
// failed or answered null: an unknown element leaves the answer unknown unless the known
// ones already decide it
std::optional<bool> decideListPredicate(ListPredicateKind kind, size_t trues, size_t falses, size_t nulls) {
    switch (kind) {
        case ListPredicateKind::All:
            if (falses > 0) {
                return false;
            } else if (nulls > 0) {
                return std::nullopt;
            }

            return true;
        break;

        case ListPredicateKind::Any:
            if (trues > 0) {
                return true;
            } else if (nulls > 0) {
                return std::nullopt;
            }

            return false;
        break;

        case ListPredicateKind::None:
            if (trues > 0) {
                return false;
            } else if (nulls > 0) {
                return std::nullopt;
            }

            return true;
        break;

        case ListPredicateKind::Single:
            if (trues > 1) {
                return false;
            } else if (nulls > 0) {
                return std::nullopt;
            }

            return trues == 1;
        break;
    }

    return std::nullopt;
}

bool isListPredicateDecided(ListPredicateKind kind, size_t trues, size_t falses) {
    switch (kind) {
        case ListPredicateKind::All:
            return falses > 0;
        break;

        case ListPredicateKind::Any:
            return trues > 0;
        break;

        case ListPredicateKind::None:
            return trues > 0;
        break;

        case ListPredicateKind::Single:
            return trues > 1;
        break;
    }

    return false;
}

// Runs the body of an op over the elements of one row's list a chunk of (row, element)
// pairs at a time, and hands @param takeYield the tags of the elements each chunk yielded
template <typename TakeYield>
void runElementBody(NLExecutionContext* context, NLElementBodyData* data, NLElementCursor& cursor, TakeYield takeYield) {
    const Column* source = data->getSource();
    const NLStmtContainer* body = data->getStmts();
    const NLUnwindElementEmitFunction elementEmit = data->getElementEmitFunc();
    const size_t chunkSize = context->getChunkSize();

    ColumnVector<size_t>* rows = data->getRows();
    ColumnVector<size_t>* positions = data->getPositions();
    ColumnVector<uint64_t>* rowTags = data->getRowTags();

    while (!cursor.exhausted()) {
        cursor.fillChunk(chunkSize, rows->getRaw(), elementEmit ? &positions->getRaw() : nullptr);

        // The tag of an element is the row the pair came from: the body hands back the
        // tags of the elements its WHERE kept
        const std::vector<size_t>& rowsRaw = rows->getRaw();
        std::vector<uint64_t>& rowTagsRaw = rowTags->getRaw();
        rowTagsRaw.assign(rowsRaw.begin(), rowsRaw.end());

        if (elementEmit) {
            elementEmit(source, rows, positions, data->getElementOutput());
        }

        for (const NLCarriedColumn& carriedColumn : data->carriedColumns()) {
            const auto gatherFunc = carriedColumn.getGatherFunc();
            gatherFunc(carriedColumn.getInput(), rows, carriedColumn.getOutput());
        }

        runBody(context, body);

        const std::vector<uint64_t>& yieldedRaw =
            static_cast<const ColumnVector<uint64_t>*>(data->getYieldedRowTags())->getRaw();

        bioassert(data->getValue()->size() == yieldedRaw.size(),
                  "Yielded value of an op over list elements is not row-aligned with its row tags.");

        takeYield(yieldedRaw);
    }
}

// Folds each row's list into the accumulator, the body running once per element position
// over the rows whose list reaches it. @param testStep runs after each chunk of rows a step
// folded has its accumulators written, and hands back the rows whose fold ends there.
template <typename TestStep>
void foldElements(NLExecutionContext* context, NLFoldData* fold, TestStep testStep) {
    const Column* source = fold->getSource();
    const NLUnwindElementCountFunction elementCount = fold->getElementCountFunc();
    const NLUnwindElementEmitFunction elementEmit = fold->getElementEmitFunc();
    const NLCellAbsentFunction cellAbsent = fold->getCellAbsentFunc();
    const Column* initialValue = fold->getInitialValue();
    const Column* value = fold->getValue();
    const NLStmtContainer* body = fold->getStmts();
    Column* accumulator = fold->getAccumulator();
    NLStepBuffers* const memory = fold->getStepBuffers();
    memory->clear();
    NLCompactingValueStore& accumulatorValues = fold->accumulatorValues();
    const NLReownFunction reown = fold->getReown();
    const NLReownRowsFunction reownRows = fold->getReownRows();

    const size_t sourceRows = source->size();
    const size_t chunkSize = context->getChunkSize();

    const auto write = [&](const NLCellWrite& rowWrite, size_t row, const Column* written, size_t writtenRow) {
        writeCell(accumulator, row, rowWrite, written, writtenRow, memory, accumulatorValues.getActive());
    };

    accumulatorValues.clear();
    fold->getReset()(accumulator, sourceRows);

    std::vector<size_t>& elementCounts = fold->elementCounts();
    std::vector<size_t>& reachingRows = fold->reachingRows();
    std::vector<size_t>& endedRows = fold->endedRows();
    elementCounts.assign(sourceRows, 0);
    reachingRows.clear();

    // A row whose cell holds no list keeps the null the reset left it, as Cypher reads
    // `reduce(s = 0, x IN null | s + x)`
    for (size_t row = 0; row < sourceRows; row++) {
        if (cellAbsent(source, row)) {
            continue;
        }

        write(fold->getInitialWrite(), row, initialValue, row);

        elementCounts[row] = elementCount(source, row);
        if (elementCounts[row] > 0) {
            reachingRows.push_back(row);
        }
    }

    std::vector<size_t>& rowsRaw = fold->getRows()->getRaw();
    std::vector<size_t>& positionsRaw = fold->getPositions()->getRaw();

    for (size_t position = 0; !reachingRows.empty(); position++) {
        for (size_t first = 0; first < reachingRows.size(); first += chunkSize) {
            const size_t last = std::min(first + chunkSize, reachingRows.size());
            rowsRaw.assign(reachingRows.begin() + first, reachingRows.begin() + last);

            if (elementEmit) {
                positionsRaw.assign(rowsRaw.size(), position);
                elementEmit(source, fold->getRows(), fold->getPositions(), fold->getElementOutput());
            }

            for (const NLCarriedColumn& carriedColumn : fold->carriedColumns()) {
                const auto gatherFunc = carriedColumn.getGatherFunc();
                gatherFunc(carriedColumn.getInput(), fold->getRows(), carriedColumn.getOutput());
            }

            runBody(context, body);

            bioassert(value->size() == rowsRaw.size(),
                      "Yielded accumulator of a fold is not row-aligned with its elements.");

            for (size_t index = 0; index < rowsRaw.size(); index++) {
                write(fold->getValueWrite(), rowsRaw[index], value, index);
            }

            if (reownRows) {
                reownRows(accumulator, rowsRaw, accumulatorValues.getActive());
            }

            endedRows.clear();
            testStep(endedRows);

            for (const size_t row : endedRows) {
                elementCounts[row] = position + 1;
            }
        }

        std::erase_if(reachingRows, [&](size_t row) { return elementCounts[row] == position + 1; });

        if (reown && !reachingRows.empty() && accumulatorValues.needsCompaction(sourceRows)) {
            reown(accumulator, accumulatorValues.getSpare());
            accumulatorValues.flip();
        }
    }

    const NLCaseFinishFn finish = fold->getFinish();
    if (finish) {
        finish(accumulator);
    }
}

}

void NLExecutor::runListPredicate(NLExecutionContext* context, NLFunctionData* data) {
    NLListPredicateData* predicate = static_cast<NLListPredicateData*>(data);

    const Column* source = predicate->getSource();

    const NLCellAbsentFunction cellAbsent = predicate->getCellAbsentFunc();
    const NLTruthReadFunction truthRead = predicate->getTruthRead();
    const Column* value = predicate->getValue();

    NLElementCursor cursor(source, predicate->getElementCountFunc());
    const size_t sourceRows = cursor.getSourceRows();

    std::vector<size_t>& trueCounts = predicate->trueCounts();
    std::vector<size_t>& falseCounts = predicate->falseCounts();
    std::vector<size_t>& nullCounts = predicate->nullCounts();
    trueCounts.assign(sourceRows, 0);
    falseCounts.assign(sourceRows, 0);
    nullCounts.assign(sourceRows, 0);

    const ListPredicateKind kind = predicate->getKind();

    runElementBody(context, predicate, cursor, [&](const std::vector<uint64_t>& taggedRaw) {
        for (size_t element = 0; element < taggedRaw.size(); element++) {
            const std::optional<bool> truth = truthRead(value, element);
            const size_t row = taggedRaw[element];

            if (!truth.has_value()) {
                nullCounts[row]++;
            } else if (*truth) {
                trueCounts[row]++;
            } else {
                falseCounts[row]++;
            }
        }

        if (!cursor.exhausted()) {
            const size_t openRow = cursor.getRow();
            if (isListPredicateDecided(kind, trueCounts[openRow], falseCounts[openRow])) {
                cursor.skipRow();
            }
        }
    });

    ColumnOptVector<CustomBool>* result = static_cast<ColumnOptVector<CustomBool>*>(predicate->getResult());
    resetForRowWrites(result, sourceRows);

    for (size_t row = 0; row < sourceRows; row++) {
        if (cellAbsent(source, row)) {
            result->setNull(row);
            continue;
        }

        const std::optional<bool> decided = decideListPredicate(kind, trueCounts[row], falseCounts[row], nullCounts[row]);
        if (decided.has_value()) {
            result->set(row, CustomBool(*decided));
        } else {
            result->setNull(row);
        }
    }
}

void NLExecutor::runListComprehension(NLExecutionContext* context, NLFunctionData* data) {
    NLListComprehensionData* comprehension = static_cast<NLListComprehensionData*>(data);

    const Column* source = comprehension->getSource();

    const NLCellAbsentFunction cellAbsent = comprehension->getCellAbsentFunc();
    const NLListItemReadFunction valueRead = comprehension->getValueRead();
    const Column* value = comprehension->getValue();
    NLStepBuffers* const memory = comprehension->getStepBuffers();
    memory->clear();
    ListBuffer<>& listBuffer = memory->listBuffer();

    NLElementCursor cursor(source, comprehension->getElementCountFunc());
    const size_t sourceRows = cursor.getSourceRows();

    // What the rows the cursor still has open gathered: the elements arrive in row order,
    // so a row's elements are one contiguous run of this buffer and the counts locate it
    std::vector<ListBuffer<>::ListItemVariant>& staged = comprehension->stagedElements();
    std::vector<size_t>& stagedCounts = comprehension->stagedCounts();
    NLValueStore& stagedValues = comprehension->stagedValues();
    staged.clear();
    stagedValues.clear();
    stagedCounts.assign(sourceRows, 0);

    ColumnOptVector<ListView>* result = static_cast<ColumnOptVector<ListView>*>(comprehension->getResult());
    result->clear();
    result->reserve(sourceRows);

    // A row's list is complete as soon as the run past it starts, so the rows behind the
    // cursor are built and dropped at every step rather than the whole source being held
    const auto buildRowsBefore = [&](size_t end) {
        size_t builtElements = 0;

        while (result->size() < end) {
            const size_t row = result->size();
            const size_t rowElements = stagedCounts[row];
            const std::span<const ListBuffer<>::ListItemVariant> elements {staged.data() + builtElements,
                                                                          rowElements};

            // A cell holding no list gives its row no list either, which is what Cypher
            // reads `[x IN null | x]` as - as opposed to the empty list a cell whose
            // elements the predicate all dropped gives
            if (cellAbsent(source, row)) {
                result->pushNull();
            } else {
                result->push_back(listBuffer.insert(elements));
            }

            builtElements += rowElements;
        }

        staged.erase(staged.begin(), staged.begin() + builtElements);
    };

    runElementBody(context, comprehension, cursor, [&](const std::vector<uint64_t>& keptRaw) {
        for (size_t element = 0; element < keptRaw.size(); element++) {
            staged.push_back(stagedValues.ownItem(valueRead(value, element, memory)));
            stagedCounts[keptRaw[element]]++;
        }

        buildRowsBefore(cursor.getRow());
    });

    buildRowsBefore(sourceRows);
}

void NLExecutor::runReduce(NLExecutionContext* context, NLFunctionData* data) {
    NLFoldData* fold = static_cast<NLFoldData*>(data);

    foldElements(context, fold, [](std::vector<size_t>& endedRows) {
    });
}

void NLExecutor::runAllReduce(NLExecutionContext* context, NLFunctionData* data) {
    NLAllReduceData* allReduce = static_cast<NLAllReduceData*>(data);

    const Column* source = allReduce->getSource();
    const NLCellAbsentFunction cellAbsent = allReduce->getCellAbsentFunc();
    ColumnVector<size_t>* rows = allReduce->getRows();
    const Column* accumulator = allReduce->getAccumulator();
    Column* testedAccumulator = allReduce->getTestedAccumulator();
    const NLGatherFunction accumulatorGather = allReduce->getAccumulatorGather();
    const NLStmtContainer* predicate = allReduce->getPredicateStmts();
    const NLTruthReadFunction truthRead = allReduce->getTruthRead();
    const Column* holds = allReduce->getHolds();

    const size_t sourceRows = source->size();

    std::vector<bool>& failedRows = allReduce->failedRows();
    std::vector<bool>& unknownRows = allReduce->unknownRows();
    failedRows.assign(sourceRows, false);
    unknownRows.assign(sourceRows, false);

    foldElements(context, allReduce, [&](std::vector<size_t>& endedRows) {
        accumulatorGather(accumulator, rows, testedAccumulator);
        runBody(context, predicate);

        const std::vector<size_t>& rowsRaw = rows->getRaw();

        bioassert(holds->size() == rowsRaw.size(),
                  "Yielded predicate of an nl.all_reduce is not row-aligned with its elements.");

        for (size_t index = 0; index < rowsRaw.size(); index++) {
            const size_t row = rowsRaw[index];
            const std::optional<bool> truth = truthRead(holds, index);

            if (!truth.has_value()) {
                unknownRows[row] = true;
            } else if (!*truth) {
                failedRows[row] = true;
                endedRows.push_back(row);
            }
        }
    });

    ColumnOptVector<CustomBool>* result = static_cast<ColumnOptVector<CustomBool>*>(allReduce->getResult());
    resetForRowWrites(result, sourceRows);

    for (size_t row = 0; row < sourceRows; row++) {
        if (cellAbsent(source, row)) {
            result->setNull(row);
        } else if (failedRows[row]) {
            result->set(row, CustomBool(false));
        } else if (unknownRows[row]) {
            result->setNull(row);
        } else {
            result->set(row, CustomBool(true));
        }
    }
}
