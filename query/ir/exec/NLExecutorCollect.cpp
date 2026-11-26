#include "NLExecutor.h"

#include <algorithm>
#include <span>

#include "ID.h"
#include "columns/ColumnOptVector.h"
#include "columns/ColumnVector.h"
#include "CellOrder.h"
#include "metadata/PropertyType.h"

#include "NLProgram.h"
#include "NLRuntimeProperty.h"
#include "NLColumnKernels.h"
#include "NLLoopSteps.h"
#include "NLTaggedCells.h"

#include "IRException.h"
#include "BioAssert.h"

using namespace db;

namespace {

// Append a chunk's present values to the flat value buffer, recording each element's
// position in its group's list. A null value is skipped (Cypher collect ignores
// nulls). The flat buffer holds the collected type's primitive; the input is its
// nullable value chunk.
template <typename Primitive>
void collectFold(Column* values,
                 const Column* input,
                 const std::vector<size_t>& groups,
                 std::vector<std::vector<size_t>>& groupPositions,
                 const NLGroupDistinctTally* distinct,
                 NLValueStore& store) {
    auto& valuesRaw = static_cast<ColumnVector<Primitive>*>(values)->getRaw();
    const ColumnOptVector<Primitive>* typedInput = static_cast<const ColumnOptVector<Primitive>*>(input);
    const std::vector<Primitive>& inputValues = typedInput->getValues();

    typedInput->forEachPresentRun(0, inputValues.size(), [&](size_t first, size_t last) {
        for (size_t row = first; row < last; row++) {
            const size_t position = valuesRaw.size();
            valuesRaw.push_back(store.ownElement(inputValues[row]));
            groupPositions[groups[row]].push_back(position);
        }
    });
}

// Append this step's values a group has not collected yet - collect(DISTINCT x). A value
// repeated within its group is dropped rather than buffered a second time, so the list is
// the group's distinct values in first-seen order.
template <typename Primitive>
void collectFoldDistinct(Column* values,
                         const Column* input,
                         const std::vector<size_t>& groups,
                         std::vector<std::vector<size_t>>& groupPositions,
                         const NLGroupDistinctTally* distinct,
                         NLValueStore& store) {
    auto& valuesRaw = static_cast<ColumnVector<Primitive>*>(values)->getRaw();
    const ColumnOptVector<Primitive>* typedInput = static_cast<const ColumnOptVector<Primitive>*>(input);
    const std::vector<Primitive>& inputValues = typedInput->getValues();

    typedInput->forEachPresentRun(0, inputValues.size(), [&](size_t first, size_t last) {
        for (size_t row = first; row < last; row++) {
            if (!distinct->isNew(row)) {
                continue;
            }

            const size_t position = valuesRaw.size();
            valuesRaw.push_back(store.ownElement(inputValues[row]));
            groupPositions[groups[row]].push_back(position);
        }
    });
}

// The cell sibling of collectFold, for a chunk holding a value in every row: every row of
// the input joins its group's list.
template <typename Cell>
void collectCellFold(Column* values,
                     const Column* input,
                     const std::vector<size_t>& groups,
                     std::vector<std::vector<size_t>>& groupPositions,
                     const NLGroupDistinctTally* distinct,
                     NLValueStore& store) {
    auto& valuesRaw = static_cast<ColumnVector<Cell>*>(values)->getRaw();
    const auto& inputRaw = static_cast<const ColumnVector<Cell>*>(input)->getRaw();

    for (size_t row = 0; row < inputRaw.size(); row++) {
        groupPositions[groups[row]].push_back(valuesRaw.size());
        valuesRaw.push_back(store.ownElement(inputRaw[row]));
    }
}

// The ID sibling of collectFold: an entity an OPTIONAL MATCH did not match is an invalid
// ID, which is how a null entity is spelled, so it is dropped the way collectFold drops an
// absent optional rather than collected as the value 2^64-1.
template <typename IDType>
void collectValidIDFold(Column* values,
                        const Column* input,
                        const std::vector<size_t>& groups,
                        std::vector<std::vector<size_t>>& groupPositions,
                        const NLGroupDistinctTally* distinct,
                        NLValueStore& store) {
    auto& valuesRaw = static_cast<ColumnVector<IDType>*>(values)->getRaw();
    const auto& inputRaw = static_cast<const ColumnVector<IDType>*>(input)->getRaw();

    const size_t base = valuesRaw.size();
    const auto isValid = [](const IDType id) { return id.isValid(); };

    if (std::ranges::all_of(inputRaw, isValid)) {
        valuesRaw.insert(valuesRaw.end(), inputRaw.begin(), inputRaw.end());

        for (size_t row = 0; row < inputRaw.size(); row++) {
            groupPositions[groups[row]].push_back(base + row);
        }

        return;
    }

    valuesRaw.reserve(base + inputRaw.size());

    for (size_t row = 0; row < inputRaw.size(); row++) {
        if (!inputRaw[row].isValid()) {
            continue;
        }

        const size_t position = valuesRaw.size();
        valuesRaw.push_back(inputRaw[row]);
        groupPositions[groups[row]].push_back(position);
    }
}

// The ID sibling of collectFoldDistinct: an entity repeated within its group joins the
// list once, keyed by the ID's underlying integer, and the null an unmatched pattern left
// is no key of its own.
template <typename IDType>
void collectValidIDFoldDistinct(Column* values,
                                const Column* input,
                                const std::vector<size_t>& groups,
                                std::vector<std::vector<size_t>>& groupPositions,
                                const NLGroupDistinctTally* distinct,
                                NLValueStore& store) {
    auto& valuesRaw = static_cast<ColumnVector<IDType>*>(values)->getRaw();
    const auto& inputRaw = static_cast<const ColumnVector<IDType>*>(input)->getRaw();

    for (size_t row = 0; row < inputRaw.size(); row++) {
        if (!inputRaw[row].isValid()) {
            continue;
        }

        const size_t group = groups[row];

        if (!distinct->isNew(row)) {
            continue;
        }

        const size_t position = valuesRaw.size();
        valuesRaw.push_back(inputRaw[row]);
        groupPositions[group].push_back(position);
    }
}

// The path sibling of collectValidIDFold: the path an OPTIONAL MATCH left where its pattern
// missed holds no node, which is how a null path is spelled, so it is dropped. A kept path is
// copied into the store as a tagged cell, so its lists emit as a type-erased collect's do.
void collectPathFold(Column* values,
                     const Column* input,
                     const std::vector<size_t>& groups,
                     std::vector<std::vector<size_t>>& groupPositions,
                     const NLGroupDistinctTally* distinct,
                     NLValueStore& store) {
    auto& valuesRaw = static_cast<ColumnVector<CellView>*>(values)->getRaw();
    const std::vector<GraphPath>& inputRaw = static_cast<const ColumnVector<GraphPath>*>(input)->getRaw();

    for (size_t row = 0; row < inputRaw.size(); row++) {
        const GraphPath& path = inputRaw[row];
        if (path.isNull()) {
            continue;
        }

        ListBuffer<>::ListItemVariant item {PathView {path.getIDs()}};

        const size_t position = valuesRaw.size();
        valuesRaw.push_back(store.ownCell(item));
        groupPositions[groups[row]].push_back(position);
    }
}

void collectPathFoldDistinct(Column* values,
                             const Column* input,
                             const std::vector<size_t>& groups,
                             std::vector<std::vector<size_t>>& groupPositions,
                             const NLGroupDistinctTally* distinct,
                             NLValueStore& store) {
    auto& valuesRaw = static_cast<ColumnVector<CellView>*>(values)->getRaw();
    const std::vector<GraphPath>& inputRaw = static_cast<const ColumnVector<GraphPath>*>(input)->getRaw();

    for (size_t row = 0; row < inputRaw.size(); row++) {
        const GraphPath& path = inputRaw[row];
        if (path.isNull()) {
            continue;
        }

        const size_t group = groups[row];

        if (!distinct->isNew(row)) {
            continue;
        }

        ListBuffer<>::ListItemVariant item {PathView {path.getIDs()}};

        const size_t position = valuesRaw.size();
        valuesRaw.push_back(store.ownCell(item));
        groupPositions[group].push_back(position);
    }
}

// The type-erased sibling of collectFold: a tagged cell is there in every row, but one
// tagged null is the null Cypher's collect drops, so only the rest join the group's list.
void collectTaggedFold(Column* values,
                       const Column* input,
                       const std::vector<size_t>& groups,
                       std::vector<std::vector<size_t>>& groupPositions,
                       const NLGroupDistinctTally* distinct,
                       NLValueStore& store) {
    auto& valuesRaw = static_cast<ColumnVector<CellView>*>(values)->getRaw();
    const std::vector<CellView>& inputRaw = static_cast<const ColumnVector<CellView>*>(input)->getRaw();

    for (size_t row = 0; row < inputRaw.size(); row++) {
        const std::optional<CellView> element = presentCell(inputRaw[row]);
        if (!element) {
            continue;
        }

        const size_t position = valuesRaw.size();
        valuesRaw.push_back(store.ownElement(*element));
        groupPositions[groups[row]].push_back(position);
    }
}

// The dedup sibling: a tagged cell keys by the value its tag names, so the same number
// reached under two tags keys once - the ordering rules read these cells the same way.
void collectTaggedFoldDistinct(Column* values,
                               const Column* input,
                               const std::vector<size_t>& groups,
                               std::vector<std::vector<size_t>>& groupPositions,
                               const NLGroupDistinctTally* distinct,
                               NLValueStore& store) {
    auto& valuesRaw = static_cast<ColumnVector<CellView>*>(values)->getRaw();
    const std::vector<CellView>& inputRaw = static_cast<const ColumnVector<CellView>*>(input)->getRaw();

    for (size_t row = 0; row < inputRaw.size(); row++) {
        const std::optional<CellView> element = presentCell(inputRaw[row]);
        if (!element) {
            continue;
        }

        const size_t group = groups[row];

        if (!distinct->isNew(row)) {
            continue;
        }

        const size_t position = valuesRaw.size();
        valuesRaw.push_back(store.ownElement(*element));
        groupPositions[group].push_back(position);
    }
}

// The type-erased sibling of collectListEmit: each buffered cell goes into the list
// buffer as the value its own tag names, so the list keeps the types it gathered.
void collectTaggedListEmit(const Column* values,
                           const std::vector<std::vector<size_t>>& groupPositions,
                           size_t begin,
                           size_t count,
                           ListBuffer<>& listBuffer,
                           Column* output) {
    const auto& valuesRaw = static_cast<const ColumnVector<CellView>*>(values)->getRaw();
    auto& outputRaw = static_cast<ColumnVector<ListView>*>(output)->getRaw();

    outputRaw.clear();
    outputRaw.reserve(count);

    std::vector<ListBuffer<>::ListItemVariant> elements;
    for (size_t index = 0; index < count; index++) {
        const std::vector<size_t>& positions = groupPositions[begin + index];

        elements.clear();
        elements.reserve(positions.size());
        for (const size_t position : positions) {
            elements.push_back(taggedItem<ListBuffer<>::ListItemVariant>(valuesRaw[position]));
        }

        outputRaw.push_back(listBuffer.insert(elements));
    }
}

// The list and map sibling of collectValidIDFoldDistinct: two cells are the same value
// when their elements are, so a cell keys by the container serialized element by element
// rather than by its view's own span, which two equal containers never share.
template <typename Container>
void collectContainerFoldDistinct(Column* values,
                                  const Column* input,
                                  const std::vector<size_t>& groups,
                                  std::vector<std::vector<size_t>>& groupPositions,
                                  const NLGroupDistinctTally* distinct,
                                  NLValueStore& store) {
    auto& valuesRaw = static_cast<ColumnVector<Container>*>(values)->getRaw();
    const auto& inputRaw = static_cast<const ColumnVector<Container>*>(input)->getRaw();

    for (size_t row = 0; row < inputRaw.size(); row++) {
        const size_t group = groups[row];

        if (!distinct->isNew(row)) {
            continue;
        }

        const size_t position = valuesRaw.size();
        valuesRaw.push_back(store.ownElement(inputRaw[row]));
        groupPositions[group].push_back(position);
    }
}

// The nullable siblings of collectCellFold and collectContainerFoldDistinct for a list or a
// map chunk: a row an OPTIONAL MATCH left with no container is dropped, as collectFold drops
// an absent optional, so the buffer they fill is a plain container column either way.
template <typename Container>
void collectOptContainerFold(Column* values,
                             const Column* input,
                             const std::vector<size_t>& groups,
                             std::vector<std::vector<size_t>>& groupPositions,
                             const NLGroupDistinctTally* distinct,
                             NLValueStore& store) {
    auto& valuesRaw = static_cast<ColumnVector<Container>*>(values)->getRaw();
    const ColumnOptVector<Container>* typedInput = static_cast<const ColumnOptVector<Container>*>(input);
    const std::vector<Container>& inputValues = typedInput->getValues();

    typedInput->forEachPresentRun(0, inputValues.size(), [&](size_t first, size_t last) {
        for (size_t row = first; row < last; row++) {
            const size_t position = valuesRaw.size();
            valuesRaw.push_back(store.ownElement(inputValues[row]));
            groupPositions[groups[row]].push_back(position);
        }
    });
}

template <typename Container>
void collectOptContainerFoldDistinct(Column* values,
                                     const Column* input,
                                     const std::vector<size_t>& groups,
                                     std::vector<std::vector<size_t>>& groupPositions,
                                     const NLGroupDistinctTally* distinct,
                                     NLValueStore& store) {
    auto& valuesRaw = static_cast<ColumnVector<Container>*>(values)->getRaw();
    const ColumnOptVector<Container>* typedInput = static_cast<const ColumnOptVector<Container>*>(input);
    const std::vector<Container>& inputValues = typedInput->getValues();

    typedInput->forEachPresentRun(0, inputValues.size(), [&](size_t first, size_t last) {
        for (size_t row = first; row < last; row++) {
            if (!distinct->isNew(row)) {
                continue;
            }

            const size_t position = valuesRaw.size();
            valuesRaw.push_back(store.ownElement(inputValues[row]));
            groupPositions[groups[row]].push_back(position);
        }
    });
}

// Emit a chunk of unwound values (nl.unwind_collect): for each flat-buffer position this chunk
// covers, write the present value into the nullable value output. collect dropped
// nulls, so every emitted element is present.
template <typename Primitive>
void unwindCollectValueEmit(const Column* values,
                     const ColumnVector<size_t>* positions,
                     Column* output) {
    const auto& valuesRaw = static_cast<const ColumnVector<Primitive>*>(values)->getRaw();
    ColumnOptVector<Primitive>* typedOutput = static_cast<ColumnOptVector<Primitive>*>(output);
    const auto& positionsRaw = positions->getRaw();

    resetForRowWrites(typedOutput, positionsRaw.size());
    std::vector<Primitive>& outputRaw = typedOutput->getValues();

    for (size_t row = 0; row < positionsRaw.size(); row++) {
        outputRaw[row] = valuesRaw[positionsRaw[row]];
    }
}

// Emit a chunk of per-group lists (nl.collect): for each group in [begin, begin+count),
// gather its elements from the flat buffer into the list buffer as one contiguous run
// and store the resulting ListView in the list output.
template <typename Primitive>
void collectListEmit(const Column* values,
                     const std::vector<std::vector<size_t>>& groupPositions,
                     size_t begin,
                     size_t count,
                     ListBuffer<>& listBuffer,
                     Column* output) {
    const auto& valuesRaw = static_cast<const ColumnVector<Primitive>*>(values)->getRaw();
    auto& outputRaw = static_cast<ColumnVector<ListView>*>(output)->getRaw();

    outputRaw.clear();
    outputRaw.reserve(count);

    std::vector<ListBuffer<>::ListItemVariant> elements;
    for (size_t index = 0; index < count; index++) {
        const std::vector<size_t>& positions = groupPositions[begin + index];

        elements.clear();
        elements.reserve(positions.size());
        for (const size_t position : positions) {
            elements.push_back(ListBuffer<>::ListItemVariant {valuesRaw[position]});
        }

        outputRaw.push_back(listBuffer.insert(elements));
    }
}

// Invoke handler with the element a collect of a nullable chunk of @param kind buffers: a
// scalar, an embedding, a schema ID or a list. A map collects through handlers of its own.
template <typename Handler>
void dispatchCollectedChunkKind(NLChunkKind kind, Handler&& handler) {
    switch (kind) {
        case NLChunkKind::EdgeTypeID:
            return handler.template operator()<EdgeTypeID>();
        break;

        case NLChunkKind::LabelID:
            return handler.template operator()<LabelID>();
        break;

        case NLChunkKind::PropertyTypeID:
            return handler.template operator()<PropertyTypeID>();
        break;

        case NLChunkKind::ValueTypeCode:
            return handler.template operator()<ValueType>();
        break;

        case NLChunkKind::Int64:
            return handler.template operator()<types::Int64::Primitive>();
        break;

        case NLChunkKind::UInt64:
            return handler.template operator()<types::UInt64::Primitive>();
        break;

        case NLChunkKind::Double:
            return handler.template operator()<types::Double::Primitive>();
        break;

        case NLChunkKind::Bool:
            return handler.template operator()<types::Bool::Primitive>();
        break;

        case NLChunkKind::String:
            return handler.template operator()<types::String::Primitive>();
        break;

        case NLChunkKind::Embedding:
            return handler.template operator()<types::Embedding::Primitive>();
        break;

        case NLChunkKind::List:
            return handler.template operator()<ListView>();
        break;

        case NLChunkKind::DateTime:
            return handler.template operator()<types::DateTime::Primitive>();
        break;

        case NLChunkKind::Duration:
            return handler.template operator()<types::Duration::Primitive>();
        break;

        case NLChunkKind::Point:
            return handler.template operator()<types::Point::Primitive>();
        break;

        case NLChunkKind::Map:
        case NLChunkKind::NodeID:
        case NLChunkKind::EdgeID:
        case NLChunkKind::LabelSetID:
        case NLChunkKind::Path:
        case NLChunkKind::PathRef:
            throw IRException("collect does not support this value type");
        break;
    }

    bioassert(false, "Unknown NLChunkKind");
}

// The fold and the list emit an entity collect of this ID reads. The list emits through
// the value path's template - its elements are the IDs the fold appended - so there is
// no entity emit of its own.
template <typename IDType>
void selectCollectIDHandlers(bool distinctValues,
                             NLCollectFoldFunction& fold,
                             NLCollectListEmitFunction& listEmit) {
    fold = distinctValues ? &collectValidIDFoldDistinct<IDType> : &collectValidIDFold<IDType>;
    listEmit = &collectListEmit<IDType>;
}

}

void NLExecutor::runCollectReset(NLExecutionContext* context, NLFunctionData* data) {
    const NLCollectResetData* reset = static_cast<NLCollectResetData*>(data);
    reset->getState()->reset();
}

void NLExecutor::runCollectUpdate(NLExecutionContext* context, NLFunctionData* data) {
    NLCollectUpdateData* update = static_cast<NLCollectUpdateData*>(data);
    NLCollectState* state = update->getState();

    NLGroupTable& groupTable = state->groupTable();
    const std::vector<NLGroupTable::KeyColumn>& keyColumns = groupTable.keyColumns();
    std::vector<NLCollectState::ValueColumn>& valueColumns = state->valueColumns();

    // Every column is row-aligned. With grouping keys the first key sizes this step's
    // rows; ungrouped (no key), the first collected value column sizes it.
    const size_t rowCount = keyColumns.empty() ? valueColumns.front()._input->size()
                                               : keyColumns.front()._input->size();

    // That alignment is this step's precondition, not a property of the op's types:
    // the group assignment is computed once per key row, then indexed by the fold as it
    // walks the value column, so a column of another length appends values to the wrong
    // group - or, when the value column is the longer one, reads past the assignments
    // entirely. Generated IR places the update where every column is bound together, so
    // a mismatch here means malformed IR.
    for (const NLGroupTable::KeyColumn& keyColumn : keyColumns) {
        bioassert(keyColumn._input->size() == rowCount,
                  "nl.collect_update grouping keys must be row-aligned");
    }

    for (const NLCollectState::ValueColumn& valueColumn : valueColumns) {
        bioassert(valueColumn._input->size() == rowCount,
                  "nl.collect_update value columns must be row-aligned with the grouping keys");
    }

    NLGroupAggregates& aggregates = state->aggregates();
    for (const NLGroupAggregates::Aggregate& aggregate : aggregates.getAggregates()) {
        bioassert(aggregate._input->size() == rowCount,
                  "nl.collect_update aggregate inputs must be row-aligned with the grouping keys");
    }

    if (rowCount == 0) {
        return;
    }

    groupTable.assign(rowCount);

    const size_t groupCount = groupTable.getGroupCount();
    const ColumnVector<size_t>* rowGroups = groupTable.getRowGroupColumn();
    const std::vector<size_t>& groupIndices = rowGroups->getRaw();

    // Grow the per-group position lists to the new group count, then append this step's
    // present values to their groups' lists.
    for (NLCollectState::ValueColumn& valueColumn : valueColumns) {
        valueColumn._groupPositions.resize(groupCount);

        if (valueColumn._distinct) {
            valueColumn._distinct->assign(rowGroups, valueColumn._input);
        }

        valueColumn._fold(valueColumn._buffer,
                          valueColumn._input,
                          groupIndices,
                          valueColumn._groupPositions,
                          valueColumn._distinct,
                          state->collectedValues());
    }

    // The reductions taken over the same groups fold beside the list, off the group
    // assignment computed once above, exactly as nl.group_aggregate_update folds them.
    aggregates.grow(groupCount);
    aggregates.fold(rowGroups);
    aggregates.compactValues();
}

NLCollectFoldFunction NLExecutor::selectCollectFold(NLChunkKind kind) {
    NLCollectFoldFunction selected = nullptr;
    dispatchCollectedChunkKind(kind, [&]<typename Element>() { selected = &collectFold<Element>; });

    return selected;
}

// The collect(DISTINCT x) fold for a column of this kind: collect's own domain, with each of
// a group's values buffered once
NLCollectFoldFunction NLExecutor::selectCollectDistinctFold(NLChunkKind kind) {
    NLCollectFoldFunction selected = nullptr;
    dispatchCollectedChunkKind(kind, [&]<typename Element>() { selected = &collectFoldDistinct<Element>; });

    return selected;
}

NLUnwindCollectValueEmitFunction NLExecutor::selectUnwindCollectValueEmit(NLChunkKind kind) {
    NLUnwindCollectValueEmitFunction selected = nullptr;
    dispatchCollectedChunkKind(kind, [&]<typename Element>() { selected = &unwindCollectValueEmit<Element>; });

    return selected;
}

NLCollectListEmitFunction NLExecutor::selectCollectListEmit(NLChunkKind kind) {
    NLCollectListEmitFunction selected = nullptr;
    dispatchCollectedChunkKind(kind, [&]<typename Element>() { selected = &collectListEmit<Element>; });

    return selected;
}

void NLExecutor::selectCollectEntityHandlers(NLChunkKind kind,
                                             bool distinctValues,
                                             NLCollectFoldFunction& fold,
                                             NLCollectListEmitFunction& listEmit) {
    switch (kind) {
        case NLChunkKind::NodeID:
            return selectCollectIDHandlers<NodeID>(distinctValues, fold, listEmit);
        break;

        case NLChunkKind::EdgeID:
            return selectCollectIDHandlers<EdgeID>(distinctValues, fold, listEmit);
        break;

        default:
            throw IRException("collect does not support this chunk kind");
        break;
    }
}

// A list cell is present in every row, so it folds with no null to drop; only the dedup
// differs, keying on the elements rather than on the cell.
template <typename Container>
void NLExecutor::selectCollectContainerHandlers(bool distinctValues,
                                                NLCollectFoldFunction& fold,
                                                NLCollectListEmitFunction& listEmit) {
    fold = distinctValues ? &collectContainerFoldDistinct<Container> : &collectCellFold<Container>;
    listEmit = &collectListEmit<Container>;
}

template <typename Container>
void NLExecutor::selectCollectOptContainerHandlers(bool distinctValues,
                                                   NLCollectFoldFunction& fold,
                                                   NLCollectListEmitFunction& listEmit) {
    fold = distinctValues ? &collectOptContainerFoldDistinct<Container> : &collectOptContainerFold<Container>;
    listEmit = &collectListEmit<Container>;
}

template void NLExecutor::selectCollectContainerHandlers<ListView>(bool distinctValues,
                                                                   NLCollectFoldFunction& fold,
                                                                   NLCollectListEmitFunction& listEmit);
template void NLExecutor::selectCollectContainerHandlers<MapView>(bool distinctValues,
                                                                  NLCollectFoldFunction& fold,
                                                                  NLCollectListEmitFunction& listEmit);
template void NLExecutor::selectCollectOptContainerHandlers<ListView>(bool distinctValues,
                                                                      NLCollectFoldFunction& fold,
                                                                      NLCollectListEmitFunction& listEmit);
template void NLExecutor::selectCollectOptContainerHandlers<MapView>(bool distinctValues,
                                                                     NLCollectFoldFunction& fold,
                                                                     NLCollectListEmitFunction& listEmit);

// A type-erased cell carries its own type, so the fold drops the ones tagged null and the
// emit writes each survivor back under the type its tag names.
void NLExecutor::selectCollectTaggedHandlers(bool distinctValues,
                                             NLCollectFoldFunction& fold,
                                             NLCollectListEmitFunction& listEmit) {
    fold = distinctValues ? &collectTaggedFoldDistinct
                          : &collectTaggedFold;
    listEmit = &collectTaggedListEmit;
}

void NLExecutor::selectCollectPathHandlers(bool distinctValues,
                                           NLCollectFoldFunction& fold,
                                           NLCollectListEmitFunction& listEmit) {
    fold = distinctValues ? &collectPathFoldDistinct : &collectPathFold;
    listEmit = &collectTaggedListEmit;
}

void NLExecutor::runUnwindCollectLoop(NLExecutionContext* context, NLFunctionData* data) {
    NLUnwindCollectLoopData* loopData = static_cast<NLUnwindCollectLoopData*>(data);
    NLCollectState* state = loopData->getState();

    const size_t chunkSize = context->getChunkSize();
    const NLStmtContainer* loopBody = loopData->getStmts();

    NLGroupTable& groupTable = state->groupTable();
    NLCollectState::ValueColumn& unwound = state->unwoundColumn();
    const std::vector<std::vector<size_t>>& groupPositions = unwound._groupPositions;
    const size_t totalGroups = groupPositions.size();

    ColumnVector<size_t>* groupIndices = loopData->getGroupIndices();
    ColumnVector<size_t>* positions = loopData->getPositions();

    // Walk every (group, element) pair in group order. currentGroup / indexInGroup is
    // the cursor into that flattened sequence; empty groups (all values null) are
    // skipped, so they contribute no row - matching UNWIND of an empty list.
    size_t currentGroup = 0;
    size_t indexInGroup = 0;
    while (currentGroup < totalGroups && groupPositions[currentGroup].empty()) {
        currentGroup++;
    }

    while (currentGroup < totalGroups) {
        std::vector<size_t>& groupIndicesRaw = groupIndices->getRaw();
        std::vector<size_t>& positionsRaw = positions->getRaw();
        groupIndicesRaw.clear();
        positionsRaw.clear();

        // Fill up to chunkSize rows, each the next (group, element) pair.
        while (groupIndicesRaw.size() < chunkSize && currentGroup < totalGroups) {
            const std::vector<size_t>& groupPos = groupPositions[currentGroup];

            groupIndicesRaw.push_back(currentGroup);
            positionsRaw.push_back(groupPos[indexInGroup]);
            indexInGroup++;

            if (indexInGroup == groupPos.size()) {
                indexInGroup = 0;
                currentGroup++;
                while (currentGroup < totalGroups && groupPositions[currentGroup].empty()) {
                    currentGroup++;
                }
            }
        }

        // The key values repeat once per element (gather by the per-row group index);
        // the element values come from the flat buffer at this chunk's positions.
        groupTable.gatherKeys(groupIndices);

        unwound._unwindCollectEmit(unwound._buffer, positions, unwound._output);

        runBody(context, loopBody);
    }
}

void NLExecutor::runCollectLoop(NLExecutionContext* context, NLFunctionData* data) {
    NLCollectLoopData* loopData = static_cast<NLCollectLoopData*>(data);
    NLCollectState* state = loopData->getState();

    NLGroupTable& groupTable = state->groupTable();
    const size_t totalGroups = groupTable.getGroupCount();
    const size_t chunkSize = context->getChunkSize();
    const NLStmtContainer* loopBody = loopData->getStmts();

    NLGroupAggregates& aggregates = state->aggregates();
    NLStepBuffers* const stepBuffers = loopData->getStepBuffers();

    // Re-chunk the groups: each step slices the next chunk of key values from the key
    // buffers and materializes one list cell per group (a ListView over that group's
    // run in the list buffer), then runs the body. A grouped collect over no row has no
    // group, so the body runs zero times and it emits nothing; an ungrouped collect
    // always carries the single group the reset created, so it emits exactly one row -
    // holding the empty list when no value was folded.
    for (size_t offset = 0; offset < totalGroups; offset += chunkSize) {
        const size_t stepGroups = std::min(chunkSize, totalGroups - offset);

        groupTable.emitKeys(offset, stepGroups);

        for (NLCollectState::ValueColumn& valueColumn : state->valueColumns()) {
            valueColumn._listEmit(valueColumn._buffer,
                                  valueColumn._groupPositions,
                                  offset,
                                  stepGroups,
                                  state->listBuffer(),
                                  valueColumn._output);
        }

        stepBuffers->clear();
        aggregates.emit(offset, stepGroups, stepBuffers);

        runBody(context, loopBody);
    }
}
