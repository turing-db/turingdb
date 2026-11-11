#include "NLProgram.h"

#include <algorithm>
#include <array>
#include <limits>
#include <numeric>

#include "NLExecutionContext.h"
#include "NLRuntimeProperty.h"
#include "NLMergeWorkingSet.h"
#include "Procedure.h"
#include "ProcedureData.h"
#include "NumberOrder.h"

#include "BioAssert.h"
#include "IRException.h"

using namespace db;

namespace {

// The aggregates take turns folding a batch of rows, so the blocks a batch reaches stay in
// the L1 cache from one aggregate to the next
constexpr size_t FOLD_BATCH_ROWS = 256;

size_t alignOffset(size_t offset, size_t alignment) {
    return (offset + alignment - 1) / alignment * alignment;
}

template <Number Target>
bool convertNumber(const NLScanByPropertyValueLoopData::Literal& given, NLScanByPropertyValueLoopData::Literal& converted) {
    Target number {};

    bool converts = false;
    if (const int64_t* integer = std::get_if<int64_t>(&given)) {
        converts = convertNumberExactly(*integer, number);
    } else if (const double* floating = std::get_if<double>(&given)) {
        converts = convertNumberExactly(*floating, number);
    }

    if (converts) {
        converted = number;
    }

    return converts;
}

}

NLProgram::NLProgram() {
}

NLProgram::~NLProgram() {
}

void NLProgram::setColumnNames(std::span<const std::string_view> names) {
    _columnNames.assign(names.begin(), names.end());
}

void NLProgram::setOutputData(NLOutputData* outputData) {
    if (!_outputData) {
        _outputData = outputData;
        return;
    }

    // The first output's columns are the schema the sink is handed, so a later one writing
    // a column of another type would stream rows the declared header does not describe
    const NLOutputData::OutputColumns& declared = _outputData->outputs();
    const NLOutputData::OutputColumns& emitted = outputData->outputs();
    bioassert(declared.size() == emitted.size(), "Every output of a program declares the same columns");

    for (size_t index = 0; index < declared.size(); index++) {
        const Column* const declaredColumn = declared[index];
        const Column* const emittedColumn = emitted[index];

        const bool sameContainer = declaredColumn->getContainerKind() == emittedColumn->getContainerKind();
        const bool sameInternal = declaredColumn->getInternalKind() == emittedColumn->getInternalKind();

        bioassert(sameContainer && sameInternal, "Every output of a program declares the same column types");
    }
}

NLMergeNodeIndex* NLProgram::findMergeNodeIndex(const std::string& signature) const {
    const auto findIt = _mergeNodeIndexes.find(signature);
    if (findIt == end(_mergeNodeIndexes)) {
        return nullptr;
    }

    return findIt->second.get();
}

NLMergeNodeIndex* NLProgram::addMergeNodeIndex(const std::string& signature,
                                               const LabelSet& labels,
                                               const LabelSet& writeLabels,
                                               bool matchable,
                                               ColumnNodeIDs* scanNodes) {
    auto index = std::make_unique<NLMergeNodeIndex>(labels, writeLabels, matchable, scanNodes);
    NLMergeNodeIndex* indexPtr = index.get();
    _mergeNodeIndexes.emplace(signature, std::move(index));

    return indexPtr;
}

Column* db::allocOptValueColumn(LocalMemory* memory, ValueType valueType, size_t reserveSize) {
    Column* column = nullptr;
    const auto allocate = [&]<SupportedType T>() {
        ColumnOptVector<typename T::Primitive>* typed = memory->alloc<ColumnOptVector<typename T::Primitive>>();
        typed->reserve(reserveSize);
        column = typed;
    };
    ValueTypeDispatcher(valueType).execute(allocate);

    return column;
}

NLChunkKind db::chunkKindFromValueType(ValueType valueType) {
    switch (valueType) {
        case ValueType::Int64:
            return NLChunkKind::Int64;
        break;

        case ValueType::UInt64:
            return NLChunkKind::UInt64;
        break;

        case ValueType::Double:
            return NLChunkKind::Double;
        break;

        case ValueType::String:
            return NLChunkKind::String;
        break;

        case ValueType::Bool:
            return NLChunkKind::Bool;
        break;

        case ValueType::Embedding:
            return NLChunkKind::Embedding;
        break;

        case ValueType::List:
            return NLChunkKind::List;
        break;

        case ValueType::DateTime:
            return NLChunkKind::DateTime;
        break;

        case ValueType::Map:
            return NLChunkKind::Map;
        break;

        case ValueType::Duration:
            return NLChunkKind::Duration;
        break;

        case ValueType::Point:
            return NLChunkKind::Point;
        break;

        case ValueType::Invalid:
        case ValueType::_SIZE:
            throw IRException("No column is laid out for an invalid value type");
        break;
    }

    bioassert(false, "Unknown ValueType");
    return NLChunkKind::Int64;
}

NLRuntimeProperty* NLProgram::getRuntimeProperty(std::string_view name) {
    std::unique_ptr<NLRuntimeProperty>& property = _runtimeProperties[std::string(name)];
    if (!property) {
        property = std::make_unique<NLRuntimeProperty>(name);
    }

    return property.get();
}

void NLScanByPropertyValueLoopData::setBoundValue(const Column* boundValue, ValueType boundType) {
    _boundValue = boundValue;
    _boundType = boundType;
}

bool NLScanByPropertyValueLoopData::convertToStored(ValueType stored, const Literal& given, Literal& converted) {
    if (stored == ValueType::Int64) {
        return convertNumber<types::Int64::Primitive>(given, converted);
    } else if (stored == ValueType::UInt64) {
        return convertNumber<types::UInt64::Primitive>(given, converted);
    } else if (stored == ValueType::Double) {
        return convertNumber<types::Double::Primitive>(given, converted);
    } else if (stored == ValueType::Bool && std::holds_alternative<CustomBool>(given)) {
        converted = given;
        return true;
    } else if (stored == ValueType::String && std::holds_alternative<std::string>(given)) {
        converted = given;
        return true;
    }

    return false;
}

NLProcedureState::NLProcedureState(const Procedure* procedure,
                                   ProcedureData* data,
                                   const ProcedureContext* context)
    : _procedure(procedure),
    _data(data),
    _context(*context)
{
    _context.setListBuffer(&_stepBuffers.listBuffer());
    _context.setStringBuffer(&_stepBuffers.stringBuffer());

    _procedureState.setData(data);
    _procedureState.setContext(&_context);
}

NLProcedureState::~NLProcedureState() {
    const Procedure::DeallocCallback dealloc = _procedure->getDeallocCallback();
    if (dealloc) {
        dealloc(_data);
    }
}

NLShortestPathState::~NLShortestPathState() {
}

NLExploreStep::NLExploreStep(PathExplorationDir direction,
                             bool filtersByType,
                             std::span<const EdgeTypeID> edgeTypes,
                             bool matchable)
    : _direction(direction),
    _filtersByType(filtersByType),
    _edgeTypes(edgeTypes.begin(), edgeTypes.end()),
    _matchable(matchable)
{
}

NLExploreStep::~NLExploreStep() {
}

bool NLVectorSearchLoopData::hasSearched(std::string_view indexName,
                                         size_t neighbourCount,
                                         std::span<const float> queryVector) const {
    return _searched
        && _searchedIndexName == indexName
        && _searchedNeighbourCount == neighbourCount
        && std::ranges::equal(_searchedVector, queryVector);
}

void NLVectorSearchLoopData::markSearched(std::string_view indexName,
                                          size_t neighbourCount,
                                          std::span<const float> queryVector) {
    _searchedIndexName = indexName;
    _searchedNeighbourCount = neighbourCount;
    _searchedVector.assign(queryVector.begin(), queryVector.end());
    _searched = true;
}

NLExploreStep* NLExplorePathsLoopData::addStep(PathExplorationDir direction,
                                               bool filtersByType,
                                               std::span<const EdgeTypeID> edgeTypes,
                                               bool matchable) {
    _steps.push_back(std::make_unique<NLExploreStep>(direction, filtersByType, edgeTypes, matchable));

    return _steps.back().get();
}

bool NLExplorePathsLoopData::isMatchable() const {
    return std::ranges::all_of(_steps, [](const std::unique_ptr<NLExploreStep>& step) {
        return step->isMatchable();
    });
}

NLNodeSetState::NLNodeSetState() {
}

NLNodeSetState::~NLNodeSetState() {
}

void NLNodeSetState::reset() {
    _nodes.clear();
    _ordered = false;
}

void NLNodeSetState::add(const std::vector<NodeID>& nodes) {
    _nodes.insert(_nodes.end(), nodes.begin(), nodes.end());
    _ordered = false;
}

std::span<const NodeID> NLNodeSetState::getNodes() {
    if (!_ordered) {
        std::sort(_nodes.begin(), _nodes.end());
        _nodes.erase(std::unique(_nodes.begin(), _nodes.end()), _nodes.end());
        _ordered = true;
    }

    return _nodes;
}

void NLProcedureState::setExecutionContext(const NLExecutionContext* context) {
    _context.setWriteBuffer(context->getWriteBuffer());
    _context.setFirstQueryNode(context->getFirstQueryNode());
}

void NLProcedureState::prepareOrResetForNewDrive() {
    // A procedure reads its argument columns in its prepare step - gnn.neighbourhoodSample
    // builds its sampling iterator over the input nodes there - so the call is prepared
    // on its first drive, once the producing loop has filled them, rather than when its
    // handle is bound.
    if (!_prepared) {
        prepare();
        return;
    }

    // Preparing the call already left the procedure at its first row, so the drive that
    // prepared it needs no rewind - and a procedure whose rows cannot be re-read would
    // refuse one it never needed.
    if (!_driven) {
        return;
    }

    reset();
}

void NLProcedureState::prepare() {
    _procedureState.setStep(ProcedureState::Step::PREPARE);
    _procedureState.clearFinished();
    _procedure->getExecCallback()(&_procedureState);

    if (_procedureState.isFinished()) {
        throw IRException("A procedure cannot finish in its prepare step");
    }

    _prepared = true;
    _driven = false;
}

void NLProcedureState::reset() {
    _procedureState.setStep(ProcedureState::Step::RESET);
    _procedureState.clearFinished();
    _procedure->getExecCallback()(&_procedureState);

    if (_procedureState.isFinished()) {
        throw IRException("A procedure cannot finish in its reset step");
    }

    _driven = false;
}

void NLProcedureState::execute() {
    _stepBuffers.clear();

    _procedureState.setStep(ProcedureState::Step::EXECUTE);
    _procedureState.clearFinished();
    _procedure->getExecCallback()(&_procedureState);

    _driven = true;
}

size_t NLProcedureState::getRowCount() const {
    if (_resultColumns.empty()) {
        return 0;
    }

    return _resultColumns.front()->size();
}

size_t NLProcedureState::getInputRowCount() const {
    // A constant argument holds a single value however many rows the chunk has, so
    // only a row-aligned argument can size the input the procedure was handed; a
    // call passing constants alone hands it no rows a carry set could align with.
    const size_t argumentCount = _procedure->argumentTypes().size();

    for (size_t argumentIndex = 0; argumentIndex < argumentCount; argumentIndex++) {
        const Column* argument = _data->getInputColumn(argumentIndex);
        if (!argument) {
            continue;
        }

        if (argument->getContainerKind() != ContainerKind::code<ColumnConst>()) {
            return argument->size();
        }
    }

    return 0;
}

size_t NLOptionalState::getRowCount() const {
    if (_inputColumns.empty()) {
        return 1;
    }

    return _inputColumns.front()->size();
}

size_t NLOptionalState::getMatchedRowCount() const {
    if (_buffers.empty()) {
        return 0;
    }

    return _buffers.front()->size();
}

void NLOptionalState::reset() {
    for (Column* buffer : _buffers) {
        buffer->clear();
    }

    _values.clear();
    _matched.assign(getRowCount(), false);

    _missedRows.clear();
    _swept = false;
}

const ColumnVector<size_t>& NLOptionalState::missedRows() {
    if (_swept) {
        return _missedRows;
    }

    std::vector<size_t>& missedRaw = _missedRows.getRaw();
    for (size_t row = 0; row < _matched.size(); row++) {
        if (!_matched[row]) {
            missedRaw.push_back(row);
        }
    }

    _swept = true;

    return _missedRows;
}

NLSubqueryExpressionState::~NLSubqueryExpressionState() {
}

size_t NLSubqueryExpressionState::getRowCount() const {
    if (_inputColumns.empty()) {
        return 1;
    }

    return _inputColumns.front()->size();
}

void NLExistsState::reset() {
    _matched.assign(getRowCount(), false);
}

void NLCountSubqueryState::reset() {
    _counts.assign(getRowCount(), 0);
}

void NLCountSubqueryState::addRows(uint64_t row, size_t rowCount) {
    bioassert(row < _counts.size(), "Row {} is outside the {} rows of the step", row, _counts.size());
    _counts[row] += rowCount;
}

void NLCountSubqueryState::addTaggedRows(std::span<const uint64_t> tag) {
    for (const uint64_t row : tag) {
        bioassert(row < _counts.size(), "Row tag {} is outside the {} rows of the step", row, _counts.size());
        _counts[row]++;
    }
}

NLSubqueryExpressionResetData::NLSubqueryExpressionResetData(NLSubqueryExpressionState* state, ColumnVector<uint64_t>* tag)
    : _state(state),
    _tag(tag)
{
}

NLCountSubqueryTallyData::NLCountSubqueryTallyData(NLCountSubqueryState* state,
                                                   const ColumnVector<uint64_t>* tag,
                                                   const ColumnVector<uint64_t>* row,
                                                   const Column* rows)
    : _state(state),
    _tag(tag),
    _row(row),
    _rows(rows)
{
}

NLCountSubqueryResultData::NLCountSubqueryResultData(NLCountSubqueryState* state, ColumnVector<uint64_t>* result)
    : _state(state),
    _result(result)
{
}

void NLRowListsState::reset(size_t rowCount) {
    _rowCount = rowCount;
    _values.clear();
    _rows.clear();
    _stagedValues.clear();
}

void NLRowListsState::stage(size_t row, const ListBuffer<>::ListItemVariant& value) {
    bioassert(row < _rowCount, "Row tag {} is outside the {} rows of the step", row, _rowCount);

    _values.push_back(_stagedValues.ownItem(value));
    _rows.push_back(row);
}

void NLRowListsState::buildLists(ListBuffer<>& listBuffer, std::vector<ListView>& lists) {
    _starts.assign(_rowCount + 1, 0);

    for (const size_t row : _rows) {
        _starts[row + 1]++;
    }

    for (size_t row = 0; row < _rowCount; row++) {
        _starts[row + 1] += _starts[row];
    }

    // The values under the row they belong to: each row's run of the buffer is the list it
    // gets, and a run of its own is what the insert needs.
    _ordered.resize(_values.size());

    _next.assign(_starts.begin(), _starts.end() - 1);

    for (size_t valueIndex = 0; valueIndex < _values.size(); valueIndex++) {
        const size_t row = _rows[valueIndex];

        _ordered[_next[row]] = _values[valueIndex];
        _next[row]++;
    }

    lists.clear();
    lists.reserve(_rowCount);

    for (size_t row = 0; row < _rowCount; row++) {
        const std::span<const ListBuffer<>::ListItemVariant> values {_ordered.data() + _starts[row],
                                                                     _starts[row + 1] - _starts[row]};

        lists.push_back(listBuffer.insert(values));
    }
}

void NLSortState::reset() {
    for (Column* buffer : _buffers) {
        buffer->clear();
    }

    _values.clear();
    _permutation.clear();
    _sorted = false;
}

size_t NLRowState::getRowCount() const {
    if (_buffers.empty()) {
        return 0;
    }

    return _buffers.front()->size();
}

void NLRowState::reset() {
    for (Column* buffer : _buffers) {
        buffer->clear();
    }

    _values.clear();
}

void NLHashJoinState::reset() {
    for (Column* buffer : _buffers) {
        buffer->clear();
    }

    _values.clear();
    _index.clear();
}

NLAggregateState::NLAggregateState() {
}

NLAggregateState::~NLAggregateState() {
}

void NLAggregateState::setStateType(const NLGroupAggregateStateType& stateType) {
    _stateType = stateType;
    _place.resize(stateType._size);
}

void NLAggregateState::reset() {
    _stateType._init(NLGroupStatePlaces(_place.data(), _stateType._size), 0, 1);
    _values.clear();
}

void NLAggregateState::emit(Column* output, NLStepBuffers* stepBuffers) {
    _stateType._emit(NLGroupStatePlaces(_place.data(), _stateType._size), 0, 1, output, stepBuffers);
}

NLGroupDistinctTally::NLGroupDistinctTally() {
}

NLGroupDistinctTally::~NLGroupDistinctTally() {
}

void NLGroupDistinctTally::setValueKey(const NLGroupHashKey& valueKey) {
    _values.addKey(valueKey);
    _values.chooseTable();

    NLGroupHashKey rowGroupKey;
    NLGroupHashTable::buildRowGroupKey(rowGroupKey);

    _pairs.addKey(rowGroupKey);
    _pairs.addKey(rowGroupKey);
    _pairs.chooseTable();
}

void NLGroupDistinctTally::assign(const Column* rowGroups, const Column* values) {
    const size_t rowCount = rowGroups->size();

    const std::array<const Column*, 1> valueColumns {values};
    _values.assign(valueColumns, rowCount);

    const std::array<const Column*, 2> pairColumns {rowGroups, _values.getRowGroupColumn()};
    _pairs.assign(pairColumns, rowCount);

    _newRows.assign(rowCount, 0);
    for (const size_t row : _pairs.getNewGroupRows()) {
        _newRows[row] = 1;
    }
}

void NLGroupDistinctTally::clear() {
    _values.clear();
    _pairs.clear();
}

NLGroupTable::NLGroupTable() {
}

NLGroupTable::~NLGroupTable() {
}

void NLGroupTable::addKeyColumn(const NLGroupHashKey& hashKey, const KeyColumn& key) {
    _hashTable.addKey(hashKey);
    _keyColumns.push_back(key);
    _keyInputs.push_back(key._input);
}

void NLGroupTable::assign(size_t rowCount) {
    _hashTable.assign(_keyInputs, rowCount);

    const std::vector<size_t>& newGroupRows = _hashTable.getNewGroupRows();

    for (const KeyColumn& key : _keyColumns) {
        key._gatherAppend(key._input, newGroupRows, key._buffer, _keyValues);
    }
}

void NLGroupTable::emitKeys(size_t begin, size_t count) {
    for (const KeyColumn& key : _keyColumns) {
        key._emitCopy(key._buffer, begin, count, key._output);
    }
}

void NLGroupTable::gatherKeys(const ColumnVector<size_t>* groups) {
    for (const KeyColumn& key : _keyColumns) {
        key._gather(key._buffer, groups, key._output);
    }
}

void NLGroupTable::clear() {
    _hashTable.clear();
    _keyValues.clear();

    for (const KeyColumn& key : _keyColumns) {
        key._buffer->clear();
    }
}

void NLGroupAggregates::add(const Aggregate& aggregate) {
    const NLGroupAggregateStateType& stateType = aggregate._stateType;

    bioassert(_groupCount == 0, "An aggregate is placed in the blocks before any group has one");

    const size_t offset = alignOffset(_statesEnd, stateType._alignment);

    _statesEnd = offset + stateType._size;
    _alignment = std::max(_alignment, stateType._alignment);
    _stride = alignOffset(_statesEnd, _alignment);

    _aggregates.push_back(aggregate);
    _aggregates.back()._stateOffset = offset;
}

void NLGroupAggregates::grow(size_t groupCount) {
    const size_t heldCount = _groupCount;
    if (groupCount <= heldCount) {
        return;
    }

    _blocks.resize(groupCount * _stride);
    _groupCount = groupCount;

    for (const Aggregate& aggregate : _aggregates) {
        aggregate._stateType._init(getPlaces(aggregate), heldCount, groupCount - heldCount);
    }
}

void NLGroupAggregates::fold(const ColumnVector<size_t>* rowGroups) {
    NLValueStore& values = _values.getActive();
    const std::vector<size_t>& groups = rowGroups->getRaw();
    const size_t rowCount = groups.size();

    for (const Aggregate& aggregate : _aggregates) {
        if (aggregate._distinct) {
            aggregate._distinct->assign(rowGroups, aggregate._input);
        }
    }

    for (size_t beginRow = 0; beginRow < rowCount; beginRow += FOLD_BATCH_ROWS) {
        const size_t endRow = std::min(rowCount, beginRow + FOLD_BATCH_ROWS);

        for (Aggregate& aggregate : _aggregates) {
            aggregate._fold(getPlaces(aggregate),
                            aggregate._input,
                            groups,
                            beginRow,
                            endRow,
                            aggregate._distinct,
                            values);
        }
    }
}

void NLGroupAggregates::emit(size_t begin, size_t count, NLStepBuffers* stepBuffers) {
    for (const Aggregate& aggregate : _aggregates) {
        aggregate._stateType._emit(getPlaces(aggregate), begin, count, aggregate._output, stepBuffers);
    }
}

void NLGroupAggregates::compactValues() {
    size_t liveCount = 0;
    for (const Aggregate& aggregate : _aggregates) {
        if (aggregate._stateType._reown) {
            liveCount += _groupCount;
        }
    }

    if (!_values.needsCompaction(liveCount)) {
        return;
    }

    NLValueStore& spare = _values.getSpare();
    for (const Aggregate& aggregate : _aggregates) {
        const NLGroupAggregateReownFunction reown = aggregate._stateType._reown;
        if (reown) {
            reown(getPlaces(aggregate), _groupCount, spare);
        }
    }

    _values.flip();
}

void NLGroupAggregates::clear() {
    _blocks.clear();
    _groupCount = 0;
    _values.clear();

    for (Aggregate& aggregate : _aggregates) {
        if (aggregate._distinct) {
            aggregate._distinct->clear();
        }
    }
}

NLGroupStatePlaces NLGroupAggregates::getPlaces(const Aggregate& aggregate) {
    return NLGroupStatePlaces(_blocks.data() + aggregate._stateOffset, _stride);
}

void NLGroupAggregateState::reset() {
    _groupTable.clear();
    _aggregates.clear();
}

NLCollectState::ValueColumn& NLCollectState::unwoundColumn() {
    if (_valueColumns.size() != 1) {
        throw IRException("nl.unwind_collect must drain an accumulator holding a single collected column");
    }

    return _valueColumns.front();
}

void NLCollectState::reset() {
    _groupTable.clear();

    const size_t groupCount = _groupTable.getGroupCount();

    for (ValueColumn& value : _valueColumns) {
        if (value._buffer) {
            value._buffer->clear();
        }

        if (value._distinct) {
            value._distinct->clear();
        }
        value._groupPositions.clear();
        value._groupPositions.resize(groupCount);
    }

    _listBuffer.clear();
    _collectedValues.clear();
    _aggregates.clear();
    _aggregates.grow(groupCount);
}

bool NLSortState::rowLess(size_t leftRow, size_t rightRow) const {
    // The first key that breaks the tie decides, most significant first; its
    // direction flips the comparator's sign.
    for (const Key& key : _keys) {
        int comparison = key._compare(key._buffer, leftRow, rightRow);
        if (!key._ascending) {
            comparison = -comparison;
        }

        if (comparison != 0) {
            return comparison < 0;
        }
    }

    return false;
}

void NLSortState::trimIfNeeded() {
    if (!_bounded || _buffers.empty()) {
        return;
    }

    // Trim only once the buffers have grown well past the bound, so the cost is
    // amortized: the buffers hold at most ~2 * topK rows (plus the chunk just
    // appended) between trims, never the full input. Saturate the doubled bound so
    // a pathologically large topK (2 * topK overflowing size_t) never fires a trim
    // rather than wrapping to a small threshold.
    const size_t rowCount = _buffers.front()->size();
    const size_t maxSize = std::numeric_limits<size_t>::max();
    const size_t trimThreshold = _topK > maxSize / 2 ? maxSize : 2 * _topK;
    if (rowCount <= trimThreshold) {
        return;
    }

    trimToTopK(rowCount);
}

void NLSortState::trimToTopK(size_t rowCount) {
    // Select the best topK rows: nth_element partitions the index permutation so
    // its first topK entries are the smallest by the sort order (ties at the
    // boundary broken arbitrarily, as LIMIT allows), then we keep that prefix.
    std::vector<size_t>& kept = _keptIndices.getRaw();
    kept.resize(rowCount);
    std::iota(kept.begin(), kept.end(), size_t {0});

    if (_topK < rowCount) {
        const auto less = [this](size_t leftRow, size_t rightRow) { return rowLess(leftRow, rightRow); };
        std::nth_element(kept.begin(), kept.begin() + _topK, kept.end(), less);
        kept.resize(_topK);
    }

    // Compact every buffer to the kept rows: gather the survivors into the scratch
    // column, then copy them back. Each column is independent and reads its own
    // buffer before overwriting it, so the shared kept-index list stays valid.
    for (size_t columnIndex = 0; columnIndex < _buffers.size(); columnIndex++) {
        _gathers[columnIndex](_buffers[columnIndex], &_keptIndices, _tempBuffers[columnIndex]);
        _buffers[columnIndex]->assign(_tempBuffers[columnIndex]);
    }
}

void NLSortState::sort() {
    // The first emit step sorts; a later call (there is only one emit loop today)
    // finds the order already computed and returns.
    if (_sorted) {
        return;
    }

    // Every buffer is row-aligned, so any one gives the accumulated row count. For
    // a bounded accumulator this is at most ~2 * topK (the residual the last trim
    // left), not the full input.
    const size_t rowCount = _buffers.empty() ? 0 : _buffers.front()->size();

    std::vector<size_t>& permutation = _permutation.getRaw();
    permutation.resize(rowCount);
    std::iota(permutation.begin(), permutation.end(), size_t {0});

    // Order rows by the keys; stable_sort keeps rows that tie on every key in
    // their collected order, so the result is deterministic regardless of how the
    // producing loop chunked them.
    const auto less = [this](size_t leftRow, size_t rightRow) { return rowLess(leftRow, rightRow); };
    std::stable_sort(permutation.begin(), permutation.end(), less);

    // A bounded accumulator emits only its best topK rows: cap the permutation
    // after the sort, dropping the residual the amortized trim left in the buffer.
    if (_bounded && permutation.size() > _topK) {
        permutation.resize(_topK);
    }

    _sorted = true;
}

NLMergeNodeIndex::NLMergeNodeIndex(const LabelSet& labels,
                                   const LabelSet& writeLabels,
                                   bool matchable,
                                   ColumnNodeIDs* scanNodes)
    : _labels(labels),
    _writeLabels(writeLabels),
    _scanNodes(scanNodes),
    _matchable(matchable)
{
}

NLMergeNodeIndex::~NLMergeNodeIndex() {
}

std::span<const NLMergeRef> NLMergeNodeIndex::find(std::string_view key) const {
    const auto findIt = _byKey.find(key);
    if (findIt == end(_byKey)) {
        return {};
    }

    return findIt->second;
}

NLMergePendingEdges::NLMergePendingEdges() {
}

NLMergePendingEdges::~NLMergePendingEdges() {
}

void NLMergePendingEdges::add(const NLMergeRef& source, const NLMergeRef& target, EdgeTypeID edgeType, uint64_t offset) {
    _outgoing[source.asKey()].push_back({._other=target, ._edgeType=edgeType, ._offset=offset});
    _incoming[target.asKey()].push_back({._other=source, ._edgeType=edgeType, ._offset=offset});
}

std::span<const NLMergePendingEdges::Entry> NLMergePendingEdges::outOf(const NLMergeRef& node) const {
    return lookup(_outgoing, node);
}

std::span<const NLMergePendingEdges::Entry> NLMergePendingEdges::into(const NLMergeRef& node) const {
    return lookup(_incoming, node);
}

std::span<const NLMergePendingEdges::Entry> NLMergePendingEdges::lookup(
        const std::unordered_map<uint64_t, std::vector<Entry>>& edges,
        const NLMergeRef& node) {
    const auto findIt = edges.find(node.asKey());
    if (findIt == end(edges)) {
        return {};
    }

    return findIt->second;
}

NLMergeData::NLMergeData(NLMergePendingEdges* pendingEdges, ColumnMask* created, LocalMemory* memory)
    : _pendingEdges(pendingEdges),
    _created(created),
    _memory(memory),
    _workingSet(std::make_unique<NLMergeWorkingSet>())
{
}

NLMergeData::~NLMergeData() {
}

NLMakeMapData::NLMakeMapData(Column* result, NLStepBuffers* stepBuffers)
    : _result(result),
    _stepBuffers(stepBuffers)
{
}

NLMakeMapData::~NLMakeMapData() {
}

void NLMakeMapData::addEntry(const Entry& entry) {
    _entries.push_back(entry);
}

NLKeylessValue::NLKeylessValue(CellTag tag)
    : _tag(tag)
{
}

NLNeitherPositionNorKey::NLNeitherPositionNorKey(CellTag tag)
    : _tag(tag)
{
}

NLContainerIndexData::NLContainerIndexData(const Column* base,
                                           NLContainerReadFunction baseRead,
                                           const Column* index,
                                           NLSubscriptReadFunction indexRead,
                                           Column* result,
                                           NLStepBuffers* stepBuffers)
    : _base(base),
    _baseRead(baseRead),
    _index(index),
    _indexRead(indexRead),
    _result(result),
    _stepBuffers(stepBuffers)
{
}

NLContainerIndexData::~NLContainerIndexData() {
}

NLMergeMapsData::NLMergeMapsData(Column* result, NLStepBuffers* stepBuffers)
    : _result(result),
    _stepBuffers(stepBuffers)
{
}

NLMergeMapsData::~NLMergeMapsData() {
}

void NLMergeMapsData::addOperand(const Operand& operand) {
    _operands.push_back(operand);
}

NLFillNullsData::NLFillNullsData(const Column* value,
                                 const ColumnMask* nullRows,
                                 const Column* fill,
                                 Column* result,
                                 NLFillNullRowsFn fillNullRows)
    : _value(value),
    _nullRows(nullRows),
    _fill(fill),
    _result(result),
    _fillNullRows(fillNullRows)
{
}

NLFillNullsData::~NLFillNullsData() {
}
