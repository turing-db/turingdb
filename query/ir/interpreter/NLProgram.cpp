#include "NLProgram.h"

#include <algorithm>
#include <limits>
#include <numeric>

#include "NLMergeWorkingSet.h"
#include "Procedure.h"
#include "ProcedureData.h"

#include "IRException.h"

using namespace db;

NLProgram::NLProgram() {
}

NLProgram::~NLProgram() {
}

void NLProgram::setColumnNames(std::span<const std::string_view> names) {
    _columnNames.assign(names.begin(), names.end());
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
                                               bool matchable,
                                               ColumnNodeIDs* scanNodes) {
    auto index = std::make_unique<NLMergeNodeIndex>(labels, matchable, scanNodes);
    NLMergeNodeIndex* indexPtr = index.get();
    _mergeNodeIndexes.emplace(signature, std::move(index));

    return indexPtr;
}

NLProcedureState::NLProcedureState(const Procedure* procedure,
                                   ProcedureData* data,
                                   const ProcedureContext* context)
    : _procedure(procedure),
    _data(data)
{
    _procedureState.setData(data);
    _procedureState.setContext(context);
}

NLProcedureState::~NLProcedureState() {
    const Procedure::DeallocCallback dealloc = _procedure->getDeallocCallback();
    if (dealloc) {
        dealloc(_data);
    }
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

void NLSortState::reset() {
    for (Column* buffer : _buffers) {
        buffer->clear();
    }

    _permutation.clear();
    _sorted = false;
}

NLGroupTable::Assignment NLGroupTable::assign(const std::string& key) {
    const size_t nextGroup = _groups.size();
    const auto [slot, inserted] = _groups.try_emplace(key, nextGroup);

    Assignment assignment;
    assignment._index = slot->second;
    assignment._created = inserted;

    return assignment;
}

void NLGroupTable::clear() {
    _groups.clear();
}

void NLGroupAggregateState::reset() {
    _groupTable.clear();

    for (KeyColumn& key : _keyColumns) {
        key._buffer->clear();
    }

    // count has no accumulator column (only a tally); the others carry the reduced
    // value per group. Clear whichever the aggregate holds.
    for (Aggregate& aggregate : _aggregates) {
        if (aggregate._accumulator) {
            aggregate._accumulator->clear();
        }

        aggregate._counts.clear();
        aggregate._distinct.clear();
    }
}

NLCollectState::ValueColumn& NLCollectState::unwoundColumn() {
    if (_valueColumns.size() != 1) {
        throw IRException("nl.unwind_collect must drain an accumulator holding a single collected column");
    }

    return _valueColumns.front();
}

void NLCollectState::reset() {
    _groupTable.clear();

    for (KeyColumn& key : _keyColumns) {
        key._buffer->clear();
    }

    for (ValueColumn& value : _valueColumns) {
        if (value._buffer) {
            value._buffer->clear();
        }

        value._distinct.clear();
        value._groupPositions.clear();
    }

    _listBuffer.clear();

    for (NLGroupAggregateState::Aggregate& aggregate : _aggregates) {
        if (aggregate._accumulator) {
            aggregate._accumulator->clear();
        }

        aggregate._counts.clear();
        aggregate._distinct.clear();
    }

    // An ungrouped collect has exactly one group - the empty key tuple - and that group
    // exists whether or not a row ever arrives: collect() over no row is the empty list,
    // not the absence of a row. Creating it here rather than on the first update keeps
    // the group count right for a producer that yields nothing, so the drain still emits
    // that one row.
    if (_keyColumns.empty()) {
        _key.clear();
        _groupTable.assign(_key);

        for (ValueColumn& value : _valueColumns) {
            value._groupPositions.resize(1);
        }

        for (NLGroupAggregateState::Aggregate& aggregate : _aggregates) {
            aggregate._grow(aggregate._accumulator, aggregate._counts, 1);
        }
    }
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

NLMergeNodeIndex::NLMergeNodeIndex(const LabelSet& labels, bool matchable, ColumnNodeIDs* scanNodes)
    : _labels(labels),
    _scanNodes(scanNodes),
    _matchable(matchable)
{
}

NLMergeNodeIndex::~NLMergeNodeIndex() {
}

std::span<const NLMergeRef> NLMergeNodeIndex::find(const std::string& key) const {
    const auto findIt = _byKey.find(key);
    if (findIt == end(_byKey)) {
        return {};
    }

    return findIt->second;
}

NLMergePendingNodes::NLMergePendingNodes() {
}

NLMergePendingNodes::~NLMergePendingNodes() {
}

std::span<const NLMergeRef> NLMergePendingNodes::find(const std::string& key) const {
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

void NLMergePendingEdges::add(const NLMergeRef& source,
                              const NLMergeRef& target,
                              EdgeTypeID edgeType,
                              uint64_t offset,
                              const std::string& propertyKey) {
    _outgoing[source.asKey()].push_back({._other=target,
                                         ._edgeType=edgeType,
                                         ._offset=offset,
                                         ._propertyKey=propertyKey});

    _incoming[target.asKey()].push_back({._other=source,
                                        ._edgeType=edgeType,
                                        ._offset=offset,
                                        ._propertyKey=propertyKey});
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

NLMergeData::NLMergeData(NLMergePendingNodes* pendingNodes,
                         NLMergePendingEdges* pendingEdges,
                         ColumnMask* created)
    : _pendingNodes(pendingNodes),
    _pendingEdges(pendingEdges),
    _created(created),
    _workingSet(std::make_unique<NLMergeWorkingSet>())
{
}

NLMergeData::~NLMergeData() {
}
