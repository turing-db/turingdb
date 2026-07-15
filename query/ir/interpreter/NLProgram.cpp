#include "NLProgram.h"

#include <algorithm>
#include <limits>
#include <numeric>

using namespace db;

NLProgram::NLProgram() {
}

NLProgram::~NLProgram() {
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
