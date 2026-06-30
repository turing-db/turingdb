#include "NLProgram.h"

#include <algorithm>
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

void NLSortState::sort() {
    // The first emit step sorts; a later call (there is only one emit loop today)
    // finds the order already computed and returns.
    if (_sorted) {
        return;
    }

    // Every buffer is row-aligned, so any one gives the accumulated row count.
    const size_t rowCount = _buffers.empty() ? 0 : _buffers.front()->size();

    std::vector<size_t>& permutation = _permutation.getRaw();
    permutation.resize(rowCount);
    std::iota(permutation.begin(), permutation.end(), size_t {0});

    // Order rows by the keys, most significant first; the first key that breaks
    // the tie decides, and its direction flips the comparator's sign. stable_sort
    // keeps rows that tie on every key in their collected order, so the result is
    // deterministic regardless of how the producing loop chunked them.
    const std::vector<Key>& keys = _keys;
    std::stable_sort(permutation.begin(), permutation.end(), [&keys](size_t leftRow, size_t rightRow) {
        for (const Key& key : keys) {
            int comparison = key._compare(key._buffer, leftRow, rightRow);
            if (!key._ascending) {
                comparison = -comparison;
            }

            if (comparison != 0) {
                return comparison < 0;
            }
        }

        return false;
    });

    _sorted = true;
}
