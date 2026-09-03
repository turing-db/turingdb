#include "ScanNodesByPropertyValueIterator.h"

#include <stdint.h>
#include <string.h>
#include <algorithm>
#include <iterator>
#include <span>
#include <type_traits>

#if defined(__AVX2__)
#include <immintrin.h>
#endif

#include "datapart/DataPart.h"
#include "properties/PropertyManager.h"

#include "BioAssert.h"
#include "FatalException.h"

using namespace db;

namespace {

template <typename Lane>
struct EqualityLanes;

template <>
struct EqualityLanes<int64_t> {
    using Vector = int64_t __attribute__((vector_size(32)));
};

template <>
struct EqualityLanes<uint64_t> {
    using Vector = uint64_t __attribute__((vector_size(32)));
};

template <>
struct EqualityLanes<double> {
    using Vector = double __attribute__((vector_size(32)));
};

template <typename Lane>
concept VectorisedLane = requires { typename EqualityLanes<Lane>::Vector; };

using EqualityMask = int64_t __attribute__((vector_size(32)));

constexpr size_t equalityLaneCount = 4;

// Every row's ID is stored and the cursor advances by the comparison, so a run of
// misses costs no mispredicted branch.
template <typename Primitive>
size_t scanEqualScalar(const Primitive* values, const EntityID* ids, size_t rows, const Primitive& needle, NodeID* hits) {
    size_t count = 0;

    for (size_t row = 0; row < rows; row++) {
        hits[count] = NodeID {ids[row].getValue()};
        count += static_cast<size_t>(values[row] == needle);
    }

    return count;
}

#if defined(__AVX2__)

static_assert(std::is_trivially_copyable_v<EntityID>);
static_assert(std::is_trivially_copyable_v<NodeID>);
static_assert(sizeof(EntityID) == sizeof(uint64_t));
static_assert(sizeof(NodeID) == sizeof(uint64_t));

constexpr size_t vectorsPerBlock = 8;

struct CompactionTable {
    uint32_t _lanes[1 << equalityLaneCount][2 * equalityLaneCount] {};
};

constexpr CompactionTable makeCompactionTable() {
    CompactionTable table {};

    for (size_t mask = 0; mask < (1u << equalityLaneCount); mask++) {
        size_t write = 0;

        for (size_t lane = 0; lane < equalityLaneCount; lane++) {
            if ((mask & (1u << lane)) == 0) {
                continue;
            }

            table._lanes[mask][2 * write] = static_cast<uint32_t>(2 * lane);
            table._lanes[mask][2 * write + 1] = static_cast<uint32_t>(2 * lane + 1);
            write++;
        }

        for (; write < equalityLaneCount; write++) {
            table._lanes[mask][2 * write] = 0;
            table._lanes[mask][2 * write + 1] = 1;
        }
    }

    return table;
}

constexpr CompactionTable compactionTable = makeCompactionTable();

bool anyLaneMatches(EqualityMask matches) {
    const __m256i lanes = (__m256i)matches;
    return _mm256_testz_si256(lanes, lanes) == 0;
}

// The whole vector of IDs is stored and the cursor advances by the population count, so
// the misses trailing the last match are overwritten by the next group. The cursor never
// runs ahead of the rows already read, which keeps that store inside the caller's buffer.
size_t compactMatchedIDs(const EntityID* ids, EqualityMask matches, NodeID* hits) {
    const int mask = _mm256_movemask_pd(_mm256_castsi256_pd((__m256i)matches));

    const __m256i loaded = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(ids));
    const __m256i lanes = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(compactionTable._lanes[mask]));

    _mm256_storeu_si256(reinterpret_cast<__m256i*>(hits), _mm256_permutevar8x32_epi32(loaded, lanes));

    return static_cast<size_t>(__builtin_popcount(static_cast<unsigned>(mask)));
}

template <VectorisedLane Lane>
size_t scanEqualVectorised(const Lane* values, const EntityID* ids, size_t rows, Lane needle, NodeID* hits) {
    using Vector = EqualityLanes<Lane>::Vector;
    static_assert(sizeof(Vector) / sizeof(Lane) == equalityLaneCount);

    constexpr size_t blockRows = vectorsPerBlock * equalityLaneCount;

    Vector needles {};
    for (size_t lane = 0; lane < equalityLaneCount; lane++) {
        needles[lane] = needle;
    }

    size_t count = 0;
    size_t row = 0;

    for (; row + blockRows <= rows; row += blockRows) {
        EqualityMask matches[vectorsPerBlock];
        EqualityMask any {};

        for (size_t block = 0; block < vectorsPerBlock; block++) {
            Vector chunk;
            memcpy(&chunk, values + row + block * equalityLaneCount, sizeof(Vector));

            matches[block] = (chunk == needles);
            any |= matches[block];
        }

        if (!anyLaneMatches(any)) {
            continue;
        }

        for (size_t block = 0; block < vectorsPerBlock; block++) {
            count += compactMatchedIDs(ids + row + block * equalityLaneCount, matches[block], hits + count);
        }
    }

    return count + scanEqualScalar(values + row, ids + row, rows - row, needle, hits + count);
}

#else

template <VectorisedLane Lane>
size_t scanEqualVectorised(const Lane* values, const EntityID* ids, size_t rows, Lane needle, NodeID* hits) {
    using Vector = EqualityLanes<Lane>::Vector;
    static_assert(sizeof(Vector) / sizeof(Lane) == equalityLaneCount);

    constexpr size_t blockRows = 2 * equalityLaneCount;

    Vector needles {};
    for (size_t lane = 0; lane < equalityLaneCount; lane++) {
        needles[lane] = needle;
    }

    size_t count = 0;
    size_t row = 0;

    for (; row + blockRows <= rows; row += blockRows) {
        Vector low;
        Vector high;
        memcpy(&low, values + row, sizeof(Vector));
        memcpy(&high, values + row + equalityLaneCount, sizeof(Vector));

        const EqualityMask lowHits = (low == needles);
        const EqualityMask highHits = (high == needles);

        for (size_t lane = 0; lane < equalityLaneCount; lane++) {
            hits[count] = NodeID {ids[row + lane].getValue()};
            count += static_cast<size_t>(lowHits[lane] != 0);
        }

        for (size_t lane = 0; lane < equalityLaneCount; lane++) {
            hits[count] = NodeID {ids[row + equalityLaneCount + lane].getValue()};
            count += static_cast<size_t>(highHits[lane] != 0);
        }
    }

    return count + scanEqualScalar(values + row, ids + row, rows - row, needle, hits + count);
}

#endif

// The candidates come from one ascending slice of a sorted container, so one cursor gallops
// through the newer part's IDs instead of hashing every candidate. Disjoint ID ranges — a
// newer part that only added nodes — are rejected outright by the bounds test.
size_t dropHitsInSortedIDs(std::span<const EntityID> ids, NodeID* hits, size_t count) {
    if (count == 0 || ids.empty()) {
        return count;
    }

    const EntityID first {hits[0].getValue()};
    const EntityID last {hits[count - 1].getValue()};

    if (last < ids.front() || first > ids.back()) {
        return count;
    }

    const EntityID* const end = ids.data() + ids.size();
    const EntityID* cursor = ids.data();

    size_t write = 0;
    for (size_t read = 0; read < count; read++) {
        const NodeID hit = hits[read];
        const EntityID wanted {hit.getValue()};

        const size_t remaining = static_cast<size_t>(end - cursor);
        size_t step = 1;
        while (step < remaining && cursor[step] < wanted) {
            step *= 2;
        }

        cursor = std::lower_bound(cursor, cursor + std::min(step + 1, remaining), wanted);

        hits[write] = hit;
        write += static_cast<size_t>(cursor == end || *cursor != wanted);
    }

    return write;
}

template <SupportedType T>
size_t dropHitsInContainer(const TypedPropertyContainer<T>& container, NodeID* hits, size_t count) {
    size_t write = 0;

    for (size_t read = 0; read < count; read++) {
        const NodeID hit = hits[read];
        hits[write] = hit;
        write += static_cast<size_t>(!container.has(EntityID {hit.getValue()}));
    }

    return write;
}

template <typename Primitive>
size_t scanEqual(const Primitive* values, const EntityID* ids, size_t rows, const Primitive& needle, NodeID* hits) {
    if constexpr (VectorisedLane<Primitive>) {
        return scanEqualVectorised(values, ids, rows, needle, hits);
    } else {
        return scanEqualScalar(values, ids, rows, needle, hits);
    }
}

}

template <SupportedType T>
ScanNodesByPropertyValueChunkWriter<T>::ScanNodesByPropertyValueChunkWriter(const GraphView& view,
                                                                            PropertyTypeID propTypeID,
                                                                            const Primitive& value)
    : ScanNodesByPropertyValueChunkWriter(view, propTypeID, value, LabelSetHandle {})
{
}

// An invalid label set handle leaves the scan unconstrained: every range of the property
// column is walked, not only those of the label sets carrying the requested labels.
template <SupportedType T>
ScanNodesByPropertyValueChunkWriter<T>::ScanNodesByPropertyValueChunkWriter(const GraphView& view,
                                                                            PropertyTypeID propTypeID,
                                                                            const Primitive& value,
                                                                            const LabelSetHandle& labelset)
    : Iterator(view),
    _propTypeID(propTypeID),
    _value(value),
    _labelset(labelset),
    _filter(view.tombstones())
{
    seekSliceAcrossParts();
}

template <SupportedType T>
ScanNodesByPropertyValueChunkWriter<T>::~ScanNodesByPropertyValueChunkWriter() = default;

template <SupportedType T>
void ScanNodesByPropertyValueChunkWriter<T>::next() {
    throw FatalException("ScanNodesByPropertyValueChunkWriter is filled by chunk, not stepped");
}

template <SupportedType T>
bool ScanNodesByPropertyValueChunkWriter<T>::startPart() {
    const PropertyManager& properties = _partIt.get()->nodeProperties();
    _container = properties.tryGetContainer<T>(_propTypeID);
    if (!_container || _container->size() == 0) {
        return false;
    }

    collectNewerContainers();

    if (!_labelset.isValid()) {
        _wholeContainer.assign(1, PropertyRange {._offset = 0, ._count = _container->size()});
        _rangeIt = _wholeContainer.begin();
        _rangeEnd = _wholeContainer.end();
        return true;
    }

    const LabelSetPropertyIndexer* indexer = properties.tryGetIndexer(_propTypeID);
    if (!indexer) {
        return false;
    }

    _labelsetIt = indexer->matchIterate(_labelset);
    if (!_labelsetIt.isValid()) {
        return false;
    }

    const std::vector<PropertyRange>& ranges = _labelsetIt.getValue();
    _rangeIt = ranges.begin();
    _rangeEnd = ranges.end();
    return true;
}

template <SupportedType T>
void ScanNodesByPropertyValueChunkWriter<T>::collectNewerContainers() {
    _newerContainers.clear();

    const DataPartIterator end = _partIt.getEndIterator();
    for (DataPartIterator newer = std::next(_partIt.getIterator()); newer != end; ++newer) {
        const PropertyManager& properties = newer->get()->nodeProperties();
        const TypedPropertyContainer<T>* container = properties.tryGetContainer<T>(_propTypeID);
        if (container) {
            _newerContainers.push_back(container);
        }
    }
}

template <SupportedType T>
bool ScanNodesByPropertyValueChunkWriter<T>::nextSliceInPart() {
    for (;;) {
        while (_rangeIt != _rangeEnd) {
            const PropertyRange range = *_rangeIt;
            ++_rangeIt;

            if (range._count > 0) {
                _values = _container->getSpan(range._offset, range._count);
                _ids = std::span<const EntityID>(_container->ids()).subspan(range._offset, range._count);
                _offset = 0;
                return true;
            }
        }

        if (!_labelset.isValid()) {
            return false;
        }

        _labelsetIt.next();
        if (!_labelsetIt.isValid()) {
            return false;
        }

        const std::vector<PropertyRange>& ranges = _labelsetIt.getValue();
        _rangeIt = ranges.begin();
        _rangeEnd = ranges.end();
    }
}

template <SupportedType T>
void ScanNodesByPropertyValueChunkWriter<T>::seekSliceAcrossParts() {
    for (; _partIt.isNotEnd(); _partIt.next()) {
        if (startPart() && nextSliceInPart()) {
            return;
        }
    }
}

template <SupportedType T>
void ScanNodesByPropertyValueChunkWriter<T>::nextSlice() {
    if (nextSliceInPart()) {
        return;
    }

    _partIt.next();
    seekSliceAcrossParts();
}

// A node updated after this part was written holds its current value in a newer part,
// so a hit on the value stored here is stale and must not be emitted.
template <SupportedType T>
size_t ScanNodesByPropertyValueChunkWriter<T>::dropOverriddenHits(NodeID* hits, size_t count) const {
    const bool candidatesAscend = _container->isSorted();

    size_t kept = count;

    for (const TypedPropertyContainer<T>* container : _newerContainers) {
        if (kept == 0) {
            return 0;
        }

        if (candidatesAscend && container->isSorted()) {
            kept = dropHitsInSortedIDs(container->ids(), hits, kept);
        } else {
            kept = dropHitsInContainer(*container, hits, kept);
        }
    }

    return kept;
}

template <SupportedType T>
void ScanNodesByPropertyValueChunkWriter<T>::filterTombstones() {
    _filter.populateRanges(_nodeIDs);
    _filter.filter(_nodeIDs);
    _filter.reset();
}

template <SupportedType T>
void ScanNodesByPropertyValueChunkWriter<T>::fill(size_t maxCount) {
    bioassert(_nodeIDs, "ScanNodesByPropertyValueChunkWriter must be initialized with a valid column");

    std::vector<NodeID>& hits = _nodeIDs->getRaw();

    size_t count = 0;
    while (isValid() && count < maxCount) {
        const size_t available = _values.size() - _offset;
        const size_t rows = std::min(available, maxCount - count);

        if (hits.size() < count + rows) {
            hits.resize(count + rows);
        }

        const size_t candidates = scanEqual(_values.data() + _offset, _ids.data() + _offset, rows, _value, hits.data() + count);

        count += dropOverriddenHits(hits.data() + count, candidates);
        _offset += rows;

        if (_offset == _values.size()) {
            nextSlice();
        }
    }

    hits.resize(count);

    if (_view.tombstones().hasNodes()) {
        filterTombstones();
    }
}

namespace db {

template class ScanNodesByPropertyValueChunkWriter<types::Int64>;
template class ScanNodesByPropertyValueChunkWriter<types::UInt64>;
template class ScanNodesByPropertyValueChunkWriter<types::Double>;
template class ScanNodesByPropertyValueChunkWriter<types::String>;
template class ScanNodesByPropertyValueChunkWriter<types::Bool>;

}
