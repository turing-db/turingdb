#include "ScanNodesByPropertyValueIterator.h"

#include <stdint.h>
#include <string.h>
#include <algorithm>
#include <iterator>

#include "datapart/DataPart.h"
#include "properties/PropertyManager.h"

#include "BioAssert.h"

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

template <VectorisedLane Lane>
size_t scanEqualVectorised(const Lane* values, const EntityID* ids, size_t rows, Lane needle, NodeID* hits) {
    using Vector = EqualityLanes<Lane>::Vector;
    constexpr size_t laneCount = sizeof(Vector) / sizeof(Lane);
    constexpr size_t blockRows = 2 * laneCount;

    Vector needles {};
    for (size_t lane = 0; lane < laneCount; lane++) {
        needles[lane] = needle;
    }

    size_t count = 0;
    size_t row = 0;

    for (; row + blockRows <= rows; row += blockRows) {
        Vector low;
        Vector high;
        memcpy(&low, values + row, sizeof(Vector));
        memcpy(&high, values + row + laneCount, sizeof(Vector));

        const auto lowHits = (low == needles);
        const auto highHits = (high == needles);
        const auto anyHits = lowHits | highHits;

        int64_t any = 0;
        for (size_t lane = 0; lane < laneCount; lane++) {
            any |= anyHits[lane];
        }

        if (any == 0) {
            continue;
        }

        for (size_t lane = 0; lane < laneCount; lane++) {
            hits[count] = NodeID {ids[row + lane].getValue()};
            count += static_cast<size_t>(lowHits[lane] != 0);
        }

        for (size_t lane = 0; lane < laneCount; lane++) {
            hits[count] = NodeID {ids[row + laneCount + lane].getValue()};
            count += static_cast<size_t>(highHits[lane] != 0);
        }
    }

    return count + scanEqualScalar(values + row, ids + row, rows - row, needle, hits + count);
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
    _offset++;

    if (_offset == _values.size()) {
        nextSlice();
    }
}

template <SupportedType T>
bool ScanNodesByPropertyValueChunkWriter<T>::startPart() {
    const PropertyManager& properties = _partIt.get()->nodeProperties();
    _container = properties.tryGetContainer<T>(_propTypeID);
    if (!_container || _container->size() == 0) {
        return false;
    }

    if (!_labelset.isValid()) {
        _wholeContainer.assign(1, PropertyRange {._offset = 0, ._count = _container->size()});
        _rangeIt = _wholeContainer.begin();
        _rangeEnd = _wholeContainer.end();
        return true;
    }

    const LabelSetPropertyIndexer& indexer = properties.getIndexer(_propTypeID);
    _labelsetIt = indexer.matchIterate(_labelset);
    if (!_labelsetIt.isValid()) {
        return false;
    }

    const std::vector<PropertyRange>& ranges = _labelsetIt.getValue();
    _rangeIt = ranges.begin();
    _rangeEnd = ranges.end();
    return true;
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
    size_t kept = count;

    const DataPartIterator end = _partIt.getEndIterator();
    for (DataPartIterator newer = std::next(_partIt.getIterator()); kept > 0 && newer != end; ++newer) {
        const PropertyManager& properties = newer->get()->nodeProperties();
        const TypedPropertyContainer<T>* container = properties.tryGetContainer<T>(_propTypeID);
        if (!container) {
            continue;
        }

        size_t write = 0;
        for (size_t read = 0; read < kept; read++) {
            const NodeID hit = hits[read];
            hits[write] = hit;
            write += static_cast<size_t>(!container->has(EntityID {hit.getValue()}));
        }

        kept = write;
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
    hits.resize(maxCount);

    size_t count = 0;
    while (isValid() && count < maxCount) {
        const size_t available = _values.size() - _offset;
        const size_t rows = std::min(available, maxCount - count);
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
