#include "ScanNodesByPropertyValueIterator.h"

#include <stdint.h>
#include <algorithm>
#include <span>

#include "datapart/DataPart.h"
#include "properties/PropertyManager.h"

#include "PropertyValueScan.h"

#include "BioAssert.h"
#include "FatalException.h"

using namespace db;

namespace {

// The candidates must ascend: one cursor gallops through the newer part's IDs rather than
// restarting the search for each of them.
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

// Sorted, distinct and gap-free: the slice then holds first, first + 1, ... and nothing
// else, which is the one shape the ID of a row can be computed from its index. Distinct
// matters: over ascending IDs alone, a repeat and a gap cancel out in the span.
template <SupportedType T>
bool hasDenseIDs(const TypedPropertyContainer<T>& container, std::span<const EntityID> ids) {
    if (ids.empty() || !container.isSorted() || !container.hasDistinctIDs()) {
        return false;
    }

    return ids.back().getValue() - ids.front().getValue() + 1 == ids.size();
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
    collectPartContainers();
    seekSliceAcrossParts();
}

template <SupportedType T>
ScanNodesByPropertyValueChunkWriter<T>::~ScanNodesByPropertyValueChunkWriter() = default;

template <SupportedType T>
void ScanNodesByPropertyValueChunkWriter<T>::next() {
    throw FatalException("ScanNodesByPropertyValueChunkWriter is filled by chunk, not stepped");
}

template <SupportedType T>
void ScanNodesByPropertyValueChunkWriter<T>::collectPartContainers() {
    const DataPartSpan parts = _view.dataparts();

    _partContainers.clear();
    _partContainers.reserve(parts.size());

    for (const WeakArc<DataPart>& part : parts) {
        const PropertyManager& properties = part->nodeProperties();
        const TypedPropertyContainer<T>* container = properties.tryGetContainer<T>(_propTypeID);

        PartContainer entry;
        if (container && container->size() > 0) {
            const PropertyContainer::IDs& ids = container->ids();

            entry._container = container;
            entry._first = ids.front();
            entry._last = ids.back();
            entry._sorted = container->isSorted();
        }

        _partContainers.push_back(entry);
    }
}

template <SupportedType T>
bool ScanNodesByPropertyValueChunkWriter<T>::startPart() {
    const size_t partIndex = static_cast<size_t>(_partIt.getIterator() - _view.dataparts().begin());

    _container = _partContainers[partIndex]._container;
    if (!_container) {
        return false;
    }

    collectNewerContainers(partIndex);

    if (!_labelset.isValid()) {
        _wholeContainer.assign(1, PropertyRange {._offset = 0, ._count = _container->size()});
        _rangeIt = _wholeContainer.begin();
        _rangeEnd = _wholeContainer.end();
        return true;
    }

    const PropertyManager& properties = _partIt.get()->nodeProperties();
    const LabelSetPropertyIndexer* indexer = properties.tryGetIndexer(_propTypeID);
    bioassert(indexer, "A part holding a property container has no label set indexer for it");

    _labelsetIt = indexer->matchIterate(_labelset);
    if (!_labelsetIt.isValid()) {
        return false;
    }

    const std::vector<PropertyRange>& ranges = _labelsetIt.getValue();
    _rangeIt = ranges.begin();
    _rangeEnd = ranges.end();
    return true;
}

// A newer part whose IDs cannot reach this one's holds no override of any hit found here,
// so the whole part is dropped once rather than tested against every chunk of candidates.
template <SupportedType T>
void ScanNodesByPropertyValueChunkWriter<T>::collectNewerContainers(size_t partIndex) {
    _newerContainers.clear();

    const PartContainer& current = _partContainers[partIndex];

    for (size_t newer = partIndex + 1; newer < _partContainers.size(); newer++) {
        const PartContainer& candidate = _partContainers[newer];
        if (!candidate._container) {
            continue;
        }

        const bool bounded = current._sorted && candidate._sorted;
        const bool disjoint = bounded && (candidate._last < current._first || candidate._first > current._last);

        if (!disjoint) {
            _newerContainers.push_back(candidate);
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
                _denseIDs = hasDenseIDs(*_container, _ids);
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

    for (const PartContainer& newer : _newerContainers) {
        if (kept == 0) {
            return 0;
        }

        if (candidatesAscend && newer._sorted) {
            kept = dropHitsInSortedIDs(newer._container->ids(), hits, kept);
        } else {
            kept = dropHitsInContainer(*newer._container, hits, kept);
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
size_t ScanNodesByPropertyValueChunkWriter<T>::scanSlice(size_t rows, NodeID* hits) const {
    if (_denseIDs) {
        return PropertyValueScan::equalDenseIDs(_values.data() + _offset, _ids[_offset].getValue(), rows, _value, hits);
    } else {
        return PropertyValueScan::equal(_values.data() + _offset, _ids.data() + _offset, rows, _value, hits);
    }
}

template <SupportedType T>
void ScanNodesByPropertyValueChunkWriter<T>::fill(size_t maxCount) {
    bioassert(_nodeIDs, "ScanNodesByPropertyValueChunkWriter must be initialized with a valid column");
    bioassert(maxCount > 0, "ScanNodesByPropertyValueChunkWriter cannot fill an empty chunk");

    std::vector<NodeID>& hits = _nodeIDs->getRaw();

    size_t count = 0;
    while (isValid() && count < maxCount) {
        const size_t available = _values.size() - _offset;
        const size_t rows = std::min(available, maxCount - count);

        if (hits.size() < count + rows) {
            hits.resize(count + rows);
        }

        const size_t candidates = scanSlice(rows, hits.data() + count);

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
