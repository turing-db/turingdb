#include "ScanEdgesIterator.h"

#include "TombstoneFilter.h"
#include "EdgeContainer.h"
#include "IteratorUtils.h"
#include "Bitmask.h"
#include "BioAssert.h"
#include "DataPart.h"
#include "Panic.h"

namespace db {

ScanEdgesIterator::ScanEdgesIterator(const GraphView& view)
    : Iterator(view)
{
    init();
}

ScanEdgesIterator::~ScanEdgesIterator() {
}

void ScanEdgesIterator::init() {
    for (; _partIt.isNotEnd(); _partIt.next()) {
        _edges = _partIt.get()->edges().getOuts();
        _edgeIt = _edges.begin();

        if (_edgeIt == _edges.end()) {
            continue;
        }

        return;
    }
}

void ScanEdgesIterator::reset() {
    Iterator::reset();
    init();
}

void ScanEdgesIterator::next() {
    _edgeIt++;
    nextValid();
}

void ScanEdgesIterator::nextValid() {
    while (_edgeIt == _edges.end()) {
        _partIt.next();

        if (!_partIt.isNotEnd()) {
            // No more entry
            return;
        }

        _edges = _partIt.get()->edges().getOuts();
        _edgeIt = _edges.begin();
    }
}

ScanEdgesChunkWriter::ScanEdgesChunkWriter(const GraphView& view)
    : ScanEdgesIterator(view),
    _filter(view.tombstones())
{
}

void ScanEdgesChunkWriter::filterTombstones() {
    // Base column of this ChunkWriter is _edgeIDs
    bioassert(_edgeIDs);

    _filter.populateRanges(_edgeIDs);
    _filter.filter(_edgeIDs);

    if (_srcs) {
        _filter.filter(_srcs);
        bioassert(_srcs->size() == _edgeIDs->size());
    }
    if (_tgts) {
        _filter.filter(_tgts);
        bioassert(_tgts->size() == _edgeIDs->size());
    }
    if (_types) {
        _filter.filter(_types);
        bioassert(_types->size() == _edgeIDs->size());
    }

    _filter.reset();
}

static constexpr size_t NColumns = 4;
static constexpr size_t NCombinations = 1 << NColumns;

void ScanEdgesChunkWriter::fill(size_t maxCount) {
    size_t remainingToMax = maxCount;
    static constexpr auto bools = generateArray<NColumns, NCombinations>();
    static constexpr auto masks = generateBitmasks<NColumns, NCombinations>();
    msgbioassert(_srcs || _tgts || _edgeIDs || _types,
                 "ScanEdgesChunkWriter must be initialized with at least one valid column");

    const auto getPrevSize = [&]() {
        if (_srcs) {
            return _srcs->size();
        }
        if (_edgeIDs) {
            return _edgeIDs->size();
        }
        if (_tgts) {
            return _tgts->size();
        }
        if (_types) {
            return _types->size();
        }
        panic("At least one column must be set");
    };

    if (_srcs) {
        _srcs->clear();
    }
    if (_tgts) {
        _tgts->clear();
    }
    if (_edgeIDs) {
        _edgeIDs->clear();
    }
    if (_types) {
        _types->clear();
    }

    const auto fill = [&]<std::array<bool, NColumns> conditions>() {
        while (isValid() && remainingToMax > 0) {
            const auto partOutEdges = _partIt.get()->edges().getOuts();
            const auto partOutEnd = partOutEdges.end();
            const size_t availInPart = std::distance(_edgeIt, partOutEnd);
            const size_t rangeSize = std::min(remainingToMax, availInPart);
            const size_t prevSize = getPrevSize();
            const size_t newSize = prevSize + rangeSize;

            if constexpr (conditions[0]) {
                _srcs->resize(newSize);
            }
            if constexpr (conditions[1]) {
                _edgeIDs->resize(newSize);
            }
            if constexpr (conditions[2]) {
                _tgts->resize(newSize);
            }
            if constexpr (conditions[3]) {
                _types->resize(newSize);
            }
            remainingToMax -= rangeSize;
            for (size_t i = prevSize; i < newSize; i++) {
                if constexpr (conditions[0]) {
                    (*_srcs)[i] = _edgeIt->_nodeID;
                }
                if constexpr (conditions[1]) {
                    (*_edgeIDs)[i] = _edgeIt->_edgeID;
                }
                if constexpr (conditions[2]) {
                    (*_tgts)[i] = _edgeIt->_otherID;
                }
                if constexpr (conditions[3]) {
                    (*_types)[i] = _edgeIt->_edgeTypeID;
                }
                ++_edgeIt;
            }
            nextValid();
        }
    };

    switch (bitmask::create(_srcs, _edgeIDs, _tgts, _types)) {
        CASE(0);
        CASE(1);
        CASE(2);
        CASE(3);
        CASE(4);
        CASE(5);
        CASE(6);
        CASE(7);
        CASE(8);
        CASE(9);
        CASE(10);
        CASE(11);
        CASE(12);
        CASE(13);
        CASE(14);
        CASE(15);
    }

    if (_view.tombstones().hasEdges()) {
        filterTombstones();
    }
}

}
