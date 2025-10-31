#include "ScanOutEdgesByLabelIterator.h"

#include "DataPart.h"
#include "EdgeContainer.h"
#include "indexers/EdgeIndexer.h"
#include "TombstoneFilter.h"
#include "IteratorUtils.h"
#include "Panic.h"

namespace db {

ScanOutEdgesByLabelIterator::ScanOutEdgesByLabelIterator(const GraphView& view,
                                                         const LabelSetHandle& labelset)
    : Iterator(view),
      _labelset(labelset)
{
    init();
}

ScanOutEdgesByLabelIterator::~ScanOutEdgesByLabelIterator() {
}

void ScanOutEdgesByLabelIterator::init() {
    for (; _partIt.isNotEnd(); _partIt.next()) {
        const DataPart* part = _partIt.get();
        const auto& indexer = part->edgeIndexer().getOutsByLabelSet();
        _labelsetIt = indexer.matchIterate(_labelset);

        for (; _labelsetIt.isValid(); _labelsetIt.next()) {
            _spans = _labelsetIt.getValue();
            _spanIt = _spans.begin();

            if (_spanIt != _spans.end()) {
                _edges = *_spanIt;
                _edgeIt = _edges.begin();
                return;
            }
        }
    }
}

void ScanOutEdgesByLabelIterator::next() {
    ++_edgeIt;
    nextValid();
}

void ScanOutEdgesByLabelIterator::nextValid() {
    while (_edgeIt == _edges.end()) {
        ++_spanIt;

        while (_spanIt == _spans.end()) {
            _labelsetIt.next();

            while (!_labelsetIt.isValid()) {
                _partIt.next();

                if (!isValid()) {
                    return;
                }

                const auto* part = _partIt.get();
                const auto& indexer = part->edgeIndexer().getOutsByLabelSet();
                _labelsetIt = indexer.matchIterate(_labelset);
            }

            _spans = _labelsetIt.getValue();
            _spanIt = _spans.begin();
        }

        _edges = *_spanIt;
        _edgeIt = _edges.begin();
    }
}

ScanOutEdgesByLabelChunkWriter::ScanOutEdgesByLabelChunkWriter(const GraphView& view, const LabelSetHandle& labelset)
    : ScanOutEdgesByLabelIterator(view, labelset),
    _filter(view.tombstones())
{
}

void ScanOutEdgesByLabelChunkWriter::filterTombstones() {
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

void ScanOutEdgesByLabelChunkWriter::fill(size_t maxCount) {
    size_t remainingToMax = maxCount;
    static constexpr auto bools = generateArray<NColumns, NCombinations>();
    static constexpr auto masks = generateBitmasks<NColumns, NCombinations>();

    msgbioassert(_srcs || _tgts || _edgeIDs || _types,
                 "ScanOutEdgesByLabelChunkWriter must be initialized with at least one valid column");

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
            const size_t leftInRange = std::distance(_edgeIt, _edges.end());
            const size_t rangeSize = std::min(leftInRange, remainingToMax);
            const size_t prevSize = getPrevSize();
            const size_t newSize = prevSize + rangeSize;

            if constexpr (conditions[0]) {
                _srcs->resize(newSize);
                std::generate((_srcs)->begin() + prevSize,
                              (_srcs)->end(),
                              [edgeIt = this->_edgeIt]() mutable {
                                  const NodeID id = edgeIt->_nodeID;
                                  ++edgeIt;
                                  return id;
                              });
            }
            if constexpr (conditions[1]) {
                _edgeIDs->resize(newSize);
                std::generate((_edgeIDs)->begin() + prevSize,
                              (_edgeIDs)->end(),
                              [edgeIt = this->_edgeIt]() mutable {
                                  const EdgeID id = edgeIt->_edgeID;
                                  ++edgeIt;
                                  return id;
                              });
            }
            if constexpr (conditions[2]) {
                _tgts->resize(newSize);
                std::generate((_tgts)->begin() + prevSize,
                              (_tgts)->end(),
                              [edgeIt = this->_edgeIt]() mutable {
                                  const NodeID id = edgeIt->_otherID;
                                  ++edgeIt;
                                  return id;
                              });
            }
            if constexpr (conditions[3]) {
                _types->resize(newSize);
                std::generate((_types)->begin() + prevSize,
                              (_types)->end(),
                              [edgeIt = this->_edgeIt]() mutable {
                                  const EdgeTypeID id = edgeIt->_edgeTypeID;
                                  ++edgeIt;
                                  return id;
                              });
            }
            remainingToMax -= rangeSize;
            _edgeIt += rangeSize;
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

