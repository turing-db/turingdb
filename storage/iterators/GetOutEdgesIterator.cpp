#include "GetOutEdgesIterator.h"

#include <algorithm>

#include "columns/ColumnIDs.h"
#include "indexers/EdgeIndexer.h"
#include "datapart/DataPart.h"
#include "datapart/EdgeContainer.h"
#include "IteratorUtils.h"

namespace db {

GetOutEdgesIterator::GetOutEdgesIterator(const GraphView& view, const ColumnNodeIDs* inputNodeIDs)
    : Iterator(view),
    _inputNodeIDs(inputNodeIDs),
    _nodeIt(inputNodeIDs->cend())
{
    init();
}

GetOutEdgesIterator::~GetOutEdgesIterator() {
}

void GetOutEdgesIterator::reset() {
    Iterator::reset();
    _nodeIt = _inputNodeIDs->cend();
    init();
}

void GetOutEdgesIterator::init() {
    for (; _partIt.isNotEnd(); _partIt.next()) {
        _nodeIt = _inputNodeIDs->cbegin();

        const DataPart* part = _partIt.get();
        const EdgeIndexer& indexer = part->edgeIndexer();

        for (; _nodeIt != _inputNodeIDs->cend(); _nodeIt++) {
            const NodeID nodeID = *_nodeIt;
            _edges = indexer.getNodeOutEdges(nodeID);

            if (!_edges.empty()) {
                _edgeIt = _edges.begin();
                return;
            }
        }
    }
}

void GetOutEdgesIterator::next() {
    _edgeIt++;
    nextValid();
}

void GetOutEdgesIterator::goToPart(size_t partIdx) {
    Iterator::reset();
    advancePartIterator(partIdx);
}

void GetOutEdgesIterator::advancePartIterator(size_t n) {
    // Advance n dataparts forward
    for (size_t i = 0; i < n && _partIt.isNotEnd(); i++) {
        _partIt.next();
    }
    // If we have not reached the end, update the _node members
    if (_partIt.isNotEnd()) {
        _nodeIt = _inputNodeIDs->cbegin();
        const DataPart* part = _partIt.get();
        const NodeID nodeID = *_nodeIt;
        const EdgeIndexer& indexer = part->edgeIndexer();

        _edges = indexer.getNodeOutEdges(nodeID);
        _edgeIt = _edges.begin();

        // This datapart might have no out edges for this NodeID. Advance again until we
        // are at a valid part
        nextValid();
    }
}

void GetOutEdgesIterator::nextValid() {
    while (_edgeIt == _edges.end()) {
        // No more edges for the current node -> next node
        _nodeIt++;

        while (_nodeIt == _inputNodeIDs->cend()) {
            // No more requested node in the current datapart.
            // -> Next datapart
            _nodeIt = _inputNodeIDs->cbegin();
            _partIt.next();
        }

        if (!_partIt.isNotEnd()) {
            return;
        }

        const DataPart* part = _partIt.get();
        const NodeID nodeID = *_nodeIt;
        const EdgeIndexer& indexer = part->edgeIndexer();

        _edges = indexer.getNodeOutEdges(nodeID);
        _edgeIt = _edges.begin();
    }
}

GetOutEdgesChunkWriter::GetOutEdgesChunkWriter(const GraphView& view,
                                               const ColumnNodeIDs* inputNodeIDs)
    : GetOutEdgesIterator(view, inputNodeIDs),
    _filter(view)
{
}

void GetOutEdgesChunkWriter::filterTombstones() {
    // Base column of this ChunkWriter is _edgeIDs
    _filter.populateRanges(_edgeIDs);

    _filter.filter(_edgeIDs);

    _filter.filter(_indices);

    if (_tgts) {
        _filter.filter(_tgts);
    }

    if (_types) {
        _filter.filter(_types);
    }

    _filter.reset();
}

static constexpr size_t NColumns = 3;
static constexpr size_t NCombinations = 1 << NColumns;

void GetOutEdgesChunkWriter::fill(size_t maxCount) {
    size_t remainingToMax = maxCount;
    static constexpr auto bools = generateArray<NColumns, NCombinations>();
    static constexpr auto masks = generateBitmasks<NColumns, NCombinations>();

    _indices->clear();

    if (_edgeIDs) {
        _edgeIDs->clear();
    }
    if (_tgts) {
        _tgts->clear();
    }
    if (_types) {
        _types->clear();
    }

    const auto fill = [&]<std::array<bool, NColumns> conditions>() {
        constexpr bool indicesOnly = !conditions[0] && !conditions[1] && !conditions[2];

        while (isValid() && remainingToMax > 0) {
            const size_t avail = std::distance(_edgeIt, _edges.end());
            const size_t rangeSize = std::min(remainingToMax, avail);
            const size_t prevSize = _indices->size();
            const size_t index = std::distance(_inputNodeIDs->cbegin(), _nodeIt);

            std::span<const EdgeID> excluded;
            if (_excluded.isSet() && _edgeIt == _edges.begin()) {
                excluded = _excluded.rowEdges(index);

                const DataPart* part = _partIt.get();
                _heldInRun = ExcludedEdges::countInOutRun(excluded, part->edges(), _edges);
            } else if (_heldInRun > 0) {
                excluded = _excluded.rowEdges(index);
            }

            // With only indices written every row of a run is the same, so the rows
            // its excluded edges would have produced can come off the slice's end
            size_t dropped = 0;
            if constexpr (indicesOnly) {
                dropped = std::min(_heldInRun, rangeSize);
                _heldInRun -= dropped;
            }

            const size_t newSize = prevSize + rangeSize - dropped;
            _indices->resize(newSize);
            std::fill(_indices->begin() + prevSize, _indices->end(), index);
            remainingToMax -= rangeSize - dropped;

            if constexpr (!indicesOnly) {
                if (_heldInRun > 0) {
                    const DataPart* part = _partIt.get();
                    const std::span<const EdgeRecord> run(&*_edgeIt, rangeSize);
                    const size_t kept = ExcludedEdges::copyRunLeavingOut(excluded,
                                                                         _heldInRun,
                                                                         run,
                                                                         prevSize,
                                                                         &part->edges(),
                                                                         _indices,
                                                                         _edgeIDs,
                                                                         _tgts,
                                                                         _types);
                    _heldInRun -= newSize - kept;
                    remainingToMax += newSize - kept;
                } else {
                    if constexpr (conditions[0]) {
                        _edgeIDs->resize(newSize);
                        std::generate((_edgeIDs)->begin() + prevSize,
                                      (_edgeIDs)->end(),
                                      [edgeIt = this->_edgeIt]() mutable {
                                          const EdgeID id = edgeIt->_edgeID;
                                          ++edgeIt;
                                          return id;
                                      });
                    }
                    if constexpr (conditions[1]) {
                        _tgts->resize(newSize);
                        std::generate((_tgts)->begin() + prevSize,
                                      (_tgts)->end(),
                                      [edgeIt = this->_edgeIt]() mutable {
                                          const NodeID id = edgeIt->_otherID;
                                          ++edgeIt;
                                          return id;
                                      });
                    }
                    if constexpr (conditions[2]) {
                        _types->resize(newSize);
                        std::generate((_types)->begin() + prevSize,
                                      (_types)->end(),
                                      [edgeIt = this->_edgeIt]() mutable {
                                          const EdgeTypeID id = edgeIt->_edgeTypeID;
                                          ++edgeIt;
                                          return id;
                                      });
                    }
                }
            }

            _edgeIt += rangeSize;
            nextValid();
        };
    };

    switch (bitmask::create(_edgeIDs, _tgts, _types)) {
        CASE(0);
        CASE(1);
        CASE(2);
        CASE(3);
        CASE(4);
        CASE(5);
        CASE(6);
        CASE(7);
    }

    // Base column is _edgeIDs: only need to check if there are edge tombstones
    if (_view.hasDeletedEdges()) {
        filterTombstones();
    }
}

}
