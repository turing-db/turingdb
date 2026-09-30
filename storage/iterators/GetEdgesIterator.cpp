#include "GetEdgesIterator.h"

#include <algorithm>

#include "columns/ColumnIDs.h"
#include "indexers/EdgeIndexer.h"
#include "datapart/DataPart.h"
#include "datapart/EdgeContainer.h"
#include "IteratorUtils.h"

#include "BioAssert.h"

namespace db {

GetEdgesIterator::GetEdgesIterator(const GraphView& view, const ColumnNodeIDs* inputNodeIDs)
    : Iterator(view),
    _inputNodeIDs(inputNodeIDs),
    _nodeIt(inputNodeIDs->cend())
{
    init();
}

GetEdgesIterator::~GetEdgesIterator() {
}

void GetEdgesIterator::reset() {
    Iterator::reset();
    _nodeIt = _inputNodeIDs->cend();
    init();
}

void GetEdgesIterator::init() {
    for (; _partIt.isNotEnd(); _partIt.next()) {
        _nodeIt = _inputNodeIDs->cbegin();

        const DataPart* part = _partIt.get();
        const EdgeIndexer& indexer = part->edgeIndexer();

        for (; _nodeIt != _inputNodeIDs->cend(); _nodeIt++) {
            const NodeID nodeID = *_nodeIt;
            _edges = indexer.getNodeOutEdges(nodeID);
            _nodeOutEdges = _edges;

            if (!_edges.empty()) {
                _edgeIt = _edges.begin();
                _direction = Direction::Outgoing;
                return;
            }

            _edges = indexer.getNodeInEdges(nodeID);

            if (!_edges.empty()) {
                _edgeIt = _edges.begin();
                _direction = Direction::Incoming;
                return;
            }
        }
    }
}

void GetEdgesIterator::next() {
    _edgeIt++;
    nextValid();
}

void GetEdgesIterator::goToPart(size_t partIdx) {
    Iterator::reset();
    advancePartIterator(partIdx);
}

void GetEdgesIterator::advancePartIterator(size_t n) {
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
        _nodeOutEdges = _edges;
        _edgeIt = _edges.begin();
        _direction = Direction::Outgoing;

        // If no outgoing edges in the current datapart, nextValid will check incoming edges,
        // then advance to the next datapart.
        nextValid();
    }
}

void GetEdgesIterator::nextValid() {
    while (true) {
        if (_edgeIt != _edges.end()) {
            // Valid edge found
            return;
        }

        if (_direction == Direction::Outgoing) {
            // Now iterate incoming edges
            const EdgeIndexer* indexer = &_partIt.get()->edgeIndexer();
            const NodeID nodeID = *_nodeIt;

            _edges = indexer->getNodeInEdges(nodeID);
            _edgeIt = _edges.begin();
            _direction = Direction::Incoming;
            continue;
        }

        // No more edges for the current node -> next node
        _nodeIt++;
        _direction = Direction::Outgoing;

        while (_nodeIt == _inputNodeIDs->cend()) {
            // No more requested node in the current datapart.
            // -> Next datapart
            _nodeIt = _inputNodeIDs->cbegin();
            _partIt.next();
        }

        if (!_partIt.isNotEnd()) {
            return;
        }

        const NodeID nodeID = *_nodeIt;
        const EdgeIndexer* indexer = &_partIt.get()->edgeIndexer();

        _edges = indexer->getNodeOutEdges(nodeID);
        _nodeOutEdges = _edges;
        _edgeIt = _edges.begin();
    }
}

GetEdgesChunkWriter::GetEdgesChunkWriter(const GraphView& view,
                                         const ColumnNodeIDs* inputNodeIDs)
    : GetEdgesIterator(view, inputNodeIDs),
    _filter(view)
{
}

void GetEdgesChunkWriter::classifyRow(std::span<const EdgeID> excluded) {
    const DataPart* part = _partIt.get();
    if (part != _boundPart) {
        const EdgeContainer& edges = part->edges();
        _boundPart = part;
        _partOutEdges = edges.getOuts();
        _partFirstEdgeID = edges.getFirstEdgeID();
        _partSelfLoopOffsets = edges.getSelfLoopOffsets();
    }

    _heldInOutRun = 0;
    _heldInInRun = 0;

    const size_t nodeFirst = _nodeOutEdges.empty() ? 0 : _nodeOutEdges.data() - _partOutEdges.data();
    const size_t nodeCount = _nodeOutEdges.size();

    for (size_t position = 0; position < excluded.size(); position++) {
        const EdgeID edge = excluded[position];
        const size_t offset = (edge - _partFirstEdgeID).getValue();

        bool repeated = false;
        for (size_t previous = 0; previous < position; previous++) {
            repeated |= excluded[previous] == edge;
        }

        if (repeated || offset >= _partOutEdges.size()) {
            continue;
        }

        if (offset >= nodeFirst && offset < nodeFirst + nodeCount) {
            _heldInOutRun++;

            // An edge leaving the node enters it only as a self-loop
            if (std::binary_search(_partSelfLoopOffsets.begin(), _partSelfLoopOffsets.end(), offset)) {
                _heldInInRun++;
            }
        } else if (_partOutEdges[offset]._otherID == *_nodeIt) {
            _heldInInRun++;
        }
    }
}

void GetEdgesChunkWriter::filterTombstones() {
    // Base column of this ChunkWriter is _edgeIDs
    _filter.populateRanges(_edgeIDs);

    _filter.filter(_edgeIDs);
    _filter.filter(_indices);

    if (_others) {
        _filter.filter(_others);
    }

    if (_types) {
        _filter.filter(_types);
    }

    _filter.reset();
}

static constexpr size_t NColumns = 3;
static constexpr size_t NCombinations = 1 << NColumns;

template <std::array<bool, NColumns> conditions>
size_t GetEdgesChunkWriter::copyRunLeavingOut(std::span<const EdgeID> excluded, size_t begin, size_t count) {
    EdgeID* edgeIDs = nullptr;
    NodeID* others = nullptr;
    EdgeTypeID* types = nullptr;

    if constexpr (conditions[0]) {
        _edgeIDs->resize(begin + count);
        edgeIDs = _edgeIDs->data() + begin;
    }
    if constexpr (conditions[1]) {
        _others->resize(begin + count);
        others = _others->data() + begin;
    }
    if constexpr (conditions[2]) {
        _types->resize(begin + count);
        types = _types->data() + begin;
    }

    const EdgeRecord* records = &*_edgeIt;
    const bool outgoing = _direction == Direction::Outgoing;
    const size_t outSliceFirst = outgoing ? records - _partOutEdges.data() : 0;

    const auto nextHeld = [&](size_t from) {
        if (outgoing) {
            size_t next = count;
            for (const EdgeID edge : excluded) {
                const size_t position = (edge - _partFirstEdgeID).getValue() - outSliceFirst;
                if (position >= from && position < next) {
                    next = position;
                }
            }

            return next;
        } else {
            for (size_t offset = from; offset < count; offset++) {
                const EdgeID edge = records[offset]._edgeID;
                for (const EdgeID held : excluded) {
                    if (held == edge) {
                        return offset;
                    }
                }
            }

            return count;
        }
    };

    size_t from = 0;
    size_t written = 0;
    size_t found = 0;

    while (from < count) {
        const size_t hole = found < _heldInRun ? nextHeld(from) : count;
        const size_t length = hole - from;

        if constexpr (conditions[0]) {
            std::transform(records + from, records + hole, edgeIDs + written, [](const EdgeRecord& record) { return record._edgeID; });
        }
        if constexpr (conditions[1]) {
            std::transform(records + from, records + hole, others + written, [](const EdgeRecord& record) { return record._otherID; });
        }
        if constexpr (conditions[2]) {
            std::transform(records + from, records + hole, types + written, [](const EdgeRecord& record) { return record._edgeTypeID; });
        }

        written += length;
        from = hole + 1;
        found += hole < count;
    }

    const size_t kept = begin + written;
    _indices->resize(kept);

    if constexpr (conditions[0]) {
        _edgeIDs->resize(kept);
    }
    if constexpr (conditions[1]) {
        _others->resize(kept);
    }
    if constexpr (conditions[2]) {
        _types->resize(kept);
    }

    return kept;
}

void GetEdgesChunkWriter::fill(size_t maxCount) {
    size_t remainingToMax = maxCount;
    static constexpr auto bools = generateArray<NColumns, NCombinations>();
    static constexpr auto masks = generateBitmasks<NColumns, NCombinations>();

    _indices->clear();

    if (_edgeIDs) {
        _edgeIDs->clear();
    }

    if (_others) {
        _others->clear();
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
                if (_direction == Direction::Outgoing || _nodeOutEdges.empty()) {
                    classifyRow(excluded);
                }

                _heldInRun = _direction == Direction::Outgoing ? _heldInOutRun : _heldInInRun;
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
                    const size_t kept = copyRunLeavingOut<conditions>(excluded, prevSize, rangeSize);
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
                        _others->resize(newSize);
                        std::generate((_others)->begin() + prevSize,
                                      (_others)->end(),
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

    switch (bitmask::create(_edgeIDs, _others, _types)) {
        CASE(0);
        CASE(1);
        CASE(2);
        CASE(3);
        CASE(4);
        CASE(5);
        CASE(6);
        CASE(7);

        default:
            bioassert(false, "Unexpected column combination");
    }

    // Base column is _edgeIDs: only need to check if there are edge tombstones
    if (_view.hasDeletedEdges()) {
        filterTombstones();
    }
}

}
