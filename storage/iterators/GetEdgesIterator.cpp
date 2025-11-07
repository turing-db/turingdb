#include "GetEdgesIterator.h"

#include <algorithm>

#include "Panic.h"
#include "columns/ColumnIDs.h"
#include "indexers/EdgeIndexer.h"
#include "DataPart.h"
#include "IteratorUtils.h"

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
        _edgeIt = _edges.begin();
    }
}

GetEdgesChunkWriter::GetEdgesChunkWriter(const GraphView& view,
                                         const ColumnNodeIDs* inputNodeIDs)
    : GetEdgesIterator(view, inputNodeIDs),
    _filter(view.tombstones())
{
}

void GetEdgesChunkWriter::filterTombstones() {
    // Base column of this ChunkWriter is _edgeIDs
    if (!_edgeIDs) {
        panic("Cannot filter tombstones if the edge ID column is not set");
    }

    _filter.populateRanges(_edgeIDs);

    _filter.filter(_edgeIDs);
    _filter.filter(_indices);

    bioassert(_indices->size() == _edgeIDs->size());

    if (_others) {
        _filter.filter(_others);
        bioassert(_others->size() == _edgeIDs->size());
    }

    if (_types) {
        _filter.filter(_types);
        bioassert(_types->size() == _edgeIDs->size());
    }

    _filter.reset();
}

static constexpr size_t NColumns = 3;
static constexpr size_t NCombinations = 1 << NColumns;

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
        while (isValid() && remainingToMax > 0) {
            const size_t avail = std::distance(_edgeIt, _edges.end());
            const size_t rangeSize = std::min(remainingToMax, avail);
            const size_t prevSize = _indices->size();
            const size_t newSize = prevSize + rangeSize;
            _indices->resize(newSize);

            const size_t index = std::distance(_inputNodeIDs->cbegin(), _nodeIt);
            std::fill(_indices->begin() + prevSize, _indices->end(), index);
            remainingToMax -= rangeSize;

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
            panic("Unexpected column combination");
    }

    // Base column is _edgeIDs: only need to check if there are edge tombstones
    if (_view.tombstones().hasEdges()) {
        filterTombstones();
    }
}

}
