#include "GetOutEdgesIterator.h"

#include <algorithm>

#include "indexers/EdgeIndexer.h"
#include "DataPart.h"
#include "IteratorUtils.h"
#include "iterators/TombstoneFilter.h"

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
    : GetOutEdgesIterator(view, inputNodeIDs)
{
}

void GetOutEdgesChunkWriter::filterTombstones() {
    TombstoneFilter filter(_view.tombstones());
    filter.filterGetOutEdges(_edgeIDs, _tgts, _types, _indices);

    size_t newSize = _indices->size();
    if (_edgeIDs) {
        bioassert(_edgeIDs->size() == newSize);
    }
    if (_tgts) {
        bioassert(_tgts->size() == newSize);
    }
    if (_types) {
        bioassert(_types->size() == newSize);
    }
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
        while (isValid() && remainingToMax > 0) {
            const auto nodeOutEdges = _partIt.get()->edgeIndexer().getNodeOutEdges(*_nodeIt);
            const auto nodeOutEdgesEnd = nodeOutEdges.end();
            const size_t availOutPart = std::distance(_edgeIt, nodeOutEdgesEnd);
            const size_t rangeSize = std::min(remainingToMax, availOutPart);
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

    if (_view.tombstones().hasEdges() || _view.tombstones().hasNodes()) {
        filterTombstones();
    }
}

}
