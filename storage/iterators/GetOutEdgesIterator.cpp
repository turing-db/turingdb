#include "GetOutEdgesIterator.h"

#include <algorithm>
#include <vector>

#include "BioAssert.h"
#include "ID.h"
#include "indexers/EdgeIndexer.h"
#include "DataPart.h"
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
    for (; _partIt.isValid(); _partIt.next()) {
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

        if (!_partIt.isValid()) {
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

    // NOTE: Is it always right to check the `back` commit?
    if (_view.commits().back().hasTombstones()) {
        filterTombstones();
    }
}

void GetOutEdgesChunkWriter::filterTombstones() {
    // Filtering
    std::unordered_set<size_t> indexesToRemove;
    getIndexesToRemove(indexesToRemove);
    size_t initialSize = _indices->size();
    removeDeletedIndexes(indexesToRemove, initialSize);

    if (_tgts) {
        bioassert(_tgts->size() == _indices->size());
    }
    if (_edgeIDs) {
        bioassert(_edgeIDs->size() == _indices->size());
    }
    if (_types) {
        bioassert(_types->size() == _indices->size());
    }
}

void GetOutEdgesChunkWriter::getIndexesToRemove(std::unordered_set<size_t>& indexesToRemove) {
    const Tombstones& tombstones = _view.commits().back().tombstones();

    size_t n = _indices->size();

    if (_edgeIDs) {
        bioassert(_edgeIDs->size() == n);
        // Find indexes to delete based on deleted edges
        for (size_t i = 0; i < _edgeIDs->size(); i++) {
            EdgeID edge = _edgeIDs->getRaw()[i];
            if (tombstones.containsEdge(edge)) {
                indexesToRemove.insert(i);
            }
        }
    }
    if (_tgts) {
        bioassert(_tgts->size() == n);
        // Find indexes to delete based on deleted nodes
        for (size_t i = 0; i < _tgts->size(); i++) {
            NodeID tgt = _tgts->getRaw()[i];
            if (tombstones.containsNode(tgt)) {
                indexesToRemove.insert(i);
            }
        }
    }
}

void GetOutEdgesChunkWriter::removeDeletedIndexes(std::unordered_set<size_t> indexesToRemove,
                                                  size_t initialSize) {
    if (_edgeIDs) { // Remove deleted rows from the edge column
        std::vector<EdgeID>& rawEdges = _edgeIDs->getRaw();
        size_t write = 0;
        for (size_t read = 0; read < initialSize; read++) {
            if (!indexesToRemove.contains(read)) {
                rawEdges[write] = rawEdges[read];
                write++;
            }
        }
        rawEdges.resize(write);
    }

    if (_tgts) { // Remove deleted rows from the target column
        std::vector<NodeID>& rawTgts = _tgts->getRaw();
        size_t write = 0;
        for (size_t read = 0; read < initialSize; read++) {
            if (!indexesToRemove.contains(read)) {
                rawTgts[write] = rawTgts[read];
                write++;
            }
        }
        rawTgts.resize(write);
    }

    if (_types) { // Remove deleted rows from the types column
        std::vector<EdgeTypeID>& rawTypes = _types->getRaw();
        size_t write = 0;
        for (size_t read = 0; read < initialSize; read++) {
            if (!indexesToRemove.contains(read)) {
                rawTypes[write] = rawTypes[read];
                write++;
            }
        }
        rawTypes.resize(write);
    }

    { // Remove deleted rows from the indices column
        std::vector<size_t>& rawIndices = _indices->getRaw();
        size_t write = 0;
        for (size_t read = 0; read < initialSize; read++) {
            if (!indexesToRemove.contains(read)) {
                rawIndices[write] = rawIndices[read];
                write++;
            }
        }
        rawIndices.resize(write);
    }
}
}
