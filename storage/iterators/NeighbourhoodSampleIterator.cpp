#include "NeighbourhoodSampleIterator.h"

#include "columns/ColumnIDs.h"
#include "datapart/DataPart.h"

#include "datapart/EdgeRecord.h"
#include "indexers/EdgeIndexer.h"
#include "iterators/PartIterator.h"

using namespace db;

NeighbourhoodSampleIterator::NeighbourhoodSampleIterator(const GraphView& view,
                                                         const ColumnNodeIDs* inputNodeIDs)
    : Iterator(view),
    _inputNodeIDs(inputNodeIDs),
    _nodeIt(inputNodeIDs->cend())
{
    init();
}

void NeighbourhoodSampleIterator::init() {
    for (_nodeIt = _inputNodeIDs->begin(); _nodeIt != _inputNodeIDs->cend(); _nodeIt++) {
        const NodeID nodeID = *_nodeIt;
        for (; _partIt.isNotEnd(); _partIt.next()) {
            const DataPart* part = _partIt.get();
            const EdgeIndexer& indexer = part->edgeIndexer();
            _edges = indexer.getNodeOutEdges(nodeID);
            if (!_edges.empty()) {
                _edgeIt = _edges.begin();
                return;
            }
        }
    }
}

void NeighbourhoodSampleIterator::reset() {
    Iterator::reset();
    _nodeIt = _inputNodeIDs->cend();
    init();
}

void NeighbourhoodSampleIterator::next() {
    _edgeIt++;
    nextValid();
}

void NeighbourhoodSampleIterator::nextValid() {
    while (_edgeIt == _edges.cend()) {
        while (_nodeIt != _inputNodeIDs->cend()) {
            _partIt.next();
            if (_partIt.isEnd()) {
                _nodeIt++;

                if (_nodeIt == _inputNodeIDs->cend()) {
                    return;
                }
                _partIt.goToStart();
            }

            const DataPart* part = _partIt.get();
            const EdgeIndexer& indexer = part->edgeIndexer();
            const NodeID curNode = *_nodeIt;

            _edges = indexer.getNodeOutEdges(curNode);
            _edgeIt = _edges.begin();
        }
    }
}

void NeighbourhoodSampleIterator::nextValidForCurrentNode() {
    _edgeIt++;
    if (_edgeIt != _edges.end()) {
        return;
    }

    while (_partIt.isNotEnd()) {
        _partIt.next();
        const DataPart* part = _partIt.get();
        const EdgeIndexer& indexer = part->edgeIndexer();
        const NodeID curNode = *_nodeIt;

        _edges = indexer.getNodeOutEdges(curNode);
        if (_edges.empty()) {
            continue;
        }
        _edgeIt = _edges.begin();
    }
}

NeighbourhoodSampleChunkWriter::NeighbourhoodSampleChunkWriter(const GraphView& view,
                                                               const ColumnNodeIDs* input,
                                                               size_t sampleSize)
    : NeighbourhoodSampleIterator(view, input),
    _sampleSize(sampleSize)
{
}


void NeighbourhoodSampleChunkWriter::fill(size_t maxCount) {
    bioassert(_sampleSize <= maxCount, "Invalid sample size.");

    const size_t samplesPerChunk = maxCount / _sampleSize;
    const size_t nodesRemaining = std::distance(_nodeIt, _inputNodeIDs->begin());
    const size_t nodesToSample = std::min(nodesRemaining, samplesPerChunk);
    const size_t thisSize = nodesToSample * _sampleSize;

    _edgeIDs->resize(thisSize);
    _edgeTypes->resize(thisSize);
    _otherIDs->resize(thisSize);

    size_t writeIndex = 0;

    const double sampleRatio = 1.0 / _sampleSize;

    for (size_t sampleNumber = 0; sampleNumber < samplesPerChunk; sampleNumber++) {
        // Sample range [start, end)
        const size_t sampleStart = sampleNumber * _sampleSize;
        const size_t sampleEnd = sampleStart + _sampleSize;

        size_t i = 0;
        while (_edgeIt != _edges.end()) {
            // No more edges for this node
            if (_edgeIt == _edges.end()) {
                break;
            }
            i++;

            double W = std::pow(rand01(), sampleRatio);

            // Unconditionally take the first k elements
            if (i <= _sampleSize) {
                const EdgeRecord& e = *_edgeIt;
                _srcIDs->operator[](writeIndex) = e._nodeID;
                _edgeIDs->operator[](writeIndex) = e._edgeID;
                _edgeTypes->operator[](writeIndex) = e._edgeTypeID;
                _otherIDs->operator[](writeIndex) = e._otherID;
                writeIndex++;
                continue;
            }

            // Otherwise replace with a random probability
            // We know that [sampleStart, sampleEnd) is populated with this node's samples

            nextValidForCurrentNode();
        }
        _nodeIt++;
    }
}

