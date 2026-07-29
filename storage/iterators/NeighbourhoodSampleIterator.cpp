#include "NeighbourhoodSampleIterator.h"

#include "columns/ColumnIDs.h"
#include "datapart/DataPart.h"

#include "datapart/EdgeRecord.h"
#include "indexers/EdgeIndexer.h"
#include "iterators/PartIterator.h"
#include <random>

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

    if (_partIt.isEnd()) {
        return;
    }

    _partIt.next();
    while (_partIt.isNotEnd()) {
        const DataPart* part = _partIt.get();
        const EdgeIndexer& indexer = part->edgeIndexer();
        const NodeID curNode = *_nodeIt;

        _edges = indexer.getNodeOutEdges(curNode);
        if (!_edges.empty()) {
            _edgeIt = _edges.begin();
            return;
        }

        _partIt.next();
    }
}

NeighbourhoodSampleChunkWriter::NeighbourhoodSampleChunkWriter(const GraphView& view,
                                                               const ColumnNodeIDs* input,
                                                               size_t sampleSize)
    : NeighbourhoodSampleIterator(view, input),
    _sampleSize(sampleSize),
    _sampleRatio(1.0 / _sampleSize),
    _replacementGenerator(0, _sampleSize - 1)
{
}

size_t NeighbourhoodSampleChunkWriter::geometricSample(double W) {
    const double u = rand01();
    return std::floor(std::log(u) / std::log(1 - W));
}

void NeighbourhoodSampleChunkWriter::fill(size_t maxCount) {
    bioassert(_sampleSize <= maxCount, "Invalid sample size.");

    const size_t samplesPerChunk = maxCount / _sampleSize;
    const size_t nodesRemaining = std::distance(_nodeIt, _inputNodeIDs->cend());
    const size_t nodesToSample = std::min(nodesRemaining, samplesPerChunk);
    const size_t thisSize = nodesToSample * _sampleSize;

    _srcIDs->resize(thisSize);
    _edgeIDs->resize(thisSize);
    _edgeTypes->resize(thisSize);
    _otherIDs->resize(thisSize);

    if (thisSize == 0) {
        return;
    }

    size_t writeIndex = 0;

    for (size_t sampleNumber = 0; sampleNumber < nodesToSample; sampleNumber++) {
        // Sample range [start, end)
        const size_t sampleStart = writeIndex;
        // Expected value of the largest u_j of the first k samples
        double W = std::pow(rand01(), _sampleRatio);

        size_t i = 0;
        while (_edgeIt != _edges.end()) {
            // No more edges for this node
            if (_edgeIt == _edges.end()) {
                break;
            }
            i++;

            // Unconditionally take the first k elements
            if (i <= _sampleSize) {
                const EdgeRecord& e = *_edgeIt;
                _srcIDs->operator[](writeIndex) = e._nodeID;
                _edgeIDs->operator[](writeIndex) = e._edgeID;
                _edgeTypes->operator[](writeIndex) = e._edgeTypeID;
                _otherIDs->operator[](writeIndex) = e._otherID;
                writeIndex++;
                nextValidForCurrentNode();
                continue;
            }

            const size_t samplesToSkip = geometricSample(W);
            for (size_t skip = 0; skip < samplesToSkip; skip++) {
                nextValidForCurrentNode();
                if (_edgeIt == _edges.end()) {
                    break;
                }
            }

            if (_edgeIt == _edges.end()) {
                    break;
            }

            // Otherwise replace with a random probability
            // We know that [sampleStart, sampleEnd) is populated with this node's samples
            const size_t replacedIndex = sampleStart + _replacementGenerator(_generator);

            const EdgeRecord& sample = *_edgeIt;
            _srcIDs->operator[](replacedIndex) = sample._nodeID;
            _edgeIDs->operator[](replacedIndex) = sample._edgeID;
            _edgeTypes->operator[](replacedIndex) = sample._edgeTypeID;
            _otherIDs->operator[](replacedIndex) = sample._otherID;
            nextValidForCurrentNode();

            W *= std::pow(rand01(), _sampleRatio);
        }
        _nodeIt++;
    }

    // writeIndex ensures that we wrote to the first writeIndex contiguous elements. If
    // any node had out degree < _sampleSize, we truncate the arrays
    _srcIDs->resize(writeIndex);
    _edgeIDs->resize(writeIndex);
    _edgeTypes->resize(writeIndex);
    _otherIDs->resize(writeIndex);
}
