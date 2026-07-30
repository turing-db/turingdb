#include "NeighbourhoodSampleIterator.h"

#include "columns/ColumnIDs.h"
#include "datapart/DataPart.h"

#include "datapart/EdgeRecord.h"
#include "indexers/EdgeIndexer.h"
#include "iterators/PartIterator.h"

using namespace db;

namespace {

uint64_t makeSeed(std::optional<uint64_t> seed) {
    if (seed.has_value()) {
        return *seed;
    }
    return std::random_device{}();
}

}

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
        syncEdges();
        if (!_edges.empty()) {
            return;
        }
    }
}

void NeighbourhoodSampleIterator::reset() {
    Iterator::reset();
    _nodeIt = _inputNodeIDs->cend();
    init();
}

void NeighbourhoodSampleIterator::next() {
    nextValidForCurrentNode();
}

void NeighbourhoodSampleIterator::syncEdges() {
    _partIt.goToStart();
    while (_partIt.isNotEnd()) {
        const DataPart* part = _partIt.get();
        const EdgeIndexer& indexer = part->edgeIndexer();
        _edges = indexer.getNodeOutEdges(*_nodeIt);
        _edgeIt = _edges.begin();
        if (!_edges.empty()) {
            return;
        }
        _partIt.next();
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
        _edgeIt = _edges.begin();

        if (!_edges.empty()) {
            return;
        }

        _partIt.next();
    }
}

NeighbourhoodSampleChunkWriter::NeighbourhoodSampleChunkWriter(const GraphView& view,
                                                               const ColumnNodeIDs* input,
                                                               size_t sampleSize,
                                                               std::optional<uint64_t> seed)
    : NeighbourhoodSampleIterator(view, input),
    _sampleSize(sampleSize),
    _sampleRatio(1.0 / _sampleSize),
    _generator(makeSeed(seed)),
    _replacementGenerator(0, _sampleSize == 0 ? 0 : _sampleSize - 1)
{
}

void NeighbourhoodSampleChunkWriter::setOutputColumns(ColumnNodeIDs* srcIDs,
                                                      ColumnEdgeIDs* edgeIDs,
                                                      ColumnEdgeTypes* edgeTypes,
                                                      ColumnNodeIDs* otherIDs) {
    _srcIDs = srcIDs;
    _edgeIDs = edgeIDs;
    _edgeTypes = edgeTypes;
    _otherIDs = otherIDs;
}

size_t NeighbourhoodSampleChunkWriter::geometricSample(double W) {
    const double u = rand01();
    return std::floor(std::log(u) / std::log(1 - W));
}

size_t NeighbourhoodSampleChunkWriter::randomSampleOffset() {
    return _replacementGenerator(_generator);
}

void NeighbourhoodSampleChunkWriter::fill(size_t maxCount) {
    bioassert(_sampleSize <= maxCount, "Invalid sample size.");

    const size_t samplesPerChunk = _sampleSize == 0 ? 0 : maxCount / _sampleSize;
    const size_t nodesRemaining = std::distance(_nodeIt, _inputNodeIDs->cend());
    const size_t nodesToSample = std::min(nodesRemaining, samplesPerChunk);
    const size_t thisSize = nodesToSample * _sampleSize;

    if (_srcIDs) {
        _srcIDs->resize(thisSize);
    }
    if (_edgeIDs) {
        _edgeIDs->resize(thisSize);
    }
    if (_edgeTypes) {
        _edgeTypes->resize(thisSize);
    }
    if (_otherIDs) {
        _otherIDs->resize(thisSize);
    }
    if (_indices) {
        _indices->resize(thisSize);
    }

    if (thisSize == 0) {
        return;
    }

    size_t writeIndex = 0;
    size_t nodeIndex = std::distance(_inputNodeIDs->cbegin(), _nodeIt);

    // Algorithm L (improvement on Reservoir sampling)
    // https://en.wikipedia.org/wiki/Reservoir_sampling
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
                if (_srcIDs) {
                    _srcIDs->operator[](writeIndex) = e._nodeID;
                }
                if (_edgeIDs) {
                    _edgeIDs->operator[](writeIndex) = e._edgeID;
                }
                if (_edgeTypes) {
                    _edgeTypes->operator[](writeIndex) = e._edgeTypeID;
                }
                if (_otherIDs) {
                    _otherIDs->operator[](writeIndex) = e._otherID;
                }
                if (_indices) {
                    _indices->operator[](writeIndex) = nodeIndex;
                }
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
            const size_t replacedIndex = sampleStart + randomSampleOffset();

            const EdgeRecord& sample = *_edgeIt;
            if (_srcIDs) {
                _srcIDs->operator[](replacedIndex) = sample._nodeID;
            }
            if (_edgeIDs) {
                _edgeIDs->operator[](replacedIndex) = sample._edgeID;
            }
            if (_edgeTypes) {
                _edgeTypes->operator[](replacedIndex) = sample._edgeTypeID;
            }
            if (_otherIDs) {
                _otherIDs->operator[](replacedIndex) = sample._otherID;
            }
            nextValidForCurrentNode();

            W *= std::pow(rand01(), _sampleRatio);
        }
        _nodeIt++;
        nodeIndex++;
        if (_nodeIt != _inputNodeIDs->cend()) {
            syncEdges();
        }
    }

    // writeIndex ensures that we wrote to the first writeIndex contiguous elements. If
    // any node had out degree < _sampleSize, we truncate the arrays
    if (_srcIDs) {
        _srcIDs->resize(writeIndex);
    }
    if (_edgeIDs) {
        _edgeIDs->resize(writeIndex);
    }
    if (_edgeTypes) {
        _edgeTypes->resize(writeIndex);
    }
    if (_otherIDs) {
        _otherIDs->resize(writeIndex);
    }
    if (_indices) {
        _indices->resize(writeIndex);
    }
}
