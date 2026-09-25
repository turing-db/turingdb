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
    return std::random_device {}();
}

}

NeighbourhoodSampleIterator::NeighbourhoodSampleIterator(const GraphView& view,
                                                         const ColumnNodeIDs* inputNodeIDs)
    : Iterator(view),
    _inputNodeIDs(inputNodeIDs),
    _nodeIndex(inputNodeIDs->size())
{
    initFrom(0);
}

bool NeighbourhoodSampleIterator::deleted(const EdgeRecord& e) const {
    return _view.isDeleted(e._edgeID);
}

void NeighbourhoodSampleIterator::initFrom(size_t index) {
    const size_t nodeCount = _inputNodeIDs->size();

    for (_nodeIndex = index; _nodeIndex < nodeCount; _nodeIndex++) {
        syncEdges();
        if (_edgeIt != _edges.end()) {
            return;
        }
    }
}

void NeighbourhoodSampleIterator::reset() {
    Iterator::reset();
    initFrom(0);
}

void NeighbourhoodSampleIterator::next() {
    nextValidForCurrentNode();
}

// Point @ref _edgeIt to the first edge for the current value of @ref _nodeIndex
void NeighbourhoodSampleIterator::syncEdges() {
    bioassert(_nodeIndex < _inputNodeIDs->size(), "Node index past the input.");

    const NodeID curNode = (*_inputNodeIDs)[_nodeIndex];

    _partIt.goToStart();
    while (_partIt.isNotEnd()) {
        const DataPart* part = _partIt.get();
        const EdgeIndexer& indexer = part->edgeIndexer();
        _edges = indexer.getNodeInEdges(curNode);
        _edgeIt = _edges.begin();

        while (_edgeIt != _edges.end() and deleted(*_edgeIt)) {
            _edgeIt++;
        }
        if (_edgeIt != _edges.end()) {
            return;
        }

        _partIt.next();
    }
}

// Traverses each DataPart for every NodeID
void NeighbourhoodSampleIterator::nextValidForCurrentNode() {
    _edgeIt++;

    // Skip deleted edges
    while (_edgeIt != _edges.end() and deleted(*_edgeIt)) {
        _edgeIt++;
    }
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
        const NodeID curNode = (*_inputNodeIDs)[_nodeIndex];

        _edges = indexer.getNodeInEdges(curNode);
        _edgeIt = _edges.begin();

        while (_edgeIt != _edges.end() and deleted(*_edgeIt)) {
            _edgeIt++;
        }
        if (_edgeIt != _edges.end()) {
            return;
        }

        _partIt.next();
    }
}

template <typename NodeColumn>
NeighbourhoodSampleChunkWriter<NodeColumn>::NeighbourhoodSampleChunkWriter(const GraphView& view,
                                                                           const ColumnNodeIDs* input,
                                                                           size_t sampleSize,
                                                                           std::optional<uint64_t> seed)
    : NeighbourhoodSampleIterator(view, input),
    _sampleSize(sampleSize),
    _sampleRatio(_sampleSize == 0 ? 1 :  1.0 / _sampleSize),
    _generator(makeSeed(seed)),
    _replacementGenerator(0, _sampleSize == 0 ? 0 : _sampleSize - 1)
{
}

template <typename NodeColumn>
void NeighbourhoodSampleChunkWriter<NodeColumn>::setOutputColumns(NodeColumn* srcIDs,
                                                                  ColumnEdgeIDs* edgeIDs,
                                                                  ColumnEdgeTypes* edgeTypes,
                                                                  NodeColumn* otherIDs) {
    _srcIDs = srcIDs;
    _edgeIDs = edgeIDs;
    _edgeTypes = edgeTypes;
    _otherIDs = otherIDs;
}

template <typename NodeColumn>
size_t NeighbourhoodSampleChunkWriter<NodeColumn>::geometricSample(double W) {
    const double u = rand01();
    return std::floor(std::log(u) / std::log(1 - W));
}

template <typename NodeColumn>
size_t NeighbourhoodSampleChunkWriter<NodeColumn>::randomSampleOffset() {
    return _replacementGenerator(_generator);
}

template <typename NodeColumn>
void NeighbourhoodSampleChunkWriter<NodeColumn>::fill(size_t maxCount) {
    bioassert(_sampleSize <= maxCount, "Invalid sample size.");

	if (_sampleSize == 0) {
		if (_srcIDs) {
			_srcIDs->clear();
		}
		if (_edgeIDs) {
			_edgeIDs->clear();
		}
		if (_edgeTypes) {
			_edgeTypes->clear();
		}
		if (_otherIDs) {
			_otherIDs->clear();
		}
		if (_indices) {
			_indices->clear();
		}
		_nodeIndex = _inputNodeIDs->size();
		return;
    }

    if (_edgeIt == _edges.end() && !isDone()) {
        initFrom(_nodeIndex);
    }

    const size_t samplesPerChunk = maxCount / _sampleSize;
    const size_t nodesRemaining = _inputNodeIDs->size() - _nodeIndex;
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
    size_t nodeIndex = _nodeIndex;

    // Algorithm L (improvement on Reservoir sampling)
    // https://en.wikipedia.org/wiki/Reservoir_sampling
    for (size_t sampleNumber = 0; sampleNumber < nodesToSample; sampleNumber++) {
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
                    _srcIDs->operator[](writeIndex) = e._otherID;
                }
                if (_edgeIDs) {
                    _edgeIDs->operator[](writeIndex) = e._edgeID;
                }
                if (_edgeTypes) {
                    _edgeTypes->operator[](writeIndex) = e._edgeTypeID;
                }
                if (_otherIDs) {
                    _otherIDs->operator[](writeIndex) = e._nodeID;
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
                _srcIDs->operator[](replacedIndex) = sample._otherID;
            }
            if (_edgeIDs) {
                _edgeIDs->operator[](replacedIndex) = sample._edgeID;
            }
            if (_edgeTypes) {
                _edgeTypes->operator[](replacedIndex) = sample._edgeTypeID;
            }
            if (_otherIDs) {
                _otherIDs->operator[](replacedIndex) = sample._nodeID;
            }
            nextValidForCurrentNode();

            // Update sample threshold to make next sample less likely
            W *= std::pow(rand01(), _sampleRatio);
        }
        _nodeIndex++;
        nodeIndex++;
        if (!isDone()) {
            syncEdges();
        }
    }

    if (writeIndex == thisSize) {
        return;
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

template class db::NeighbourhoodSampleChunkWriter<db::ColumnNodeIDs>;
template class db::NeighbourhoodSampleChunkWriter<db::ColumnOptVector<db::NodeID>>;
