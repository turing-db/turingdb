#include "PathExplorator.h"

#include <algorithm>

#include "PathDistanceIndex.h"
#include "PathHopFilter.h"
#include "datapart/NodeContainer.h"
#include "indexers/EdgeIndexer.h"
#include "list/PathTrie.h"
#include "versioning/Tombstones.h"

#include "BioAssert.h"

using namespace db;

namespace {

uint64_t signatureBit(EdgeID edge) {
    return 1ull << ((edge.getValue() * 0x9E3779B97F4A7C15ull) >> 58);
}

}

PathExplorator::PathExplorator(const GraphView& view,
                               const ColumnNodeIDs* inputNodeIDs,
                               PathExplorationDir direction,
                               uint64_t minHops,
                               uint64_t maxHops)
    : _view(view),
    _input(inputNodeIDs),
    _direction(direction),
    _minHops(minHops),
    _maxHops(maxHops),
    _parts(view),
    _tombstones(&view.tombstones()),
    _filterTombstones(view.tombstones().hasEdges()),
    _walkers(1)
{
    reset();
}

PathExplorator::~PathExplorator() {
}

void PathExplorator::setPaths(ColumnVector<PathRef>* paths, PathTrie* trie) {
    bioassert((paths == nullptr) == (trie == nullptr), "A path column needs the trie it indexes");
    _paths = paths;
    _trie = trie;
}

void PathExplorator::setEdgeTypeFilter(EdgeTypeID edgeType) {
    _filterByType = true;
    _edgeType = edgeType;
}

void PathExplorator::setEndLabels(const LabelSet* labels) {
    _endLabels = labels ? LabelSetHandle(*labels) : LabelSetHandle();
}

void PathExplorator::setWalkerCount(size_t walkerCount) {
    bioassert(_activeWalkers == 0, "The walker count cannot change while seeds are being walked");
    _walkers.resize(std::max<size_t>(walkerCount, 1));
    _turn = 0;
}

void PathExplorator::prefetchNodeData(NodeID node, size_t partIndex) const {
    if (partIndex >= _parts.size()) {
        return;
    }

    const PartDirectory::Entry& part = _parts.get(partIndex);
    const std::span<const NodeEdgeData> nodeData = part._indexer->getNodeData();
    const size_t offset = part._indexer->getPatchNodeCount() + (node - part._firstNodeID).getValue();

    if (offset < nodeData.size()) {
        __builtin_prefetch(&nodeData[offset]);
    }
}

bool PathExplorator::hasWork() const {
    return _activeWalkers > 0 || _seedCursor < _input->size();
}

bool PathExplorator::isEnd(NodeID node) const {
    if (_distances) {
        return _distances->isEnd(node);
    }

    if (!_endLabels.isValid()) {
        return true;
    }

    const size_t owner = _parts.ownerIndex(node);
    if (owner == _parts.size()) {
        return false;
    }

    const LabelSetHandle labels = _parts.get(owner)._nodes->getNodeLabelSet(node);

    return labels.isValid() && labels.hasAtLeastLabels(_endLabels);
}

void PathExplorator::reset() {
    for (Walker& walker : _walkers) {
        walker = Walker {};
    }

    _turn = 0;
    _activeWalkers = 0;
    _seedCursor = 0;
    _written = 0;
    _candidateChecks = 0;
    _valid = hasWork();
}

void PathExplorator::fill(size_t maxCount) {
    _indices->resize(maxCount);
    if (_targets) {
        _targets->resize(maxCount);
    }
    if (_paths) {
        _paths->resize(maxCount);
    }
    _written = 0;

    const size_t inputSize = _input->size();
    const size_t walkerCount = _walkers.size();

    while (_written < maxCount) {
        Walker& walker = _walkers[_turn];
        _turn = _turn + 1 == walkerCount ? 0 : _turn + 1;

        if (walker._active) {
            advance(walker);
        } else if (_seedCursor < inputSize) {
            startSeed(walker, _seedCursor);
            _seedCursor++;
        } else if (_activeWalkers == 0) {
            break;
        }
    }

    _indices->resize(_written);
    if (_targets) {
        _targets->resize(_written);
    }
    if (_paths) {
        _paths->resize(_written);
    }

    _valid = hasWork();
}

void PathExplorator::startSeed(Walker& walker, size_t row) {
    walker._seedRow = row;
    walker._pathEdges.clear();
    walker._pathSignatures.clear();
    walker._pathSignatures.push_back(0);
    walker._pathEntries.clear();
    walker._pathEntries.push_back(PathTrie::ROOT);
    walker._frames.clear();
    walker._candidateNodes.clear();
    walker._candidateEdges.clear();

    const NodeID seed = (*_input)[row];

    if (_minHops == 0 && isEnd(seed)) {
        emit(row, seed, PathTrie::ROOT);
    }

    const bool doomed = _distances && !_distances->canReachEndWithin(seed, _maxHops);
    if (_maxHops > 0 && !doomed) {
        walker._active = true;
        _activeWalkers++;
        requestDescent(walker, seed);
    }
}

void PathExplorator::advance(Walker& walker) {
    switch (walker._stage) {
        case Stage::Idle:
            consume(walker);
        break;

        case Stage::RangeRequested:
            readRanges(walker);
        break;

        case Stage::SpanRequested:
            pushFrame(walker);
        break;
    }
}

void PathExplorator::consume(Walker& walker) {
    Frame& frame = walker._frames.back();
    if (frame._next == frame._candidateEnd) {
        popFrame(walker);
        return;
    }

    const size_t candidate = frame._next;
    const NodeID node = walker._candidateNodes[candidate];
    const EdgeID edge = walker._candidateEdges[candidate];
    frame._next++;

    const uint64_t depth = walker._frames.size();
    const bool emits = depth >= _minHops && isEnd(node);
    const bool expands = depth < _maxHops;

    if (!emits && !expands) {
        return;
    }

    PathRef entry = PathTrie::ROOT;
    if (_paths) {
        entry = _trie->append(walker._pathEntries.back(), edge, node, depth);
    }

    if (emits) {
        emit(walker._seedRow, node, entry);
    }

    if (!expands) {
        return;
    }

    const size_t ahead = candidate + _lookahead;
    if (_lookahead > 0 && ahead < frame._candidateEnd) {
        const NodeID aheadNode = walker._candidateNodes[ahead];
        prefetchNodeData(aheadNode, _parts.ownerIndex(aheadNode));
    }

    walker._pathEdges.push_back(edge);
    walker._pathSignatures.push_back(walker._pathSignatures.back() | signatureBit(edge));
    if (_paths) {
        walker._pathEntries.push_back(entry);
    }

    requestDescent(walker, node);
}

void PathExplorator::popFrame(Walker& walker) {
    const Frame frame = walker._frames.back();
    walker._candidateNodes.resize(frame._candidateBegin);
    walker._candidateEdges.resize(frame._candidateBegin);
    walker._frames.pop_back();

    if (walker._frames.empty()) {
        walker._active = false;
        _activeWalkers--;
        return;
    }

    walker._pathEdges.pop_back();
    walker._pathSignatures.pop_back();
    if (_paths) {
        walker._pathEntries.pop_back();
    }
}

void PathExplorator::requestDescent(Walker& walker, NodeID node) {
    walker._pendingNode = node;
    walker._pendingOwner = _parts.ownerIndex(node);
    prefetchNodeData(node, walker._pendingOwner);
    walker._stage = Stage::RangeRequested;
}

void PathExplorator::readRanges(Walker& walker) {
    walker._pendingOuts = {};
    walker._pendingIns = {};

    if (walker._pendingOwner < _parts.size()) {
        const EdgeIndexer& indexer = *_parts.get(walker._pendingOwner)._indexer;
        const NodeID node = walker._pendingNode;

        if (_direction != PathExplorationDir::BACKWARD) {
            walker._pendingOuts = indexer.getNodeOutEdges(node);
            __builtin_prefetch(walker._pendingOuts.data());
        }

        if (_direction != PathExplorationDir::FORWARD) {
            walker._pendingIns = indexer.getNodeInEdges(node);
            __builtin_prefetch(walker._pendingIns.data());
        }
    }

    walker._stage = Stage::SpanRequested;
}

void PathExplorator::pushFrame(Walker& walker) {
    const NodeID node = walker._pendingNode;
    const size_t begin = walker._candidateNodes.size();

    generateCandidates(walker, walker._pendingOuts);
    generateCandidates(walker, walker._pendingIns);

    for (const size_t patchIndex : _parts.patchPartsAfter(walker._pendingOwner)) {
        const EdgeIndexer& indexer = *_parts.get(patchIndex)._indexer;

        if (_direction != PathExplorationDir::BACKWARD) {
            generateCandidates(walker, indexer.getNodeOutEdges(node));
        }

        if (_direction != PathExplorationDir::FORWARD) {
            generateCandidates(walker, indexer.getNodeInEdges(node));
        }
    }

    size_t end = walker._candidateNodes.size();
    if (_hopFilter && end > begin) {
        const std::span<NodeID> candidateNodes(walker._candidateNodes.data() + begin, end - begin);
        const std::span<EdgeID> candidateEdges(walker._candidateEdges.data() + begin, end - begin);
        const size_t survivors = _hopFilter->filter(node, candidateNodes, candidateEdges);

        end = begin + survivors;
        walker._candidateNodes.resize(end);
        walker._candidateEdges.resize(end);
    }

    walker._frames.push_back({begin, end, begin});
    walker._stage = Stage::Idle;
}

void PathExplorator::generateCandidates(Walker& walker, std::span<const EdgeRecord> edges) {
    const uint64_t signature = walker._pathSignatures.back();
    const bool hasPathEdges = !walker._pathEdges.empty();
    const EdgeID lastEdge = hasPathEdges ? walker._pathEdges.back() : EdgeID();

    // The hops a candidate may still take after the one that reaches it
    const uint64_t remainingHops = _maxHops - (walker._pathEdges.size() + 1);

    const auto isOnPath = [&walker](EdgeID edge) {
        return std::find(walker._pathEdges.begin(), walker._pathEdges.end(), edge) != walker._pathEdges.end();
    };

    _candidateChecks += edges.size();

    for (const EdgeRecord& record : edges) {
        const EdgeID edge = record._edgeID;

        const bool backtracks = hasPathEdges && edge == lastEdge;
        const bool wrongType = _filterByType && record._edgeTypeID != _edgeType;
        const bool deleted = _filterTombstones && _tombstones->containsEdge(edge);
        const bool onTrail = (signature & signatureBit(edge)) != 0 && isOnPath(edge);
        const bool doomed = _distances && !_distances->canReachEndWithin(record._otherID, remainingHops);

        if (backtracks || wrongType || deleted || onTrail || doomed) {
            continue;
        }

        walker._candidateNodes.push_back(record._otherID);
        walker._candidateEdges.push_back(edge);
    }
}

void PathExplorator::emit(size_t seedRow, NodeID target, PathRef path) {
    (*_indices)[_written] = seedRow;

    if (_targets) {
        (*_targets)[_written] = target;
    }

    if (_paths) {
        (*_paths)[_written] = path;
    }

    _written++;
}
