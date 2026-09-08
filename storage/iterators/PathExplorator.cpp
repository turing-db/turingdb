#include "PathExplorator.h"

#include <algorithm>
#include <bit>
#include <math.h>

#include "PathDistanceIndex.h"
#include "PathHopFilter.h"
#include "datapart/NodeContainer.h"
#include "indexers/EdgeIndexer.h"
#include "list/PathTrie.h"
#include "versioning/Tombstones.h"

#include "BioAssert.h"

using namespace db;

namespace {

// Interleaved walks stop paying off once a core's outstanding cache misses are all in use:
// measured with samples/path_bench, the walk plateaus at sixteen on every shape tried and
// loses nothing in cache
constexpr size_t defaultWalkerCount = 16;

// How many frontier nodes ahead the distinct mode's search fetches adjacency
constexpr size_t frontierLookahead = 16;

// An edge the distinct mode's search relaxes costs this many candidate checks of the walk,
// measured with samples/path_bench where the seeds' balls do not overlap at all; the search
// pays for itself where a batch's balls overlap enough to be expanded once instead of once
// per seed
constexpr double relaxedEdgeCostInChecks = 1.5;

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
    _walkers(defaultWalkerCount)
{
    reset();
}

PathExplorator::~PathExplorator() {
}

void PathExplorator::setPaths(ColumnVector<PathRef>* paths, PathTrie* trie) {
    bioassert((paths == nullptr) == (trie == nullptr), "A path column needs the trie it indexes");
    bioassert(!paths || !_distinctEnds, "The distinct mode emits no path");
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

void PathExplorator::setDistinctEnds(bool distinct) {
    bioassert(!distinct || !_paths, "The distinct mode emits no path");
    bioassert(!distinct || _minHops <= 1, "The distinct mode is exact for a minimum of one hop at most");
    bioassert(!distinct || _minHops == 0 || _direction != PathExplorationDir::BOTH,
              "An undirected distinct mode is exact for a minimum of zero hops alone");
    _distinctEnds = distinct;

    if (distinct) {
        _reach._words.assign(_parts.getAllocatedNodeCount(), ReachWords {});
    }
}

bool PathExplorator::searchPaysForDistinctEnds(const GraphView& view,
                                              PathExplorationDir direction,
                                              std::optional<EdgeTypeID> edgeType,
                                              uint64_t maxHops) {
    const PartDirectory parts(view);
    const double nodeCount = static_cast<double>(parts.getAllocatedNodeCount());
    const double edgeCount = static_cast<double>(parts.getAllocatedEdgeCount());
    if (nodeCount == 0.0 || edgeCount == 0.0) {
        return false;
    }

    // Enumerating the trails of an unbounded walk has no cost bound at all - their number
    // grows exponentially with the depth reached, from a single seed - where the search
    // stops at its fixpoint, and under DISTINCT the two answer the same question
    if (maxHops == unboundedHops) {
        return true;
    }

    PathDistanceIndex::TypeBranching branching;
    PathDistanceIndex::sampleBranching(parts, direction, edgeType, branching);

    const double candidatesPerSeed = PathDistanceIndex::estimatedEnumerationChecks(parts, branching, 1, maxHops);
    if (candidatesPerSeed == 0.0) {
        return false;
    }

    // The search expands every node a batch's seeds reach short of the last hop, once: the
    // seeds and what they fan out to within max - 1 hops, drawn over the graph, are expected
    // to be this many distinct nodes, each expanded along its edges
    const double seeds = static_cast<double>(PathTargetIndex::targetsPerBatch);
    const double expandedPerSeed = 1.0 + PathDistanceIndex::estimatedEnumerationChecks(parts, branching, 1, maxHops - 1);
    const double expanded = nodeCount * (1.0 - exp(-seeds * expandedPerSeed / nodeCount));

    const double fanOut = std::max(1.0, branching._fanOut);

    const double walked = seeds * candidatesPerSeed;
    const double relaxed = expanded * fanOut;

    return walked > relaxedEdgeCostInChecks * relaxed;
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
    return _activeWalkers > 0 || _reach._batchActive || _seedCursor < _input->size();
}

bool PathExplorator::isEnd(size_t seedRow, NodeID node) const {
    if (_endNodes && node != (*_endNodes)[seedRow]) {
        return false;
    }

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

void PathExplorator::resizeOutputs(size_t count) {
    _indices->resize(count);
    if (_targets) {
        _targets->resize(count);
    }
    if (_paths) {
        _paths->resize(count);
    }
}

void PathExplorator::reset() {
    for (Walker& walker : _walkers) {
        walker = Walker {};
    }

    if (_reach._batchActive) {
        finishBatch();
    }
    _reach._batch = 0;

    _turn = 0;
    _activeWalkers = 0;
    _seedCursor = 0;
    _written = 0;
    _candidateChecks = 0;
    _valid = hasWork();
}

void PathExplorator::fill(size_t maxCount) {
    if (_distinctEnds) {
        fillDistinct(maxCount);
        return;
    }

    resizeOutputs(maxCount);
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

    resizeOutputs(_written);
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
    walker._target = PathTargetHandle {};

    const NodeID seed = (*_input)[row];

    if (_endNodes) {
        walker._targetNode = (*_endNodes)[row];
        if (_targetIndex) {
            walker._target = _targetIndex->find(walker._targetNode);
        }
    }

    if (_minHops == 0 && isEnd(row, seed)) {
        emit(row, seed, PathTrie::ROOT);
    }

    const bool beyondLabels = _distances && !_distances->canReachEndWithin(seed, _maxHops);
    const bool beyondTarget = !walker._target.canReachWithin(seed, _maxHops);
    if (_maxHops > 0 && !beyondLabels && !beyondTarget) {
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
    const bool emits = depth >= _minHops && isEnd(walker._seedRow, node);
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
        const bool beyondLabels = _distances && !_distances->canReachEndWithin(record._otherID, remainingHops);
        const bool beyondTarget = !walker._target.canReachWithin(record._otherID, remainingHops);

        if (backtracks || wrongType || deleted || onTrail || beyondLabels || beyondTarget) {
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

void PathExplorator::fillDistinct(size_t maxCount) {
    resizeOutputs(maxCount);
    _written = 0;

    while (_written < maxCount) {
        if (!_reach._batchActive) {
            if (_seedCursor >= _input->size()) {
                break;
            }

            startBatch();
        }

        if (_reach._emitNode < _reach._next.size()) {
            emitGainedRows(maxCount);
            continue;
        }

        const bool exhausted = _reach._next.empty() || _reach._level >= _maxHops;
        if (exhausted) {
            finishBatch();
        } else {
            expandLevel();
        }
    }

    resizeOutputs(_written);
    _valid = hasWork();
}

PathExplorator::ReachWords& PathExplorator::wordsOf(NodeID node) {
    ReachWords& words = _reach._words[node.getValue()];
    if (words._batch != _reach._batch) {
        words = ReachWords {};
        words._batch = _reach._batch;
    }

    return words;
}

void PathExplorator::startBatch() {
    Reachability& reach = _reach;
    const size_t nodeCount = reach._words.size();
    const size_t count = std::min(PathTargetIndex::targetsPerBatch, _input->size() - _seedCursor);

    reach._batchFirstRow = _seedCursor;
    reach._batch++;
    reach._level = 0;
    reach._next.clear();
    reach._emitNode = 0;
    reach._emitBits = 0;
    reach._batchActive = true;
    _seedCursor += count;

    // A seed's own bit is left out of its seen word when the minimum is one hop, so a
    // closed trail back to the seed is reported once at the level of its shortest cycle,
    // which in a directed walk is a simple cycle; at a minimum of zero the seed is its own
    // zero-length end and the bit stays set
    for (size_t bit = 0; bit < count; bit++) {
        const size_t row = reach._batchFirstRow + bit;
        const size_t seed = (*_input)[row].getValue();
        if (seed >= nodeCount) {
            continue;
        }

        const uint64_t mask = 1ull << bit;
        ReachWords& words = wordsOf(NodeID(seed));

        if (_minHops == 0) {
            words._seen |= mask;
        }

        if (words._gained == 0) {
            reach._next.push_back(NodeID(seed));
        }

        words._frontier |= mask;
        words._gained |= mask;
    }
}

void PathExplorator::emitGainedRows(size_t maxCount) {
    Reachability& reach = _reach;

    while (_written < maxCount && reach._emitNode < reach._next.size()) {
        const NodeID node = reach._next[reach._emitNode];

        if (reach._emitBits == 0) {
            reach._emitBits = reach._words[node.getValue()]._gained;
        }

        const unsigned bit = static_cast<unsigned>(std::countr_zero(reach._emitBits));
        reach._emitBits &= reach._emitBits - 1;

        const size_t row = reach._batchFirstRow + bit;
        if (reach._level >= _minHops && isEnd(row, node)) {
            emit(row, node, PathTrie::ROOT);
        }

        if (reach._emitBits == 0) {
            reach._emitNode++;
        }
    }
}

void PathExplorator::expandLevel() {
    Reachability& reach = _reach;

    std::swap(reach._frontier, reach._next);
    reach._next.clear();
    reach._emitNode = 0;
    reach._emitBits = 0;
    reach._level++;

    for (const NodeID node : reach._frontier) {
        reach._words[node.getValue()]._gained = 0;
    }

    // The frontier is known ahead, so each node's adjacency is fetched a few nodes before
    // its turn: the search has no interleaved walkers to hide that miss behind
    const size_t frontierSize = reach._frontier.size();
    for (size_t index = 0; index < frontierSize; index++) {
        const size_t ahead = index + frontierLookahead;
        if (ahead < frontierSize) {
            const NodeID aheadNode = reach._frontier[ahead];
            prefetchNodeData(aheadNode, _parts.ownerIndex(aheadNode));
        }

        const NodeID node = reach._frontier[index];
        const uint64_t word = reach._words[node.getValue()]._frontier;
        collectReachCandidates(node);

        for (const NodeID candidate : reach._candidateNodes) {
            ReachWords& words = wordsOf(candidate);
            const uint64_t gained = word & ~words._seen;
            if (gained == 0) {
                continue;
            }

            words._seen |= gained;
            if (words._gained == 0) {
                reach._next.push_back(candidate);
            }
            words._gained |= gained;
        }
    }

    // The frontier words move one level on: a node keeps only what it gained this level
    for (const NodeID node : reach._frontier) {
        reach._words[node.getValue()]._frontier = 0;
    }

    for (const NodeID node : reach._next) {
        ReachWords& words = reach._words[node.getValue()];
        words._frontier = words._gained;
    }
}

void PathExplorator::collectReachCandidates(NodeID node) {
    Reachability& reach = _reach;
    reach._candidateNodes.clear();
    reach._candidateEdges.clear();

    const size_t owner = _parts.ownerIndex(node);
    if (owner == _parts.size()) {
        return;
    }

    const EdgeIndexer& indexer = *_parts.get(owner)._indexer;
    if (_direction != PathExplorationDir::BACKWARD) {
        appendReachCandidates(indexer.getNodeOutEdges(node));
    }
    if (_direction != PathExplorationDir::FORWARD) {
        appendReachCandidates(indexer.getNodeInEdges(node));
    }

    for (const size_t patchIndex : _parts.patchPartsAfter(owner)) {
        const EdgeIndexer& patchIndexer = *_parts.get(patchIndex)._indexer;
        if (_direction != PathExplorationDir::BACKWARD) {
            appendReachCandidates(patchIndexer.getNodeOutEdges(node));
        }
        if (_direction != PathExplorationDir::FORWARD) {
            appendReachCandidates(patchIndexer.getNodeInEdges(node));
        }
    }

    if (_hopFilter && !reach._candidateNodes.empty()) {
        const size_t survivors = _hopFilter->filter(node, reach._candidateNodes, reach._candidateEdges);
        reach._candidateNodes.resize(survivors);
        reach._candidateEdges.resize(survivors);
    }
}

void PathExplorator::appendReachCandidates(std::span<const EdgeRecord> edges) {
    _candidateChecks += edges.size();

    for (const EdgeRecord& record : edges) {
        const bool wrongType = _filterByType && record._edgeTypeID != _edgeType;
        const bool deleted = _filterTombstones && _tombstones->containsEdge(record._edgeID);
        if (wrongType || deleted) {
            continue;
        }

        _reach._candidateNodes.push_back(record._otherID);
        _reach._candidateEdges.push_back(record._edgeID);
    }
}

void PathExplorator::finishBatch() {
    Reachability& reach = _reach;

    reach._frontier.clear();
    reach._next.clear();
    reach._emitNode = 0;
    reach._emitBits = 0;
    reach._batchActive = false;
}
