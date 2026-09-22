#include "PathExplorator.h"

#include <algorithm>
#include <bit>
#include <limits>

#include "EdgeTypeMatch.h"
#include "PathDistanceIndex.h"
#include "PathHopFilter.h"
#include "datapart/NodeContainer.h"
#include "indexers/EdgeIndexer.h"
#include "list/PathTrie.h"
#include "versioning/Tombstones.h"

#include "BioAssert.h"

using namespace db;

namespace {

// How many frontier nodes ahead the distinct mode's search fetches adjacency
constexpr size_t frontierLookahead = 16;

constexpr size_t initialKeySetSlots = 1024;

uint64_t scatter(uint64_t key) {
    return (key * 0x9E3779B97F4A7C15ull) >> 32;
}

uint64_t signatureBit(EdgeID edge) {
    return 1ull << ((edge.getValue() * 0x9E3779B97F4A7C15ull) >> 58);
}

}

void PathExplorator::KeySet::clear() {
    _used = 0;
    _generation++;

    if (_generation != 0) {
        return;
    }

    std::fill(_stamps.begin(), _stamps.end(), 0);
    _generation = 1;
}

void PathExplorator::KeySet::grow() {
    const size_t slots = _keys.empty() ? initialKeySetSlots : _keys.size() * 2;
    std::vector<uint64_t> keys(_keys);
    std::vector<uint32_t> stamps(_stamps);

    _keys.assign(slots, 0);
    _stamps.assign(slots, 0);
    const size_t carried = _used;
    _used = 0;

    for (size_t slot = 0; slot < keys.size() && _used < carried; slot++) {
        if (stamps[slot] == _generation) {
            insert(keys[slot]);
        }
    }
}

bool PathExplorator::KeySet::insert(uint64_t key) {
    if ((_used + 1) * 2 >= _keys.size()) {
        grow();
    }

    const size_t mask = _keys.size() - 1;
    size_t slot = scatter(key) & mask;

    while (_stamps[slot] == _generation) {
        if (_keys[slot] == key) {
            return false;
        }

        slot = (slot + 1) & mask;
    }

    _keys[slot] = key;
    _stamps[slot] = _generation;
    _used++;

    return true;
}

bool PathExplorator::KeySet::contains(uint64_t key) const {
    if (_keys.empty()) {
        return false;
    }

    const size_t mask = _keys.size() - 1;
    size_t slot = scatter(key) & mask;

    while (_stamps[slot] == _generation) {
        if (_keys[slot] == key) {
            return true;
        }

        slot = (slot + 1) & mask;
    }

    return false;
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
    _filterTombstones(view.tombstones().hasEdges())
{
    reset();
}

PathExplorator::~PathExplorator() {
    if (_trie) {
        releaseArena();
    }
}

void PathExplorator::setPaths(ColumnVector<PathRef>* paths, PathTrie* trie) {
    bioassert((paths == nullptr) == (trie == nullptr), "A path column needs the trie it indexes");
    bioassert(!paths || !_distinctEnds, "The distinct mode emits no path");

    if (_trie) {
        releaseArena();
    }

    _paths = paths;
    _trie = trie;

    if (_trie) {
        acquireArena();
    }
}

void PathExplorator::setEdgeTypeFilter(std::span<const EdgeTypeID> edgeTypes) {
    _filterByType = true;
    _edgeTypes = edgeTypes;
}

void PathExplorator::setEndLabels(const LabelSet* labels) {
    _endLabels = labels ? LabelSetHandle(*labels) : LabelSetHandle();
}

void PathExplorator::setEndNodeSet(std::span<const NodeID> endNodeSet) {
    _endNodeSet = endNodeSet;
    _filtersByEndNodeSet = true;
}

void PathExplorator::setDistinctEnds(bool distinct) {
    bioassert(!distinct || !_paths, "The distinct mode emits no path");
    _distinctEnds = distinct;
}

// The level search answers "reached within k", which coincides with "reached by a trail
// within k" only when the walk may stop at its first hop; deeper minimums walk instead
bool PathExplorator::searchesLevels() const {
    if (!_distinctEnds || _minHops > 1) {
        return false;
    }

    return _minHops == 0 || _direction != PathExplorationDir::BOTH;
}

uint64_t PathExplorator::expansionKey(NodeID node, uint64_t budget) const {
    const uint64_t offset = _keysDepth ? std::min(_maxHops - budget, _expansionSpan) : budget;

    return node.getValue() * (_expansionSpan + 1) + offset;
}

void PathExplorator::setPendingAdjacency(const PendingAdjacency* adjacency, size_t edgeIDBound) {
    _pendingAdjacency = adjacency;
    _pendingEdgeIDBound = edgeIDBound;
}

bool PathExplorator::isPendingNode(NodeID node) const {
    return node.getValue() >= _parts.getAllocatedNodeCount();
}

size_t PathExplorator::nodeIDBound() const {
    if (_pendingAdjacency) {
        return _pendingAdjacency->getNodeIDBound();
    }

    return _parts.getAllocatedNodeCount();
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
    return _active || _reach._batchActive || _seedCursor < _input->size();
}

LabelSetHandle PathExplorator::labelSetOf(NodeID node) const {
    if (isPendingNode(node)) {
        return _pendingAdjacency ? _pendingAdjacency->labelSetOf(node) : LabelSetHandle {};
    }

    const size_t owner = _parts.ownerIndex(node);
    if (owner == _parts.size()) {
        return {};
    }

    return _parts.get(owner)._nodes->getNodeLabelSet(node);
}

bool PathExplorator::isEnd(size_t seedRow, NodeID node) const {
    if (_endNodes && node != (*_endNodes)[seedRow]) {
        return false;
    }

    if (_filtersByEndNodeSet && !std::binary_search(_endNodeSet.begin(), _endNodeSet.end(), node)) {
        return false;
    }

    // Both indices are built over the committed parts alone, so a node this change wrote is
    // outside them: reading one would report it unreachable and drop the row it ends
    const bool indexed = !isPendingNode(node);

    if (_distances && indexed) {
        return _distances->isEnd(node);
    }

    if (!_endLabels.isValid()) {
        return true;
    }

    const LabelSetHandle labels = labelSetOf(node);

    return labels.isValid() && labels.hasAtLeastLabels(_endLabels);
}

bool PathExplorator::canReachTargetWithin(NodeID node, uint64_t hops) const {
    if (isPendingNode(node)) {
        return true;
    }

    if (_filtersByEndNodeSet) {
        return !_targetIndex || _targetIndex->canReachAnyWithin(node, hops);
    }

    return _target.canReachWithin(node, hops);
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
    _active = false;
    _seedRow = 0;
    _pinned = 0;
    _target = PathTargetHandle {};
    _pathEdges.clear();
    _pathEntries.clear();
    _pathSignatures.clear();
    _frames.clear();
    _candidateNodes.clear();
    _candidateEdges.clear();

    if (_trie) {
        _trie->truncateArena(_arena, 0);
    }

    if (_reach._batchActive) {
        finishBatch();
    }

    _seedCursor = 0;
    _written = 0;
    _candidateChecks = 0;
    _valid = hasWork();
}

void PathExplorator::fill(size_t maxCount) {
    if (searchesLevels()) {
        fillDistinct(maxCount);
        return;
    }

    _prunes = _distinctEnds && _hopFilter == nullptr;

    if (_prunes) {
        // A trail spends each edge once, so a maximum past the edge count never binds the
        // walk: the budget is then the same at every depth and would key every node alike.
        // What separates two arrivals there is the depth, which decides how much of the
        // subtree clears the minimum, and it stops mattering once the minimum is reached
        const uint64_t hopCeiling = std::max<uint64_t>(_parts.getAllocatedEdgeCount(), _pendingEdgeIDBound);
        _keysDepth = _maxHops > hopCeiling;
        _expansionSpan = _keysDepth ? std::min(_minHops, hopCeiling) : _maxHops;

        const uint64_t nodeBound = std::max<uint64_t>(nodeIDBound(), 1);
        _prunes = _expansionSpan < std::numeric_limits<uint64_t>::max() / nodeBound;
    }

    if (_paths) {
        retainWalkedPath();
    }

    resizeOutputs(maxCount);
    _written = 0;

    const size_t inputSize = _input->size();

    while (_written < maxCount) {
        if (_active) {
            step();
        } else if (_seedCursor < inputSize) {
            startSeed(_seedCursor);
            _seedCursor++;
        } else {
            break;
        }
    }

    resizeOutputs(_written);
    _valid = hasWork();
}

void PathExplorator::startSeed(size_t row) {
    if (_prunes) {
        _emittedEnds.clear();
        _cleanExpansions.clear();
    }

    _seedRow = row;
    _pathEdges.clear();
    _pathSignatures.clear();
    _pathSignatures.push_back(0);
    _pathEntries.clear();
    _pathEntries.push_back(PathTrie::ROOT);
    _frames.clear();
    _candidateNodes.clear();
    _candidateEdges.clear();
    _target = PathTargetHandle {};

    const NodeID seed = (*_input)[row];

    if (_endNodes) {
        _targetNode = (*_endNodes)[row];
        if (_targetIndex) {
            _target = _targetIndex->find(_targetNode);
        }
    }

    if (_minHops == 0 && isEnd(row, seed) && (!_prunes || _emittedEnds.insert(seed.getValue()))) {
        emit(row, seed, PathTrie::ROOT);
    }

    const bool beyondLabels = _distances && !_distances->canReachEndWithin(seed, _maxHops);
    const bool beyondTarget = !canReachTargetWithin(seed, _maxHops);
    if (_maxHops > 0 && !beyondLabels && !beyondTarget) {
        _active = true;
        descend(seed);
    }
}

void PathExplorator::step() {
    Frame& frame = _frames.back();
    if (frame._next == frame._candidateEnd) {
        popFrame();
        return;
    }

    const size_t candidate = frame._next;
    const NodeID node = _candidateNodes[candidate];
    const EdgeID edge = _candidateEdges[candidate];
    frame._next++;

    const uint64_t depth = _frames.size();
    const bool emits = depth >= _minHops && isEnd(_seedRow, node);
    const bool expands = depth < _maxHops;

    if (!emits && !expands) {
        return;
    }

    PathRef entry = PathTrie::ROOT;
    if (_paths) {
        entry = _trie->append(_arena, _pathEntries.back(), edge, node, depth);
    }

    if (emits && (!_prunes || _emittedEnds.insert(node.getValue()))) {
        emit(_seedRow, node, entry);
        if (_paths) {
            _pinned = _trie->getArenaSize(_arena);
        }
    }

    if (!expands) {
        return;
    }

    if (_prunes && _cleanExpansions.contains(expansionKey(node, _maxHops - depth))) {
        return;
    }

    const size_t ahead = candidate + _lookahead;
    if (_lookahead > 0 && ahead < frame._candidateEnd) {
        const NodeID aheadNode = _candidateNodes[ahead];
        prefetchNodeData(aheadNode, _parts.ownerIndex(aheadNode));
    }

    _pathEdges.push_back(edge);
    _pathSignatures.push_back(_pathSignatures.back() | signatureBit(edge));
    if (_paths) {
        _pathEntries.push_back(entry);
    }

    descend(node);
}

void PathExplorator::popFrame() {
    const Frame frame = _frames.back();
    const size_t depth = _frames.size() - 1;
    _candidateNodes.resize(frame._candidateBegin);
    _candidateEdges.resize(frame._candidateBegin);
    _frames.pop_back();

    if (_prunes) {
        // Every edge this subtree could not take was one it held itself, so the same walk
        // leaves this node whatever the prefix above it used
        if (frame._taint >= depth) {
            _cleanExpansions.insert(expansionKey(frame._node, frame._budget));
        }

        if (!_frames.empty()) {
            _frames.back()._taint = std::min(_frames.back()._taint, frame._taint);
        }
    }

    if (_frames.empty()) {
        _active = false;
        return;
    }

    _pathEdges.pop_back();
    _pathSignatures.pop_back();
    if (_paths) {
        releasePathEntry();
    }
}

void PathExplorator::generatePendingCandidates(NodeID node) {
    if (!_pendingAdjacency) {
        return;
    }

    if (_direction != PathExplorationDir::BACKWARD) {
        generateCandidates(_pendingAdjacency->outOf(node, _pendingEdgeIDBound));
    }

    if (_direction != PathExplorationDir::FORWARD) {
        generateCandidates(_pendingAdjacency->into(node, _pendingEdgeIDBound));
    }
}

// Reads the node's adjacency wherever it lives and pushes the frame of the candidates it
// offers, which the next steps walk one at a time
void PathExplorator::descend(NodeID node) {
    _descentTaint = NO_TAINT;
    const size_t owner = isPendingNode(node) ? _parts.size() : _parts.ownerIndex(node);
    prefetchNodeData(node, owner);

    std::span<const EdgeRecord> outs;
    std::span<const EdgeRecord> ins;

    if (owner < _parts.size()) {
        const EdgeIndexer& indexer = *_parts.get(owner)._indexer;

        if (_direction != PathExplorationDir::BACKWARD) {
            outs = indexer.getNodeOutEdges(node);
        }

        if (_direction != PathExplorationDir::FORWARD) {
            ins = indexer.getNodeInEdges(node);
        }
    }

    const size_t begin = _candidateNodes.size();

    generateCandidates(outs);
    generateCandidates(ins);
    generatePendingCandidates(node);

    for (const size_t patchIndex : _parts.patchPartsAfter(owner)) {
        const EdgeIndexer& indexer = *_parts.get(patchIndex)._indexer;

        if (_direction != PathExplorationDir::BACKWARD) {
            generateCandidates(indexer.getNodeOutEdges(node));
        }

        if (_direction != PathExplorationDir::FORWARD) {
            generateCandidates(indexer.getNodeInEdges(node));
        }
    }

    size_t end = _candidateNodes.size();
    if (_hopFilter && end > begin) {
        const std::span<NodeID> candidateNodes(_candidateNodes.data() + begin, end - begin);
        const std::span<EdgeID> candidateEdges(_candidateEdges.data() + begin, end - begin);
        const size_t survivors = _hopFilter->filter(node, candidateNodes, candidateEdges);

        end = begin + survivors;
        _candidateNodes.resize(end);
        _candidateEdges.resize(end);
    }

    _frames.push_back({begin, end, begin, node, _maxHops - _pathEdges.size(), _descentTaint});
}

void PathExplorator::generateCandidates(std::span<const EdgeRecord> edges) {
    const uint64_t signature = _pathSignatures.back();
    const bool hasPathEdges = !_pathEdges.empty();
    const EdgeID lastEdge = hasPathEdges ? _pathEdges.back() : EdgeID();

    // The hops a candidate may still take after the one that reaches it
    const uint64_t remainingHops = _maxHops - (_pathEdges.size() + 1);

    const auto positionOnPath = [this](EdgeID edge) {
        const auto found = std::find(_pathEdges.begin(), _pathEdges.end(), edge);

        return found == _pathEdges.end() ? NO_TAINT : static_cast<size_t>(found - _pathEdges.begin());
    };

    const std::span<const EdgeTypeID> edgeTypes = _edgeTypes;

    _candidateChecks += edges.size();

    for (const EdgeRecord& record : edges) {
        const EdgeID edge = record._edgeID;

        const bool backtracks = hasPathEdges && edge == lastEdge;
        const bool wrongType = _filterByType && !edgeTypeMatches(edgeTypes, record._edgeTypeID);
        const bool deleted = _filterTombstones && _tombstones->containsEdge(edge);
        const size_t heldAt = backtracks ? _pathEdges.size() - 1
            : (signature & signatureBit(edge)) != 0 ? positionOnPath(edge) : NO_TAINT;
        const bool onTrail = heldAt != NO_TAINT;
        const bool beyondLabels = _distances && !_distances->canReachEndWithin(record._otherID, remainingHops);
        const bool beyondTarget = !canReachTargetWithin(record._otherID, remainingHops);

        if (backtracks || wrongType || deleted || onTrail || beyondLabels || beyondTarget) {
            if (_prunes && onTrail) {
                _descentTaint = std::min(_descentTaint, heldAt);
            }

            continue;
        }

        _candidateNodes.push_back(record._otherID);
        _candidateEdges.push_back(edge);
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

void PathExplorator::acquireArena() {
    _arena = _trie->acquireArena();
}

void PathExplorator::releaseArena() {
    _trie->releaseArena(_arena);
    _arena = 0;
}

// The rows of the last chunk have been read once the next fill starts, so the arena keeps
// the walk's current path alone
void PathExplorator::retainWalkedPath() {
    _trie->retainChain(_arena, _pathEntries);
    _pinned = 0;
}

// A backtracked entry no emitted row holds is the top of the arena: the entries above it
// were its descendants, each released on its own backtrack or pinned, which pins it too
void PathExplorator::releasePathEntry() {
    const size_t index = PathTrie::indexOf(_pathEntries.back());
    _pathEntries.pop_back();

    if (index >= _pinned) {
        _trie->truncateArena(_arena, index);
    }
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

void PathExplorator::startBatch() {
    Reachability& reach = _reach;
    const size_t nodeCount = nodeIDBound();
    const size_t count = std::min(PathTargetIndex::targetsPerBatch, _input->size() - _seedCursor);

    reach._batchFirstRow = _seedCursor;
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
        const NodeID seed = (*_input)[row];
        if (seed.getValue() >= nodeCount) {
            continue;
        }

        const uint64_t mask = 1ull << bit;
        PathReachTable::Slot& slot = reach._reached.reach(seed);

        if (_minHops == 0) {
            slot._seen |= mask;
        }

        if (slot._frontier == 0) {
            reach._next.push_back(seed);
        }

        slot._frontier |= mask;
    }
}

void PathExplorator::emitGainedRows(size_t maxCount) {
    Reachability& reach = _reach;

    while (_written < maxCount && reach._emitNode < reach._next.size()) {
        const NodeID node = reach._next[reach._emitNode];

        if (reach._emitBits == 0) {
            reach._emitBits = reach._reached.get(node)._frontier;
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
    PathReachTable& reached = reach._reached;

    std::swap(reach._frontier, reach._next);
    reach._next.clear();
    reach._emitNode = 0;
    reach._emitBits = 0;
    reach._level++;

    // The frontier is known ahead, so each node's adjacency is fetched a few nodes before
    // the level reaches it
    const size_t frontierSize = reach._frontier.size();
    for (size_t index = 0; index < frontierSize; index++) {
        const size_t ahead = index + frontierLookahead;
        if (ahead < frontierSize) {
            const NodeID aheadNode = reach._frontier[ahead];
            prefetchNodeData(aheadNode, _parts.ownerIndex(aheadNode));
        }

        // The word is taken before the candidates are reached: reaching one may grow the
        // table under the expanded slot
        const NodeID node = reach._frontier[index];
        PathReachTable::Slot& expanded = reached.get(node);
        const uint64_t word = expanded._frontier;
        expanded._frontier = 0;

        collectReachCandidates(node);

        for (const NodeID candidate : reach._candidateNodes) {
            PathReachTable::Slot& slot = reached.reach(candidate);
            const uint64_t gained = word & ~slot._seen;
            if (gained == 0) {
                continue;
            }

            slot._seen |= gained;
            if (slot._gained == 0) {
                reach._next.push_back(candidate);
            }
            slot._gained |= gained;
        }
    }

    for (const NodeID node : reach._next) {
        PathReachTable::Slot& slot = reached.get(node);
        slot._frontier = slot._gained;
        slot._gained = 0;
    }
}

void PathExplorator::collectReachCandidates(NodeID node) {
    Reachability& reach = _reach;
    reach._candidateNodes.clear();
    reach._candidateEdges.clear();

    appendPendingReachCandidates(node);

    const size_t owner = isPendingNode(node) ? _parts.size() : _parts.ownerIndex(node);
    if (owner < _parts.size()) {
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
    }

    if (_hopFilter && !reach._candidateNodes.empty()) {
        const size_t survivors = _hopFilter->filter(node, reach._candidateNodes, reach._candidateEdges);
        reach._candidateNodes.resize(survivors);
        reach._candidateEdges.resize(survivors);
    }
}

void PathExplorator::appendPendingReachCandidates(NodeID node) {
    if (!_pendingAdjacency) {
        return;
    }

    if (_direction != PathExplorationDir::BACKWARD) {
        appendReachCandidates(_pendingAdjacency->outOf(node, _pendingEdgeIDBound));
    }

    if (_direction != PathExplorationDir::FORWARD) {
        appendReachCandidates(_pendingAdjacency->into(node, _pendingEdgeIDBound));
    }
}

void PathExplorator::appendReachCandidates(std::span<const EdgeRecord> edges) {
    const std::span<const EdgeTypeID> edgeTypes = _edgeTypes;

    _candidateChecks += edges.size();

    for (const EdgeRecord& record : edges) {
        const bool wrongType = _filterByType && !edgeTypeMatches(edgeTypes, record._edgeTypeID);
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

    reach._reached.clear();
    reach._frontier.clear();
    reach._next.clear();
    reach._emitNode = 0;
    reach._emitBits = 0;
    reach._batchActive = false;
}
