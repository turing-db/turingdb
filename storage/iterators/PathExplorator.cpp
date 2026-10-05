#include "PathExplorator.h"

#include <algorithm>
#include <bit>
#include <limits>

#include "EdgeTypeMatch.h"
#include "PathDistanceIndex.h"
#include "datapart/NodeContainer.h"
#include "indexers/EdgeIndexer.h"
#include "list/PathTrie.h"
#include "versioning/Tombstones.h"

#include "BioAssert.h"

using namespace db;

namespace {

// How many frontier nodes ahead the distinct mode's search fetches adjacency
constexpr size_t frontierLookahead = 16;

// The candidates a level search gathers before filtering them in one call. A directed
// distinct walk under a hop predicate on reactome took 18.9 ms at one call a node, 18.6 to
// 18.8 ms at 256 to 4096 candidates and 19.6 ms at 64Ki.
constexpr size_t REACH_BATCH_CANDIDATES = 4096;

constexpr size_t initialKeySetSlots = 1024;

constexpr size_t initialPathEdgeSlots = 128;

// A path of up to this many edges is scanned for an edge its 64-bit signature does not rule
// out; past it the signature saturates and the scan costs more than keeping the table
constexpr size_t scannedPathEdges = 32;

uint64_t scatter(uint64_t key) {
    return (key * 0x9E3779B97F4A7C15ull) >> 32;
}

uint64_t signatureBit(EdgeID edge) {
    return 1ull << ((edge.getValue() * 0x9E3779B97F4A7C15ull) >> 58);
}

// The words a node holds in the cycle search, each one bit per seed of the batch
constexpr size_t seedsWord = 0;
constexpr size_t firstArrivalWord = 1;
constexpr size_t secondArrivalWord = 2;
constexpr size_t firstFrontierWord = 3;
constexpr size_t secondFrontierWord = 4;
constexpr size_t firstGainedWord = 5;
constexpr size_t secondGainedWord = 6;
constexpr size_t identityWord = 7;

bool hasGains(const uint64_t* words) {
    return (words[firstGainedWord] | words[secondGainedWord]) != 0;
}

// An arrival over the seeds of the mask, each having left its seed by the edge the identity
// words give: the first arrival of a seed is kept, a later one gives a second when it differs
void offerFirstArrival(uint64_t* words, uint64_t mask, std::span<const uint64_t> identity) {
    const uint64_t fresh = mask & ~words[firstArrivalWord];
    const uint64_t held = mask & words[firstArrivalWord] & ~words[secondArrivalWord];

    uint64_t differs = 0;
    for (size_t plane = 0; plane < identity.size(); plane++) {
        uint64_t& word = words[identityWord + plane];
        differs |= word ^ identity[plane];
        word = (word & ~fresh) | (identity[plane] & fresh);
    }

    const uint64_t second = held & differs;

    words[firstArrivalWord] |= fresh;
    words[firstGainedWord] |= fresh;
    words[secondArrivalWord] |= second;
    words[secondGainedWord] |= second;
}

// Arrivals over the seeds of the mask from a node two different edges out of each seed reach
void offerSecondArrival(uint64_t* words, uint64_t mask, std::span<const uint64_t> identity) {
    offerFirstArrival(words, mask & ~words[firstArrivalWord], identity);

    const uint64_t second = mask & ~words[secondArrivalWord];
    words[secondArrivalWord] |= second;
    words[secondGainedWord] |= second;
}

}

void PathExplorator::PathEdgeTable::push(std::span<const EdgeID> path) {
    if (!_active || path.size() * 2 > _slots.size()) {
        rebuild(path);
        return;
    }

    place(path.back(), path.size() - 1);
}

void PathExplorator::PathEdgeTable::pop(std::span<const EdgeID> path) {
    if (path.size() == scannedPathEdges + 1) {
        clear();
        return;
    }

    const size_t mask = _slots.size() - 1;
    const EdgeID edge = path.back();
    size_t slot = scatter(edge.getValue()) & mask;

    while (_slots[slot]._edge != edge || _slots[slot]._stamp != _generation) {
        slot = (slot + 1) & mask;
    }

    _slots[slot]._stamp = 0;
}

void PathExplorator::PathEdgeTable::clear() {
    _active = false;
}

size_t PathExplorator::PathEdgeTable::find(std::span<const EdgeID> path, EdgeID edge) const {
    if (!_active) {
        const auto found = std::find(path.begin(), path.end(), edge);

        return found == path.end() ? NO_TAINT : static_cast<size_t>(found - path.begin());
    }

    const size_t mask = _slots.size() - 1;
    size_t slot = scatter(edge.getValue()) & mask;

    while (_slots[slot]._stamp == _generation) {
        if (_slots[slot]._edge == edge) {
            return _slots[slot]._position;
        }

        slot = (slot + 1) & mask;
    }

    return NO_TAINT;
}

void PathExplorator::PathEdgeTable::rebuild(std::span<const EdgeID> path) {
    const size_t slotCount = std::max(initialPathEdgeSlots, std::bit_ceil(path.size() * 2));
    if (slotCount != _slots.size()) {
        _slots.assign(slotCount, Slot {});
        _generation = 1;
    } else {
        _generation++;
        if (_generation == 0) {
            std::fill(_slots.begin(), _slots.end(), Slot {});
            _generation = 1;
        }
    }

    _active = true;
    for (size_t position = 0; position < path.size(); position++) {
        place(path[position], position);
    }
}

void PathExplorator::PathEdgeTable::place(EdgeID edge, size_t position) {
    const size_t mask = _slots.size() - 1;
    size_t slot = scatter(edge.getValue()) & mask;

    while (_slots[slot]._stamp == _generation) {
        slot = (slot + 1) & mask;
    }

    _slots[slot] = Slot {._edge = edge, ._position = position, ._stamp = _generation};
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

void PathExplorator::ExpansionMemo::clear() {
    _used = 0;
    _dependencyLists.clear();
    _generation++;

    if (_generation != 0) {
        return;
    }

    for (Slot& slot : _slots) {
        slot._stamp = 0;
    }

    _generation = 1;
}

void PathExplorator::ExpansionMemo::grow() {
    const size_t slotCount = _slots.empty() ? initialKeySetSlots : _slots.size() * 2;
    const std::vector<Slot> slots(_slots);

    _slots.assign(slotCount, Slot {});

    const size_t mask = slotCount - 1;
    for (const Slot& slot : slots) {
        if (slot._stamp != _generation) {
            continue;
        }

        size_t target = scatter(slot._key) & mask;
        while (_slots[target]._stamp == _generation) {
            target = (target + 1) & mask;
        }

        _slots[target] = slot;
    }
}

// A list with fewer dependencies serves more arrivals, so it replaces a longer one; between two
// of one length the newer is kept, being the one the prefixes walked next are likelier to hold
void PathExplorator::ExpansionMemo::remember(uint64_t key, std::span<const Dependency> dependencies) {
    if ((_used + 1) * 2 >= _slots.size()) {
        grow();
    }

    const size_t mask = _slots.size() - 1;
    size_t index = scatter(key) & mask;

    while (_slots[index]._stamp == _generation && _slots[index]._key != key) {
        index = (index + 1) & mask;
    }

    Slot& slot = _slots[index];
    const bool present = slot._stamp == _generation;
    const uint32_t list = present ? slot._list : 0;

    if (present) {
        const size_t presentCount = list == 0 ? 0 : _dependencyLists[list - 1]._count;
        if (dependencies.size() > presentCount) {
            return;
        }
    } else {
        slot._key = key;
        slot._stamp = _generation;
        _used++;
    }

    if (dependencies.empty()) {
        slot._list = 0;
        return;
    }

    if (list == 0) {
        _dependencyLists.emplace_back();
        slot._list = static_cast<uint32_t>(_dependencyLists.size());
    }

    DependencyList& stored = _dependencyLists[slot._list - 1];
    stored._count = dependencies.size();
    for (size_t index = 0; index < dependencies.size(); index++) {
        stored._edges[index] = dependencies[index]._edge;
    }
}

const PathExplorator::DependencyList* PathExplorator::ExpansionMemo::find(uint64_t key) const {
    if (_slots.empty()) {
        return nullptr;
    }

    const size_t mask = _slots.size() - 1;
    size_t index = scatter(key) & mask;

    while (_slots[index]._stamp == _generation) {
        const Slot& slot = _slots[index];
        if (slot._key == key) {
            return slot._list == 0 ? &_noDependencies : &_dependencyLists[slot._list - 1];
        }

        index = (index + 1) & mask;
    }

    return nullptr;
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

    _edgeTypeWords.clear();
    for (const EdgeTypeID edgeType : edgeTypes) {
        const size_t word = edgeType.getValue() >> 6;
        if (word >= _edgeTypeWords.size()) {
            _edgeTypeWords.resize(word + 1, 0);
        }

        _edgeTypeWords[word] |= 1ull << (edgeType.getValue() & 63);
    }
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
    return _distinctEnds && _minHops <= 1;
}

bool PathExplorator::searchesSeedCycles() const {
    return _minHops == 1 && _direction == PathExplorationDir::BOTH;
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
    if (_distances && !isPendingNode(node)) {
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
    _pathEdgeTable.clear();
    _pathEdges.clear();
    _pathEntries.clear();
    _pathSignatures.clear();
    _frames.clear();
    _candidateNodes.clear();
    _candidateEdges.clear();
    _dependencies.clear();

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

    _prunes = _distinctEnds;

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
        _expansions.clear();
    }

    _seedRow = row;
    _pathEdgeTable.clear();
    _pathEdges.clear();
    _pathSignatures.clear();
    _pathSignatures.push_back(0);
    _pathEntries.clear();
    _pathEntries.push_back(PathTrie::ROOT);
    _frames.clear();
    _candidateNodes.clear();
    _candidateEdges.clear();
    _dependencies.clear();
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

    if (_prunes) {
        const DependencyList* remembered = _expansions.find(expansionKey(node, _maxHops - depth));

        if (remembered && remembered->_count == 0) {
            return;
        } else if (remembered && holdsDependencies(*remembered, edge)) {
            dependOn(*remembered, edge);
            return;
        }
    }

    const size_t ahead = candidate + _lookahead;
    if (_lookahead > 0 && ahead < frame._candidateEnd) {
        const NodeID aheadNode = _candidateNodes[ahead];
        prefetchNodeData(aheadNode, _parts.ownerIndex(aheadNode));
    }

    _pathEdges.push_back(edge);
    if (_pathEdges.size() > scannedPathEdges) {
        _pathEdgeTable.push(_pathEdges);
    }
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
        size_t taint = frame._taint;

        const bool dependsOnHeldEdges = _dependencies.size() > frame._dependencyBegin;
        if (dependsOnHeldEdges) {
            keepDependenciesAbove(frame._dependencyBegin, depth, taint);
        }

        if (taint >= depth) {
            const std::span<const Dependency> dependencies(_dependencies.data() + frame._dependencyBegin,
                                                           _dependencies.size() - frame._dependencyBegin);
            _expansions.remember(expansionKey(frame._node, frame._budget), dependencies);
        }

        if (!_frames.empty()) {
            _frames.back()._taint = std::min(_frames.back()._taint, taint);
        }
    }

    if (_frames.empty()) {
        _active = false;
        return;
    }

    if (_pathEdgeTable._active) {
        _pathEdgeTable.pop(_pathEdges);
    }
    _pathEdges.pop_back();
    _pathSignatures.pop_back();
    if (_paths) {
        releasePathEntry();
    }
}

void PathExplorator::keepDependenciesAbove(size_t begin, size_t depth, size_t& taint) {
    size_t kept = begin;

    for (size_t index = begin; index < _dependencies.size(); index++) {
        const Dependency dependency = _dependencies[index];
        const bool heldInside = dependency._position >= depth;
        const bool repeated = std::any_of(_dependencies.begin() + begin,
                                          _dependencies.begin() + kept,
                                          [&](const Dependency& other) { return other._edge == dependency._edge; });

        if (heldInside || repeated) {
            continue;
        }

        if (kept - begin < MAX_DEPENDENCIES) {
            _dependencies[kept] = dependency;
            kept++;
        } else {
            taint = std::min(taint, dependency._position);
        }
    }

    _dependencies.resize(kept);
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
    const size_t dependencyBegin = _dependencies.size();
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
        PathHopFrame frame {._seedRow = _seedRow, ._source = node, ._candidateCount = end - begin};
        const size_t survivors = _hopFilter->filter({&frame, 1}, candidateNodes, candidateEdges);

        end = begin + survivors;
        _candidateNodes.resize(end);
        _candidateEdges.resize(end);
    }

    _frames.push_back({begin, end, begin, node, _maxHops - _pathEdges.size(), dependencyBegin, NO_TAINT});
}

void PathExplorator::generateCandidates(std::span<const EdgeRecord> edges) {
    const uint64_t signature = _pathSignatures.back();
    const bool hasPathEdges = !_pathEdges.empty();
    const EdgeID lastEdge = hasPathEdges ? _pathEdges.back() : EdgeID();

    const uint64_t candidateDepth = _pathEdges.size() + 1;

    // The hops a candidate may still take after the one that reaches it
    const uint64_t remainingHops = _maxHops - candidateDepth;

    const std::span<const EdgeTypeID> edgeTypes = _edgeTypes;
    const std::span<const uint64_t> edgeTypeWords = _edgeTypeWords;
    const bool checksTarget = _target.isValid() || (_filtersByEndNodeSet && _targetIndex);

    _candidateChecks += edges.size();

    for (const EdgeRecord& record : edges) {
        const EdgeID edge = record._edgeID;

        const bool backtracks = hasPathEdges && edge == lastEdge;
        const bool wrongType = _filterByType && !edgeTypeMatches(edgeTypes, edgeTypeWords, record._edgeTypeID);
        const bool deleted = _filterTombstones && _tombstones->containsEdge(edge);
        const size_t heldAt = backtracks ? _pathEdges.size() - 1
            : (signature & signatureBit(edge)) != 0 ? positionOnPath(edge) : NO_TAINT;
        const bool onTrail = heldAt != NO_TAINT;
        const bool ruledOut = backtracks || wrongType || deleted || onTrail;
        const bool beyondLabels = !ruledOut && _distances && !_distances->canReachEndWithin(record._otherID, remainingHops);
        const bool beyondTarget = !ruledOut && !beyondLabels && checksTarget && !canReachTargetWithin(record._otherID, remainingHops);

        if (ruledOut || beyondLabels || beyondTarget) {
            if (_prunes && onTrail) {
                dependOnBlockedEdge(edge, record._otherID, candidateDepth, heldAt);
            }

            continue;
        }

        _candidateNodes.push_back(record._otherID);
        _candidateEdges.push_back(edge);
    }
}

size_t PathExplorator::positionOnPath(EdgeID edge) const {
    if ((_pathSignatures.back() & signatureBit(edge)) == 0) {
        return NO_TAINT;
    }

    return _pathEdgeTable.find(_pathEdges, edge);
}

bool PathExplorator::holdsDependencies(const DependencyList& dependencies, EdgeID arrival) const {
    for (size_t index = 0; index < dependencies._count; index++) {
        const EdgeID dependency = dependencies._edges[index];
        const bool held = dependency == arrival || positionOnPath(dependency) != NO_TAINT;

        if (!held) {
            return false;
        }
    }

    return true;
}

const PathExplorator::DependencyList* PathExplorator::findReusableExpansion(NodeID node, uint64_t depth, EdgeID arrival) const {
    const DependencyList* dependencies = _expansions.find(expansionKey(node, _maxHops - depth));
    if (!dependencies || !holdsDependencies(*dependencies, arrival)) {
        return nullptr;
    }

    return dependencies;
}

void PathExplorator::dependOnBlockedEdge(EdgeID edge, NodeID node, uint64_t depth, size_t position) {
    const uint64_t remainingHops = _maxHops - depth;
    const bool checksTarget = _target.isValid() || (_filtersByEndNodeSet && _targetIndex);
    const bool beyondLabels = _distances && !_distances->canReachEndWithin(node, remainingHops);
    const bool beyondTarget = checksTarget && !canReachTargetWithin(node, remainingHops);

    if (beyondLabels || beyondTarget) {
        return;
    }

    const bool emitsThere = depth >= _minHops && isEnd(_seedRow, node);
    const bool emitted = !emitsThere || _emittedEnds.contains(node.getValue());
    const bool expandsThere = depth < _maxHops;
    const DependencyList* covering = emitted && expandsThere ? findReusableExpansion(node, depth, edge) : nullptr;

    if (emitted && !expandsThere) {
        return;
    } else if (covering) {
        dependOn(*covering, edge);
    } else {
        _dependencies.push_back({edge, position});
    }
}

void PathExplorator::dependOn(const DependencyList& dependencies, EdgeID arrival) {
    for (size_t index = 0; index < dependencies._count; index++) {
        const EdgeID dependency = dependencies._edges[index];
        const size_t position = positionOnPath(dependency);
        const bool arrivesThrough = position == NO_TAINT && dependency == arrival;

        _dependencies.push_back({dependency, arrivesThrough ? _pathEdges.size() : position});
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
    reach._closedSeeds = 0;
    reach._batchActive = true;
    _seedCursor += count;

    // A seed's own bit stays out of its seen word at a minimum of one hop, so a directed walk
    // reports the seed at the level of its shortest cycle. Undirected, stepping back along the
    // edge it left by would report it with no trail, so the bit is set as at a minimum of zero
    // and a cycle through the seed decides its row.
    const bool seedCycles = searchesSeedCycles();
    uint64_t searchedSeeds = 0;
    for (size_t bit = 0; bit < count; bit++) {
        const size_t row = reach._batchFirstRow + bit;
        const NodeID seed = (*_input)[row];
        if (seed.getValue() >= nodeCount) {
            continue;
        }

        const uint64_t mask = 1ull << bit;
        PathReachTable::Slot& slot = reach._reached.reach(seed);

        if (_minHops == 0 || seedCycles) {
            slot._seen |= mask;
        }

        if (slot._frontier == 0) {
            reach._next.push_back(seed);
        }

        slot._frontier |= mask;

        if (seedCycles && isEnd(row, seed)) {
            searchedSeeds |= mask;
        }
    }

    if (searchedSeeds != 0) {
        reach._closedSeeds = closedSeedsOf(searchedSeeds);
    }
}

// A search that finds no cycle has walked the component of every seed it gave up on when the
// hops are unbounded: one labelling answers the later seeds of those components instead
uint64_t PathExplorator::closedSeedsOf(uint64_t seeds) {
    CycleSearch& search = _cycleSearch;
    const size_t firstRow = _reach._batchFirstRow;

    uint64_t closed = 0;
    uint64_t unknown = 0;
    for (uint64_t remaining = seeds; remaining != 0; remaining &= remaining - 1) {
        const uint64_t mask = remaining & -remaining;
        const uint64_t seed = (*_input)[firstRow + std::countr_zero(remaining)].getValue();

        const auto labelled = search._components.find(seed);
        const auto searched = search._seeds.find(seed);
        if (labelled != search._components.end()) {
            closed |= labelled->second._onCycle ? mask : 0;
        } else if (searched != search._seeds.end()) {
            closed |= searched->second ? mask : 0;
        } else {
            unknown |= mask;
        }
    }

    if (unknown == 0) {
        return closed;
    }

    const uint64_t found = searchCycles(unknown);
    const bool labels = labelsComponents();

    for (uint64_t remaining = unknown; remaining != 0; remaining &= remaining - 1) {
        const bool closes = (found & remaining & -remaining) != 0;
        const NodeID seed = (*_input)[firstRow + std::countr_zero(remaining)];

        if (!closes && labels) {
            if (!search._components.contains(seed.getValue())) {
                labelComponentOf(seed);
            }
        } else {
            search._seeds.emplace(seed.getValue(), closes);
        }
    }

    return closed | found;
}

bool PathExplorator::labelsComponents() const {
    const uint64_t hopCeiling = std::max<uint64_t>(_parts.getAllocatedEdgeCount(), _pendingEdgeIDBound);

    return _maxHops >= hopCeiling && !_hopFilter;
}

// Tarjan's bridges: a node is on a cycle when it carries an edge off the depth-first tree, a
// self-loop among them, or a tree edge that is no bridge
void PathExplorator::labelComponentOf(NodeID root) {
    CycleSearch& search = _cycleSearch;
    std::unordered_map<uint64_t, CycleSearch::ComponentNode>& components = search._components;
    std::vector<CycleSearch::ComponentFrame>& frames = search._frames;

    discoverComponentNode(root, EdgeID {});

    while (!frames.empty()) {
        CycleSearch::ComponentFrame& frame = frames.back();

        if (frame._next < frame._candidateEnd) {
            const size_t index = frame._next++;
            const NodeID node = frame._node;
            const EdgeID edge = search._candidateEdges[index];
            const NodeID other = search._candidateNodes[index];

            if (edge == frame._parentEdge) {
                continue;
            }

            const auto found = components.find(other.getValue());
            if (found == components.end()) {
                discoverComponentNode(other, edge);
            } else {
                CycleSearch::ComponentNode& current = components.at(node.getValue());
                current._low = std::min(current._low, found->second._index);
                current._onCycle = true;
                found->second._onCycle = true;
            }
        } else {
            const NodeID finished = frame._node;
            search._candidateNodes.resize(frame._candidateBegin);
            search._candidateEdges.resize(frame._candidateBegin);
            frames.pop_back();

            if (!frames.empty()) {
                CycleSearch::ComponentNode& child = components.at(finished.getValue());
                CycleSearch::ComponentNode& parent = components.at(frames.back()._node.getValue());
                parent._low = std::min(parent._low, child._low);

                if (child._low <= parent._index) {
                    child._onCycle = true;
                    parent._onCycle = true;
                }
            }
        }
    }
}

void PathExplorator::discoverComponentNode(NodeID node, EdgeID parentEdge) {
    CycleSearch& search = _cycleSearch;
    const size_t index = search._components.size();
    search._components.emplace(node.getValue(), CycleSearch::ComponentNode {._index = index, ._low = index});

    collectReachCandidates(node);

    const size_t begin = search._candidateNodes.size();
    search._candidateNodes.insert(search._candidateNodes.end(), _reach._candidateNodes.begin(), _reach._candidateNodes.end());
    search._candidateEdges.insert(search._candidateEdges.end(), _reach._candidateEdges.begin(), _reach._candidateEdges.end());

    search._frames.push_back(CycleSearch::ComponentFrame {._node = node,
                                                          ._parentEdge = parentEdge,
                                                          ._candidateBegin = begin,
                                                          ._candidateEnd = search._candidateNodes.size(),
                                                          ._next = begin});
}

// A walk out of a seed that comes back by another edge than it left by shortens to a closed
// trail, and every closed trail is such a walk. Two arrivals per node and seed suffice: the edge
// back to the seed can rule out the first edge of only one of them.
uint64_t PathExplorator::searchCycles(uint64_t seeds) {
    if (_maxHops == 0) {
        return 0;
    }

    CycleSearch& search = _cycleSearch;
    PathCycleTable& reached = search._reached;
    const std::vector<NodeID>& candidateNodes = _reach._candidateNodes;
    const std::vector<EdgeID>& candidateEdges = _reach._candidateEdges;
    const size_t firstRow = _reach._batchFirstRow;

    clearReachFrames();
    for (uint64_t remaining = seeds; remaining != 0; remaining &= remaining - 1) {
        appendReachFrame((*_input)[firstRow + std::countr_zero(remaining)]);
    }
    filterReachFrames();

    uint64_t closed = 0;
    size_t widestDegree = 0;
    size_t seedFrame = 0;
    size_t seedCandidate = 0;
    for (uint64_t remaining = seeds; remaining != 0; remaining &= remaining - 1) {
        const unsigned bit = std::countr_zero(remaining);
        const NodeID seed = (*_input)[firstRow + bit];
        std::vector<CycleSearch::FirstHop>& hops = search._firstHops[bit];
        const size_t frameEnd = seedCandidate + _reach._frames[seedFrame]._candidateCount;
        seedFrame++;

        hops.clear();
        for (; seedCandidate < frameEnd; seedCandidate++) {
            if (candidateNodes[seedCandidate] == seed) {
                closed |= 1ull << bit;
            } else {
                hops.push_back(CycleSearch::FirstHop {._edge = candidateEdges[seedCandidate], ._node = candidateNodes[seedCandidate]});
            }
        }

        std::sort(hops.begin(), hops.end(), [](const CycleSearch::FirstHop& left, const CycleSearch::FirstHop& right) {
            return left._edge < right._edge;
        });

        widestDegree = std::max(widestDegree, hops.size());
    }

    const size_t identityBits = std::bit_width(widestDegree > 0 ? widestDegree - 1 : 0);
    std::array<uint64_t, 64> identityPlanes {};
    const std::span<uint64_t> identity(identityPlanes.data(), identityBits);

    reached.reset(identityWord + identityBits);
    search._next.clear();

    for (uint64_t remaining = seeds; remaining != 0; remaining &= remaining - 1) {
        reached.reach((*_input)[firstRow + std::countr_zero(remaining)])[seedsWord] |= remaining & -remaining;
    }

    if (_maxHops > 1) {
        for (uint64_t remaining = seeds & ~closed; remaining != 0; remaining &= remaining - 1) {
            const uint64_t mask = remaining & -remaining;
            const std::vector<CycleSearch::FirstHop>& hops = search._firstHops[std::countr_zero(remaining)];

            for (size_t index = 0; index < hops.size(); index++) {
                for (size_t plane = 0; plane < identityBits; plane++) {
                    identity[plane] = ((index >> plane) & 1) != 0 ? mask : 0;
                }

                uint64_t* words = reached.reach(hops[index]._node);
                const bool waiting = hasGains(words);
                offerFirstArrival(words, mask & ~words[seedsWord], identity);

                if (!waiting && hasGains(words)) {
                    search._next.push_back(hops[index]._node);
                }
            }
        }
    }

    for (uint64_t level = 2; level <= _maxHops && !search._next.empty() && (seeds & ~closed) != 0; level++) {
        std::swap(search._frontier, search._next);
        search._next.clear();

        for (const NodeID node : search._frontier) {
            uint64_t* words = reached.reach(node);
            words[firstFrontierWord] = words[firstGainedWord];
            words[secondFrontierWord] = words[secondGainedWord];
            words[firstGainedWord] = 0;
            words[secondGainedWord] = 0;
        }

        // The closed seeds are taken off again when a frame is offered: an earlier frame of
        // its batch may have closed more since it was gathered
        const bool offers = level < _maxHops;
        const auto offerFrames = [&]() {
            filterReachFrames();

            size_t index = 0;
            for (const PathHopFrame& frame : _reach._frames) {
                const size_t frameEnd = index + frame._candidateCount;
                const uint64_t* words = reached.reach(frame._source);
                const uint64_t first = words[firstFrontierWord] & ~closed;
                const uint64_t second = words[secondFrontierWord] & ~closed;
                if ((first | second) == 0) {
                    index = frameEnd;
                    continue;
                }

                std::copy_n(words + identityWord, identityBits, identity.begin());

                for (; index < frameEnd; index++) {
                    const NodeID candidate = candidateNodes[index];
                    uint64_t* candidateWords = offers ? reached.reach(candidate) : reached.find(candidate);
                    if (!candidateWords) {
                        continue;
                    }

                    const uint64_t seedsThere = candidateWords[seedsWord];
                    const uint64_t returning = (first | second) & seedsThere;
                    if (returning != 0) {
                        closed |= closingReturns(returning, second, identity, candidateEdges[index]);
                    }

                    if (offers) {
                        const bool waiting = hasGains(candidateWords);
                        offerFirstArrival(candidateWords, first & ~seedsThere, identity);
                        offerSecondArrival(candidateWords, second & ~seedsThere, identity);

                        if (!waiting && hasGains(candidateWords)) {
                            search._next.push_back(candidate);
                        }
                    }
                }
            }

            clearReachFrames();
        };

        clearReachFrames();
        for (const NodeID node : search._frontier) {
            const uint64_t* words = reached.reach(node);
            const uint64_t open = (words[firstFrontierWord] | words[secondFrontierWord]) & ~closed;
            if (open == 0) {
                continue;
            }

            appendReachFrame(node);
            if (_reach._candidateNodes.size() >= REACH_BATCH_CANDIDATES) {
                offerFrames();
            }
        }

        offerFrames();
    }

    return closed & seeds;
}

// The seeds among the returning ones that an arrival left by another edge than the one back
uint64_t PathExplorator::closingReturns(uint64_t returning, uint64_t twice, std::span<const uint64_t> identity, EdgeID edge) const {
    uint64_t closing = returning & twice;

    for (uint64_t remaining = returning & ~twice; remaining != 0; remaining &= remaining - 1) {
        const unsigned bit = std::countr_zero(remaining);

        size_t firstIndex = 0;
        for (size_t plane = 0; plane < identity.size(); plane++) {
            firstIndex |= ((identity[plane] >> bit) & 1) << plane;
        }

        const std::vector<CycleSearch::FirstHop>& hops = _cycleSearch._firstHops[bit];
        const auto back = std::lower_bound(hops.begin(), hops.end(), edge, [](const CycleSearch::FirstHop& hop, EdgeID value) {
            return hop._edge < value;
        });

        const bool leftByAnotherEdge = back == hops.end() || back->_edge != edge || static_cast<size_t>(back - hops.begin()) != firstIndex;
        if (leftByAnotherEdge) {
            closing |= 1ull << bit;
        }
    }

    return closing;
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
        const bool closesOnSeed = reach._level == 0 && ((reach._closedSeeds >> bit) & 1) != 0;
        if ((reach._level >= _minHops || closesOnSeed) && isEnd(row, node)) {
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

    clearReachFrames();

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
        reach._frameWords.push_back(expanded._frontier);
        expanded._frontier = 0;

        appendReachFrame(node);
        if (reach._candidateNodes.size() >= REACH_BATCH_CANDIDATES) {
            reachFrames();
        }
    }

    reachFrames();

    for (const NodeID node : reach._next) {
        PathReachTable::Slot& slot = reached.get(node);
        slot._frontier = slot._gained;
        slot._gained = 0;
    }
}

void PathExplorator::reachFrames() {
    Reachability& reach = _reach;
    PathReachTable& reached = reach._reached;

    filterReachFrames();

    size_t index = 0;
    for (size_t frame = 0; frame < reach._frames.size(); frame++) {
        const uint64_t word = reach._frameWords[frame];
        const size_t frameEnd = index + reach._frames[frame]._candidateCount;

        for (; index < frameEnd; index++) {
            const NodeID candidate = reach._candidateNodes[index];
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

    clearReachFrames();
}

void PathExplorator::collectReachCandidates(NodeID node) {
    clearReachFrames();
    appendReachFrame(node);
    filterReachFrames();
}

void PathExplorator::appendReachFrame(NodeID node) {
    Reachability& reach = _reach;
    const size_t frameBegin = reach._candidateNodes.size();

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

    reach._frames.push_back(PathHopFrame {._seedRow = reach._batchFirstRow,
                                          ._source = node,
                                          ._candidateCount = reach._candidateNodes.size() - frameBegin});
}

void PathExplorator::filterReachFrames() {
    Reachability& reach = _reach;
    if (!_hopFilter || reach._candidateNodes.empty()) {
        return;
    }

    const size_t survivors = _hopFilter->filter(reach._frames, reach._candidateNodes, reach._candidateEdges);
    reach._candidateNodes.resize(survivors);
    reach._candidateEdges.resize(survivors);
}

void PathExplorator::clearReachFrames() {
    Reachability& reach = _reach;
    reach._frames.clear();
    reach._frameWords.clear();
    reach._candidateNodes.clear();
    reach._candidateEdges.clear();
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
    const std::span<const uint64_t> edgeTypeWords = _edgeTypeWords;

    _candidateChecks += edges.size();

    for (const EdgeRecord& record : edges) {
        const bool wrongType = _filterByType && !edgeTypeMatches(edgeTypes, edgeTypeWords, record._edgeTypeID);
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
    reach._closedSeeds = 0;
    reach._batchActive = false;
}
