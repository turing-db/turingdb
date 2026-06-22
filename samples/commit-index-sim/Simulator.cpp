#include "Simulator.h"

#include <math.h>
#include <stdint.h>
#include <algorithm>
#include <unordered_set>
#include <vector>

using namespace db;

namespace {

// Deterministic splitmix64: cheap, seedable, no global state. Reproducible runs
// matter more here than statistical perfection.
uint64_t nextRandom(uint64_t& state) {
    state += 0x9E3779B97F4A7C15ull;
    uint64_t z = state;
    z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ull;
    z = (z ^ (z >> 27)) * 0x94D049BB133111EBull;
    return z ^ (z >> 31);
}

double nextUniform(uint64_t& state) {
    return (double)(nextRandom(state) >> 11) * (1.0 / 9007199254740992.0);
}

// Draws a NodeID with a power-law-ish bias toward low ids. `skew` == 1 is
// uniform; `skew` > 1 concentrates draws on the first (hub) nodes, modelling
// preferential attachment where old nodes keep accruing edges.
size_t sampleNode(uint64_t& state, size_t numNodes, double skew) {
    const double uniform = nextUniform(state);
    const double biased = skew == 1.0 ? uniform : pow(uniform, skew);
    size_t node = (size_t)(biased * (double)numNodes);
    if (node >= numNodes) {
        node = numNodes - 1;
    }
    return node;
}

void computeReadStats(std::vector<double>& samples, ReadStats& out) {
    if (samples.empty()) {
        return;
    }

    std::sort(samples.begin(), samples.end());

    double sum = 0.0;
    for (const double value : samples) {
        sum += value;
    }

    const size_t count = samples.size();
    const size_t medianIndex = count / 2;
    const size_t p99Index = (count * 99) / 100 >= count ? count - 1 : (count * 99) / 100;

    out._meanNs = sum / (double)count;
    out._medianNs = samples[medianIndex];
    out._p99Ns = samples[p99Index];
}

}

double LatencyModel::cacheLatencyForBytes(size_t bytes) const {
    if (bytes <= _l1Bytes) {
        return _l1Ns;
    } else if (bytes <= _l2Bytes) {
        return _l2Ns;
    } else if (bytes <= _l3Bytes) {
        return _l3Ns;
    } else {
        return _dramNs;
    }
}

void PageTableConfig::compute(size_t numNodes, const LatencyModel& model) {
    // Bits required to address the populated id space.
    size_t bits = 1;
    while ((size_t(1) << bits) < numNodes && bits < 63) {
        bits++;
    }
    _effectiveBits = bits;

    // The root cannot resolve more bits than the id space has.
    size_t level1 = _level1Bits;
    if (level1 > _effectiveBits) {
        level1 = _effectiveBits;
    }

    const size_t remaining = _effectiveBits - level1;
    size_t lowerLevels = 0;
    if (remaining > 0) {
        lowerLevels = (remaining + _pageBits - 1) / _pageBits;
    }
    _levels = 1 + lowerLevels;

    // _pagePrefixShift[d]: shift applied to a NodeID to identify which page is
    // visited at depth d. Depth 0 is the single root directory, so its shift is
    // the full width and every node maps to selector 0.
    _pagePrefixShift.clear();
    _pagePrefixShift.reserve(_levels);

    size_t consumed = 0;  // index bits resolved by the depths above this one
    for (size_t depth = 0; depth < _levels; depth++) {
        _pagePrefixShift.push_back(_effectiveBits - consumed);

        if (depth == 0) {
            consumed += level1;
        } else {
            consumed += _pageBits;
        }

        if (consumed > _effectiveBits) {
            consumed = _effectiveBits;
        }
    }

    _rootBytes = (size_t(1) << level1) * model._pointerBytes;
    _innerPageBytes = (size_t(1) << _pageBits) * model._pointerBytes;
}

CommitIndexSimulator::CommitIndexSimulator(const LatencyModel& model,
                                           const Workload& workload,
                                           const PageTableConfig& pageTable)
    : _model(model)
    , _workload(workload)
    , _pageTable(pageTable)
{
}

CommitIndexSimulator::~CommitIndexSimulator() {
}

void CommitIndexSimulator::run() {
    const size_t numNodes = _workload._numNodes;

    _coreDegree.assign(numNodes, 0);
    _patchDegree.assign(numNodes, 0);
    _touchCount.assign(numNodes, 0);
    _lastTouchCommit.assign(numNodes, UINT32_MAX);

    uint64_t rngState = _workload._seed;

    bulkLoad(rngState);
    simulateMutations(rngState);
    computeGraphStats();
    measureReads(rngState);
    computeIndexSpace();
}

void CommitIndexSimulator::bulkLoad(uint64_t& rngState) {
    const size_t numNodes = _workload._numNodes;
    const size_t loadEdges = numNodes * _workload._avgInitialDegree;

    for (size_t edge = 0; edge < loadEdges; edge++) {
        const size_t source = sampleNode(rngState, numNodes, _workload._skew);
        _coreDegree[source] += 1;
    }

    _results._totalEdges += loadEdges;
}

void CommitIndexSimulator::simulateMutations(uint64_t& rngState) {
    const size_t numNodes = _workload._numNodes;
    const size_t mutationCommits = _workload._mutationCommits;
    const size_t edgesPerCommit = _workload._avgEdgesPerCommit;
    const size_t levels = _pageTable._levels;

    // Per-commit scratch, cleared and reused each iteration.
    std::vector<size_t> touchedNodes;
    std::vector<size_t> touchedPreDegree;
    std::vector<std::unordered_set<uint64_t>> pageSets(levels);

    double sumCurrentNs = 0.0;
    double maxCurrentNs = 0.0;
    double sumDeltaNs = 0.0;
    double maxDeltaNs = 0.0;
    double sumConsolidatedNs = 0.0;
    double maxConsolidatedNs = 0.0;
    double sumRewrittenPages = 0.0;
    double sumDeltaBytes = 0.0;
    double sumConsolidatedBytes = 0.0;
    double sumCurrentIndexBytes = 0.0;

    const double logEdges = log2((double)(edgesPerCommit < 2 ? 2 : edgesPerCommit));
    const double buildNs = (double)edgesPerCommit * _model._edgePlaceNs
                         + (double)edgesPerCommit * logEdges * _model._compareNs;

    for (size_t commit = 0; commit < mutationCommits; commit++) {
        touchedNodes.clear();
        touchedPreDegree.clear();
        for (size_t depth = 0; depth < levels; depth++) {
            pageSets[depth].clear();
        }

        for (size_t edge = 0; edge < edgesPerCommit; edge++) {
            const size_t source = sampleNode(rngState, numNodes, _workload._skew);

            const bool firstTouchThisCommit = _lastTouchCommit[source] != (uint32_t)commit;
            if (firstTouchThisCommit) {
                _lastTouchCommit[source] = (uint32_t)commit;
                _touchCount[source] += 1;

                const size_t preDegree = _coreDegree[source] + _patchDegree[source];
                touchedNodes.push_back(source);
                touchedPreDegree.push_back(preDegree);

                for (size_t depth = 0; depth < levels; depth++) {
                    const size_t shift = _pageTable._pagePrefixShift[depth];
                    const uint64_t selector = shift >= 64 ? 0 : ((uint64_t)source >> shift);
                    pageSets[depth].insert(selector);
                }
            }

            _patchDegree[source] += 1;
        }

        const size_t distinctNodes = touchedNodes.size();

        // Current design: build the new datapart and its patch-node hash map.
        // No existing datapart is touched.
        const double currentNs = buildNs + (double)distinctNodes * _model._hashInsertNs;

        // Path copying rewrites every dirty page from each touched leaf up to the
        // shared root. Distinct pages per depth = distinct NodeID prefixes.
        size_t rewrittenPages = 0;
        double directoryBytes = 0.0;
        for (size_t depth = 0; depth < levels; depth++) {
            const size_t pages = pageSets[depth].size();
            rewrittenPages += pages;

            const size_t pageBytes = depth == 0 ? _pageTable._rootBytes : _pageTable._innerPageBytes;
            directoryBytes += (double)pages * (double)pageBytes;
        }

        const double pageCopyNs = directoryBytes / _model._bandwidthBytesPerNs
                                + (double)rewrittenPages * _model._allocNs;

        // Delta variant: append a small per-commit neighborhood delta and rewrite
        // only the directory pages on the path to the new root.
        const double deltaNs = buildNs + pageCopyNs + (double)distinctNodes * _model._edgePlaceNs;

        // Consolidated variant: each touched leaf must hold the full, merged
        // neighborhood, so the node's existing adjacency is re-copied before the
        // new edges are appended. That re-copy is the write amplification.
        double recopyEdges = 0.0;
        for (const size_t preDegree : touchedPreDegree) {
            recopyEdges += (double)preDegree;
        }

        const double recopyNs = recopyEdges * (double)_model._edgeRecordBytes / _model._bandwidthBytesPerNs
                              + (double)distinctNodes * _model._dramNs;
        const double consolidatedNs = buildNs + pageCopyNs + recopyNs;

        sumCurrentNs += currentNs;
        sumDeltaNs += deltaNs;
        sumConsolidatedNs += consolidatedNs;
        maxCurrentNs = std::max(maxCurrentNs, currentNs);
        maxDeltaNs = std::max(maxDeltaNs, deltaNs);
        maxConsolidatedNs = std::max(maxConsolidatedNs, consolidatedNs);

        sumRewrittenPages += (double)rewrittenPages;
        sumDeltaBytes += directoryBytes;
        sumConsolidatedBytes += directoryBytes + recopyEdges * (double)_model._edgeRecordBytes;

        // Current design retains, per mutation datapart, a patch-node hash map
        // plus a NodeEdgeData entry for each patched node.
        sumCurrentIndexBytes +=
            (double)distinctNodes * (double)(_model._hashEntryBytes + _model._nodeEdgeDataBytes);
    }

    _results._totalEdges += mutationCommits * edgesPerCommit;

    const double commits = mutationCommits == 0 ? 1.0 : (double)mutationCommits;

    _results._currentWrite._meanNsPerCommit = sumCurrentNs / commits;
    _results._currentWrite._maxNsPerCommit = maxCurrentNs;

    _results._pageTableDeltaWrite._meanNsPerCommit = sumDeltaNs / commits;
    _results._pageTableDeltaWrite._maxNsPerCommit = maxDeltaNs;
    _results._pageTableDeltaWrite._meanRewrittenPages = sumRewrittenPages / commits;
    _results._pageTableDeltaWrite._meanBytesPerCommit = sumDeltaBytes / commits;

    _results._pageTableConsolidatedWrite._meanNsPerCommit = sumConsolidatedNs / commits;
    _results._pageTableConsolidatedWrite._maxNsPerCommit = maxConsolidatedNs;
    _results._pageTableConsolidatedWrite._meanRewrittenPages = sumRewrittenPages / commits;
    _results._pageTableConsolidatedWrite._meanBytesPerCommit = sumConsolidatedBytes / commits;

    // Patch structures retained across the whole mutation history; the load
    // datapart's core index is added in computeIndexSpace().
    _results._currentIndexBytes = sumCurrentIndexBytes;
    _results._currentWrite._meanBytesPerCommit = sumCurrentIndexBytes / commits;
    _results._pageTableConsolidatedIndexBytes = sumConsolidatedBytes;
}

void CommitIndexSimulator::computeGraphStats() {
    const size_t numNodes = _workload._numNodes;

    size_t maxDegree = 0;
    size_t maxTouchCount = 0;
    double sumDegree = 0.0;
    double sumTouchCount = 0.0;

    for (size_t node = 0; node < numNodes; node++) {
        const size_t degree = _coreDegree[node] + _patchDegree[node];
        sumDegree += (double)degree;
        maxDegree = std::max(maxDegree, degree);

        const size_t touches = _touchCount[node];
        sumTouchCount += (double)touches;
        maxTouchCount = std::max(maxTouchCount, touches);
    }

    _results._maxDegree = maxDegree;
    _results._avgDegree = sumDegree / (double)numNodes;
    _results._maxTouchCount = maxTouchCount;
    _results._avgTouchCount = sumTouchCount / (double)numNodes;
}

void CommitIndexSimulator::measureReads(uint64_t& rngState) {
    const size_t numNodes = _workload._numNodes;
    const size_t samples = _workload._readSamples;

    for (size_t mixIndex = 0; mixIndex < 2; mixIndex++) {
        const double skew = mixIndex == (size_t)ReadMix::Hot ? _workload._skew : 1.0;

        std::vector<double> currentSamples;
        std::vector<double> consolidatedSamples;
        std::vector<double> deltaSamples;
        currentSamples.reserve(samples);
        consolidatedSamples.reserve(samples);
        deltaSamples.reserve(samples);

        for (size_t sample = 0; sample < samples; sample++) {
            const size_t node = sampleNode(rngState, numNodes, skew);
            const size_t touches = _touchCount[node];
            const size_t degree = _coreDegree[node] + _patchDegree[node];
            const bool hasCoreEdges = _coreDegree[node] > 0;

            currentSamples.push_back(currentReadNs(touches, degree, hasCoreEdges));
            consolidatedSamples.push_back(pageTableReadNs(true, touches, degree, hasCoreEdges));
            deltaSamples.push_back(pageTableReadNs(false, touches, degree, hasCoreEdges));
        }

        computeReadStats(currentSamples, _results._currentRead[mixIndex]);
        computeReadStats(consolidatedSamples, _results._pageTableConsolidatedRead[mixIndex]);
        computeReadStats(deltaSamples, _results._pageTableDeltaRead[mixIndex]);
    }
}

void CommitIndexSimulator::computeIndexSpace() {
    const size_t numNodes = _workload._numNodes;
    const size_t levels = _pageTable._levels;

    double initialBytes = 0.0;
    for (size_t depth = 0; depth < levels; depth++) {
        const size_t shift = _pageTable._pagePrefixShift[depth];
        const size_t pages = shift >= 64 ? 1 : (((numNodes - 1) >> shift) + 1);
        const size_t pageBytes = depth == 0 ? _pageTable._rootBytes : _pageTable._innerPageBytes;
        initialBytes += (double)pages * (double)pageBytes;
    }

    _results._initialIndexBytes = initialBytes;
    _results._projectedDeltaGrowthBytes =
        _results._pageTableDeltaWrite._meanBytesPerCommit * (double)_workload._mutationCommits;

    // Total index memory once the full commit history is retained. The current
    // design adds the load datapart's core NodeEdgeData array on top of the
    // accumulated patch structures; the page-table totals add the shared root
    // and inner pages of the initial tree to the path-copied growth.
    _results._currentIndexBytes += (double)numNodes * (double)_model._nodeEdgeDataBytes;
    _results._pageTableDeltaIndexBytes = initialBytes + _results._projectedDeltaGrowthBytes;
    _results._pageTableConsolidatedIndexBytes += initialBytes;
}

double CommitIndexSimulator::currentReadNs(size_t touchCount, size_t degree, bool hasCoreEdges) const {
    const size_t mutations = _workload._mutationCommits;
    const size_t hits = touchCount > mutations ? mutations : touchCount;
    const size_t misses = mutations - hits;

    // The owning load datapart resolves the node by direct array index (one
    // NodeEdgeData read); any other load dataparts are cheap out-of-range checks.
    double ns = _model._dramNs;
    if (_workload._loadDataparts > 1) {
        ns += (double)(_workload._loadDataparts - 1) * _model._boundsCheckNs;
    }

    // Every mutation datapart is probed once, whether or not it holds a patch.
    ns += (double)hits * _model._hashProbeHitNs;
    ns += (double)misses * _model._hashProbeMissNs;

    // Gather the edges: one disjoint span per source (load core + each patching
    // commit), each starting with a first-touch miss, then sequential bandwidth.
    const size_t spans = (hasCoreEdges ? 1 : 0) + hits;
    ns += (double)spans * _model._dramNs;
    ns += (double)(degree * _model._edgeRecordBytes) / _model._bandwidthBytesPerNs;

    return ns;
}

double CommitIndexSimulator::pageTableWalkNs() const {
    double ns = _model.cacheLatencyForBytes(_pageTable._rootBytes);
    if (_pageTable._levels > 1) {
        ns += (double)(_pageTable._levels - 1) * _model._dramNs;
    }
    return ns;
}

double CommitIndexSimulator::pageTableReadNs(bool consolidated,
                                             size_t touchCount,
                                             size_t degree,
                                             bool hasCoreEdges) const {
    const size_t mutations = _workload._mutationCommits;
    const size_t hits = touchCount > mutations ? mutations : touchCount;

    double ns = pageTableWalkNs();

    // The walk lands directly on the node's entry: no per-datapart probing. The
    // consolidated leaf yields a single neighborhood span; the delta leaf still
    // chains one span per patching commit, but pays no miss probes.
    size_t spans = 0;
    if (consolidated) {
        spans = 1;
    } else {
        spans = (hasCoreEdges ? 1 : 0) + hits;
        if (spans == 0) {
            spans = 1;
        }
    }

    ns += (double)spans * _model._dramNs;
    ns += (double)(degree * _model._edgeRecordBytes) / _model._bandwidthBytesPerNs;

    return ns;
}
