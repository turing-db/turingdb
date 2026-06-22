#pragma once

#include <stddef.h>
#include <stdint.h>
#include <vector>

namespace db {

// Approximate per-operation latencies (nanoseconds) and memory characteristics
// driving the cost model. Defaults model one core of a modern server. They are
// deliberately coarse: the goal is relative comparison between designs, not an
// absolute prediction. The most impactful constants are overridable on the CLI.
struct LatencyModel {
    double _l1Ns {1.0};
    double _l2Ns {4.0};
    double _l3Ns {14.0};
    double _dramNs {90.0};              // cold load that misses all caches

    double _hashProbeHitNs {160.0};     // populated unordered_map probe (~2 misses)
    double _hashProbeMissNs {95.0};     // probe that misses (~1 miss)
    double _boundsCheckNs {2.0};        // hot in-range / out-of-range branch

    double _bandwidthBytesPerNs {12.0}; // sequential read bandwidth, single core
    double _allocNs {30.0};             // allocate + link one fresh index page
    double _hashInsertNs {120.0};       // insert one entry into a patch map
    double _edgePlaceNs {6.0};          // place one edge while building a datapart
    double _compareNs {2.0};            // one comparison in the build-time sort

    size_t _edgeRecordBytes {32};       // EdgeID + NodeID + NodeID + EdgeTypeID
    size_t _nodeEdgeDataBytes {32};     // out/in edge ranges per node in a datapart
    size_t _hashEntryBytes {48};        // one patch-node entry in an unordered_map
    size_t _pointerBytes {8};
    size_t _l1Bytes {32 * 1024};
    size_t _l2Bytes {1024 * 1024};
    size_t _l3Bytes {32 * 1024 * 1024};

    double cacheLatencyForBytes(size_t bytes) const;
};

// The workload whose cost we estimate: an initial bulk load that creates every
// node, followed by a long tail of small mutation commits, each adding edges to
// already-existing nodes (the "patch edge" case that the current design pays for
// on every reachable datapart).
struct Workload {
    size_t _numNodes {1000000};
    size_t _avgInitialDegree {8};       // out-edges per node from the bulk load
    size_t _loadDataparts {1};          // dataparts produced by the bulk load
    size_t _mutationCommits {2000};     // small commits appended afterwards
    size_t _avgEdgesPerCommit {500};    // edges added by each mutation commit
    double _skew {1.6};                 // >1 concentrates edges on hub nodes
    size_t _readSamples {100000};
    uint64_t _seed {42};
};

// Geometry of the radix / page-table index. The top `_level1Bits` of a NodeID
// index the root directory; each deeper level consumes `_pageBits` more bits
// until the populated id-space is fully resolved at the leaves.
struct PageTableConfig {
    size_t _level1Bits {16};
    size_t _pageBits {8};

    // Derived once the populated id-space width is known.
    size_t _effectiveBits {0};
    size_t _levels {0};
    std::vector<size_t> _pagePrefixShift;   // shift identifying a page at depth d
    size_t _rootBytes {0};
    size_t _innerPageBytes {0};

    void compute(size_t numNodes, const LatencyModel& model);
};

struct ReadStats {
    double _meanNs {0.0};
    double _medianNs {0.0};
    double _p99Ns {0.0};
};

struct WriteStats {
    double _meanNsPerCommit {0.0};
    double _maxNsPerCommit {0.0};
    double _meanRewrittenPages {0.0};   // page-table designs only
    double _meanBytesPerCommit {0.0};   // page-table designs only: persistent growth
};

// Read mixes: index 0 reads hub nodes proportionally to how often they are
// touched (the realistic hot path); index 1 reads nodes uniformly at random.
enum class ReadMix : size_t {
    Hot = 0,
    Uniform = 1,
};

struct SimResults {
    size_t _totalEdges {0};
    size_t _maxDegree {0};
    double _avgDegree {0.0};
    size_t _maxTouchCount {0};
    double _avgTouchCount {0.0};

    ReadStats _currentRead[2];
    ReadStats _pageTableConsolidatedRead[2];
    ReadStats _pageTableDeltaRead[2];

    WriteStats _currentWrite;
    WriteStats _pageTableConsolidatedWrite;
    WriteStats _pageTableDeltaWrite;

    double _initialIndexBytes {0.0};
    double _projectedDeltaGrowthBytes {0.0};

    // Total index-structure memory once the whole commit history is retained.
    double _currentIndexBytes {0.0};
    double _pageTableDeltaIndexBytes {0.0};
    double _pageTableConsolidatedIndexBytes {0.0};
};

// Estimates the per-operation cost of the current "probe every reachable
// datapart" design against a per-commit page-table directory of node
// neighborhoods, in two variants (consolidated leaves vs. per-commit deltas).
class CommitIndexSimulator {
public:
    CommitIndexSimulator(const LatencyModel& model,
                         const Workload& workload,
                         const PageTableConfig& pageTable);
    ~CommitIndexSimulator();

    void run();

    const SimResults& getResults() const { return _results; }

private:
    const LatencyModel& _model;
    const Workload& _workload;
    const PageTableConfig& _pageTable;
    SimResults _results;

    // Per-node accumulated state (indexed by NodeID, size = _numNodes).
    std::vector<uint32_t> _coreDegree;        // out-edges from the bulk load
    std::vector<uint32_t> _patchDegree;       // out-edges added by mutations
    std::vector<uint32_t> _touchCount;        // distinct commits that patched the node
    std::vector<uint32_t> _lastTouchCommit;   // de-dup marker within one commit

    void bulkLoad(uint64_t& rngState);
    void simulateMutations(uint64_t& rngState);
    void computeGraphStats();
    void measureReads(uint64_t& rngState);
    void computeIndexSpace();

    // Per-operation cost model.
    double currentReadNs(size_t touchCount, size_t degree, bool hasCoreEdges) const;
    double pageTableReadNs(bool consolidated,
                           size_t touchCount,
                           size_t degree,
                           bool hasCoreEdges) const;
    double pageTableWalkNs() const;
};

}
