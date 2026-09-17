#pragma once

#include <span>
#include <stdint.h>
#include <stddef.h>
#include <vector>

#include "ChunkWriter.h"
#include "PartDirectory.h"
#include "PathExplorationDir.h"
#include "PathReachTable.h"
#include "PathTargetIndex.h"
#include "versioning/PendingAdjacency.h"
#include "columns/ColumnIDs.h"
#include "columns/ColumnVector.h"
#include "datapart/EdgeRecord.h"
#include "metadata/LabelSetHandle.h"
#include "views/GraphView.h"
#include "ID.h"

namespace db {

class PathDistanceIndex;
class PathHopFilter;
class PathTrie;
class Tombstones;

// Enumerates every trail of minHops to maxHops edges leaving each input node, depth first,
// as a chunk writer: each fill emits up to maxCount rows of (input row, end node, path).
// With end labels or end nodes set only the paths ending on a node carrying them, or on the
// seed's own target, are emitted, and with a distance or target index set the prefixes that
// cannot reach such a node in time are not walked. In the distinct mode the walk is a
// multi-source breadth-first search instead, emitting each (seed, end) pair once and no path.
class PathExplorator {
public:
    PathExplorator(const GraphView& view,
                   const ColumnNodeIDs* inputNodeIDs,
                   PathExplorationDir direction,
                   uint64_t minHops,
                   uint64_t maxHops);
    ~PathExplorator();

    void setIndices(ColumnVector<size_t>* indices) { _indices = indices; }
    void setTargets(ColumnNodeIDs* targets) { _targets = targets; }
    void setPaths(ColumnVector<PathRef>* paths, PathTrie* trie);
    void setHopFilter(PathHopFilter* filter) { _hopFilter = filter; }
    void setEdgeTypeFilter(EdgeTypeID edgeType);
    void setEndLabels(const LabelSet* labels);
    void setEndNodes(const ColumnNodeIDs* endNodes) { _endNodes = endNodes; }
    void setDistanceIndex(const PathDistanceIndex* index) { _distances = index; }
    void setTargetIndex(const PathTargetIndex* index) { _targetIndex = index; }
    // The edges this change has written and not committed, walked beside the graph's own, up
    // to @param edgeIDBound: the index is shared between the walks of one program and only
    // grows, so the bound is what holds this walk to the edges that existed when it started.
    void setPendingAdjacency(const PendingAdjacency* adjacency, size_t edgeIDBound);

    void setDistinctEnds(bool distinct);
    void setWalkerCount(size_t walkerCount);
    void setCandidateLookahead(size_t lookahead) { _lookahead = lookahead; }

    void reset();
    void fill(size_t maxCount);
    bool isValid() const { return _valid; }

    // How many edge records the walk has examined since the last reset
    size_t getCandidateCheckCount() const { return _candidateChecks; }

private:
    enum class Stage : uint8_t {
        Idle,
        RangeRequested,
        SpanRequested,
    };

    // The candidates of one node on the path, a range of the walker's candidate stacks
    struct Frame {
        size_t _candidateBegin {0};
        size_t _candidateEnd {0};
        size_t _next {0};
    };

    // One depth-first walk from one seed; several run interleaved so their memory
    // accesses overlap
    struct Walker {
        bool _active {false};
        Stage _stage {Stage::Idle};
        size_t _seedRow {0};
        size_t _arena {0};
        // The arena's entries below this size are held by rows of the chunk being filled
        size_t _pinned {0};
        NodeID _targetNode;
        PathTargetHandle _target;

        std::vector<EdgeID> _pathEdges;
        std::vector<PathRef> _pathEntries;
        std::vector<uint64_t> _pathSignatures;
        std::vector<Frame> _frames;
        std::vector<NodeID> _candidateNodes;
        std::vector<EdgeID> _candidateEdges;

        NodeID _pendingNode;
        size_t _pendingOwner {0};
        std::span<const EdgeRecord> _pendingOuts;
        std::span<const EdgeRecord> _pendingIns;
    };

    // The multi-source search of the distinct mode: one bit per seed of the current batch in
    // the words of every node it reaches, the rows a level gained emitted before the next
    // level is expanded
    struct Reachability {
        PathReachTable _reached;
        std::vector<NodeID> _frontier;
        std::vector<NodeID> _next;
        std::vector<NodeID> _candidateNodes;
        std::vector<EdgeID> _candidateEdges;
        size_t _batchFirstRow {0};
        uint64_t _level {0};
        size_t _emitNode {0};
        uint64_t _emitBits {0};
        bool _batchActive {false};
    };

    GraphView _view;
    const ColumnNodeIDs* _input {nullptr};
    PathExplorationDir _direction {PathExplorationDir::FORWARD};
    uint64_t _minHops {0};
    uint64_t _maxHops {0};

    ColumnVector<size_t>* _indices {nullptr};
    ColumnNodeIDs* _targets {nullptr};
    ColumnVector<PathRef>* _paths {nullptr};
    PathTrie* _trie {nullptr};
    PathHopFilter* _hopFilter {nullptr};
    bool _filterByType {false};
    EdgeTypeID _edgeType;
    LabelSetHandle _endLabels;
    const ColumnNodeIDs* _endNodes {nullptr};
    const PathDistanceIndex* _distances {nullptr};
    const PathTargetIndex* _targetIndex {nullptr};
    const PendingAdjacency* _pendingAdjacency {nullptr};
    size_t _pendingEdgeIDBound {0};
    bool _distinctEnds {false};
    size_t _lookahead {1};

    PartDirectory _parts;
    const Tombstones* _tombstones {nullptr};
    bool _filterTombstones {false};

    std::vector<Walker> _walkers;
    Reachability _reach;
    size_t _turn {0};
    size_t _activeWalkers {0};
    size_t _seedCursor {0};
    size_t _written {0};
    size_t _candidateChecks {0};
    bool _valid {false};

    // Whether the node is one this change wrote, which no data part holds and whose
    // adjacency and labels are read from the write buffer instead
    bool isPendingNode(NodeID node) const;
    size_t nodeIDBound() const;

    // The labels of a node, wherever it lives: a data part, or this change's buffer
    LabelSetHandle labelSetOf(NodeID node) const;

    void prefetchNodeData(NodeID node, size_t partIndex) const;
    bool hasWork() const;
    bool isEnd(size_t seedRow, NodeID node) const;
    void resizeOutputs(size_t count);

    void startSeed(Walker& walker, size_t row);
    void advance(Walker& walker);
    void consume(Walker& walker);
    void popFrame(Walker& walker);
    void requestDescent(Walker& walker, NodeID node);
    void readRanges(Walker& walker);
    void pushFrame(Walker& walker);
    void generateCandidates(Walker& walker, std::span<const EdgeRecord> edges);
    void generatePendingCandidates(Walker& walker, NodeID node);
    void emit(size_t seedRow, NodeID target, PathRef path);

    void acquireArenas();
    void releaseArenas();
    void retainWalkedPaths();
    void releasePathEntry(Walker& walker);

    void fillDistinct(size_t maxCount);
    void startBatch();
    void emitGainedRows(size_t maxCount);
    void expandLevel();
    void collectReachCandidates(NodeID node);
    void appendReachCandidates(std::span<const EdgeRecord> edges);
    void appendPendingReachCandidates(NodeID node);
    void finishBatch();
};

static_assert(NonRootChunkWriter<PathExplorator>);

}
