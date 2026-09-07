#pragma once

#include <span>
#include <stdint.h>
#include <stddef.h>
#include <vector>

#include "ChunkWriter.h"
#include "PartDirectory.h"
#include "PathExplorationDir.h"
#include "PathTargetIndex.h"
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
    void setDistinctEnds(bool distinct);
    void setWalkerCount(size_t walkerCount);
    void setCandidateLookahead(size_t lookahead) { _lookahead = lookahead; }

    void reset();
    void fill(size_t maxCount);
    bool isValid() const { return _valid; }

    // How many edge records the walk has examined since the last reset
    size_t getCandidateCheckCount() const { return _candidateChecks; }

    // Whether the distinct mode's search is expected to beat the walk on this graph: only
    // once the balls of a batch of seeds overlap enough to be walked together
    static bool searchPaysForDistinctEnds(const GraphView& view, PathExplorationDir direction, uint64_t maxHops);

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

    // One node's words in the multi-source search: the seeds that have reached it, those
    // reaching it at the level being expanded, and those it gained at the level just closed.
    // The batch that wrote them dates them: words of an earlier batch read as empty, so no
    // batch clears what it touched.
    struct ReachWords {
        uint64_t _seen {0};
        uint64_t _frontier {0};
        uint64_t _gained {0};
        uint64_t _batch {0};
    };

    // The multi-source search of the distinct mode: one bit per seed of the current batch in
    // the words of every node, the rows a level gained emitted before the next level is
    // expanded
    struct Reachability {
        std::vector<ReachWords> _words;
        std::vector<NodeID> _frontier;
        std::vector<NodeID> _next;
        std::vector<NodeID> _candidateNodes;
        std::vector<EdgeID> _candidateEdges;
        size_t _batchFirstRow {0};
        uint64_t _batch {0};
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
    void emit(size_t seedRow, NodeID target, PathRef path);

    void fillDistinct(size_t maxCount);
    ReachWords& wordsOf(NodeID node);
    void startBatch();
    void emitGainedRows(size_t maxCount);
    void expandLevel();
    void collectReachCandidates(NodeID node);
    void appendReachCandidates(std::span<const EdgeRecord> edges);
    void finishBatch();
};

static_assert(NonRootChunkWriter<PathExplorator>);

}
