#pragma once

#include <span>
#include <stdint.h>
#include <stddef.h>
#include <limits.h>
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
// With end labels, end nodes or an end node set only the paths ending on a node carrying
// them, on the seed's own target, or on a node of the set are emitted, and with a distance
// or target index set the prefixes that cannot reach such a node in time are not walked. In
// the distinct mode the walk is a multi-source breadth-first search instead, emitting each
// (seed, end) pair once and no path.
class PathExplorator {
public:
    static constexpr size_t NO_TAINT = SIZE_MAX;

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
    void setEdgeTypeFilter(std::span<const EdgeTypeID> edgeTypes);
    void setEndLabels(const LabelSet* labels);
    void setEndNodes(const ColumnNodeIDs* endNodes) { _endNodes = endNodes; }
    // The ends every seed shares, sorted and without duplicates
    void setEndNodeSet(std::span<const NodeID> endNodeSet);
    void setDistanceIndex(const PathDistanceIndex* index) { _distances = index; }
    void setTargetIndex(const PathTargetIndex* index) { _targetIndex = index; }
    // The edges this change has written and not committed, walked beside the graph's own, up
    // to @param edgeIDBound: the index is shared between the walks of one program and only
    // grows, so the bound is what holds this walk to the edges that existed when it started.
    void setPendingAdjacency(const PendingAdjacency* adjacency, size_t edgeIDBound);

    void setDistinctEnds(bool distinct);
    void setCandidateLookahead(size_t lookahead) { _lookahead = lookahead; }

    void reset();
    void fill(size_t maxCount);
    bool isValid() const { return _valid; }

    // How many edge records the walk has examined since the last reset
    size_t getCandidateCheckCount() const { return _candidateChecks; }

private:
    // The candidates of one node on the path, a range of the candidate stacks. _taint is the
    // shallowest path position of an edge this frame's subtree could not take because the walk
    // already held it, which is what decides whether the subtree may be remembered
    struct Frame {
        size_t _candidateBegin {0};
        size_t _candidateEnd {0};
        size_t _next {0};
        NodeID _node;
        uint64_t _budget {0};
        size_t _taint {NO_TAINT};
    };

    // A set of 64-bit keys emptied in constant time: an entry counts only while its stamp
    // matches the generation, which a clear bumps
    struct KeySet {
        std::vector<uint64_t> _keys;
        std::vector<uint32_t> _stamps;
        uint32_t _generation {1};
        size_t _used {0};

        void clear();
        bool insert(uint64_t key);
        bool contains(uint64_t key) const;
        void grow();
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
    std::span<const EdgeTypeID> _edgeTypes;
    LabelSetHandle _endLabels;
    const ColumnNodeIDs* _endNodes {nullptr};
    std::span<const NodeID> _endNodeSet;
    bool _filtersByEndNodeSet {false};
    const PathDistanceIndex* _distances {nullptr};
    const PathTargetIndex* _targetIndex {nullptr};
    const PendingAdjacency* _pendingAdjacency {nullptr};
    size_t _pendingEdgeIDBound {0};
    bool _distinctEnds {false};
    size_t _lookahead {1};

    PartDirectory _parts;
    const Tombstones* _tombstones {nullptr};
    bool _filterTombstones {false};

    Reachability _reach;

    // The depth-first walk leaving the seed of row _seedRow: the edges and nodes it stands
    // on, and the candidates of every node it descended through
    bool _active {false};
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

    // Set while the walk only owes its caller the set of nodes it ends on, which lets a
    // subtree that no held edge constrained stand in for every later arrival at its node
    bool _prunes {false};
    uint64_t _expansionSpan {0};
    bool _keysDepth {false};
    KeySet _emittedEnds;
    KeySet _cleanExpansions;
    size_t _descentTaint {NO_TAINT};

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
    bool canReachTargetWithin(NodeID node, uint64_t hops) const;
    void resizeOutputs(size_t count);

    bool searchesLevels() const;
    uint64_t expansionKey(NodeID node, uint64_t budget) const;

    void startSeed(size_t row);
    void step();
    void popFrame();
    void descend(NodeID node);
    void generateCandidates(std::span<const EdgeRecord> edges);
    void generatePendingCandidates(NodeID node);
    void emit(size_t seedRow, NodeID target, PathRef path);

    void acquireArena();
    void releaseArena();
    void retainWalkedPath();
    void releasePathEntry();

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
