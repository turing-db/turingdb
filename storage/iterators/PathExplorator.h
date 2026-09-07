#pragma once

#include <span>
#include <stdint.h>
#include <stddef.h>
#include <vector>

#include "ChunkWriter.h"
#include "PathExplorationDir.h"
#include "columns/ColumnIDs.h"
#include "columns/ColumnVector.h"
#include "datapart/EdgeRecord.h"
#include "views/GraphView.h"
#include "ID.h"

namespace db {

class EdgeIndexer;
class PathHopFilter;
class PathTrie;
class Tombstones;

// Enumerates every trail of minHops to maxHops edges leaving each input node, depth first,
// as a chunk writer: each fill emits up to maxCount rows of (input row, end node, path).
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
    void setWalkerCount(size_t walkerCount);
    void setCandidateLookahead(size_t lookahead) { _lookahead = lookahead; }

    void reset();
    void fill(size_t maxCount);
    bool isValid() const { return _valid; }

private:
    enum class Stage : uint8_t {
        Idle,
        RangeRequested,
        SpanRequested,
    };

    struct PartAdjacency {
        NodeID _firstNodeID;
        const EdgeIndexer* _indexer {nullptr};
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
    size_t _lookahead {1};

    std::vector<PartAdjacency> _parts;
    std::vector<NodeID> _partFirstNodeIDs;
    std::vector<size_t> _patchPartIndices;
    const Tombstones* _tombstones {nullptr};
    bool _filterTombstones {false};

    std::vector<Walker> _walkers;
    size_t _turn {0};
    size_t _activeWalkers {0};
    size_t _seedCursor {0};
    size_t _written {0};
    bool _valid {false};

    void buildPartDirectory();
    size_t ownerPartIndex(NodeID node) const;
    void prefetchNodeData(NodeID node, size_t partIndex) const;
    bool hasWork() const;

    void startSeed(Walker& walker, size_t row);
    void advance(Walker& walker);
    void consume(Walker& walker);
    void popFrame(Walker& walker);
    void requestDescent(Walker& walker, NodeID node);
    void readRanges(Walker& walker);
    void pushFrame(Walker& walker);
    void generateCandidates(Walker& walker, std::span<const EdgeRecord> edges);
    void emit(size_t seedRow, NodeID target, PathRef path);
};

static_assert(NonRootChunkWriter<PathExplorator>);

}
