#pragma once

#include <stddef.h>
#include <stdint.h>

#include <optional>
#include <unordered_map>
#include <vector>

#include "columns/ColumnEdgeTypes.h"
#include "columns/ColumnIDs.h"
#include "columns/ColumnVector.h"
#include "metadata/LabelSetHandle.h"
#include "versioning/CommitWriteBuffer.h"
#include "views/GraphView.h"

#include "ID.h"

namespace db {

class NLEdgeLoopData;
class NLExecutionContext;
class NLScanEdgesLoopData;

// The edges this change has written, under the node each hangs off. A hop reads the graph
// by the node it walks from, and the write buffer is a flat log, so it is indexed here
// rather than walked once per row.
class NLPendingEdgeIndex {
public:
    using Offsets = std::vector<size_t>;

    NLPendingEdgeIndex();
    ~NLPendingEdgeIndex();

    // Takes in every edge the buffer has gathered since the last call. It only ever
    // appends while a program runs, so what is indexed stays indexed.
    void indexEdges(const CommitWriteBuffer* writeBuffer, size_t firstPendingNodeID);

    // The offsets of the edges hanging off @param node, null when it holds none. A later
    // indexEdges() appends to that vector, which moves what a span into it would point at
    const Offsets* outOf(NodeID node) const;
    const Offsets* into(NodeID node) const;

private:
    using Edges = std::unordered_map<uint64_t, Offsets>;

    Edges _outgoing;
    Edges _incoming;

    size_t _indexedEdges {0};

    static const Offsets* lookup(const Edges& edges, NodeID node);
};

// The step of a hop that walks the edges this change has written and not committed. The
// graph holds none of them until the commit, so a hop reading only the graph would miss
// every edge the query itself wrote. It fills the same chunks the committed step fills and
// runs once that one is drained, so the rows of a hop are the graph's edges then this
// change's.
class NLPendingEdgeHop {
public:
    enum class Direction {
        Out,
        In,
        Either,
    };

    NLPendingEdgeHop(NLExecutionContext* context,
                     NLEdgeLoopData* loopData,
                     Direction direction,
                     ColumnNodeIDs* others);
    ~NLPendingEdgeHop();

    void setEdgeType(EdgeTypeID edgeType) { _edgeType = edgeType; }

    // The label set the endpoint the hop reaches must carry at least for it to walk the
    // edge - the target of an out-hop, the source of an in-hop. The label set is borrowed,
    // so it must outlive the hop.
    void setEndpointLabelSet(const LabelSet& labelset);

    bool isValid() const { return _row < _inputNodeIDs->size(); }

    void fill(size_t maxCount);

private:
    const CommitWriteBuffer* _writeBuffer {nullptr};
    const NLPendingEdgeIndex* _index {nullptr};
    const GraphView* _view {nullptr};
    const ColumnNodeIDs* _inputNodeIDs {nullptr};

    ColumnVector<size_t>* _indices {nullptr};
    ColumnEdgeIDs* _edgeIDs {nullptr};
    ColumnEdgeTypes* _types {nullptr};
    ColumnNodeIDs* _others {nullptr};

    // Where this change's provisional IDs start, which is what turns a write-buffer offset
    // into the ID the entity will commit as
    size_t _firstPendingNodeID {0};
    size_t _firstPendingEdgeID {0};

    // The range of the buffer the hop walks: what an earlier statement of the change staged
    // is below it and is read once it commits, and a create in the hop's own body writes
    // past it, so what the hop walks is what its own query wrote before it ran.
    size_t _firstQueryEdge {0};
    size_t _pendingEdgeCount {0};

    Direction _direction {Direction::Out};
    std::optional<EdgeTypeID> _edgeType;
    LabelSetHandle _endpointLabels;

    // The input row the walk is on, its edges, how far into them it has read, and - for a
    // hop that walks either way - whether these are the edges into the row's node rather
    // than the ones out of it
    size_t _row {0};
    const NLPendingEdgeIndex::Offsets* _offsets {nullptr};
    size_t _position {0};
    bool _incoming {false};

    bool walksIn() const;

    void clearChunks();

    void beginRun();
    void nextRun();

    // Whether the hop keeps the pending edge at @param offset, and the node at its other
    // end when it does
    bool walks(size_t offset, NodeID& other) const;
};

// The step of an edge scan that reads the edges this change has written, the scan sibling
// of NLPendingEdgeHop: it walks the write buffer in order rather than off a chunk of input
// nodes, and fills the same four chunks the committed step fills.
class NLPendingEdgeScan {
public:
    NLPendingEdgeScan(NLExecutionContext* context, NLScanEdgesLoopData* loopData);
    ~NLPendingEdgeScan();

    void setEdgeType(EdgeTypeID edgeType) { _edgeType = edgeType; }

    // The label set one endpoint of the edge must carry at least for the scan to keep it -
    // its source for an out-edge scan, its target for an in-edge scan. The label set is
    // borrowed, so it must outlive the scan.
    void setSourceLabelSet(const LabelSet& labelset);
    void setTargetLabelSet(const LabelSet& labelset);

    bool isValid() const { return _edge < _pendingEdgeCount; }

    void fill(size_t maxCount);

private:
    const CommitWriteBuffer* _writeBuffer {nullptr};
    const GraphView* _view {nullptr};

    ColumnNodeIDs* _srcs {nullptr};
    ColumnEdgeIDs* _edgeIDs {nullptr};
    ColumnEdgeTypes* _types {nullptr};
    ColumnNodeIDs* _tgts {nullptr};

    size_t _firstPendingNodeID {0};
    size_t _firstPendingEdgeID {0};

    size_t _pendingEdgeCount {0};
    size_t _edge {0};

    std::optional<EdgeTypeID> _edgeType;
    LabelSetHandle _endpointLabels;
    bool _labelsTheTarget {false};

    void clearChunks();

    bool keeps(const CommitWriteBuffer::PendingEdge& edge) const;
};

}
