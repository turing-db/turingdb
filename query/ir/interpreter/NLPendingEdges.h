#pragma once

#include <stddef.h>
#include <stdint.h>

#include <optional>
#include <span>
#include <unordered_map>
#include <vector>

#include "columns/ColumnEdgeTypes.h"
#include "columns/ColumnIDs.h"
#include "columns/ColumnVector.h"
#include "versioning/CommitWriteBuffer.h"

#include "ID.h"

namespace db {

class NLEdgeLoopData;
class NLExecutionContext;

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

    std::span<const size_t> outOf(NodeID node) const;
    std::span<const size_t> into(NodeID node) const;

private:
    using Edges = std::unordered_map<uint64_t, Offsets>;

    Edges _outgoing;
    Edges _incoming;

    size_t _indexedEdges {0};

    static std::span<const size_t> lookup(const Edges& edges, NodeID node);
};

// The step of a hop that walks the edges this change has written and not committed. The
// graph holds none of them until the commit, so a hop reading only the graph would miss
// every edge the query itself wrote. It fills the same chunks the committed step fills and
// runs once that one is drained, so the rows of a hop are the graph's edges then this
// change's.
class NLPendingEdges {
public:
    enum class Direction {
        Out,
        In,
        Either,
    };

    NLPendingEdges(NLExecutionContext* context,
                   NLEdgeLoopData* loopData,
                   Direction direction,
                   ColumnNodeIDs* others);
    ~NLPendingEdges();

    void setEdgeType(EdgeTypeID edgeType) { _edgeType = edgeType; }

    bool isValid() const { return _row < _inputNodeIDs->size(); }

    void fill(size_t maxCount);

private:
    const CommitWriteBuffer* _writeBuffer {nullptr};
    const NLPendingEdgeIndex* _index {nullptr};
    const ColumnNodeIDs* _inputNodeIDs {nullptr};

    ColumnVector<size_t>* _indices {nullptr};
    ColumnEdgeIDs* _edgeIDs {nullptr};
    ColumnEdgeTypes* _types {nullptr};
    ColumnNodeIDs* _others {nullptr};

    // Where this change's provisional IDs start, which is what turns a write-buffer offset
    // into the ID the entity will commit as
    size_t _firstPendingNodeID {0};
    size_t _firstPendingEdgeID {0};

    // What the buffer held when the hop started. A create in the hop's own body writes
    // past this, and what the hop walks is what the query wrote before it ran.
    size_t _pendingEdgeCount {0};

    Direction _direction {Direction::Out};
    std::optional<EdgeTypeID> _edgeType;

    // The input row the walk is on, its edges, how far into them it has read, and - for a
    // hop that walks either way - whether these are the edges into the row's node rather
    // than the ones out of it
    size_t _row {0};
    std::span<const size_t> _offsets;
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

}
