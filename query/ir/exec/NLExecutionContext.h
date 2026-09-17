#pragma once

#include <stddef.h>

#include <memory>

namespace fs {
class Path;
}

namespace vec {
class VectorDatabase;
}

namespace db {

class CommitWriteBuffer;
class GraphView;
class NLOutputSink;
class NLPendingEdgeIndex;
class NLSystemContext;
class NLWrittenValues;
class PendingAdjacency;

// What every handler of a running NLProgram reads the world through: the graph it runs
// against, where its rows go, and the change it writes into.
class NLExecutionContext {
public:
    NLExecutionContext(const GraphView* view,
                       NLOutputSink* sink,
                       size_t chunkSize,
                       CommitWriteBuffer* writeBuffer = nullptr,
                       const NLSystemContext* system = nullptr);
    ~NLExecutionContext();

    const GraphView* getView() const { return _view; }
    NLOutputSink* getSink() const { return _sink; }
    size_t getChunkSize() const { return _chunkSize; }
    CommitWriteBuffer* getWriteBuffer() const { return _writeBuffer; }

    // The server-level facilities the system commands reach for. Every query the server
    // runs carries one, ordinary reads included; it is null only for a caller that hands
    // the interpreter none, as the IR tests do.
    const NLSystemContext* getSystem() const { return _system; }

    // The vector indexes a search reads, borrowed from the session's accessor. They are
    // the one store outside the graph an ordinary query reaches, so - unlike the rest of
    // the system context - a dataflow loop reads them; null when the session opened no
    // accessor, which the search reports as a user-facing error.
    vec::VectorDatabase* getVectorDatabase() const;

    // The directory a file the query names is resolved against - the one place outside
    // the graph a dataflow loop reads from. Null for a session that opened no system
    // manager, which the load reports as a user-facing error.
    const fs::Path* getDataDir() const;

    // What the change has written so far, as a read later in the same program sees it
    NLWrittenValues& getWrittenValues() const { return *_writtenValues; }

    // The edges of that, under the node each hangs off, so every hop of a program indexes
    // the write buffer once between them
    NLPendingEdgeIndex& getPendingEdges() const { return *_pendingEdges; }

    // The same edges in the shape a traversal reads a data part in, so a walk nested in
    // another loop indexes the write buffer once for the program rather than once per chunk
    PendingAdjacency& getPendingAdjacency() const { return *_pendingAdjacency; }

    // Where this query's own writes start in the change's buffer. A read sees what its own
    // query wrote and not what an earlier statement of the change staged: that one becomes
    // readable when it commits, as a write of any other change does.
    size_t getFirstQueryNode() const { return _firstQueryNode; }
    size_t getFirstQueryEdge() const { return _firstQueryEdge; }

private:
    const GraphView* _view {nullptr};
    NLOutputSink* _sink {nullptr};
    size_t _chunkSize {0};
    CommitWriteBuffer* _writeBuffer {nullptr};
    const NLSystemContext* _system {nullptr};
    size_t _firstQueryNode {0};
    size_t _firstQueryEdge {0};
    std::unique_ptr<NLWrittenValues> _writtenValues;
    std::unique_ptr<NLPendingEdgeIndex> _pendingEdges;
    std::unique_ptr<PendingAdjacency> _pendingAdjacency;
};

}
