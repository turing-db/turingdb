#pragma once

#include <stddef.h>

#include "metadata/PropertyType.h"

#include "NLProgram.h"

namespace db {

class GraphView;
class NLOutputSink;

class NLExecutionContext {
public:
    NLExecutionContext(const GraphView* view,
                       NLOutputSink* sink,
                       size_t chunkSize)
        : _view(view),
        _sink(sink),
        _chunkSize(chunkSize)
    {
    }

    const GraphView* getView() const { return _view; }
    NLOutputSink* getSink() const { return _sink; }
    size_t getChunkSize() const { return _chunkSize; }

private:
    const GraphView* _view {nullptr};
    NLOutputSink* _sink {nullptr};
    size_t _chunkSize {0};
};

// Executes a translated NLProgram against a graph view
class NLExecutor {
public:
    NLExecutor(const GraphView* view,
                  const NLProgram* prog,
                  NLOutputSink* sink);
    ~NLExecutor();

    void run();

    static void runScanNodesLoop(NLExecutionContext* context, NLFunctionData* data);
    static void runGetOutEdgesLoop(NLExecutionContext* context, NLFunctionData* data);
    static void runGetInEdgesLoop(NLExecutionContext* context, NLFunctionData* data);
    static void runCrossProduct(NLExecutionContext* context, NLFunctionData* data);

    // Reset a limit counter to its budget; runs each time its block runs.
    static void runLimitInit(NLExecutionContext* context, NLFunctionData* data);

    // Charge the representative chunk's rows against a limit counter, recording
    // how many rows the truncate should copy this step. The sole counter mutator.
    static void runLimitUpdate(NLExecutionContext* context, NLFunctionData* data);

    // Copy the first emitThisStep rows of each column into a fresh chunk, so a
    // downstream consumer reads a genuinely truncated chunk. Reads the counter,
    // never mutates it.
    static void runLimitTruncate(NLExecutionContext* context, NLFunctionData* data);

    static void runOutput(NLExecutionContext* context, NLFunctionData* data);
    static NLGatherFunction selectGatherFunction(NLChunkKind kind);

    // Block-repeat for an ID chunk of this kind (outer column).
    static NLBroadcastFunction selectBlockRepeatFunction(NLChunkKind kind);

    // Tile for an ID chunk of this kind (inner column).
    static NLBroadcastFunction selectTileFunction(NLChunkKind kind);

    // Block-repeat for a nullable value chunk of this value type (outer column).
    static NLBroadcastFunction selectOptBlockRepeatFunction(ValueType valueType);

    // Tile for a nullable value chunk of this value type (inner column).
    static NLBroadcastFunction selectOptTileFunction(ValueType valueType);

    // The with-null property fetch handler for an ID type (NodeID/EdgeID) and a
    // value type (types::Double, ...). The translator picks the specialization
    // from the resolved property and stores it as the statement's handler; only
    // the explicitly instantiated (ID, T) pairs in NLExecutor.cpp are available.
    template <typename ID, typename T>
    static void runPropertyFetch(NLExecutionContext* context, NLFunctionData* data);

private:
    NLExecutionContext _ctxt;
    const NLProgram* _prog {nullptr};
};

}
