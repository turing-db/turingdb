#pragma once

#include <stddef.h>

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
class NLInterpreter {
public:
    NLInterpreter(const GraphView* view,
                  const NLProgram* prog,
                  NLOutputSink* sink);
    ~NLInterpreter();

    void run();

    static void runScanNodesLoop(NLExecutionContext* context, NLFunctionData* data);
    static void runGetOutEdgesLoop(NLExecutionContext* context, NLFunctionData* data);
    static void runGetInEdgesLoop(NLExecutionContext* context, NLFunctionData* data);
    static void runOutput(NLExecutionContext* context, NLFunctionData* data);
    static NLGatherFunction selectGatherFunction(NLChunkKind kind);

private:
    NLExecutionContext _ctxt;
    const NLProgram* _prog {nullptr};
};

}
