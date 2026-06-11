#pragma once

#include <stddef.h>

#include "views/GraphView.h"

#include "NLProgram.h"

namespace db {

class NLOutputSink;

// Everything a handler needs besides its own payload, passed down the call
// chain by reference.
struct NLExecutionContext {
    GraphView _view;
    NLOutputSink* _sink {nullptr};
    size_t _chunkSize {0};
};

// Executes a translated NLProgram against a graph view. Handlers are exposed
// as statics so NLTranslator can bind them into descriptors; they are not
// meant to be called directly.
class NLInterpreter {
public:
    static void run(const GraphView& view, NLProgram& program, NLOutputSink& sink);

    static void runScanNodesLoop(NLExecutionContext& context, NLFunctionData* data);
    static void runGetOutEdgesLoop(NLExecutionContext& context, NLFunctionData* data);
    static void runGetInEdgesLoop(NLExecutionContext& context, NLFunctionData* data);
    static void runOutput(NLExecutionContext& context, NLFunctionData* data);

    static NLGatherFunction selectGatherFunction(NLChunkKind kind);
};

}
