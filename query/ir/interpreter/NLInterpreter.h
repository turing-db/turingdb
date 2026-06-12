#pragma once

#include <stddef.h>

#include "mlir/IR/BuiltinOps.h"

#include "iterators/ChunkConfig.h"

namespace db {

class GraphView;
class NLOutputSink;
class LocalMemory;

// Translates an nl-dialect MLIR module into an NLProgram and executes it
// against a graph view, pushing output chunk-wise into the sink. Wraps the
// NLTranslator and NLExecutor steps behind a single run() call.
class NLInterpreter {
public:
    NLInterpreter(const mlir::ModuleOp& module,
                  const GraphView* view,
                  NLOutputSink* sink,
                  LocalMemory* memory,
                  size_t chunkSize = ChunkConfig::CHUNK_SIZE);
    ~NLInterpreter();

    void run();

private:
    mlir::ModuleOp _module;
    const GraphView* _view {nullptr};
    NLOutputSink* _sink {nullptr};
    LocalMemory* _memory {nullptr};
    size_t _chunkSize {ChunkConfig::CHUNK_SIZE};
};

}
