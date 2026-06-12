#pragma once

#include <stddef.h>

#include "mlir/IR/BuiltinOps.h"

#include "iterators/ChunkConfig.h"

namespace db {

class GraphView;
class NLOutputSink;
class LocalMemory;

class DBDialectInterpreter {
public:
    DBDialectInterpreter(const mlir::ModuleOp& module,
                         const GraphView* view,
                         NLOutputSink* sink,
                         LocalMemory* memory,
                         size_t chunkSize = ChunkConfig::CHUNK_SIZE);
    ~DBDialectInterpreter();

    void run();

private:
    mlir::ModuleOp _module;
    const GraphView* _view {nullptr};
    NLOutputSink* _sink {nullptr};
    LocalMemory* _memory {nullptr};
    size_t _chunkSize {ChunkConfig::CHUNK_SIZE};
};

}
