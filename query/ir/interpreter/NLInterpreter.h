#pragma once

#include <stddef.h>

#include "mlir/IR/BuiltinOps.h"

#include "iterators/ChunkConfig.h"

namespace db {

class GraphView;
class NLOutputSink;
class LocalMemory;

// Executes an nl-dialect MLIR module against a GraphView
class NLInterpreter {
public:
    // Statistics of an interpreter run
    class Status {
    public:
        Status() = default;

        Status(double translateMilliseconds, double executeMilliseconds)
            : _translateMilliseconds(translateMilliseconds),
            _executeMilliseconds(executeMilliseconds)
        {
        }

        double getTranslateMilliseconds() const { return _translateMilliseconds; }
        double getExecuteMilliseconds() const { return _executeMilliseconds; }

    private:
        double _translateMilliseconds {0.0};
        double _executeMilliseconds {0.0};
    };

    NLInterpreter(const mlir::ModuleOp& module,
                  const GraphView* view,
                  NLOutputSink* sink,
                  LocalMemory* memory,
                  size_t chunkSize = ChunkConfig::CHUNK_SIZE);
    ~NLInterpreter();

    Status run();

private:
    mlir::ModuleOp _module;
    const GraphView* _view {nullptr};
    NLOutputSink* _sink {nullptr};
    LocalMemory* _memory {nullptr};
    size_t _chunkSize {ChunkConfig::CHUNK_SIZE};
};

}
