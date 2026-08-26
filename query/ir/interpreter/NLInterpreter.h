#pragma once

#include <stddef.h>

#include "mlir/IR/BuiltinOps.h"

#include "iterators/ChunkConfig.h"

namespace db {

class GraphView;
class NLOutputSink;
class LocalMemory;
class CommitWriteBuffer;
class MetadataBuilder;
class ProcedureContext;

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

    // procedureContext is what a CALL in the module needs: the registry its name is
    // resolved against and the graph, transaction and request state its callbacks
    // read. The caller owns it - only the caller knows all of those - and a module
    // with no CALL never touches it.
    NLInterpreter(const mlir::ModuleOp& module,
                  const GraphView* view,
                  NLOutputSink* sink,
                  LocalMemory* memory,
                  size_t chunkSize = ChunkConfig::CHUNK_SIZE,
                  CommitWriteBuffer* writeBuffer = nullptr,
                  MetadataBuilder* metadataBuilder = nullptr,
                  const ProcedureContext* procedureContext = nullptr);
    ~NLInterpreter();

    Status run();

private:
    mlir::ModuleOp _module;
    const GraphView* _view {nullptr};
    NLOutputSink* _sink {nullptr};
    LocalMemory* _memory {nullptr};
    size_t _chunkSize {ChunkConfig::CHUNK_SIZE};
    CommitWriteBuffer* _writeBuffer {nullptr};
    MetadataBuilder* _metadataBuilder {nullptr};
    const ProcedureContext* _procedureContext {nullptr};
};

}
