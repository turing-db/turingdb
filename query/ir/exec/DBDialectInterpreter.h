#pragma once

#include <stddef.h>

#include "mlir/IR/BuiltinOps.h"
#include "mlir/Dialect/Func/IR/FuncOps.h"

#include "iterators/ChunkConfig.h"

namespace db {

class GraphView;
class NLOutputSink;
class LocalMemory;
class CommitWriteBuffer;
class MetadataBuilder;
class NLSystemContext;
class ProcedureContext;
class ExplainReport;

class DBDialectInterpreter {
public:
    // Statistics of an interpreter run
    class Status {
    public:
        Status() = default;

        Status(double lowerMilliseconds, double translateMilliseconds, double executeMilliseconds)
            : _lowerMilliseconds(lowerMilliseconds),
            _translateMilliseconds(translateMilliseconds),
            _executeMilliseconds(executeMilliseconds)
        {
        }

        double getLowerMilliseconds() const { return _lowerMilliseconds; }
        double getTranslateMilliseconds() const { return _translateMilliseconds; }
        double getExecuteMilliseconds() const { return _executeMilliseconds; }

    private:
        double _lowerMilliseconds {0.0};
        double _translateMilliseconds {0.0};
        double _executeMilliseconds {0.0};
    };

    // procedureContext is what a CALL in the module needs: its registry resolves the
    // procedure during lowering, and its graph, transaction and request state are what
    // the procedure's callbacks read during execution. The caller owns it; a module
    // with no CALL never touches it.
    DBDialectInterpreter(const mlir::ModuleOp& module,
                         const GraphView* view,
                         NLOutputSink* sink,
                         LocalMemory* memory,
                         size_t chunkSize = ChunkConfig::CHUNK_SIZE,
                         CommitWriteBuffer* writeBuffer = nullptr,
                         MetadataBuilder* metadataBuilder = nullptr,
                         const ProcedureContext* procedureContext = nullptr,
                         const NLSystemContext* system = nullptr);

    ~DBDialectInterpreter();

    Status run();

    // Reports the stages of this module an EXPLAIN prefix asked for that only exist
    // past codegen - the nl program it lowers to - without executing anything. A
    // report asking for none of them lowers nothing.
    void explain(ExplainReport& report);

private:
    mlir::ModuleOp _module;
    const GraphView* _view {nullptr};
    NLOutputSink* _sink {nullptr};
    LocalMemory* _memory {nullptr};
    size_t _chunkSize {ChunkConfig::CHUNK_SIZE};
    CommitWriteBuffer* _writeBuffer {nullptr};
    MetadataBuilder* _metadataBuilder {nullptr};
    const ProcedureContext* _procedureContext {nullptr};
    const NLSystemContext* _system {nullptr};

    mlir::func::FuncOp requireMain();
    mlir::func::FuncOp lower(mlir::func::FuncOp dbFunction, mlir::ModuleOp nlModule);
};

}
