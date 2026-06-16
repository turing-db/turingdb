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
    // Wall-clock timing of one run, broken down by the three stages run()
    // performs: db->nl lowering, nl translation and execution, in milliseconds.
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

    DBDialectInterpreter(const mlir::ModuleOp& module,
                         const GraphView* view,
                         NLOutputSink* sink,
                         LocalMemory* memory,
                         size_t chunkSize = ChunkConfig::CHUNK_SIZE);
    ~DBDialectInterpreter();

    Status run();

private:
    mlir::ModuleOp _module;
    const GraphView* _view {nullptr};
    NLOutputSink* _sink {nullptr};
    LocalMemory* _memory {nullptr};
    size_t _chunkSize {ChunkConfig::CHUNK_SIZE};
};

}
