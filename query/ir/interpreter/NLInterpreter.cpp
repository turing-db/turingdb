#include "NLInterpreter.h"

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/SymbolTable.h"

#include "NLProgram.h"
#include "NLTranslator.h"
#include "NLExecutor.h"

#include "IRException.h"
#include "TuringTime.h"

using namespace db;

NLInterpreter::NLInterpreter(const mlir::ModuleOp& module,
                             const GraphView* view,
                             NLOutputSink* sink,
                             LocalMemory* memory,
                             size_t chunkSize,
                             CommitWriteBuffer* writeBuffer,
                             MetadataBuilder* metadataBuilder)
    : _module(module),
    _view(view),
    _sink(sink),
    _memory(memory),
    _chunkSize(chunkSize),
    _writeBuffer(writeBuffer),
    _metadataBuilder(metadataBuilder)
{
}

NLInterpreter::~NLInterpreter() {
}

NLInterpreter::Status NLInterpreter::run() {
    // Get main function
    const mlir::func::FuncOp function = _module.lookupSymbol<mlir::func::FuncOp>("main");
    if (!function) {
        throw IRException("nl module has no 'main' function to interpret");
    }

    NLProgram program;
    program.setChunkSize(_chunkSize);

    double translateMilliseconds {0.0};
    double executeMilliseconds {0.0};

    // Translation to an NLProgram (the thing that will be executed)
    {
        const TimePoint start = Clock::now();

        NLTranslator translator(&program, _memory, _view, _metadataBuilder);
        translator.translate(function);

        const TimePoint end = Clock::now();
        translateMilliseconds = duration<Milliseconds>(start, end);
    }

    // Execute nl program
    {
        const TimePoint start = Clock::now();

        NLExecutor executor(_view, &program, _sink, _writeBuffer);
        executor.run();

        const TimePoint end = Clock::now();
        executeMilliseconds = duration<Milliseconds>(start, end);
    }

    return Status(translateMilliseconds, executeMilliseconds);
}
