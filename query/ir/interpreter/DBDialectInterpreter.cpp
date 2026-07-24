#include "DBDialectInterpreter.h"

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/SymbolTable.h"

#include "DBLowering.h"
#include "NLProgram.h"
#include "NLTranslator.h"
#include "NLExecutor.h"

#include "IRException.h"
#include "TuringTime.h"

using namespace db;

DBDialectInterpreter::DBDialectInterpreter(const mlir::ModuleOp& module,
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

DBDialectInterpreter::~DBDialectInterpreter() {
}

DBDialectInterpreter::Status DBDialectInterpreter::run() {
    // Get main function
    const mlir::func::FuncOp dbFunction = _module.lookupSymbol<mlir::func::FuncOp>("main");
    if (!dbFunction) {
        throw IRException("db module has no 'main' function to interpret");
    }

    mlir::MLIRContext* context = _module.getContext();
    mlir::OwningOpRef<mlir::ModuleOp> nlModule = mlir::ModuleOp::create(mlir::UnknownLoc::get(context));

    double lowerMilliseconds {0.0};
    double translateMilliseconds {0.0};
    double executeMilliseconds {0.0};

    // Lowering db to nl
    mlir::func::FuncOp nlFunction;
    {
        const TimePoint start = Clock::now();

        DBLowering lowering(context, _view);
        nlFunction = lowering.lower(dbFunction, *nlModule);

        const TimePoint end = Clock::now();
        lowerMilliseconds = duration<Milliseconds>(start, end);
    }

    // Translation to NLProgram
    NLProgram program;
    program.setChunkSize(_chunkSize);

    {
        const TimePoint start = Clock::now();

        NLTranslator translator(&program, _memory, _view, _metadataBuilder);
        translator.translate(nlFunction);

        const TimePoint end = Clock::now();
        translateMilliseconds = duration<Milliseconds>(start, end);
    }

    // NL program execution
    {
        const TimePoint start = Clock::now();

        NLExecutor executor(_view, &program, _sink, _writeBuffer);
        executor.run();

        const TimePoint end = Clock::now();
        executeMilliseconds = duration<Milliseconds>(start, end);
    }

    return Status(lowerMilliseconds, translateMilliseconds, executeMilliseconds);
}
