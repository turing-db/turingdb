#include "NLInterpreter.h"

#include <iostream>

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
                             size_t chunkSize)
    : _module(module),
    _view(view),
    _sink(sink),
    _memory(memory),
    _chunkSize(chunkSize)
{
}

NLInterpreter::~NLInterpreter() {
}

void NLInterpreter::run() {
    // The module holds the nl program as its "main" func.func; the translator
    // lowers that function's body into the NLProgram's statement tree
    const mlir::func::FuncOp function = _module.lookupSymbol<mlir::func::FuncOp>("main");
    if (!function) {
        throw IRException("nl module has no 'main' function to interpret");
    }

    NLProgram program;
    program.setChunkSize(_chunkSize);

    float translateMilliseconds {0.0f};
    float executeMilliseconds {0.0f};

    {
        const TimePoint start = Clock::now();
        NLTranslator translator(&program, _memory);
        translator.translate(function);
        const TimePoint end = Clock::now();
        translateMilliseconds = duration<Milliseconds>(start, end);
    }

    {
        const TimePoint start = Clock::now();
        NLExecutor executor(_view, &program, _sink);
        executor.run();
        const TimePoint end = Clock::now();
        executeMilliseconds = duration<Milliseconds>(start, end);
    }

    std::cout << "[NLInterpreter] translation: " << translateMilliseconds << " ms, "
              << "execution: " << executeMilliseconds << " ms\n";
}
