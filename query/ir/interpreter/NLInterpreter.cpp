#include "NLInterpreter.h"

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/SymbolTable.h"

#include "NLProgram.h"
#include "NLTranslator.h"
#include "NLExecutor.h"

#include "IRException.h"

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

    NLTranslator translator(&program, _memory);
    translator.translate(function);

    NLExecutor executor(_view, &program, _sink);
    executor.run();
}
