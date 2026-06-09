#include "IRAssembler.h"

#include <spdlog/fmt/bundled/format.h>

#include "mlir/IR/MLIRContext.h"
#include "mlir/Parser/Parser.h"
#include "mlir/IR/BuiltinOps.h"

#include "IRException.h"

using namespace db;

IRAssembler::IRAssembler(mlir::MLIRContext* ctxt, mlir::ModuleOp* mod)
    : _ctxt(ctxt),
    _mod(mod)
{
}

IRAssembler::~IRAssembler() {
}

void IRAssembler::addFile(const fs::Path& path) {
    _files.push_back(path);
}

void IRAssembler::assemble() const {
    mlir::ParserConfig parserConfig(_ctxt);
    mlir::Block* dest = _mod->getBody();

    for (const fs::Path& file : _files) {
        llvm::StringRef filename(file.get());

        auto fileMod = mlir::parseSourceFile<mlir::ModuleOp>(filename, parserConfig);
        if (!fileMod) {
            throw CompilerException(fmt::format("MLIR file {} can not be parsed", file.get()));
        }

        dest->getOperations().splice(dest->end(), fileMod->getBody()->getOperations());
    }
}
