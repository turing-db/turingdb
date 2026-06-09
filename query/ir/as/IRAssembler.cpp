#include "IRAssembler.h"

#include <spdlog/fmt/bundled/format.h>

#include "mlir/IR/MLIRContext.h"
#include "mlir/Parser/Parser.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/SymbolTable.h"
#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "llvm/ADT/SmallVector.h"

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
    mlir::SymbolTable moduleSymbols(_mod->getOperation());

    for (const fs::Path& file : _files) {
        llvm::StringRef filename(file.get());

        auto fileMod = mlir::parseSourceFile<mlir::ModuleOp>(filename, parserConfig);
        if (!fileMod) {
            throw CompilerException(fmt::format("MLIR file {} can not be parsed", file.get()));
        }

        // A parsed module is already verified to have unique symbol names, so a single
        // file cannot redefine a function. Guard only against collisions across files.
        const auto functions = llvm::to_vector(fileMod->getBody()->getOps<mlir::func::FuncOp>());
        for (mlir::func::FuncOp func : functions) {
            const llvm::StringRef name = func.getSymName();
            const bool alreadyDefined = moduleSymbols.lookup(name) != nullptr;
            if (alreadyDefined) {
                throw CompilerException(fmt::format("Function '{}' from MLIR file {} is already defined in the module",
                                                    name.str(),
                                                    file.get()));
            }
        }

        dest->getOperations().splice(dest->end(), fileMod->getBody()->getOperations());

        // Register the spliced functions so the next files see them.
        for (mlir::func::FuncOp func : functions) {
            moduleSymbols.insert(func);
        }
    }
}
