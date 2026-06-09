#include "IRModuleInspector.h"

#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/OperationSupport.h"
#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "llvm/Support/raw_os_ostream.h"

using namespace db;

void IRModuleInspector::dumpFunctionTypes(std::ostream& out) const {
    llvm::raw_os_ostream os(out);

    os << "==== Functions in module ====\n";
    for (mlir::func::FuncOp func : _mod->getBody()->getOps<mlir::func::FuncOp>()) {
        os << func.getSymName() << " : " << func.getFunctionType() << '\n';
    }
}

void IRModuleInspector::dumpFunctions(std::ostream& out) const {
    llvm::raw_os_ostream os(out);

    mlir::OpPrintingFlags flags;
    flags.assumeVerified();

    os << "==== Function code in module ====\n";
    for (mlir::func::FuncOp func : _mod->getBody()->getOps<mlir::func::FuncOp>()) {
        func.print(os, flags);
        os << "\n\n";
    }
}
