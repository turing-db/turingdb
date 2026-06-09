#include "IRModuleInspector.h"

#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/OperationSupport.h"
#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "llvm/Support/raw_os_ostream.h"

using namespace db;

void IRModuleInspector::dump(std::ostream& out) const {
    llvm::raw_os_ostream os(out);

    os << "==== Functions in module ====\n";
    for (mlir::func::FuncOp func : _mod->getBody()->getOps<mlir::func::FuncOp>()) {
        os << func.getSymName() << " : " << func.getFunctionType() << '\n';
    }
}
