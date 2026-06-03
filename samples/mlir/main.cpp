#include <iostream>

#include "mlir/IR/MLIRContext.h"
#include "mlir/IR/Builders.h"
#include "llvm/Support/raw_ostream.h"

#include "DBOps.h"

int main() {
    std::cout << "hello from mlir" << '\n';

    mlir::MLIRContext ctx;
    ctx.loadDialect<mlir::turing::TuringDB>();

    mlir::OpBuilder builder(&ctx);
    mlir::Location loc = builder.getUnknownLoc();
    mlir::Type resultCol = mlir::turing::ColumnType::get(&ctx, "nodes");

    auto op = builder.create<mlir::turing::ScanNodes>(loc, resultCol);

    if (mlir::failed(op.verify())) {
        llvm::errs() << "op verification failed\n";
    }

    op->print(llvm::outs());
    llvm::outs() << "\n";
}
