#include <print>

#include "mlir/IR/MLIRContext.h"
#include "mlir/IR/Builders.h"
#include "llvm/Support/raw_ostream.h"

#include "DBOps.h"

int main() {
    std::println("hello from mlir");

    mlir::MLIRContext ctx;
    ctx.loadDialect<mlir::turing::TuringDB>();

    mlir::OpBuilder builder(&ctx);
    mlir::Location loc = builder.getUnknownLoc();
    mlir::Type resultType = mlir::IntegerType::get(&ctx, 64);

    auto op = builder.create<mlir::turing::ScanNodes>(loc, resultType);

    op->print(llvm::outs());
    llvm::outs() << "\n";
}
