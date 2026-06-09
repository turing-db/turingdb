#include <iostream>

#include "mlir/IR/MLIRContext.h"
#include "mlir/IR/Builders.h"
#include "llvm/Support/raw_ostream.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/BuiltinTypes.h"

#include "DBOps.h"

void print(auto op) {
    op->print(llvm::outs());
    llvm::outs() << '\n';
}

int main() {
    std::cout << "hello from mlir" << '\n';

    mlir::MLIRContext ctx;
    ctx.loadDialect<mlir::db::DB>();
    ctx.loadDialect<mlir::func::FuncDialect>();

    mlir::OpBuilder builder(&ctx);
    mlir::Location loc = builder.getUnknownLoc();

    auto module = mlir::ModuleOp::create(loc);

    auto funcType = mlir::FunctionType::get(&ctx, {}, {});
    auto func = builder.create<mlir::func::FuncOp>(loc, "main", funcType);
    auto& block = *func.addEntryBlock();
    builder.setInsertionPointToStart(&block);
    module.push_back(func);

    mlir::Type col0 = mlir::db::ColumnType::get(&ctx, "scan");
    mlir::Type col1 = mlir::db::ColumnType::get(&ctx, "srcs");
    mlir::Type col2 = mlir::db::ColumnType::get(&ctx, "eids");
    mlir::Type col3 = mlir::db::ColumnType::get(&ctx, "etypes");
    mlir::Type col4 = mlir::db::ColumnType::get(&ctx, "tgts");

    auto scan = builder.create<mlir::db::ScanNodes>(loc, col0);
    auto getout = builder.create<mlir::db::GetOutEdges>(loc, col1, col2, col3, col4,
                                                            scan.getResult());

    print(scan);
    print(getout);
}
