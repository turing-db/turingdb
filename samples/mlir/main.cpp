#include <stdlib.h>
#include <iostream>
#include <string>
#include <vector>

#include <argparse.hpp>

#include "mlir/IR/MLIRContext.h"
#include "mlir/IR/Builders.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/BuiltinTypes.h"

#include "DBOps.h"
#include "IRAssembler.h"
#include "IRModuleInspector.h"

using namespace db;

namespace {

void helloModule(mlir::OpBuilder& builder, mlir::ModuleOp& module) {
    std::cout << "hello from mlir" << '\n';

    mlir::MLIRContext* ctxt = builder.getContext();
    const mlir::Location loc = builder.getUnknownLoc();

    auto funcType = mlir::FunctionType::get(ctxt, {}, {});
    auto func = builder.create<mlir::func::FuncOp>(loc, "main", funcType);
    auto& block = *func.addEntryBlock();
    builder.setInsertionPointToStart(&block);
    module.push_back(func);

    const mlir::Type col0 = mlir::db::ColumnType::get(ctxt, "scan");
    const mlir::Type col1 = mlir::db::ColumnType::get(ctxt, "srcs");
    const mlir::Type col2 = mlir::db::ColumnType::get(ctxt, "eids");
    const mlir::Type col3 = mlir::db::ColumnType::get(ctxt, "etypes");
    const mlir::Type col4 = mlir::db::ColumnType::get(ctxt, "tgts");

    auto scan = builder.create<mlir::db::ScanNodes>(loc, col0);
    builder.create<mlir::db::GetOutEdges>(loc, col1, col2, col3, col4, scan.getResult());
}

void assembleFiles(mlir::MLIRContext& ctxt, mlir::ModuleOp& module, const std::vector<std::string>& files) {
    IRAssembler assembler(&ctxt, &module);

    for (const std::string& file : files) {
        assembler.addFile(fs::Path(file));
    }

    assembler.assemble();
}

}

int main(int argc, char** argv) {
    argparse::ArgumentParser parser("mlir", "1.0", argparse::default_arguments::help);
    parser.add_description("TuringDB MLIR dialects sample and assembler");

    std::vector<std::string> files;
    bool dumpCode = false;

    parser.add_argument("files")
        .metavar("mlir.txt")
        .nargs(argparse::nargs_pattern::any)
        .store_into(files)
        .help("Textual MLIR input files to assemble");

    parser.add_argument("-d", "--dump")
        .store_into(dumpCode)
        .help("Dump the full MLIR code of every function in the module");

    try {
        parser.parse_args(argc, argv);
    } catch (const std::exception& e) {
        std::cerr << e.what() << "\n";
        std::cerr << parser;
        return EXIT_FAILURE;
    }

    try {
        mlir::MLIRContext ctxt;
        ctxt.loadDialect<mlir::db::DB>();
        ctxt.loadDialect<mlir::func::FuncDialect>();

        mlir::OpBuilder builder(&ctxt);
        auto mainMod = mlir::ModuleOp::create(builder.getUnknownLoc());

        if (files.empty()) {
            helloModule(builder, mainMod);
        } else {
            assembleFiles(ctxt, mainMod, files);
        }

        IRModuleInspector inspector(&mainMod);
        if (dumpCode) {
            inspector.dumpFunctions(std::cout);
        } else {
            inspector.dumpFunctionTypes(std::cout);
        }
    } catch (const std::exception& e) {
        std::cerr << e.what() << "\n";
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}
