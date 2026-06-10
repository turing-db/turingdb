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

    // MATCH (a)->(b)->(c): scan `a`, then two get_out_edges hops. The second hop
    // carries the filtered `a` column so it ends up filtered to the `a` that reach a `c`.
    const mlir::Type colA   = mlir::db::ColumnType::get(ctxt, "a");
    const mlir::Type colA1  = mlir::db::ColumnType::get(ctxt, "a1");
    const mlir::Type colE0  = mlir::db::ColumnType::get(ctxt, "e0");
    const mlir::Type colEt0 = mlir::db::ColumnType::get(ctxt, "et0");
    const mlir::Type colB   = mlir::db::ColumnType::get(ctxt, "b");
    const mlir::Type colB2  = mlir::db::ColumnType::get(ctxt, "b2");
    const mlir::Type colE1  = mlir::db::ColumnType::get(ctxt, "e1");
    const mlir::Type colEt1 = mlir::db::ColumnType::get(ctxt, "et1");
    const mlir::Type colC   = mlir::db::ColumnType::get(ctxt, "c");
    const mlir::Type colA2  = mlir::db::ColumnType::get(ctxt, "a2");

    auto scan = builder.create<mlir::db::ScanNodes>(loc, colA);

    // First hop a->b with an empty carry set: four fixed result columns, no filtered
    // columns, and just the input_nodes operand.
    auto hop1 = builder.create<mlir::db::GetOutEdges>(loc,
                                                      mlir::TypeRange {colA1, colE0, colEt0, colB},
                                                      mlir::ValueRange {scan.getResult()});

    // Second hop b->c carrying the filtered `a` (hop1 srcids): one extra carry operand
    // and one extra result type for its filtered counterpart.
    builder.create<mlir::db::GetOutEdges>(loc,
                                          mlir::TypeRange {colB2, colE1, colEt1, colC, colA2},
                                          mlir::ValueRange {hop1.getTgtids(), hop1.getSrcids()});
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
