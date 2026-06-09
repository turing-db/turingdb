#include <stdlib.h>
#include <iostream>
#include <string>
#include <vector>

#include <argparse.hpp>

#include "mlir/IR/MLIRContext.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/BuiltinTypes.h"

#include "DBDialect.h"
#include "IRAssembler.h"
#include "IRModuleInspector.h"

using namespace db;

int main(int argc, char** argv) {
    argparse::ArgumentParser parser("mlir-as", "1.0", argparse::default_arguments::help);
    parser.add_description("TuringDB MLIR dialects assembler");

    std::vector<std::string> files;

    parser.add_argument("files")
        .metavar("mlir.txt")
        .nargs(argparse::nargs_pattern::any)
        .store_into(files)
        .help("Textual MLIR input files");

    try {
        parser.parse_args(argc, argv);
    } catch (const std::exception& e) {
        std::cerr << e.what() << "\n";
        std::cerr << parser;
        return EXIT_FAILURE;
    }

    if (files.empty()) {
        std::cerr << "Please provide MLIR input files.\n";
        std::cerr << parser;
        return EXIT_FAILURE;
    }

    try {
        mlir::MLIRContext ctxt;
        ctxt.loadDialect<mlir::db::DB>();
        ctxt.loadDialect<mlir::func::FuncDialect>();

        mlir::OpBuilder builder(&ctxt);
        const mlir::Location unknownLoc = builder.getUnknownLoc();

        auto mainMod = mlir::ModuleOp::create(unknownLoc);

        IRAssembler assembler(&ctxt, &mainMod);
        
        for (const std::string& file : files) {
            assembler.addFile(fs::Path(file));
        }

        assembler.assemble();

        IRModuleInspector inspector(&mainMod);
        inspector.dump(std::cout);
    } catch (const std::exception& e) {
        std::cerr << e.what() << "\n";
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}
