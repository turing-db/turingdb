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
#include "NLOps.h"
#include "IRAssembler.h"
#include "IRModuleInspector.h"

using namespace db;

namespace {

// Appends an empty function to the module and moves the builder insertion
// point inside its entry block
void startFunction(mlir::OpBuilder& builder, mlir::ModuleOp& module, const char* name) {
    mlir::MLIRContext* ctxt = builder.getContext();
    const mlir::Location loc = builder.getUnknownLoc();

    builder.setInsertionPointToEnd(module.getBody());

    auto funcType = mlir::FunctionType::get(ctxt, {}, {});
    auto func = builder.create<mlir::func::FuncOp>(loc, name, funcType);
    auto& block = *func.addEntryBlock();

    builder.setInsertionPointToStart(&block);
}

// The set-at-a-time form of the query: db dialect ops on whole columns
void addDBFunction(mlir::OpBuilder& builder, mlir::ModuleOp& module) {
    startFunction(builder, module, "db_ops");

    mlir::MLIRContext* ctxt = builder.getContext();
    const mlir::Location loc = builder.getUnknownLoc();

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

    builder.create<mlir::func::ReturnOp>(loc);
}

// The imperative nested-loop form of the same query: nl dialect iterators
// driven by chunked for loops. No type is ever spelled: the source ops infer
// their iterator types and the nl.for builder looks up the loop variable
// types on the iterator value
void addNestedLoopFunction(mlir::OpBuilder& builder, mlir::ModuleOp& module) {
    startFunction(builder, module, "nested_loop");

    const mlir::Location loc = builder.getUnknownLoc();

    auto nodes = builder.create<mlir::nl::ScanNodes>(loc);
    auto nodeLoop = builder.create<mlir::nl::For>(loc, nodes.getResult());
    builder.create<mlir::func::ReturnOp>(loc);

    // Fill the node loop body with three walks over each chunk of nodes:
    // forwards (out-edges), backwards (in-edges), and a two-hop
    // MATCH (a)->(b)->(c) that carries `a` through the second hop.
    builder.setInsertionPointToStart(nodeLoop.getBody());
    const mlir::Value nodeChunk = nodeLoop.getBody()->getArgument(0);

    // Out-edges: send each step's target (successor) node IDs to the result.
    // The edge loop binds the chunks in order: sources, edge IDs, edge type
    // IDs, targets - so argument 3 is the targets column.
    auto outEdges = builder.create<mlir::nl::GetOutEdges>(loc, nodeChunk, mlir::ValueRange {});
    auto outLoop = builder.create<mlir::nl::For>(loc, outEdges.getResult());
    builder.setInsertionPointToStart(outLoop.getBody());
    builder.create<mlir::nl::Output>(loc, mlir::ValueRange {outLoop.getBody()->getArgument(3)});

    // In-edges of the same node chunk: send the source (predecessor) node IDs.
    // Same chunk order, so argument 0 is the sources column.
    builder.setInsertionPointAfter(outLoop);
    auto inEdges = builder.create<mlir::nl::GetInEdges>(loc, nodeChunk, mlir::ValueRange {});
    auto inLoop = builder.create<mlir::nl::For>(loc, inEdges.getResult());
    builder.setInsertionPointToStart(inLoop.getBody());
    builder.create<mlir::nl::Output>(loc, mlir::ValueRange {inLoop.getBody()->getArgument(0)});

    // MATCH (a)->(b)->(c): two out-edge hops where the second carries the `a`
    // of the current step. First hop a->b with an empty carry set; its loop
    // binds sources(=a) at argument 0 and targets(=b) at argument 3.
    builder.setInsertionPointAfter(inLoop);
    auto firstHop = builder.create<mlir::nl::GetOutEdges>(loc, nodeChunk, mlir::ValueRange {});
    auto firstHopLoop = builder.create<mlir::nl::For>(loc, firstHop.getResult());
    builder.setInsertionPointToStart(firstHopLoop.getBody());
    const mlir::Value aChunk = firstHopLoop.getBody()->getArgument(0);
    const mlir::Value bChunk = firstHopLoop.getBody()->getArgument(3);

    // Second hop b->c carrying `a`: each second-hop edge binds its source (=b)
    // at argument 0 and its target (=c) at argument 3, and the carried `a` comes
    // back as the trailing chunk at argument 4, filtered to the `a` whose `b`
    // has a successor `c`. All three are row-aligned, so output the (a, b, c)
    // triple.
    auto secondHop = builder.create<mlir::nl::GetOutEdges>(loc, bChunk, mlir::ValueRange {aChunk});
    auto secondHopLoop = builder.create<mlir::nl::For>(loc, secondHop.getResult());
    builder.setInsertionPointToStart(secondHopLoop.getBody());
    const mlir::Value bFiltered = secondHopLoop.getBody()->getArgument(0);
    const mlir::Value cChunk = secondHopLoop.getBody()->getArgument(3);
    const mlir::Value filteredA = secondHopLoop.getBody()->getArgument(4);
    builder.create<mlir::nl::Output>(loc, mlir::ValueRange {filteredA, bFiltered, cChunk});
}

void helloModule(mlir::OpBuilder& builder, mlir::ModuleOp& module) {
    std::cout << "hello from mlir" << '\n';

    addDBFunction(builder, module);
    addNestedLoopFunction(builder, module);
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
        ctxt.loadDialect<mlir::nl::NL>();
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
