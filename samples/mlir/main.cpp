#include <stdlib.h>
#include <iostream>
#include <span>
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
#include "NLInterpreter.h"
#include "NLOutputSink.h"
#include "IRAssembler.h"
#include "IRModuleInspector.h"

#include "Graph.h"
#include "dump/GraphLoader.h"
#include "reader/GraphReader.h"
#include "versioning/Transaction.h"
#include "columns/ColumnIDs.h"
#include "columns/ColumnEdgeTypes.h"

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
    mlir::Block* nodeLoopBody = nodeLoop.getBody();
    builder.setInsertionPointToStart(nodeLoopBody);
    const mlir::Value nodeChunk = nodeLoopBody->getArgument(0);

    // Out-edges: send each step's target (successor) node IDs to the result.
    // The edge loop binds the chunks in order: sources, edge IDs, edge type
    // IDs, targets - so argument 3 is the targets column.
    auto outEdges = builder.create<mlir::nl::GetOutEdges>(loc, nodeChunk, mlir::ValueRange {});
    auto outLoop = builder.create<mlir::nl::For>(loc, outEdges.getResult());
    mlir::Block* outLoopBody = outLoop.getBody();
    builder.setInsertionPointToStart(outLoopBody);
    builder.create<mlir::nl::Output>(loc, mlir::ValueRange {outLoopBody->getArgument(3)});

    // In-edges of the same node chunk: send the source (predecessor) node IDs.
    // Same chunk order, so argument 0 is the sources column.
    builder.setInsertionPointAfter(outLoop);
    auto inEdges = builder.create<mlir::nl::GetInEdges>(loc, nodeChunk, mlir::ValueRange {});
    auto inLoop = builder.create<mlir::nl::For>(loc, inEdges.getResult());
    mlir::Block* inLoopBody = inLoop.getBody();
    builder.setInsertionPointToStart(inLoopBody);
    builder.create<mlir::nl::Output>(loc, mlir::ValueRange {inLoopBody->getArgument(0)});

    // MATCH (a)->(b)->(c): two out-edge hops where the second carries the `a`
    // of the current step. First hop a->b with an empty carry set; its loop
    // binds sources(=a) at argument 0 and targets(=b) at argument 3.
    builder.setInsertionPointAfter(inLoop);
    auto firstHop = builder.create<mlir::nl::GetOutEdges>(loc, nodeChunk, mlir::ValueRange {});
    auto firstHopLoop = builder.create<mlir::nl::For>(loc, firstHop.getResult());
    mlir::Block* firstHopLoopBody = firstHopLoop.getBody();
    builder.setInsertionPointToStart(firstHopLoopBody);
    const mlir::Value aChunk = firstHopLoopBody->getArgument(0);
    const mlir::Value bChunk = firstHopLoopBody->getArgument(3);

    // Second hop b->c carrying `a`: each second-hop edge binds its source (=b)
    // at argument 0 and its target (=c) at argument 3, and the carried `a` comes
    // back as the trailing chunk at argument 4, filtered to the `a` whose `b`
    // has a successor `c`. All three are row-aligned, so output the (a, b, c)
    // triple.
    auto secondHop = builder.create<mlir::nl::GetOutEdges>(loc, bChunk, mlir::ValueRange {aChunk});
    auto secondHopLoop = builder.create<mlir::nl::For>(loc, secondHop.getResult());
    mlir::Block* secondHopLoopBody = secondHopLoop.getBody();
    builder.setInsertionPointToStart(secondHopLoopBody);
    const mlir::Value bFiltered = secondHopLoopBody->getArgument(0);
    const mlir::Value cChunk = secondHopLoopBody->getArgument(3);
    const mlir::Value filteredA = secondHopLoopBody->getArgument(4);
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

// Prints each nl.output chunk set as one row per index, zipping the columns.
// The interpreter pushes one chunk per output column, all of the same length.
class PrintingSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks) override {
        if (chunks.empty()) {
            return;
        }

        const size_t rowCount = chunks.front()->size();
        for (size_t row = 0; row < rowCount; row++) {
            std::cout << "(";
            for (size_t column = 0; column < chunks.size(); column++) {
                if (column > 0) {
                    std::cout << ", ";
                }

                printCell(chunks[column], row);
            }

            std::cout << ")\n";
            _rowCount++;
        }
    }

    size_t getRowCount() const { return _rowCount; }

private:
    // The translator only ever emits node ID, edge ID and edge type ID chunks
    static void printCell(const Column* column, size_t row) {
        if (const auto* nodeIDs = dynamic_cast<const ColumnNodeIDs*>(column)) {
            std::cout << (*nodeIDs)[row].getValue();
        } else if (const auto* edgeIDs = dynamic_cast<const ColumnEdgeIDs*>(column)) {
            std::cout << (*edgeIDs)[row].getValue();
        } else if (const auto* edgeTypes = dynamic_cast<const ColumnEdgeTypes*>(column)) {
            std::cout << (*edgeTypes)[row].getValue();
        } else {
            std::cout << "?";
        }
    }

    size_t _rowCount {0};
};

// Loads the graph at graphDir and runs the module's main function against it
void executeModule(mlir::ModuleOp& module, const std::string& graphDir) {
    if (graphDir.empty()) {
        throw std::runtime_error("-exec requires a graph directory given with -graph");
    }

    auto graph = Graph::create();
    const auto loadResult = GraphLoader::load(graph.get(), fs::Path(graphDir));
    if (!loadResult) {
        throw std::runtime_error(loadResult.error().fmtMessage());
    }

    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView& view = reader.getView();

    PrintingSink sink;
    NLInterpreter interpreter(module, &view, &sink);
    interpreter.run();

    std::cout << sink.getRowCount() << " rows\n";
}

}

int main(int argc, char** argv) {
    argparse::ArgumentParser parser("mlir", "1.0", argparse::default_arguments::help);
    parser.add_description("TuringDB MLIR dialects sample and assembler");

    std::vector<std::string> files;
    bool dumpCode = false;
    bool execute = false;
    std::string graphDir;

    parser.add_argument("files")
        .metavar("mlir.txt")
        .nargs(argparse::nargs_pattern::any)
        .store_into(files)
        .help("Textual MLIR input files to assemble");

    parser.add_argument("-d", "--dump")
        .store_into(dumpCode)
        .help("Dump the full MLIR code of every function in the module");

    parser.add_argument("-exec")
        .store_into(execute)
        .help("Execute the module's main function with the NLInterpreter");

    parser.add_argument("-graph")
        .metavar("path")
        .store_into(graphDir)
        .help("Graph directory to load and execute against (requires -exec)");

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

        if (execute) {
            executeModule(mainMod, graphDir);
        }
    } catch (const std::exception& e) {
        std::cerr << e.what() << "\n";
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}
