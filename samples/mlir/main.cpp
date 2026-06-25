#include <stdlib.h>
#include <iostream>
#include <optional>
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
#include "DBTypes.h"
#include "NLInterpreter.h"
#include "DBDialectInterpreter.h"
#include "DBLowering.h"
#include "NLOutputSink.h"
#include "IRAssembler.h"
#include "IRModuleInspector.h"

#include "Graph.h"
#include "dump/GraphLoader.h"
#include "reader/GraphReader.h"
#include "versioning/Transaction.h"
#include "columns/ColumnIDs.h"
#include "columns/ColumnEdgeTypes.h"
#include "columns/ColumnOptVector.h"

#include "LocalMemory.h"

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
    const mlir::Type colA   = mlir::db::ColumnType::get(ctxt);
    const mlir::Type colA1  = mlir::db::ColumnType::get(ctxt);
    const mlir::Type colE0  = mlir::db::ColumnType::get(ctxt);
    const mlir::Type colEt0 = mlir::db::ColumnType::get(ctxt);
    const mlir::Type colB   = mlir::db::ColumnType::get(ctxt);
    const mlir::Type colB2  = mlir::db::ColumnType::get(ctxt);
    const mlir::Type colE1  = mlir::db::ColumnType::get(ctxt);
    const mlir::Type colEt1 = mlir::db::ColumnType::get(ctxt);
    const mlir::Type colC   = mlir::db::ColumnType::get(ctxt);
    const mlir::Type colA2  = mlir::db::ColumnType::get(ctxt);

    auto scan = builder.create<mlir::db::ScanNodes>(loc, colA);

    // First hop a->b with an empty carry set: four fixed result columns, no filtered
    // columns, and just the input_nodes operand.
    auto hop1 = builder.create<mlir::db::GetOutEdges>(loc,
                                                      mlir::TypeRange {colA1, colE0, colEt0, colB},
                                                      mlir::ValueRange {scan.getResult()});

    // Second hop b->c carrying the filtered `a` (hop1 srcids): one extra carry operand
    // and one extra result type for its filtered counterpart.
    auto hop2 = builder.create<mlir::db::GetOutEdges>(loc,
                                                      mlir::TypeRange {colB2, colE1, colEt1, colC, colA2},
                                                      mlir::ValueRange {hop1.getTgtids(), hop1.getSrcids()});

    // Project the (a, b, c) triple the query returns: the filtered `a` (hop2's
    // only carried column), the `b` (hop2 srcids) and the `c` (hop2 tgtids).
    builder.create<mlir::db::Output>(loc,
                                     mlir::ValueRange {hop2.getFilteredColumns()[0], hop2.getSrcids(), hop2.getTgtids()});

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
    builder.create<mlir::nl::Output>(loc, mlir::ValueRange {outLoopBody->getArgument(3)}, mlir::Value());

    // In-edges of the same node chunk: send the source (predecessor) node IDs.
    // Same chunk order, so argument 0 is the sources column.
    builder.setInsertionPointAfter(outLoop);
    auto inEdges = builder.create<mlir::nl::GetInEdges>(loc, nodeChunk, mlir::ValueRange {});
    auto inLoop = builder.create<mlir::nl::For>(loc, inEdges.getResult());
    mlir::Block* inLoopBody = inLoop.getBody();
    builder.setInsertionPointToStart(inLoopBody);
    builder.create<mlir::nl::Output>(loc, mlir::ValueRange {inLoopBody->getArgument(0)}, mlir::Value());

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
    builder.create<mlir::nl::Output>(loc, mlir::ValueRange {filteredA, bFiltered, cChunk}, mlir::Value());
}

// A disconnected pattern in the set-at-a-time db dialect: MATCH (a), (b)
// RETURN a, b crosses every node with every node. The two scans share no
// variable, so each lives in its own factor region of a db.cross_product and
// names, via db.yield, the single column it contributes. The op's results are
// the left factor's yielded columns followed by the right factor's.
void addCrossProductFunction(mlir::OpBuilder& builder, mlir::ModuleOp& module) {
    startFunction(builder, module, "cross_product");

    mlir::MLIRContext* ctxt = builder.getContext();
    const mlir::Location loc = builder.getUnknownLoc();

    const mlir::Type colA = mlir::db::ColumnType::get(ctxt, mlir::Type());
    const mlir::Type colB = mlir::db::ColumnType::get(ctxt, mlir::Type());

    // Build the op with its two empty factor blocks; the result types are the
    // columns the factors will yield - here one column each, `a` then `b`.
    auto product = builder.create<mlir::db::CrossProduct>(loc, mlir::TypeRange {colA, colB});

    // Left factor: scan `a` and yield it as this factor's contribution.
    mlir::Block* leftFactor = &product.getLeftFactor().front();
    builder.setInsertionPointToStart(leftFactor);
    auto scanA = builder.create<mlir::db::ScanNodes>(loc, colA);
    builder.create<mlir::db::Yield>(loc, mlir::ValueRange {scanA.getResult()});

    // Right factor: scan `b` and yield it.
    mlir::Block* rightFactor = &product.getRightFactor().front();
    builder.setInsertionPointToStart(rightFactor);
    auto scanB = builder.create<mlir::db::ScanNodes>(loc, colB);
    builder.create<mlir::db::Yield>(loc, mlir::ValueRange {scanB.getResult()});

    // Back in the function body, project the crossed (a, b) pair.
    builder.setInsertionPointAfter(product);
    builder.create<mlir::db::Output>(loc, product.getResults());

    builder.create<mlir::func::ReturnOp>(loc);
}

void helloModule(mlir::OpBuilder& builder, mlir::ModuleOp& module) {
    std::cout << "hello from mlir" << '\n';

    addDBFunction(builder, module);
    addNestedLoopFunction(builder, module);
    addCrossProductFunction(builder, module);
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
    void appendChunks(std::span<const Column* const> chunks, size_t rowCount) override {
        if (chunks.empty()) {
            return;
        }

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
    // Output chunks are node ID, edge ID and edge type ID chunks from traversals,
    // or nullable value chunks (ColumnOptVector) from a property read
    static void printCell(const Column* column, size_t row) {
        if (const auto* nodeIDs = dynamic_cast<const ColumnNodeIDs*>(column)) {
            std::cout << (*nodeIDs)[row].getValue();
        } else if (const auto* edgeIDs = dynamic_cast<const ColumnEdgeIDs*>(column)) {
            std::cout << (*edgeIDs)[row].getValue();
        } else if (const auto* edgeTypes = dynamic_cast<const ColumnEdgeTypes*>(column)) {
            std::cout << (*edgeTypes)[row].getValue();
        } else if (printValueCell<int64_t>(column, row)
                   || printValueCell<uint64_t>(column, row)
                   || printValueCell<double>(column, row)
                   || printValueCell<std::string_view>(column, row)
                   || printEmbeddingCell(column, row)) {
            // Printed by the helper for whichever nullable value type matched
        } else {
            std::cout << "?";
        }
    }

    // Print one cell of a nullable value column if it has element type T, "null"
    // for an absent value; returns whether the column matched T
    template <typename T>
    static bool printValueCell(const Column* column, size_t row) {
        const auto* values = dynamic_cast<const ColumnOptVector<T>*>(column);
        if (!values) {
            return false;
        }

        const std::optional<T> value = (*values)[row];
        if (value) {
            std::cout << *value;
        } else {
            std::cout << "null";
        }

        return true;
    }

    // An embedding cell needs its own printer: the value is a span of floats,
    // which has no operator<<, so render it as [a, b, c]
    static bool printEmbeddingCell(const Column* column, size_t row) {
        const auto* values = dynamic_cast<const ColumnOptVector<std::span<const float>>*>(column);
        if (!values) {
            return false;
        }

        const std::optional<std::span<const float>> value = (*values)[row];
        if (!value) {
            std::cout << "null";
            return true;
        }

        std::cout << "[";
        for (size_t component = 0; component < value->size(); component++) {
            if (component > 0) {
                std::cout << ", ";
            }
            std::cout << (*value)[component];
        }
        std::cout << "]";

        return true;
    }

    size_t _rowCount {0};
};

// Counts output rows without materializing or printing them. For benchmarking
// traversal throughput, where per-row formatting would dominate the measured
// execution time and flood the terminal on a large expansion.
class CountingSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t rowCount) override {
        _rowCount += rowCount;
    }

    size_t getRowCount() const { return _rowCount; }

private:
    size_t _rowCount {0};
};

// Whether a query function is in the set-at-a-time db dialect or the
// nested-loop nl dialect
enum class QueryDialect {
    DB,
    NL,
};

// Classifies a function by the dialect of the query ops in its body: the first
// db or nl op decides. The func.* structural ops carry no query semantics, so
// they are skipped. Throws if the function holds neither, since there is then
// nothing to run.
QueryDialect classifyDialect(mlir::func::FuncOp function) {
    const llvm::StringRef dbNamespace = mlir::db::DB::getDialectNamespace();
    const llvm::StringRef nlNamespace = mlir::nl::NL::getDialectNamespace();

    std::optional<QueryDialect> dialect;
    function->walk([&](mlir::Operation* operation) {
        const llvm::StringRef opNamespace = operation->getName().getDialectNamespace();
        if (opNamespace == dbNamespace) {
            dialect = QueryDialect::DB;
            return mlir::WalkResult::interrupt();
        } else if (opNamespace == nlNamespace) {
            dialect = QueryDialect::NL;
            return mlir::WalkResult::interrupt();
        }

        return mlir::WalkResult::advance();
    });

    if (!dialect) {
        throw std::runtime_error("function contains no db or nl operation to classify");
    }

    return *dialect;
}

// Runs the module's main function against the view, pushing output into sink.
// A db-dialect "main" runs through DBDialectInterpreter, which lowers it to the
// nl dialect first; an nl-dialect "main" runs straight through NLInterpreter.
void runModuleMain(mlir::ModuleOp& module,
                   const GraphView& view,
                   LocalMemory& memory,
                   NLOutputSink& sink) {
    const mlir::func::FuncOp mainFunction = module.lookupSymbol<mlir::func::FuncOp>("main");
    if (!mainFunction) {
        throw std::runtime_error("-exec requires a 'main' function in the module");
    }

    if (classifyDialect(mainFunction) == QueryDialect::DB) {
        DBDialectInterpreter interpreter(module, &view, &sink, &memory);
        const DBDialectInterpreter::Status status = interpreter.run();
        std::cout << "[DBDialectInterpreter] lowering: " << status.getLowerMilliseconds() << " ms, "
                  << "translation: " << status.getTranslateMilliseconds() << " ms, "
                  << "execution: " << status.getExecuteMilliseconds() << " ms\n";
    } else {
        NLInterpreter interpreter(module, &view, &sink, &memory);
        const NLInterpreter::Status status = interpreter.run();
        std::cout << "[NLInterpreter] translation: " << status.getTranslateMilliseconds() << " ms, "
                  << "execution: " << status.getExecuteMilliseconds() << " ms\n";
    }
}

// Loads the graph at graphDir and runs the module's main function against it.
// With quiet set, output rows are only counted, not printed - the mode for
// benchmarking, where per-row formatting would dominate the timing.
void executeModule(mlir::ModuleOp& module, const std::string& graphDir, bool quiet) {
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

    LocalMemory memory;

    size_t rowCount = 0;
    if (quiet) {
        CountingSink sink;
        runModuleMain(module, view, memory, sink);
        rowCount = sink.getRowCount();
    } else {
        PrintingSink sink;
        runModuleMain(module, view, memory, sink);
        rowCount = sink.getRowCount();
    }

    std::cout << rowCount << " rows\n";
}

// Lowers the module's db-dialect 'main' to the nl dialect with DBLowering and
// dumps it - the loop nest the set-at-a-time db dataflow lowers to - without
// executing anything. The lowered module is built and dumped here, then
// discarded.
//
// A graph is only needed if 'main' fetches properties: lowering resolves each
// property name to its value type against the schema. When graphDir is empty
// the view is null, which is fine for scan/edge-only modules; a property fetch
// then throws a clear error asking for -graph.
void dumpLoweredModule(mlir::ModuleOp& module, const std::string& graphDir) {
    const mlir::func::FuncOp mainFunction = module.lookupSymbol<mlir::func::FuncOp>("main");
    if (!mainFunction) {
        throw std::runtime_error("-dump-lowered requires a 'main' function in the module");
    }

    if (classifyDialect(mainFunction) != QueryDialect::DB) {
        throw std::runtime_error("-dump-lowered requires a db-dialect 'main' function to lower");
    }

    auto graph = Graph::create();
    std::optional<FrozenCommitTx> transaction;
    std::optional<GraphReader> reader;
    const GraphView* view = nullptr;

    if (!graphDir.empty()) {
        const auto loadResult = GraphLoader::load(graph.get(), fs::Path(graphDir));
        if (!loadResult) {
            throw std::runtime_error(loadResult.error().fmtMessage());
        }

        transaction.emplace(graph->openTransaction());
        reader.emplace(transaction->readGraph());
        view = &reader->getView();
    }

    mlir::MLIRContext* context = module.getContext();
    mlir::OwningOpRef<mlir::ModuleOp> nlModule = mlir::ModuleOp::create(mlir::UnknownLoc::get(context));

    DBLowering lowering(context, view);
    lowering.lower(mainFunction, *nlModule);

    mlir::ModuleOp loweredModule = nlModule.get();
    IRModuleInspector inspector(&loweredModule);
    inspector.dumpFunctions(std::cout);
}

}

int main(int argc, char** argv) {
    argparse::ArgumentParser parser("mlir", "1.0", argparse::default_arguments::help);
    parser.add_description("TuringDB MLIR dialects sample and assembler");

    std::vector<std::string> files;
    bool dumpCode = false;
    bool dumpLowered = false;
    bool execute = false;
    bool quiet = false;
    std::string graphDir;

    parser.add_argument("files")
        .metavar("mlir.txt")
        .nargs(argparse::nargs_pattern::any)
        .store_into(files)
        .help("Textual MLIR input files to assemble");

    parser.add_argument("-d", "--dump")
        .store_into(dumpCode)
        .help("Dump the full MLIR code of every function in the module");

    parser.add_argument("-dump-lowered")
        .store_into(dumpLowered)
        .help("Lower the db-dialect 'main' to the nl dialect and dump it (no execution)");

    parser.add_argument("-exec")
        .store_into(execute)
        .help("Execute the module's main function with the NLInterpreter");

    parser.add_argument("-graph")
        .metavar("path")
        .store_into(graphDir)
        .help("Graph directory to load and execute against (requires -exec)");

    parser.add_argument("-quiet")
        .store_into(quiet)
        .help("Count output rows instead of printing them (for benchmarking)");

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

        if (dumpLowered) {
            dumpLoweredModule(mainMod, graphDir);
        }

        if (execute) {
            executeModule(mainMod, graphDir, quiet);
        }
    } catch (const std::exception& e) {
        std::cerr << e.what() << "\n";
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}
