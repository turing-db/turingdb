#include <stdlib.h>
#include <algorithm>
#include <iomanip>
#include <iostream>
#include <span>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include <argparse.hpp>

#include "mlir/IR/MLIRContext.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/Location.h"
#include "mlir/IR/OwningOpRef.h"
#include "mlir/Parser/Parser.h"
#include "mlir/Dialect/Func/IR/FuncOps.h"

#include "DBOps.h"
#include "NLOps.h"
#include "DBTypes.h"
#include "StorageDialect.h"
#include "DBDialectInterpreter.h"
#include "DBLowering.h"
#include "IRModuleInspector.h"
#include "NLOutputSink.h"

#include "TuringConfig.h"
#include "TuringDB.h"
#include "QueryState.h"
#include "QueryConfig.h"
#include "QueryCallbacks.h"
#include "QueryStatus.h"
#include "SystemManager.h"
#include "SystemAccessor.h"
#include "Graph.h"
#include "versioning/Transaction.h"
#include "reader/GraphReader.h"
#include "views/GraphView.h"
#include "dataframe/Dataframe.h"

#include "LocalMemory.h"
#include "TuringTime.h"
#include "Path.h"

using namespace db;

namespace {

// The exact query under benchmark: MATCH (a)-->(b)-->(c) RETURN b.name
constexpr const char* cypherQuery = "MATCH (a)-->(b)-->(c) RETURN b.name";

// The hand-authored db-dialect form of the same query. Scan a, two out-edge
// hops (the second carrying `a`), then fetch b.name and output only that column.
constexpr const char* dbDialectQuery = R"mlir(
module {
  func.func @main() {
    %a = db.scan_nodes() : !db.column<!storage.node_id>
    %a1, %e0, %et0, %b = db.get_out_edges(%a, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
    %b2, %e1, %et1, %c, %a2 = db.get_out_edges(%b, {%a1}) : (!db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>)
    %name = db.get_node_properties(%b2, "name") : (!db.column<!storage.node_id>) -> !db.column<none>
    db.output(%name) : !db.column<none>
    return
  }
}
)mlir";

// Counts result rows without materializing anything - the MLIR-side mirror of
// the pipeline's counting callback, so both paths do the same work minus the
// per-row formatting a real client would pay.
class CountingSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        _rowCount += rowCount;
    }

    size_t getRowCount() const { return _rowCount; }

private:
    size_t _rowCount {0};
};

// median of a copy-sorted vector of millisecond samples
double median(std::vector<double> samples) {
    std::sort(samples.begin(), samples.end());
    const size_t n = samples.size();
    if (n == 0) {
        return 0.0;
    } else if (n % 2 == 1) {
        return samples[n / 2];
    } else {
        return 0.5 * (samples[n / 2 - 1] + samples[n / 2]);
    }
}

// A double formatted to a fixed number of decimals, as a string.
std::string formatFixed(double value, int decimals) {
    std::ostringstream stream;
    stream << std::fixed << std::setprecision(decimals) << value;
    return stream.str();
}

// Prints a bordered ASCII table with auto-sized, left-aligned columns.
void printAsciiTable(const std::vector<std::string>& headers,
                     const std::vector<std::vector<std::string>>& rows) {
    const size_t columnCount = headers.size();

    std::vector<size_t> widths(columnCount, 0);
    for (size_t column = 0; column < columnCount; column++) {
        widths[column] = headers[column].size();
    }
    for (const auto& row : rows) {
        for (size_t column = 0; column < columnCount; column++) {
            widths[column] = std::max(widths[column], row[column].size());
        }
    }

    const auto printSeparator = [&]() {
        std::cout << "+";
        for (size_t column = 0; column < columnCount; column++) {
            std::cout << std::string(widths[column] + 2, '-') << "+";
        }
        std::cout << "\n";
    };

    const auto printRow = [&](const std::vector<std::string>& cells) {
        std::cout << "|";
        for (size_t column = 0; column < columnCount; column++) {
            const size_t padding = widths[column] - cells[column].size();
            std::cout << " " << cells[column] << std::string(padding + 1, ' ') << "|";
        }
        std::cout << "\n";
    };

    printSeparator();
    printRow(headers);
    printSeparator();
    for (const auto& row : rows) {
        printRow(row);
    }
    printSeparator();
}

// Lowers the db-dialect main function to the nl dialect and prints both the
// db (set-at-a-time) and nl (loop-nest) forms - the two implementations the
// benchmark compares. The nl form is what DBDialectInterpreter executes.
void printImplementations(mlir::ModuleOp dbModule, const GraphView& view) {
    mlir::MLIRContext* context = dbModule.getContext();
    const mlir::func::FuncOp dbFunction = dbModule.lookupSymbol<mlir::func::FuncOp>("main");

    std::cout << "==== db dialect (set-at-a-time column dataflow) ====\n";
    IRModuleInspector(&dbModule).dumpFunctions(std::cout);

    mlir::OwningOpRef<mlir::ModuleOp> nlModule = mlir::ModuleOp::create(mlir::UnknownLoc::get(context));
    DBLowering lowering(context, &view);
    lowering.lower(dbFunction, *nlModule);

    mlir::ModuleOp loweredModule = nlModule.get();
    std::cout << "==== nl dialect (lowered loop nest, what the interpreter runs) ====\n";
    IRModuleInspector(&loweredModule).dumpFunctions(std::cout);
    std::cout << "\n";
}

// Runs the current query pipeline once through TuringDB::query with a counting
// callback. Returns the wall time of the query call and fills the row count.
double runPipelineOnce(TuringDB& db,
                       const std::string& graphName,
                       LocalMemory& memory,
                       const QueryConfig& queryConfig,
                       size_t& rowCountOut) {
    size_t rowCount = 0;

    QueryCallbacks callbacks;
    callbacks.setOnOutputData([&](const Dataframe* df) {
        rowCount += df->getLogicalRowCount();
    });

    const QueryState state(graphName, &memory, &queryConfig, &callbacks, CommitHash::head(), ChangeID::head());

    const TimePoint start = Clock::now();
    const QueryStatus status = db.query(cypherQuery, state);
    const TimePoint end = Clock::now();

    if (!status.isOk()) {
        throw std::runtime_error("pipeline query failed: " + std::string(status.getError()));
    }

    rowCountOut = rowCount;
    return duration<Milliseconds>(start, end);
}

// Runs the db-dialect / nl-execution path once against the same view. Returns
// the execution-stage milliseconds (lowering + translation are reported too but
// are negligible) and fills the row count.
double runMlirOnce(mlir::ModuleOp module,
                   const GraphView& view,
                   LocalMemory& memory,
                   size_t& rowCountOut,
                   double& lowerMsOut,
                   double& translateMsOut) {
    CountingSink sink;
    DBDialectInterpreter interpreter(module, &view, &sink, &memory);
    const DBDialectInterpreter::Status status = interpreter.run();

    rowCountOut = sink.getRowCount();
    lowerMsOut = status.getLowerMilliseconds();
    translateMsOut = status.getTranslateMilliseconds();
    return status.getExecuteMilliseconds();
}

}

int main(int argc, char** argv) {
    argparse::ArgumentParser parser("query_bench");
    parser.add_description("Benchmark MATCH (a)-->(b)-->(c) RETURN b.name: current pipeline vs db-dialect MLIR path");

    std::string turingDir;
    std::string graphName;
    int iterations = 0;

    parser.add_argument("-turing-dir")
        .default_value(std::string("samples/optimuskg/turingdb.out"))
        .store_into(turingDir);
    parser.add_argument("-graph")
        .default_value(std::string("optimuskg"))
        .store_into(graphName);
    parser.add_argument("-iters")
        .default_value(3)
        .scan<'i', int>()
        .store_into(iterations);

    try {
        parser.parse_args(argc, argv);
    } catch (const std::exception& e) {
        std::cerr << e.what() << "\n" << parser;
        return EXIT_FAILURE;
    }

    try {
        // --- Set up TuringDB and load the graph once ---
        TuringConfig config;
        config.setTuringDirectory(fs::Path(turingDir));
        config.setSyncedOnDisk(false);

        TuringDB db(&config);
        db.init();

        LocalMemory memory;
        const QueryConfig& queryConfig = db.getDefaultQueryConfig();

        {
            const QueryCallbacks callbacks;
            const QueryState state("", &memory, &queryConfig, &callbacks);
            const QueryStatus loadStatus = db.query("load graph " + graphName, state);
            if (!loadStatus.isOk()) {
                throw std::runtime_error("failed to load graph '" + graphName + "': " + std::string(loadStatus.getError()));
            }
        }

        // --- Get a GraphView on the same loaded graph for the MLIR path ---
        Graph* graph = nullptr;
        {
            SystemAccessor system = db.getSystemManager().accessUnique();
            graph = system.getGraph(graphName);
        }
        if (!graph) {
            throw std::runtime_error("graph '" + graphName + "' not found after load");
        }

        const FrozenCommitTx transaction = graph->openTransaction();
        const GraphReader reader = transaction.readGraph();
        const GraphView& view = reader.getView();

        // --- Parse the db-dialect query once ---
        mlir::MLIRContext ctxt;
        ctxt.loadDialect<mlir::db::DB>();
        ctxt.loadDialect<mlir::nl::NL>();
        ctxt.loadDialect<mlir::storage::Storage>();
        ctxt.loadDialect<mlir::func::FuncDialect>();

        const mlir::ParserConfig parserConfig(&ctxt);
        mlir::OwningOpRef<mlir::ModuleOp> module =
            mlir::parseSourceString<mlir::ModuleOp>(dbDialectQuery, parserConfig);
        if (!module) {
            throw std::runtime_error("failed to parse db-dialect query");
        }

        std::cout << "Query:  " << cypherQuery << "\n";
        std::cout << "Graph:  " << graphName << " (" << turingDir << ")\n";
        std::cout << "Iters:  " << iterations << " (after 1 warmup each)\n\n";

        // --- Print the db and nl implementation code being compared ---
        printImplementations(*module, view);

        // --- Warmup: pages the graph columns into cache so both paths are ---
        // --- timed on warm memory, not first-touch disk reads. ---
        size_t warmRows = 0;
        double warmLower = 0.0;
        double warmTranslate = 0.0;
        runMlirOnce(*module, view, memory, warmRows, warmLower, warmTranslate);
        size_t warmPipelineRows = 0;
        runPipelineOnce(db, graphName, memory, queryConfig, warmPipelineRows);

        // --- Measured iterations ---
        std::vector<double> mlirExec;
        std::vector<double> pipelineTotal;
        size_t mlirRows = 0;
        size_t pipelineRows = 0;
        double lastLower = 0.0;
        double lastTranslate = 0.0;

        for (int i = 0; i < iterations; i++) {
            const double execMs = runMlirOnce(*module, view, memory, mlirRows, lastLower, lastTranslate);
            mlirExec.push_back(execMs);
            std::cout << "  [mlir]     iter " << (i + 1) << ": execution " << execMs << " ms\n";
        }
        for (int i = 0; i < iterations; i++) {
            const double totalMs = runPipelineOnce(db, graphName, memory, queryConfig, pipelineRows);
            pipelineTotal.push_back(totalMs);
            std::cout << "  [pipeline] iter " << (i + 1) << ": total " << totalMs << " ms\n";
        }

        // --- Report ---
        std::cout << "\n==== Results ====\n";
        std::cout << "Rows:  pipeline=" << pipelineRows << "  mlir=" << mlirRows;
        std::cout << (pipelineRows == mlirRows ? "  (match)\n\n" : "  (MISMATCH)\n\n");

        // Throughput uses the median time each path takes to produce all rows:
        // the pipeline's full compile+execute, and the MLIR path's
        // lowering+translation+execution (the first two are negligible).
        const double pipelineMedianMs = median(pipelineTotal);
        const double mlirMedianMs = median(mlirExec) + lastLower + lastTranslate;
        const double rows = static_cast<double>(pipelineRows);
        const double pipelineThroughput = rows / (pipelineMedianMs / 1000.0) / 1.0e6;
        const double mlirThroughput = rows / (mlirMedianMs / 1000.0) / 1.0e6;
        const double speedup = pipelineMedianMs / mlirMedianMs;

        const std::vector<std::string> headers = {"metric", "pipeline", "MLIR db dialect"};
        const std::vector<std::vector<std::string>> tableRows = {
            {"rows", std::to_string(pipelineRows), std::to_string(mlirRows)},
            {"median time (ms)", formatFixed(pipelineMedianMs, 2), formatFixed(mlirMedianMs, 2)},
            {"throughput (M rows/s)", formatFixed(pipelineThroughput, 2), formatFixed(mlirThroughput, 2)},
            {"MLIR speedup", "1.00x", formatFixed(speedup, 2) + "x"},
        };
        printAsciiTable(headers, tableRows);

    } catch (const std::exception& e) {
        std::cerr << "error: " << e.what() << "\n";
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}
