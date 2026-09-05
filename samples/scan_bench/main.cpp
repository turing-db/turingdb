#include <stdint.h>
#include <stdlib.h>
#include <algorithm>
#include <iomanip>
#include <iostream>
#include <memory>
#include <span>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include <argparse.hpp>

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/MLIRContext.h"
#include "mlir/IR/OwningOpRef.h"
#include "mlir/Parser/Parser.h"

#include "DBOps.h"
#include "NLOps.h"
#include "StorageDialect.h"
#include "DBDialectInterpreter.h"

#include "Graph.h"
#include "JobSystem.h"
#include "columns/ColumnIDs.h"
#include "datapart/DataPart.h"
#include "iterators/PropertyValueScan.h"
#include "metadata/LabelSet.h"
#include "metadata/PropertyType.h"
#include "properties/PropertyContainer.h"
#include "properties/PropertyManager.h"
#include "reader/GraphReader.h"
#include "versioning/Change.h"
#include "versioning/ChangeAccessor.h"
#include "versioning/CommitBuilder.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"
#include "writers/DataPartBuilder.h"
#include "writers/MetadataBuilder.h"

#include "LocalMemory.h"
#include "NLOutputSink.h"
#include "TuringTime.h"

using namespace db;

namespace {

// The score of a node: value v is carried by roughly one node in 2^(v+1), so a needle
// picks a selectivity instead of the benchmark needing one property per selectivity.
int64_t scoreOf(size_t node) {
    int64_t score = 0;
    for (size_t bits = node; (bits & 1) == 1 && score < 30; bits >>= 1) {
        score++;
    }

    return score;
}

// The double property mirrors the score distribution so the Double lane is measured on
// the same selectivities as the Int64 one.
double ratioOf(size_t node) {
    return static_cast<double>(scoreOf(node)) / 4.0;
}

// Counts result rows without materializing them.
class CountingSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        _rowCount += rowCount;
    }

    size_t getRowCount() const { return _rowCount; }
    void reset() { _rowCount = 0; }

private:
    size_t _rowCount {0};
};

// Median of a vector of millisecond samples, sorting it in place.
double median(std::vector<double>& samples) {
    std::sort(samples.begin(), samples.end());
    const size_t count = samples.size();

    if (count == 0) {
        return 0.0;
    } else if (count % 2 == 1) {
        return samples[count / 2];
    } else {
        return 0.5 * (samples[count / 2 - 1] + samples[count / 2]);
    }
}

// Writes a double formatted to a fixed number of decimals.
void formatFixed(std::string& text, double value, int decimals) {
    std::ostringstream stream;
    stream << std::fixed << std::setprecision(decimals) << value;
    text = stream.str();
}

// Appends a table cell holding a fixed-decimal value and an optional unit suffix.
void appendFixed(std::vector<std::string>& row, double value, int decimals, std::string_view suffix = {}) {
    std::string& cell = row.emplace_back();

    formatFixed(cell, value, decimals);
    cell.append(suffix);
}

// Prints a bordered ASCII table with auto-sized, left-aligned columns.
void printAsciiTable(const std::vector<std::string>& headers,
                     const std::vector<std::vector<std::string>>& rows) {
    const size_t columnCount = headers.size();

    std::vector<size_t> widths(columnCount, 0);
    for (size_t column = 0; column < columnCount; column++) {
        widths[column] = headers[column].size();
    }
    for (const std::vector<std::string>& row : rows) {
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
            std::cout << " " << cells[column] << std::string(widths[column] - cells[column].size() + 1, ' ') << "|";
        }
        std::cout << "\n";
    };

    printSeparator();
    printRow(headers);
    printSeparator();
    for (const std::vector<std::string>& row : rows) {
        printRow(row);
    }
    printSeparator();
}

// MATCH (n) WHERE n.score = <needle> RETURN n, as codegen emits it before the passes run:
// a full node scan, a property read per row, and a filter. This is what main executes.
std::string unfusedModule(const std::string& property, int64_t needle) {
    return R"mlir(module {
  func.func @main() {
    %0 = db.scan_nodes() : !db.column<!storage.node_id>
    %1 = db.get_node_properties(%0, ")mlir" + property + R"mlir(") : (!db.column<!storage.node_id>) -> !db.column<none>
    %2 = db.constant()mlir" + std::to_string(needle) + R"mlir( : i64)
    %3 = db.eq %1, %2 : (!db.column<none>, !db.column<i64>) -> !db.column<!storage.bool>
    %4 = db.filter(%3, {%0}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>) -> !db.column<!storage.node_id>
    db.output(%4) names ["n"] : !db.column<!storage.node_id>
    return
  }
})mlir";
}

// The same query once FuseScanByPropertyValue has folded the filter into the scan.
std::string fusedModule(const std::string& property, int64_t needle) {
    return R"mlir(module {
  func.func @main() {
    %0 = db.scan_nodes_by_property_value(")mlir" + property + R"mlir(", )mlir" + std::to_string(needle) + R"mlir( : i64) : !db.column<!storage.node_id>
    db.output(%0) names ["n"] : !db.column<!storage.node_id>
    return
  }
})mlir";
}

// Builds one data part of `nodeCount` nodes carrying the two numerical properties.
void buildGraph(Graph& graph,
                JobSystem& jobSystem,
                size_t nodeCount,
                PropertyTypeID& scoreIDOut,
                PropertyTypeID& ratioIDOut) {
    std::unique_ptr<Change> change = graph.newChange();
    CommitBuilder* commit = change->access().getTip();
    DataPartBuilder& builder = commit->newBuilder();
    MetadataBuilder& metadata = builder.getMetadata();

    const LabelID nodeLabel = metadata.getOrCreateLabel("Node");
    scoreIDOut = metadata.getOrCreatePropertyType("score", ValueType::Int64)._id;
    ratioIDOut = metadata.getOrCreatePropertyType("ratio", ValueType::Double)._id;

    LabelSet labelset;
    labelset.set(nodeLabel);

    for (size_t node = 0; node < nodeCount; node++) {
        const NodeID nodeID = builder.addNode(labelset);

        builder.addNodeProperty<types::Int64>(nodeID, scoreIDOut, scoreOf(node));
        builder.addNodeProperty<types::Double>(nodeID, ratioIDOut, ratioOf(node));
    }

    if (!change->access().submit(jobSystem)) {
        throw std::runtime_error("failed to submit the generated graph");
    }
}

// Runs one db-dialect module through the interpreter, returning execution milliseconds.
double runModuleOnce(mlir::MLIRContext& context, const std::string& source, const GraphView& view, LocalMemory& memory, size_t& rowCountOut) {
    const mlir::ParserConfig parserConfig(&context);
    mlir::OwningOpRef<mlir::ModuleOp> module = mlir::parseSourceString<mlir::ModuleOp>(source, parserConfig);
    if (!module) {
        throw std::runtime_error("failed to parse module:\n" + source);
    }

    CountingSink sink;
    DBDialectInterpreter interpreter(*module, &view, &sink, &memory);
    const DBDialectInterpreter::Status status = interpreter.run();

    rowCountOut = sink.getRowCount();
    return status.getExecuteMilliseconds();
}

// The property column of every data part, as the scan kernels see it.
template <SupportedType T>
struct PropertyRun {
    std::span<const typename T::Primitive> _values;
    std::span<const EntityID> _ids;
    bool _sorted {false};
};

template <SupportedType T>
void collectRuns(const GraphView& view, PropertyTypeID property, std::vector<PropertyRun<T>>& runs) {
    runs.clear();

    const DataPartSpan parts = view.dataparts();
    for (const WeakArc<DataPart>& part : parts) {
        const PropertyManager& properties = part->nodeProperties();
        const TypedPropertyContainer<T>* container = properties.tryGetContainer<T>(property);
        if (!container) {
            continue;
        }

        runs.push_back(PropertyRun<T> {._values = container->all(),
                                       ._ids = std::span<const EntityID>(container->ids()),
                                       ._sorted = container->isSorted()});
    }
}

// Which of the three kernels a timing run measures.
enum class ScanKernel {
    Scalar,
    Vectorised,
    DenseIDs,
};

// True when every run holds first, first + 1, ..., the shape the dense-ID kernel needs.
template <SupportedType T>
bool runsHaveDenseIDs(const std::vector<PropertyRun<T>>& runs) {
    for (const PropertyRun<T>& run : runs) {
        const std::span<const EntityID> ids = run._ids;
        const bool dense = !ids.empty() && ids.back().getValue() - ids.front().getValue() + 1 == ids.size();

        if (!run._sorted || !dense) {
            return false;
        }
    }

    return true;
}

// Times one kernel over every run of the column.
template <SupportedType T>
double runKernelOnce(const std::vector<PropertyRun<T>>& runs,
                     const typename T::Primitive& needle,
                     ScanKernel kernel,
                     std::vector<NodeID>& hits,
                     size_t& matchCountOut) {
    size_t matches = 0;

    const TimePoint start = Clock::now();
    for (const PropertyRun<T>& run : runs) {
        const size_t rows = run._values.size();
        if (rows == 0) {
            continue;
        }

        switch (kernel) {
            case ScanKernel::Scalar:
                matches += PropertyValueScan::equalScalar(run._values.data(), run._ids.data(), rows, needle, hits.data());
            break;
            case ScanKernel::Vectorised:
                matches += PropertyValueScan::equalVectorised(run._values.data(), run._ids.data(), rows, needle, hits.data());
            break;
            case ScanKernel::DenseIDs:
                matches += PropertyValueScan::equalDenseIDs(run._values.data(), run._ids.front().getValue(), rows, needle, hits.data());
            break;
        }
    }
    const TimePoint end = Clock::now();

    matchCountOut = matches;
    return duration<Milliseconds>(start, end);
}

}

int main(int argc, char** argv) {
    argparse::ArgumentParser parser("scan_bench");
    parser.add_description("Benchmark MATCH (n) WHERE n.score = <v> RETURN n: scan + filter (main) vs the fused property value scan, scalar and vectorised");

    size_t nodeCount = 0;
    int iterations = 0;

    parser.add_argument("-nodes")
        .default_value(size_t {20000000})
        .scan<'u', size_t>()
        .store_into(nodeCount);
    parser.add_argument("-iters")
        .default_value(5)
        .scan<'i', int>()
        .store_into(iterations);

    try {
        parser.parse_args(argc, argv);
    } catch (const std::exception& error) {
        std::cerr << error.what() << "\n" << parser;
        return EXIT_FAILURE;
    }

    try {
        JobSystem jobSystem;
        jobSystem.init();

        std::unique_ptr<Graph> graph = Graph::create();

        PropertyTypeID scoreID;
        PropertyTypeID ratioID;

        std::cout << "Building a graph of " << nodeCount << " nodes with an Int64 and a Double property...\n";
        const TimePoint buildStart = Clock::now();
        buildGraph(*graph, jobSystem, nodeCount, scoreID, ratioID);
        const double buildMilliseconds = duration<Milliseconds>(buildStart, Clock::now());
        std::string buildSeconds;
        formatFixed(buildSeconds, buildMilliseconds / 1000.0, 2);
        std::cout << "Built in " << buildSeconds << " s\n\n";

        const FrozenCommitTx transaction = graph->openTransaction();
        const GraphReader reader = transaction.readGraph();
        const GraphView& view = reader.getView();

        mlir::MLIRContext context;
        context.loadDialect<mlir::db::DB>();
        context.loadDialect<mlir::nl::NL>();
        context.loadDialect<mlir::storage::Storage>();
        context.loadDialect<mlir::func::FuncDialect>();

        LocalMemory memory;

        std::vector<PropertyRun<types::Int64>> scoreRuns;
        collectRuns<types::Int64>(view, scoreID, scoreRuns);

        std::vector<PropertyRun<types::Double>> ratioRuns;
        collectRuns<types::Double>(view, ratioID, ratioRuns);

        std::vector<NodeID> hits(nodeCount);

        const bool denseScoreRuns = runsHaveDenseIDs(scoreRuns);

        const std::vector<int64_t> needles {0, 3, 7, 11, 16};

        std::vector<std::vector<std::string>> endToEndRows;
        std::vector<std::vector<std::string>> kernelRows;

        for (const int64_t needle : needles) {
            const std::string unfused = unfusedModule("score", needle);
            const std::string fused = fusedModule("score", needle);

            size_t unfusedRows = 0;
            size_t fusedRows = 0;
            runModuleOnce(context, unfused, view, memory, unfusedRows);
            runModuleOnce(context, fused, view, memory, fusedRows);

            std::vector<double> unfusedSamples;
            std::vector<double> fusedSamples;
            for (int iteration = 0; iteration < iterations; iteration++) {
                unfusedSamples.push_back(runModuleOnce(context, unfused, view, memory, unfusedRows));
                fusedSamples.push_back(runModuleOnce(context, fused, view, memory, fusedRows));
            }

            const double unfusedMilliseconds = median(unfusedSamples);
            const double fusedMilliseconds = median(fusedSamples);
            const double selectivity = static_cast<double>(fusedRows) / static_cast<double>(nodeCount);

            std::vector<std::string>& endToEndRow = endToEndRows.emplace_back();
            endToEndRow.push_back(std::to_string(needle));
            appendFixed(endToEndRow, 100.0 * selectivity, 4, "%");
            endToEndRow.push_back(std::to_string(unfusedRows) + (unfusedRows == fusedRows ? "" : " MISMATCH"));
            appendFixed(endToEndRow, unfusedMilliseconds, 2);
            appendFixed(endToEndRow, fusedMilliseconds, 2);
            appendFixed(endToEndRow, unfusedMilliseconds / fusedMilliseconds, 2, "x");

            size_t scalarMatches = 0;
            size_t vectorisedMatches = 0;
            size_t denseMatches = 0;
            size_t doubleScalarMatches = 0;
            size_t doubleVectorisedMatches = 0;
            runKernelOnce<types::Int64>(scoreRuns, needle, ScanKernel::Scalar, hits, scalarMatches);
            runKernelOnce<types::Int64>(scoreRuns, needle, ScanKernel::Vectorised, hits, vectorisedMatches);

            std::vector<double> scalarSamples;
            std::vector<double> vectorisedSamples;
            std::vector<double> denseSamples;
            std::vector<double> doubleScalarSamples;
            std::vector<double> doubleVectorisedSamples;
            const double ratioNeedle = static_cast<double>(needle) / 4.0;
            for (int iteration = 0; iteration < iterations; iteration++) {
                scalarSamples.push_back(runKernelOnce<types::Int64>(scoreRuns, needle, ScanKernel::Scalar, hits, scalarMatches));
                vectorisedSamples.push_back(runKernelOnce<types::Int64>(scoreRuns, needle, ScanKernel::Vectorised, hits, vectorisedMatches));
                doubleScalarSamples.push_back(runKernelOnce<types::Double>(ratioRuns, ratioNeedle, ScanKernel::Scalar, hits, doubleScalarMatches));
                doubleVectorisedSamples.push_back(runKernelOnce<types::Double>(ratioRuns, ratioNeedle, ScanKernel::Vectorised, hits, doubleVectorisedMatches));

                if (denseScoreRuns) {
                    denseSamples.push_back(runKernelOnce<types::Int64>(scoreRuns, needle, ScanKernel::DenseIDs, hits, denseMatches));
                }
            }

            const double scalarMilliseconds = median(scalarSamples);
            const double vectorisedMilliseconds = median(vectorisedSamples);
            const double denseMilliseconds = median(denseSamples);
            const double doubleScalarMilliseconds = median(doubleScalarSamples);
            const double doubleVectorisedMilliseconds = median(doubleVectorisedSamples);

            const bool denseAgrees = denseScoreRuns && denseMatches == vectorisedMatches;

            std::vector<std::string>& kernelRow = kernelRows.emplace_back();
            kernelRow.push_back(std::to_string(needle));
            kernelRow.push_back(std::to_string(scalarMatches) + (scalarMatches == vectorisedMatches ? "" : " MISMATCH"));
            appendFixed(kernelRow, scalarMilliseconds, 2);
            appendFixed(kernelRow, vectorisedMilliseconds, 2);
            appendFixed(kernelRow, scalarMilliseconds / vectorisedMilliseconds, 2, "x");

            if (denseAgrees) {
                appendFixed(kernelRow, denseMilliseconds, 2);
                appendFixed(kernelRow, vectorisedMilliseconds / denseMilliseconds, 2, "x");
            } else {
                kernelRow.push_back("n/a");
                kernelRow.push_back("n/a");
            }

            appendFixed(kernelRow, doubleScalarMilliseconds, 2);
            appendFixed(kernelRow, doubleVectorisedMilliseconds, 2);
            appendFixed(kernelRow, doubleScalarMilliseconds / doubleVectorisedMilliseconds, 2, "x");
        }

        std::cout << "==== MATCH (n) WHERE n.score = <needle> RETURN n, end to end through the interpreter ====\n";
        printAsciiTable({"needle", "selectivity", "rows", "scan + filter (ms)", "fused scan (ms)", "speedup"}, endToEndRows);

        std::cout << "\n==== equality kernel over the same property columns, whole column per call ====\n";
        printAsciiTable({"needle",
                         "matches",
                         "Int64 scalar (ms)",
                         "Int64 vectorised (ms)",
                         "speedup",
                         "Int64 dense IDs (ms)",
                         "speedup",
                         "Double scalar (ms)",
                         "Double vectorised (ms)",
                         "speedup"},
                        kernelRows);

    } catch (const std::exception& error) {
        std::cerr << "error: " << error.what() << "\n";
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}
