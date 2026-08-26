#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <vector>

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/MLIRContext.h"
#include "mlir/Parser/Parser.h"

#include "Graph.h"
#include "JobSystem.h"
#include "columns/ColumnIDs.h"
#include "columns/ColumnVector.h"
#include "datapart/EdgeRecord.h"
#include "iterators/ChunkConfig.h"
#include "metadata/LabelSet.h"
#include "metadata/PropertyType.h"
#include "reader/GraphReader.h"
#include "versioning/Change.h"
#include "versioning/CommitBuilder.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"
#include "writers/DataPartBuilder.h"
#include "writers/MetadataBuilder.h"

#include "DBDialect.h"
#include "DBLowering.h"
#include "LocalMemory.h"
#include "NLDialect.h"
#include "StorageDialect.h"
#include "NLInterpreter.h"
#include "NLOps.h"
#include "NLOutputSink.h"

#include "StringRowSink.h"

#include "TuringTest.h"

using namespace db;
using namespace turing::test;

namespace {

const char* const shortestPathProgram = R"mlir(
func.func @main() {
  %a = db.const_scan_nodes([0]) : !db.column<!storage.node_id>
  %b = db.const_scan_nodes([2]) : !db.column<!storage.node_id>
  %dist, %path = db.shortest_path(%a, %b) weight "weight"
                   : (!db.column<!storage.node_id>, !db.column<!storage.node_id>)
                     -> (!db.column<none>, !db.column<!storage.path>)
  db.output(%dist, %path) : !db.column<none>, !db.column<!storage.path>
  return
}
)mlir";

const char* const unreachableProgram = R"mlir(
func.func @main() {
  %a = db.const_scan_nodes([3]) : !db.column<!storage.node_id>
  %b = db.const_scan_nodes([0]) : !db.column<!storage.node_id>
  %dist, %path = db.shortest_path(%a, %b) weight "weight"
                   : (!db.column<!storage.node_id>, !db.column<!storage.node_id>)
                     -> (!db.column<none>, !db.column<!storage.path>)
  db.output(%dist, %path) : !db.column<none>, !db.column<!storage.path>
  return
}
)mlir";

}

class ShortestPathLoweringTest : public TuringTest {
protected:
    void initialize() override {
        _jobSystem = std::make_unique<JobSystem>();
        _jobSystem->init();
    }

    void terminate() override {
        _jobSystem->terminate();
    }

    // A weighted graph: A(0) -[10]-> B(1) -[20]-> C(2) -[50]-> D(3), plus a direct
    // A -[50]-> C. The cheapest A -> C is A -> B -> C at 30, beating the direct edge; D
    // has no out-edge, so nothing is reachable from it.
    std::unique_ptr<Graph> buildWeightedGraph() {
        auto graph = Graph::create();

        auto change = graph->newChange();
        auto* commitBuilder = change->access().getTip();
        auto& builder = commitBuilder->newBuilder();
        auto& metadata = builder.getMetadata();

        metadata.getOrCreateLabel("0");
        metadata.getOrCreateEdgeType("0");
        const PropertyTypeID weightID = metadata.getOrCreatePropertyType("weight", ValueType::Int64)._id;

        const LabelSet labelset = LabelSet::fromList({0});
        const NodeID nodeA = builder.addNode(labelset);
        const NodeID nodeB = builder.addNode(labelset);
        const NodeID nodeC = builder.addNode(labelset);
        const NodeID nodeD = builder.addNode(labelset);

        // The reference addEdge returns points into the builder's edge vector, so the
        // weight must be attached before the next addEdge can move it.
        const EdgeRecord& edgeAB = builder.addEdge(0, nodeA, nodeB);
        builder.addEdgeProperty<types::Int64>(edgeAB, weightID, 10);

        const EdgeRecord& edgeBC = builder.addEdge(0, nodeB, nodeC);
        builder.addEdgeProperty<types::Int64>(edgeBC, weightID, 20);

        const EdgeRecord& edgeAC = builder.addEdge(0, nodeA, nodeC);
        builder.addEdgeProperty<types::Int64>(edgeAC, weightID, 50);

        const EdgeRecord& edgeCD = builder.addEdge(0, nodeC, nodeD);
        builder.addEdgeProperty<types::Int64>(edgeCD, weightID, 50);

        const auto submitResult = change->access().submit(*_jobSystem);
        EXPECT_TRUE(submitResult);

        return graph;
    }

    void runLoweredProgram(const char* programText,
                           const GraphView& view,
                           NLOutputSink& sink,
                           size_t chunkSize = ChunkConfig::CHUNK_SIZE) {
        mlir::MLIRContext context;
        context.getOrLoadDialect<mlir::func::FuncDialect>();
        context.getOrLoadDialect<mlir::storage::Storage>();
        context.getOrLoadDialect<mlir::db::DB>();
        context.getOrLoadDialect<mlir::nl::NL>();

        const mlir::ParserConfig parserConfig(&context);
        mlir::OwningOpRef<mlir::ModuleOp> dbModule = mlir::parseSourceString<mlir::ModuleOp>(programText, parserConfig);
        ASSERT_TRUE(dbModule);

        const mlir::func::FuncOp dbFunction = dbModule->lookupSymbol<mlir::func::FuncOp>("main");
        ASSERT_TRUE(dbFunction);

        mlir::OwningOpRef<mlir::ModuleOp> nlModule = mlir::ModuleOp::create(mlir::UnknownLoc::get(&context));
        DBLowering lowering(&context, &view);
        lowering.lower(dbFunction, *nlModule);

        LocalMemory memory;
        NLInterpreter interpreter(*nlModule, &view, &sink, &memory, chunkSize);
        interpreter.run();
    }

    std::unique_ptr<JobSystem> _jobSystem;
};

TEST_F(ShortestPathLoweringTest, findsCheapestMultiHopPath) {
    auto graph = buildWeightedGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    StringRowSink sink;
    runLoweredProgram(shortestPathProgram, reader.getView(), sink);

    // A -> B -> C costs 10 + 20 = 30, beating the direct A -> C edge at 50. The path is
    // stored target-first as alternating node/edge IDs: C(2), edge B->C(1), B(1),
    // edge A->B(0), A(0).
    const std::vector<StringRowSink::Row> expected {{"30", "2, 1, 1, 0, 0"}};

    std::vector<StringRowSink::Row> rows;
    sink.sortedRows(rows);
    EXPECT_EQ(rows, expected);
}

TEST_F(ShortestPathLoweringTest, emitsNoRowWhenTargetUnreachable) {
    auto graph = buildWeightedGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    StringRowSink sink;
    runLoweredProgram(unreachableProgram, reader.getView(), sink);

    // D has no out-edge, so A is unreachable from it and the search emits nothing.
    std::vector<StringRowSink::Row> rows;
    sink.sortedRows(rows);
    EXPECT_TRUE(rows.empty());
}
