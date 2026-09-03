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

// A db program that runs the weighted search from the given source node set to the given
// target node set, both as const-scan node-ID lists (e.g. "0" or "0, 1"). The two scans are
// distinct patterns, so they lower to sibling loops, one shortest_path_update spliced into
// each. The distance column is a none placeholder resolved to the weight's value type.
std::string shortestPathProgram(const std::string& sources, const std::string& targets) {
    return
        "func.func @main() {\n"
        "  %a = db.const_scan_nodes([" + sources + "]) : !db.column<!storage.node_id>\n"
        "  %b = db.const_scan_nodes([" + targets + "]) : !db.column<!storage.node_id>\n"
        "  %dist, %path = db.shortest_path(%a, %b) weight \"weight\"\n"
        "                   : (!db.column<!storage.node_id>, !db.column<!storage.node_id>)\n"
        "                     -> (!db.column<none>, !db.column<!storage.path>)\n"
        "  db.output(%dist, %path) : !db.column<none>, !db.column<!storage.path>\n"
        "  return\n"
        "}\n";
}

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

    // A directed weighted graph over seven nodes A(0)..G(6), with an Int64 `weight`:
    //
    //   A -1-> B -2-> C -1-> D -1-> E -2-> F
    //   A -4-> C          C -3-> E
    //   B -7-> D          G -1-> A
    //
    // The one-way edges make several cheapest paths run against the grain of a tempting
    // direct edge: A->C is 3 via B (not the direct 4), A->D is 4 (A->B->C->D), A->E is 5
    // (A->B->C->D->E). F is only reachable through E; G points into the graph but nothing
    // points back, so it is reachable from nothing.
    //
    // Edge IDs number the out-edges per source node in node order, then by target within a
    // source, so: A->B 0, A->C 1, B->C 2, B->D 3, C->D 4, C->E 5, D->E 6, E->F 7, G->A 8.
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
        const NodeID nodeE = builder.addNode(labelset);
        const NodeID nodeF = builder.addNode(labelset);
        const NodeID nodeG = builder.addNode(labelset);

        // The reference addEdge returns points into the builder's edge vector, so the
        // weight must be attached before the next addEdge can move it. Edges are added
        // grouped by source node so the committed edge IDs are the ones documented above.
        addWeightedEdge(builder, weightID, nodeA, nodeB, 1);
        addWeightedEdge(builder, weightID, nodeA, nodeC, 4);
        addWeightedEdge(builder, weightID, nodeB, nodeC, 2);
        addWeightedEdge(builder, weightID, nodeB, nodeD, 7);
        addWeightedEdge(builder, weightID, nodeC, nodeD, 1);
        addWeightedEdge(builder, weightID, nodeC, nodeE, 3);
        addWeightedEdge(builder, weightID, nodeD, nodeE, 1);
        addWeightedEdge(builder, weightID, nodeE, nodeF, 2);
        addWeightedEdge(builder, weightID, nodeG, nodeA, 1);

        const auto submitResult = change->access().submit(*_jobSystem);
        EXPECT_TRUE(submitResult);

        return graph;
    }

    // A -> B -> C plus a direct A -> C, weighted by a property of the given value type, so
    // the value-type dispatch in the executor is exercised for each supported weight type.
    // The cheapest A -> C is A -> B -> C when weightAB + weightBC < weightAC.
    template <typename WeightType>
    std::unique_ptr<Graph> buildTriangleGraph(ValueType valueType,
                                              typename WeightType::Primitive weightAB,
                                              typename WeightType::Primitive weightBC,
                                              typename WeightType::Primitive weightAC) {
        auto graph = Graph::create();

        auto change = graph->newChange();
        auto* commitBuilder = change->access().getTip();
        auto& builder = commitBuilder->newBuilder();
        auto& metadata = builder.getMetadata();

        metadata.getOrCreateLabel("0");
        metadata.getOrCreateEdgeType("0");
        const PropertyTypeID weightID = metadata.getOrCreatePropertyType("weight", valueType)._id;

        const LabelSet labelset = LabelSet::fromList({0});
        const NodeID nodeA = builder.addNode(labelset);
        const NodeID nodeB = builder.addNode(labelset);
        const NodeID nodeC = builder.addNode(labelset);

        const EdgeRecord& edgeAB = builder.addEdge(0, nodeA, nodeB);
        builder.addEdgeProperty<WeightType>(edgeAB, weightID, weightAB);

        const EdgeRecord& edgeAC = builder.addEdge(0, nodeA, nodeC);
        builder.addEdgeProperty<WeightType>(edgeAC, weightID, weightAC);

        const EdgeRecord& edgeBC = builder.addEdge(0, nodeB, nodeC);
        builder.addEdgeProperty<WeightType>(edgeBC, weightID, weightBC);

        const auto submitResult = change->access().submit(*_jobSystem);
        EXPECT_TRUE(submitResult);

        return graph;
    }

    void runLoweredProgram(const std::string& programText,
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

    // Runs the search over the given graph, returning the sorted result rows.
    std::vector<StringRowSink::Row> run(const Graph& graph,
                                        const std::string& sources,
                                        const std::string& targets) {
        const FrozenCommitTx transaction = graph.openTransaction();
        const GraphReader reader = transaction.readGraph();

        StringRowSink sink;
        runLoweredProgram(shortestPathProgram(sources, targets), reader.getView(), sink);

        std::vector<StringRowSink::Row> rows;
        sink.sortedRows(rows);
        return rows;
    }

    std::unique_ptr<JobSystem> _jobSystem;

private:
    static void addWeightedEdge(DataPartBuilder& builder,
                                PropertyTypeID weightID,
                                NodeID source,
                                NodeID target,
                                int64_t weight) {
        const EdgeRecord& edge = builder.addEdge(0, source, target);
        builder.addEdgeProperty<types::Int64>(edge, weightID, weight);
    }
};

TEST_F(ShortestPathLoweringTest, findsCheapestTwoHopPath) {
    const std::unique_ptr<Graph> graph = buildWeightedGraph();

    // A -> B -> C = 1 + 2 = 3 beats the direct A -> C edge at 4. Path stored target-first
    // as alternating node/edge IDs: C(2), edge B->C(2), B(1), edge A->B(0), A(0).
    const std::vector<StringRowSink::Row> expected {{"3", "2, 2, 1, 0, 0"}};
    EXPECT_EQ(run(*graph, "0", "2"), expected);
}

TEST_F(ShortestPathLoweringTest, findsCheapestThreeHopPath) {
    const std::unique_ptr<Graph> graph = buildWeightedGraph();

    // A -> B -> C -> D = 1 + 2 + 1 = 4 beats A -> C -> D (5) and A -> B -> D (8).
    const std::vector<StringRowSink::Row> expected {{"4", "3, 4, 2, 2, 1, 0, 0"}};
    EXPECT_EQ(run(*graph, "0", "3"), expected);
}

TEST_F(ShortestPathLoweringTest, findsCheapestFourHopPath) {
    const std::unique_ptr<Graph> graph = buildWeightedGraph();

    // A -> B -> C -> D -> E = 5 beats A -> C -> E (6) and A -> B -> C -> E (6).
    const std::vector<StringRowSink::Row> expected {{"5", "4, 6, 3, 4, 2, 2, 1, 0, 0"}};
    EXPECT_EQ(run(*graph, "0", "4"), expected);
}

TEST_F(ShortestPathLoweringTest, multiSourcePicksCheapest) {
    const std::unique_ptr<Graph> graph = buildWeightedGraph();

    // With both A and B as sources, D is cheapest from B (B -> C -> D = 3) rather than from
    // A (A -> B -> C -> D = 4), so the path stops at B.
    const std::vector<StringRowSink::Row> expected {{"3", "3, 4, 2, 2, 1"}};
    EXPECT_EQ(run(*graph, "0, 1", "3"), expected);
}

TEST_F(ShortestPathLoweringTest, multiTargetStopsAtNearest) {
    const std::unique_ptr<Graph> graph = buildWeightedGraph();

    // D (distance 4) is nearer than F (distance 7), so the search halts at D.
    const std::vector<StringRowSink::Row> expected {{"4", "3, 4, 2, 2, 1, 0, 0"}};
    EXPECT_EQ(run(*graph, "0", "3, 5"), expected);
}

TEST_F(ShortestPathLoweringTest, zeroLengthPathWhenSourceIsTarget) {
    const std::unique_ptr<Graph> graph = buildWeightedGraph();

    // A source that is already a target is reached at distance 0, and the path is the lone
    // node with no edges.
    const std::vector<StringRowSink::Row> expected {{"0", "0"}};
    EXPECT_EQ(run(*graph, "0", "0"), expected);
}

TEST_F(ShortestPathLoweringTest, directedEdgesAreOneWay) {
    const std::unique_ptr<Graph> graph = buildWeightedGraph();

    // A -> B exists but there is no B -> A, and nothing else reaches A from B, so A is
    // unreachable from B and the search emits nothing.
    EXPECT_TRUE(run(*graph, "1", "0").empty());
}

TEST_F(ShortestPathLoweringTest, emitsNoRowWhenTargetUnreachable) {
    const std::unique_ptr<Graph> graph = buildWeightedGraph();

    // G has no incoming edge, so it is reachable from nothing.
    EXPECT_TRUE(run(*graph, "0", "6").empty());
}

TEST_F(ShortestPathLoweringTest, handlesDoubleWeights) {
    const std::unique_ptr<Graph> graph = buildTriangleGraph<types::Double>(ValueType::Double, 1.5, 2.0, 5.0);

    // A -> B -> C = 3.5 beats the direct A -> C at 5.0, exercising the f64 search.
    const std::vector<StringRowSink::Row> expected {{"3.5", "2, 2, 1, 0, 0"}};
    EXPECT_EQ(run(*graph, "0", "2"), expected);
}

TEST_F(ShortestPathLoweringTest, handlesUnsignedWeights) {
    const std::unique_ptr<Graph> graph = buildTriangleGraph<types::UInt64>(ValueType::UInt64, 1, 2, 5);

    // A -> B -> C = 3 beats the direct A -> C at 5, exercising the ui64 search.
    const std::vector<StringRowSink::Row> expected {{"3", "2, 2, 1, 0, 0"}};
    EXPECT_EQ(run(*graph, "0", "2"), expected);
}
