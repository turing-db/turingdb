#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <span>
#include <tuple>
#include <utility>
#include <vector>

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/MLIRContext.h"

#include "Graph.h"
#include "JobSystem.h"
#include "ProcedureContext.h"
#include "ProcedureManager.h"
#include "columns/ColumnVector.h"
#include "iterators/ChunkConfig.h"
#include "list/ListBuffer.h"
#include "metadata/LabelSet.h"
#include "reader/GraphReader.h"
#include "versioning/Change.h"
#include "versioning/CommitBuilder.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"
#include "writers/DataPartBuilder.h"
#include "writers/MetadataBuilder.h"

#include "CypherAST.h"
#include "CypherAnalyzer.h"
#include "CypherParser.h"
#include "DBDialect.h"
#include "DBLowering.h"
#include "DBProgramGenerator.h"
#include "LocalMemory.h"
#include "NLDialect.h"
#include "NLInterpreter.h"
#include "NLOutputSink.h"
#include "StorageDialect.h"

#include "TuringTest.h"

using namespace db;
using namespace turing::test;

namespace {

// Collects the (seed, hop1, hop2, hop3) rows of a sampled path: one row per edge the
// three layers walked, which is the minibatch's computation graph.
class SampledPathSink : public NLOutputSink {
public:
    using Row = std::tuple<uint64_t, uint64_t, uint64_t, uint64_t>;

    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 4u);

        const auto* seeds = dynamic_cast<const ColumnVector<NodeID>*>(chunks[0]);
        const auto* firstHops = dynamic_cast<const ColumnVector<NodeID>*>(chunks[1]);
        const auto* secondHops = dynamic_cast<const ColumnVector<NodeID>*>(chunks[2]);
        const auto* thirdHops = dynamic_cast<const ColumnVector<NodeID>*>(chunks[3]);
        ASSERT_NE(seeds, nullptr);
        ASSERT_NE(firstHops, nullptr);
        ASSERT_NE(secondHops, nullptr);
        ASSERT_NE(thirdHops, nullptr);
        ASSERT_EQ(seeds->size(), firstHops->size());
        ASSERT_EQ(seeds->size(), secondHops->size());
        ASSERT_EQ(seeds->size(), thirdHops->size());

        const auto& seedRaw = seeds->getRaw();
        const auto& firstRaw = firstHops->getRaw();
        const auto& secondRaw = secondHops->getRaw();
        const auto& thirdRaw = thirdHops->getRaw();
        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            _rows.emplace_back(seedRaw[rowIndex].getValue(),
                               firstRaw[rowIndex].getValue(),
                               secondRaw[rowIndex].getValue(),
                               thirdRaw[rowIndex].getValue());
        }
    }

    void sortedRows(std::vector<Row>& rows) const {
        rows = _rows;
        std::sort(rows.begin(), rows.end());
    }

private:
    std::vector<Row> _rows;
};

// Collects the (seed, hop3) rows of a sampled path whose intermediate layers the
// projection drops.
class SampledFrontierSink : public NLOutputSink {
public:
    using Row = std::pair<uint64_t, uint64_t>;

    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 2u);

        const auto* seeds = dynamic_cast<const ColumnVector<NodeID>*>(chunks[0]);
        const auto* thirdHops = dynamic_cast<const ColumnVector<NodeID>*>(chunks[1]);
        ASSERT_NE(seeds, nullptr);
        ASSERT_NE(thirdHops, nullptr);
        ASSERT_EQ(seeds->size(), thirdHops->size());

        const auto& seedRaw = seeds->getRaw();
        const auto& thirdRaw = thirdHops->getRaw();
        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            _rows.emplace_back(seedRaw[rowIndex].getValue(), thirdRaw[rowIndex].getValue());
        }
    }

    void sortedRows(std::vector<Row>& rows) const {
        rows = _rows;
        std::sort(rows.begin(), rows.end());
    }

private:
    std::vector<Row> _rows;
};

// The three sample calls a 3-layer GraphSAGE minibatch is built from: each layer samples
// the neighbourhood of what the layer below it yielded, so the rows are the paths the
// aggregation would run over.
constexpr const char* threeLayerQuery =
    "MATCH (n:Seed) "
    "CALL gnn.neighbourhoodSample(n, 3, 42) YIELD tgt AS hop1 "
    "CALL gnn.neighbourhoodSample(hop1, 2, 42) YIELD tgt AS hop2 "
    "CALL gnn.neighbourhoodSample(hop2, 2, 42) YIELD tgt AS hop3 "
    "RETURN n, hop1, hop2, hop3";

}

class GnnSampleLayersTest : public TuringTest {
protected:
    void initialize() override {
        _jobSystem = std::make_unique<JobSystem>();
        _jobSystem->init();

        _procedures.init();
    }

    void terminate() override {
        _jobSystem->terminate();
    }

    // Eleven papers around a two-paper minibatch, laid out in the layers a 3-layer
    // GraphSAGE samples. No node cites more than two others, so a fan-out of two or more
    // takes the whole neighbourhood at every layer and the sample is the seed's full
    // three-hop reach whatever the seed value:
    //
    //   seeds     0 -> {2, 3}          1 -> {3, 4}      (3 cited by both seeds)
    //   layer 2   2 -> {5, 10}         3 -> {6, 7}      4 -> {7}
    //   layer 3   5 -> {8}             6 -> {8, 9}      7 -> {9}
    //
    // Node 10 cites nothing, so the path through it dies at the third layer rather than
    // reaching it - the drop a minibatch pads over.
    std::unique_ptr<Graph> buildCitationGraph() {
        auto graph = Graph::create();

        auto change = graph->newChange();
        auto* commitBuilder = change->access().getTip();
        auto& builder = commitBuilder->newBuilder();
        auto& metadata = builder.getMetadata();

        metadata.getOrCreateLabel("Paper");
        metadata.getOrCreateLabel("Seed");
        metadata.getOrCreateEdgeType("CITES");

        const LabelSet paper = LabelSet::fromList({0});
        const LabelSet seed = LabelSet::fromList({0, 1});

        const NodeID seed0 = builder.addNode(seed);
        const NodeID seed1 = builder.addNode(seed);
        const NodeID paper2 = builder.addNode(paper);
        const NodeID paper3 = builder.addNode(paper);
        const NodeID paper4 = builder.addNode(paper);
        const NodeID paper5 = builder.addNode(paper);
        const NodeID paper6 = builder.addNode(paper);
        const NodeID paper7 = builder.addNode(paper);
        const NodeID paper8 = builder.addNode(paper);
        const NodeID paper9 = builder.addNode(paper);
        const NodeID paper10 = builder.addNode(paper);

        builder.addEdge(0, seed0, paper2);
        builder.addEdge(0, seed0, paper3);
        builder.addEdge(0, seed1, paper3);
        builder.addEdge(0, seed1, paper4);

        builder.addEdge(0, paper2, paper5);
        builder.addEdge(0, paper2, paper10);
        builder.addEdge(0, paper3, paper6);
        builder.addEdge(0, paper3, paper7);
        builder.addEdge(0, paper4, paper7);

        builder.addEdge(0, paper5, paper8);
        builder.addEdge(0, paper6, paper8);
        builder.addEdge(0, paper6, paper9);
        builder.addEdge(0, paper7, paper9);

        const auto submitResult = change->access().submit(*_jobSystem);
        EXPECT_TRUE(submitResult);

        return graph;
    }

    // Runs a Cypher query the whole way down - parsed and analyzed against the procedure
    // registry, generated into db dialect, lowered and executed - which is the path a
    // chain of CALLs takes from the query text.
    void runQuery(const char* queryText,
                  Graph* graph,
                  const GraphView& view,
                  NLOutputSink& sink,
                  size_t chunkSize = ChunkConfig::CHUNK_SIZE) {
        CypherAST ast(&_procedures, queryText);

        CypherParser parser(&ast);
        parser.parse(queryText);

        CypherAnalyzer analyzer(&ast, view);
        analyzer.analyze();

        mlir::MLIRContext context;
        context.getOrLoadDialect<mlir::func::FuncDialect>();
        context.getOrLoadDialect<mlir::storage::Storage>();
        context.getOrLoadDialect<mlir::db::DB>();
        context.getOrLoadDialect<mlir::nl::NL>();

        mlir::OpBuilder builder(&context);
        mlir::OwningOpRef<mlir::ModuleOp> dbModule = mlir::ModuleOp::create(builder.getUnknownLoc());
        mlir::ModuleOp module = dbModule.get();

        DBProgramGenerator generator(&module);
        generator.generate(&ast);

        const mlir::func::FuncOp dbFunction = module.lookupSymbol<mlir::func::FuncOp>("main");
        ASSERT_TRUE(dbFunction);

        mlir::OwningOpRef<mlir::ModuleOp> nlModule = mlir::ModuleOp::create(mlir::UnknownLoc::get(&context));
        DBLowering lowering(&context, &view, &_procedures);
        lowering.lower(dbFunction, *nlModule);

        LocalMemory memory;

        Transaction transaction(graph->openTransaction());

        ProcedureContext procedureContext;
        procedureContext.setGraph(graph);
        procedureContext.setGraphView(&view);
        procedureContext.setTransaction(&transaction);
        procedureContext.setProcedures(&_procedures);
        procedureContext.setChunkSize(chunkSize);
        procedureContext.setListBuffer(&memory.listBuffer());

        NLInterpreter interpreter(*nlModule,
                                  &view,
                                  &sink,
                                  &memory,
                                  chunkSize,
                                  /*writeBuffer=*/nullptr,
                                  /*metadataBuilder=*/nullptr,
                                  &procedureContext);
        interpreter.run();
    }

    ProcedureManager _procedures;
    std::unique_ptr<JobSystem> _jobSystem;
};

// The eight paths the two seeds reach in three hops. Seed 0 loses the path through paper
// 10, which cites nothing; paper 3 is sampled from both seeds, so its subtree appears
// under each of them.
TEST_F(GnnSampleLayersTest, threeLayerSampleWalksEveryPath) {
    auto graph = buildCitationGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    SampledPathSink sink;
    runQuery(threeLayerQuery, graph.get(), reader.getView(), sink);

    std::vector<SampledPathSink::Row> rows;
    sink.sortedRows(rows);
    const std::vector<SampledPathSink::Row> expected {{0, 2, 5, 8},
                                                      {0, 3, 6, 8},
                                                      {0, 3, 6, 9},
                                                      {0, 3, 7, 9},
                                                      {1, 3, 6, 8},
                                                      {1, 3, 6, 9},
                                                      {1, 3, 7, 9},
                                                      {1, 4, 7, 9}};
    EXPECT_EQ(rows, expected);
}

// A chunk of two splits every layer's argument column, so each call is driven several
// times over a chunk of what the layer below it yielded - and the seed carried through
// all three calls must still land on the row its own path produced.
TEST_F(GnnSampleLayersTest, threeLayerSampleSurvivesChunking) {
    auto graph = buildCitationGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    SampledPathSink sink;
    runQuery(threeLayerQuery, graph.get(), reader.getView(), sink, /*chunkSize=*/2);

    std::vector<SampledPathSink::Row> rows;
    sink.sortedRows(rows);
    const std::vector<SampledPathSink::Row> expected {{0, 2, 5, 8},
                                                      {0, 3, 6, 8},
                                                      {0, 3, 6, 9},
                                                      {0, 3, 7, 9},
                                                      {1, 3, 6, 8},
                                                      {1, 3, 6, 9},
                                                      {1, 3, 7, 9},
                                                      {1, 4, 7, 9}};
    EXPECT_EQ(rows, expected);
}

// The aggregation only needs the outermost layer, so the two middle ones are carried past
// the calls that follow them and then dropped by the projection. One row per path all the
// same - the frontier keeps its duplicates, since nothing deduplicates it.
TEST_F(GnnSampleLayersTest, threeLayerSampleReturningTheFrontierAlone) {
    auto graph = buildCitationGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    SampledFrontierSink sink;
    runQuery("MATCH (n:Seed) "
             "CALL gnn.neighbourhoodSample(n, 3, 42) YIELD tgt AS hop1 "
             "CALL gnn.neighbourhoodSample(hop1, 2, 42) YIELD tgt AS hop2 "
             "CALL gnn.neighbourhoodSample(hop2, 2, 42) YIELD tgt AS hop3 "
             "RETURN n, hop3",
             graph.get(),
             reader.getView(),
             sink);

    std::vector<SampledFrontierSink::Row> rows;
    sink.sortedRows(rows);
    const std::vector<SampledFrontierSink::Row> expected {{0, 8}, {0, 8}, {0, 9}, {0, 9},
                                                          {1, 8}, {1, 9}, {1, 9}, {1, 9}};
    EXPECT_EQ(rows, expected);
}
