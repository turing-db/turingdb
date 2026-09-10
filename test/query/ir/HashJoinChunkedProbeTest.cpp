#include <gtest/gtest.h>

#include <stddef.h>
#include <stdint.h>

#include <algorithm>
#include <array>
#include <memory>
#include <span>
#include <vector>

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/MLIRContext.h"
#include "mlir/Parser/Parser.h"

#include "Graph.h"
#include "JobSystem.h"
#include "columns/ColumnIDs.h"
#include "iterators/ChunkConfig.h"
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
#include "NLInterpreter.h"
#include "NLOutputSink.h"
#include "StorageDialect.h"

#include "TuringTest.h"

using namespace db;
using namespace turing::test;

namespace {

// The node ids one side yields on its own, for building the expectation off the graph.
class NodeSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 1u);

        const auto* const nodes = dynamic_cast<const ColumnNodeIDs*>(chunks[0]);
        ASSERT_NE(nodes, nullptr);

        for (size_t row = offset; row < offset + rowCount; row++) {
            _nodes.push_back(nodes->getRaw()[row].getValue());
        }
    }

    void sortedNodes(std::vector<uint64_t>& nodes) const {
        nodes = _nodes;
        std::sort(nodes.begin(), nodes.end());
    }

private:
    std::vector<uint64_t> _nodes;
};

// Collects the node id pairs a join emits and the row count of every chunk they arrived
// in. A probe that laid out every match of a whole probe chunk in one step shows up as an
// oversized chunk even when the pairs themselves are right.
class PairSink : public NLOutputSink {
public:
    using Pair = std::array<uint64_t, 2>;

    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 2u);

        const auto* const left = dynamic_cast<const ColumnNodeIDs*>(chunks[0]);
        const auto* const right = dynamic_cast<const ColumnNodeIDs*>(chunks[1]);
        ASSERT_NE(left, nullptr);
        ASSERT_NE(right, nullptr);

        _chunkRowCounts.push_back(rowCount);

        for (size_t row = offset; row < offset + rowCount; row++) {
            _pairs.push_back({left->getRaw()[row].getValue(), right->getRaw()[row].getValue()});
        }
    }

    void sortedPairs(std::vector<Pair>& pairs) const {
        pairs = _pairs;
        std::sort(pairs.begin(), pairs.end());
    }

    size_t getLargestChunkRowCount() const {
        if (_chunkRowCounts.empty()) {
            return 0;
        }

        return *std::max_element(_chunkRowCounts.begin(), _chunkRowCounts.end());
    }

private:
    std::vector<Pair> _pairs;
    std::vector<size_t> _chunkRowCounts;
};

// The two sides on their own. The join pairs exactly what these yield, so the expectation
// is built from them rather than from the order addNode was called in: node ids are
// assigned per label set, not per insertion.
constexpr const char* leftScanProgram = R"mlir(
func.func @main() {
  %l = db.scan_nodes_by_label(["L"]) : !db.column<!storage.node_id>
  db.output(%l) : !db.column<!storage.node_id>
  return
}
)mlir";

constexpr const char* rightScanProgram = R"mlir(
func.func @main() {
  %r = db.scan_nodes_by_label(["R"]) : !db.column<!storage.node_id>
  db.output(%r) : !db.column<!storage.node_id>
  return
}
)mlir";

// MATCH (l:L), (r:R) WHERE l.tag = r.tag RETURN l, r, as the fusion pass leaves it. Every
// node of both sides carries tag 0, so the join is the whole product: one key group whose
// rows are every build row, and every probe row matching all of them.
constexpr const char* joinProgram = R"mlir(
func.func @main() {
  %0:4 = db.hash_join factor {
    %l = db.scan_nodes_by_label(["L"]) : !db.column<!storage.node_id>
    %lt = db.get_node_properties(%l, "tag") : (!db.column<!storage.node_id>) -> !db.column<none>
    db.yield %l, %lt : !db.column<!storage.node_id>, !db.column<none>
  } factor {
    %r = db.scan_nodes_by_label(["R"]) : !db.column<!storage.node_id>
    %rt = db.get_node_properties(%r, "tag") : (!db.column<!storage.node_id>) -> !db.column<none>
    db.yield %r, %rt : !db.column<!storage.node_id>, !db.column<none>
  } on 1, 1
  db.output(%0#0, %0#2) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

// The same join under a budget that runs out partway through one probe row's matches, so
// it has to stop mid-group and not build the rest.
constexpr const char* limitedJoinProgram = R"mlir(
func.func @main() {
  %0:4 = db.hash_join factor {
    %l = db.scan_nodes_by_label(["L"]) : !db.column<!storage.node_id>
    %lt = db.get_node_properties(%l, "tag") : (!db.column<!storage.node_id>) -> !db.column<none>
    db.yield %l, %lt : !db.column<!storage.node_id>, !db.column<none>
  } factor {
    %r = db.scan_nodes_by_label(["R"]) : !db.column<!storage.node_id>
    %rt = db.get_node_properties(%r, "tag") : (!db.column<!storage.node_id>) -> !db.column<none>
    db.yield %r, %rt : !db.column<!storage.node_id>, !db.column<none>
  } on 1, 1
  %la, %lb = db.limit(%0#0, %0#2) count 7 : (!db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>)
  db.output(%la, %lb) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

}

// A hash join emits one row per matched pair, so a key that many build rows carry makes a
// result bigger than either side - the cross product's problem, reached through a join.
// The probe has to cut that result into chunks rather than lay out every match of a probe
// chunk in one step: a side of 6345 rows sharing one key is 40e6 rows in a single step
// otherwise, which is what a reactome species join does.
//
// Every case runs at chunk sizes that split the result in different places and at the
// production one, against a single expectation, and checks no chunk carried more rows than
// the chunk size.
class HashJoinChunkedProbeTest : public TuringTest {
protected:
    void initialize() override {
        _jobSystem = std::make_unique<JobSystem>();
        _jobSystem->init();
    }

    void terminate() override {
        _jobSystem->terminate();
    }

    // Ten nodes, five under each of two labels, every one carrying tag 0. The join on the
    // tag matches every pair, so it is 25 rows, far past the small chunk sizes below.
    std::unique_ptr<Graph> buildSharedTagGraph() {
        auto graph = Graph::create();

        auto change = graph->newChange();
        auto* commitBuilder = change->access().getTip();
        auto& builder = commitBuilder->newBuilder();
        auto& metadata = builder.getMetadata();

        const LabelID leftLabel = metadata.getOrCreateLabel("L");
        const LabelID rightLabel = metadata.getOrCreateLabel("R");
        const PropertyTypeID tagID = metadata.getOrCreatePropertyType("tag", ValueType::Int64)._id;

        const LabelSet leftOnly = LabelSet::fromList({leftLabel});
        const LabelSet rightOnly = LabelSet::fromList({rightLabel});

        for (size_t index = 0; index < 10; index++) {
            const bool isLeft = index % 2 == 0;

            const NodeID node = builder.addNode(isLeft ? leftOnly : rightOnly);
            builder.addNodeProperty<types::Int64>(node, tagID, 0);
        }

        const auto submitResult = change->access().submit(*_jobSystem);
        EXPECT_TRUE(submitResult);

        return graph;
    }

    void runLoweredProgram(const char* programText,
                           const GraphView& view,
                           NLOutputSink& sink,
                           size_t chunkSize) {
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

    // The node ids one label scan yields, sorted.
    void readLabelNodes(const char* programText, const GraphView& view, std::vector<uint64_t>& nodes) {
        NodeSink sink;
        runLoweredProgram(programText, view, sink, ChunkConfig::CHUNK_SIZE);

        sink.sortedNodes(nodes);
    }

    // The two sides of the join, each read off the graph, and the shape the fixture is
    // meant to have: five nodes on each side, so 25 pairs.
    void readSides(const GraphView& view, std::vector<uint64_t>& left, std::vector<uint64_t>& right) {
        readLabelNodes(leftScanProgram, view, left);
        readLabelNodes(rightScanProgram, view, right);

        ASSERT_EQ(left.size(), 5u);
        ASSERT_EQ(right.size(), 5u);
    }

    // Chunk sizes that cut the result in different places. Every one from 2 up makes a
    // probe step whose matches multiply out past it, and the last is the production size,
    // which holds the whole fixture in one chunk.
    const std::vector<size_t> _chunkSizes {1, 2, 3, 4, 7, 25, ChunkConfig::CHUNK_SIZE};

    std::unique_ptr<JobSystem> _jobSystem;
};

TEST_F(HashJoinChunkedProbeTest, emitsEveryMatchedPairInChunks) {
    auto graph = buildSharedTagGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    std::vector<uint64_t> leftNodes;
    std::vector<uint64_t> rightNodes;
    readSides(reader.getView(), leftNodes, rightNodes);

    std::vector<PairSink::Pair> expected;
    for (const uint64_t left : leftNodes) {
        for (const uint64_t right : rightNodes) {
            expected.push_back({left, right});
        }
    }
    std::sort(expected.begin(), expected.end());

    for (const size_t chunkSize : _chunkSizes) {
        PairSink sink;
        runLoweredProgram(joinProgram, reader.getView(), sink, chunkSize);

        std::vector<PairSink::Pair> pairs;
        sink.sortedPairs(pairs);
        EXPECT_EQ(pairs, expected) << "at chunk size " << chunkSize;
        EXPECT_LE(sink.getLargestChunkRowCount(), chunkSize) << "at chunk size " << chunkSize;
    }
}

TEST_F(HashJoinChunkedProbeTest, stopsMidGroupOnALimit) {
    auto graph = buildSharedTagGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    std::vector<uint64_t> leftNodes;
    std::vector<uint64_t> rightNodes;
    readSides(reader.getView(), leftNodes, rightNodes);

    for (const size_t chunkSize : _chunkSizes) {
        PairSink sink;
        runLoweredProgram(limitedJoinProgram, reader.getView(), sink, chunkSize);

        std::vector<PairSink::Pair> pairs;
        sink.sortedPairs(pairs);

        EXPECT_EQ(pairs.size(), 7u) << "at chunk size " << chunkSize;
        EXPECT_LE(sink.getLargestChunkRowCount(), chunkSize) << "at chunk size " << chunkSize;

        for (const PairSink::Pair& pair : pairs) {
            const bool leftIsALeftNode = std::find(leftNodes.begin(), leftNodes.end(), pair[0]) != leftNodes.end();
            const bool rightIsARightNode = std::find(rightNodes.begin(), rightNodes.end(), pair[1]) != rightNodes.end();

            EXPECT_TRUE(leftIsALeftNode) << "at chunk size " << chunkSize;
            EXPECT_TRUE(rightIsARightNode) << "at chunk size " << chunkSize;
        }
    }
}
