#include <gtest/gtest.h>

#include <stdint.h>

#include <algorithm>
#include <array>
#include <memory>
#include <optional>
#include <span>
#include <utility>
#include <vector>

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/MLIRContext.h"
#include "mlir/Parser/Parser.h"

#include "Graph.h"
#include "JobSystem.h"
#include "columns/ColumnIDs.h"
#include "columns/ColumnOptVector.h"
#include "iterators/ChunkConfig.h"
#include "metadata/PropertyType.h"
#include "reader/GraphReader.h"
#include "versioning/Change.h"
#include "versioning/CommitBuilder.h"
#include "versioning/Transaction.h"
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

// Collects the node id tuples a product emits, and the row count of every chunk they
// arrived in. A product that laid out its whole N*M result in one step shows up as an
// oversized chunk even when the tuples themselves are right.
template <size_t Arity>
class NodeTupleSink : public NLOutputSink {
public:
    using Tuple = std::array<uint64_t, Arity>;

    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), Arity);

        std::array<const ColumnNodeIDs*, Arity> columns {};
        for (size_t index = 0; index < Arity; index++) {
            columns[index] = dynamic_cast<const ColumnNodeIDs*>(chunks[index]);
            ASSERT_NE(columns[index], nullptr);
        }

        _chunkRowCounts.push_back(rowCount);

        for (size_t row = offset; row < offset + rowCount; row++) {
            Tuple tuple {};
            for (size_t index = 0; index < Arity; index++) {
                tuple[index] = columns[index]->getRaw()[row].getValue();
            }

            _tuples.push_back(tuple);
        }
    }

    void sortedTuples(std::vector<Tuple>& tuples) const {
        tuples = _tuples;
        std::sort(tuples.begin(), tuples.end());
    }

    size_t getRowCount() const { return _tuples.size(); }

    size_t getLargestChunkRowCount() const {
        if (_chunkRowCounts.empty()) {
            return 0;
        }

        return *std::max_element(_chunkRowCounts.begin(), _chunkRowCounts.end());
    }

private:
    std::vector<Tuple> _tuples;
    std::vector<size_t> _chunkRowCounts;
};

using PairSink = NodeTupleSink<2>;
using TripleSink = NodeTupleSink<3>;

// The node-id sink's sibling for the Int64 property a projection over a product reads,
// so a property fetch that stopped being row-aligned with the product's rows is visible.
template <size_t Arity>
class TagTupleSink : public NLOutputSink {
public:
    using Tuple = std::array<std::optional<int64_t>, Arity>;

    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), Arity);

        std::array<const ColumnOptVector<int64_t>*, Arity> columns {};
        for (size_t index = 0; index < Arity; index++) {
            columns[index] = dynamic_cast<const ColumnOptVector<int64_t>*>(chunks[index]);
            ASSERT_NE(columns[index], nullptr);
        }

        _chunkRowCounts.push_back(rowCount);

        for (size_t row = offset; row < offset + rowCount; row++) {
            Tuple tuple {};
            for (size_t index = 0; index < Arity; index++) {
                tuple[index] = columns[index]->getRaw()[row];
            }

            _tuples.push_back(tuple);
        }
    }

    void sortedTuples(std::vector<Tuple>& tuples) const {
        tuples = _tuples;
        std::sort(tuples.begin(), tuples.end());
    }

    size_t getLargestChunkRowCount() const {
        if (_chunkRowCounts.empty()) {
            return 0;
        }

        return *std::max_element(_chunkRowCounts.begin(), _chunkRowCounts.end());
    }

private:
    std::vector<Tuple> _tuples;
    std::vector<size_t> _chunkRowCounts;
};

using TagSink = TagTupleSink<1>;
using TagPairSink = TagTupleSink<2>;

// The three sides on their own. A product of label scans crosses exactly what these
// yield, so the expectations are built from them rather than from the order addNode was
// called in: node ids are assigned per label set, not per insertion.
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

constexpr const char* thirdScanProgram = R"mlir(
func.func @main() {
  %t = db.scan_nodes_by_label(["T"]) : !db.column<!storage.node_id>
  db.output(%t) : !db.column<!storage.node_id>
  return
}
)mlir";

// The tags of each side on its own, the property sibling of the id scans above.
constexpr const char* leftTagScanProgram = R"mlir(
func.func @main() {
  %l = db.scan_nodes_by_label(["L"]) : !db.column<!storage.node_id>
  %lt = db.get_node_properties(%l, "tag") : (!db.column<!storage.node_id>) -> !db.column<none>
  db.output(%lt) : !db.column<none>
  return
}
)mlir";

constexpr const char* rightTagScanProgram = R"mlir(
func.func @main() {
  %r = db.scan_nodes_by_label(["R"]) : !db.column<!storage.node_id>
  %rt = db.get_node_properties(%r, "tag") : (!db.column<!storage.node_id>) -> !db.column<none>
  db.output(%rt) : !db.column<none>
  return
}
)mlir";

// MATCH (l:L), (r:R) RETURN l, r - the plain two-factor product.
constexpr const char* pairProgram = R"mlir(
func.func @main() {
  %0:2 = db.cross_product factor {
    %l = db.scan_nodes_by_label(["L"]) : !db.column<!storage.node_id>
    db.yield %l : !db.column<!storage.node_id>
  } factor {
    %r = db.scan_nodes_by_label(["R"]) : !db.column<!storage.node_id>
    db.yield %r : !db.column<!storage.node_id>
  }
  db.output(%0#0, %0#1) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

// MATCH (l:L), (r:R), (t:T) RETURN l, r, t - the cascade codegen builds for a three-way
// product, whose right factor is itself a product. The inner product's chunks feed the
// outer one, so each has to be chunked for the pair to be.
constexpr const char* tripleProgram = R"mlir(
func.func @main() {
  %0:3 = db.cross_product factor {
    %l = db.scan_nodes_by_label(["L"]) : !db.column<!storage.node_id>
    db.yield %l : !db.column<!storage.node_id>
  } factor {
    %1:2 = db.cross_product factor {
      %r = db.scan_nodes_by_label(["R"]) : !db.column<!storage.node_id>
      db.yield %r : !db.column<!storage.node_id>
    } factor {
      %t = db.scan_nodes_by_label(["T"]) : !db.column<!storage.node_id>
      db.yield %t : !db.column<!storage.node_id>
    }
    db.yield %1#0, %1#1 : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  }
  db.output(%0#0, %0#1, %0#2) : !db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

// MATCH (l:L), (r:R) RETURN l.tag, r.tag - the projection reads a property off each
// side of the product, so the fetch runs over the product's rows rather than the scans'.
constexpr const char* tagPairProgram = R"mlir(
func.func @main() {
  %0:2 = db.cross_product factor {
    %l = db.scan_nodes_by_label(["L"]) : !db.column<!storage.node_id>
    db.yield %l : !db.column<!storage.node_id>
  } factor {
    %r = db.scan_nodes_by_label(["R"]) : !db.column<!storage.node_id>
    db.yield %r : !db.column<!storage.node_id>
  }
  %lt = db.get_node_properties(%0#0, "tag") : (!db.column<!storage.node_id>) -> !db.column<none>
  %rt = db.get_node_properties(%0#1, "tag") : (!db.column<!storage.node_id>) -> !db.column<none>
  db.output(%lt, %rt) : !db.column<none>, !db.column<none>
  return
}
)mlir";

// MATCH (l:L), (r:R) RETURN l, r LIMIT 7 - a budget that runs out partway through the
// product, so it must stop mid-step and not build the rest.
constexpr const char* limitedPairProgram = R"mlir(
func.func @main() {
  %0:2 = db.cross_product factor {
    %l = db.scan_nodes_by_label(["L"]) : !db.column<!storage.node_id>
    db.yield %l : !db.column<!storage.node_id>
  } factor {
    %r = db.scan_nodes_by_label(["R"]) : !db.column<!storage.node_id>
    db.yield %r : !db.column<!storage.node_id>
  }
  %la, %lb = db.limit(%0#0, %0#1) count 7 : (!db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>)
  db.output(%la, %lb) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

}

// A cross product emits every pair of its two sides' rows. It is the one operator whose
// result is bigger than its inputs, so it is the one that has to cut its result into
// chunks rather than lay the whole thing out: a 64Ki-row side crossed with another is
// 4.29e9 rows in a single step otherwise.
//
// Every test here runs its program at chunk sizes that split the fixture in different
// places and at the production one, against a single expectation, and checks no chunk
// carried more rows than the chunk size. A product that materialized its whole result
// fails the chunk check while still emitting the right rows.
class CrossProductChunkedTest : public TuringTest {
protected:
    void initialize() override {
        _jobSystem = std::make_unique<JobSystem>();
        _jobSystem->init();
    }

    void terminate() override {
        _jobSystem->terminate();
    }

    // Nine nodes over three labels, interleaved so no side is a contiguous id range and
    // each "tag" says which node it is:
    //   L: n0, n2, n4, n6, n8   (5 nodes)
    //   R: n1, n3, n5, n7       (4 nodes)
    //   T: n1, n5               (2 nodes, both also R, so a side can overlap another)
    // L x R is 20 rows and L x R x T is 40, both far past the small chunk sizes below.
    std::unique_ptr<Graph> buildLabelledGraph() {
        auto graph = Graph::create();

        auto change = graph->newChange();
        auto* commitBuilder = change->access().getTip();
        auto& builder = commitBuilder->newBuilder();
        auto& metadata = builder.getMetadata();

        const LabelID leftLabel = metadata.getOrCreateLabel("L");
        const LabelID rightLabel = metadata.getOrCreateLabel("R");
        const LabelID thirdLabel = metadata.getOrCreateLabel("T");
        const PropertyTypeID tagID = metadata.getOrCreatePropertyType("tag", ValueType::Int64)._id;

        const LabelSet leftOnly = LabelSet::fromList({leftLabel});
        const LabelSet rightOnly = LabelSet::fromList({rightLabel});
        const LabelSet rightAndThird = LabelSet::fromList({rightLabel, thirdLabel});

        for (size_t index = 0; index < 9; index++) {
            const bool isLeft = index % 2 == 0;
            const bool isThird = index == 1 || index == 5;

            LabelSet labelset = leftOnly;
            if (!isLeft) {
                labelset = isThird ? rightAndThird : rightOnly;
            }

            const NodeID node = builder.addNode(labelset);
            builder.addNodeProperty<types::Int64>(node, tagID, static_cast<int64_t>(index));
        }

        const auto submitResult = change->access().submit(*_jobSystem);
        EXPECT_TRUE(submitResult);

        return graph;
    }

    // Parses a db-dialect program, lowers it to nl with DBLowering, and runs the lowered
    // function against the graph view at this chunk size.
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
        NodeTupleSink<1> sink;
        runLoweredProgram(programText, view, sink, ChunkConfig::CHUNK_SIZE);

        std::vector<NodeTupleSink<1>::Tuple> tuples;
        sink.sortedTuples(tuples);

        nodes.clear();
        for (const NodeTupleSink<1>::Tuple& tuple : tuples) {
            nodes.push_back(tuple[0]);
        }
    }

    // The tags one label scan yields, sorted: the property sibling of readLabelNodes.
    void readLabelTags(const char* programText, const GraphView& view, std::vector<std::optional<int64_t>>& tags) {
        TagSink sink;
        runLoweredProgram(programText, view, sink, ChunkConfig::CHUNK_SIZE);

        std::vector<TagSink::Tuple> tuples;
        sink.sortedTuples(tuples);

        tags.clear();
        for (const TagSink::Tuple& tuple : tuples) {
            tags.push_back(tuple[0]);
        }
    }

    // The three sides of the products below, each read off the graph, and the shape the
    // fixture is meant to have: five nodes crossed with four, and two for the third side.
    void readSides(const GraphView& view,
                   std::vector<uint64_t>& left,
                   std::vector<uint64_t>& right,
                   std::vector<uint64_t>& third) {
        readLabelNodes(leftScanProgram, view, left);
        readLabelNodes(rightScanProgram, view, right);
        readLabelNodes(thirdScanProgram, view, third);

        ASSERT_EQ(left.size(), 5u);
        ASSERT_EQ(right.size(), 4u);
        ASSERT_EQ(third.size(), 2u);
    }

    // Chunk sizes that cut the product in different places. Every one from 2 up makes a
    // step whose two sides multiply out past it - at 2 the sides are 2 and 2, so a
    // materialized product is 4 rows in a chunk that holds 2 - and the last is the
    // production size, which holds the whole fixture in one chunk.
    const std::vector<size_t> _chunkSizes {1, 2, 3, 4, 7, 20, ChunkConfig::CHUNK_SIZE};

    std::unique_ptr<JobSystem> _jobSystem;
};

TEST_F(CrossProductChunkedTest, emitsEveryPairInChunks) {
    auto graph = buildLabelledGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    std::vector<uint64_t> leftNodes;
    std::vector<uint64_t> rightNodes;
    std::vector<uint64_t> thirdNodes;
    readSides(reader.getView(), leftNodes, rightNodes, thirdNodes);

    std::vector<PairSink::Tuple> expected;
    for (const uint64_t left : leftNodes) {
        for (const uint64_t right : rightNodes) {
            expected.push_back({left, right});
        }
    }
    std::sort(expected.begin(), expected.end());

    for (const size_t chunkSize : _chunkSizes) {
        PairSink sink;
        runLoweredProgram(pairProgram, reader.getView(), sink, chunkSize);

        std::vector<PairSink::Tuple> tuples;
        sink.sortedTuples(tuples);
        EXPECT_EQ(tuples, expected) << "at chunk size " << chunkSize;
        EXPECT_LE(sink.getLargestChunkRowCount(), chunkSize) << "at chunk size " << chunkSize;
    }
}

TEST_F(CrossProductChunkedTest, emitsEveryTripleInChunks) {
    auto graph = buildLabelledGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    std::vector<uint64_t> leftNodes;
    std::vector<uint64_t> rightNodes;
    std::vector<uint64_t> thirdNodes;
    readSides(reader.getView(), leftNodes, rightNodes, thirdNodes);

    std::vector<TripleSink::Tuple> expected;
    for (const uint64_t left : leftNodes) {
        for (const uint64_t right : rightNodes) {
            for (const uint64_t third : thirdNodes) {
                expected.push_back({left, right, third});
            }
        }
    }
    std::sort(expected.begin(), expected.end());

    for (const size_t chunkSize : _chunkSizes) {
        TripleSink sink;
        runLoweredProgram(tripleProgram, reader.getView(), sink, chunkSize);

        std::vector<TripleSink::Tuple> tuples;
        sink.sortedTuples(tuples);
        EXPECT_EQ(tuples, expected) << "at chunk size " << chunkSize;
        EXPECT_LE(sink.getLargestChunkRowCount(), chunkSize) << "at chunk size " << chunkSize;
    }
}

TEST_F(CrossProductChunkedTest, readsAPropertyOffEachSideOfEveryPair) {
    auto graph = buildLabelledGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    std::vector<std::optional<int64_t>> leftTags;
    std::vector<std::optional<int64_t>> rightTags;
    readLabelTags(leftTagScanProgram, reader.getView(), leftTags);
    readLabelTags(rightTagScanProgram, reader.getView(), rightTags);
    ASSERT_EQ(leftTags.size(), 5u);
    ASSERT_EQ(rightTags.size(), 4u);

    std::vector<TagPairSink::Tuple> expected;
    for (const std::optional<int64_t>& left : leftTags) {
        for (const std::optional<int64_t>& right : rightTags) {
            expected.push_back({left, right});
        }
    }
    std::sort(expected.begin(), expected.end());

    for (const size_t chunkSize : _chunkSizes) {
        TagPairSink sink;
        runLoweredProgram(tagPairProgram, reader.getView(), sink, chunkSize);

        std::vector<TagPairSink::Tuple> tuples;
        sink.sortedTuples(tuples);
        EXPECT_EQ(tuples, expected) << "at chunk size " << chunkSize;
        EXPECT_LE(sink.getLargestChunkRowCount(), chunkSize) << "at chunk size " << chunkSize;
    }
}

TEST_F(CrossProductChunkedTest, stopsTheProductOnceTheLimitIsSpent) {
    auto graph = buildLabelledGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    std::vector<uint64_t> leftNodes;
    std::vector<uint64_t> rightNodes;
    std::vector<uint64_t> thirdNodes;
    readSides(reader.getView(), leftNodes, rightNodes, thirdNodes);

    for (const size_t chunkSize : _chunkSizes) {
        PairSink sink;
        runLoweredProgram(limitedPairProgram, reader.getView(), sink, chunkSize);

        EXPECT_EQ(sink.getRowCount(), 7u) << "at chunk size " << chunkSize;
        EXPECT_LE(sink.getLargestChunkRowCount(), chunkSize) << "at chunk size " << chunkSize;

        // Which seven pairs survive is chunk-size dependent - the budget takes a prefix
        // of the order the chunking happens to produce - so only that they are seven
        // distinct pairs of the product is invariant.
        std::vector<PairSink::Tuple> tuples;
        sink.sortedTuples(tuples);
        EXPECT_EQ(std::adjacent_find(tuples.begin(), tuples.end()), tuples.end())
            << "at chunk size " << chunkSize;

        for (const PairSink::Tuple& tuple : tuples) {
            const bool leftIsASideRow = std::find(leftNodes.begin(), leftNodes.end(), tuple[0]) != leftNodes.end();
            const bool rightIsASideRow = std::find(rightNodes.begin(), rightNodes.end(), tuple[1]) != rightNodes.end();
            EXPECT_TRUE(leftIsASideRow && rightIsASideRow) << "at chunk size " << chunkSize;
        }
    }
}
