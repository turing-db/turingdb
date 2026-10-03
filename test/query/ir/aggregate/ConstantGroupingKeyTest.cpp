#include <gtest/gtest.h>

#include <stdint.h>

#include <algorithm>
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
#include "columns/ColumnOptVector.h"
#include "columns/ColumnVector.h"
#include "iterators/ChunkConfig.h"
#include "list/ListElementView.h"
#include "list/ListView.h"
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
#include "NLOps.h"
#include "NLOutputSink.h"
#include "StorageDialect.h"

#include "TuringTest.h"

using namespace db;
using namespace turing::test;

namespace {

// Collects the (key, count) rows a grouped count emits, the key a nullable f64 chunk -
// what a scalar function over a constant is read as once it has been laid out over rows.
class KeyedCountSink : public NLOutputSink {
public:
    using Row = std::pair<std::optional<double>, uint64_t>;

    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 2u);

        const auto* keys = dynamic_cast<const ColumnOptVector<double>*>(chunks[0]);
        const auto* counts = dynamic_cast<const ColumnVector<uint64_t>*>(chunks[1]);
        ASSERT_NE(keys, nullptr);
        ASSERT_NE(counts, nullptr);
        ASSERT_EQ(keys->size(), counts->size());

        const auto& keyRaw = keys->getRaw();
        const auto& countRaw = counts->getRaw();
        for (size_t row = offset; row < offset + rowCount; row++) {
            _rows.push_back({keyRaw[row], countRaw[row]});
        }
    }

    void sortedRows(std::vector<Row>& rows) const {
        rows = _rows;
        std::sort(rows.begin(), rows.end());
    }

private:
    std::vector<Row> _rows;
};

// The collect sibling: the same key chunk beside the per-group list cell.
class KeyedInt64ListSink : public NLOutputSink {
public:
    using Row = std::pair<std::optional<double>, std::vector<int64_t>>;

    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 2u);

        const auto* keys = dynamic_cast<const ColumnOptVector<double>*>(chunks[0]);
        const auto* lists = dynamic_cast<const ColumnVector<ListView>*>(chunks[1]);
        ASSERT_NE(keys, nullptr);
        ASSERT_NE(lists, nullptr);
        ASSERT_EQ(keys->size(), lists->size());

        const auto& keyRaw = keys->getRaw();
        const auto& listRaw = lists->getRaw();
        for (size_t row = offset; row < offset + rowCount; row++) {
            std::vector<int64_t> scores;
            for (const ListElementView& element : listRaw[row]) {
                scores.push_back(element.getAs<int64_t>());
            }

            _rows.push_back({keyRaw[row], scores});
        }
    }

    void sortedRows(std::vector<Row>& rows) const {
        rows = _rows;
        std::sort(rows.begin(), rows.end());
    }

private:
    std::vector<Row> _rows;
};

// RETURN toFloat(5), count(*): one grouping key, computed from a constant alone, and a
// count anchored on another constant. No relation drives the projection, so the rows the
// group is folded over are the single row the constants stand for.
constexpr const char* constantKeyAloneProgram = R"mlir(
func.func @main() {
  %five = db.constant(5 : i64)
  %key = db.to_float(%five) : (!db.column<i64>) -> !db.column<none>
  %one = db.constant(1 : i64)
  %gkey, %n = db.group_aggregate(%key, %one) keys 1 aggregates [count_rows] : (!db.column<none>, !db.column<i64>) -> (!db.column<none>, !db.column<ui64>)
  db.output(%gkey, %n) : !db.column<none>, !db.column<ui64>
  return
}
)mlir";

// MATCH (a) RETURN toFloat(10), count(*): the same key beside a scan, so the rows it
// stands for are the scanned nodes and the one group tallies every one of them.
constexpr const char* constantKeyOverScanProgram = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %ten = db.constant(10 : i64)
  %key = db.to_float(%ten) : (!db.column<i64>) -> !db.column<none>
  %gkey, %n = db.group_aggregate(%key, %a) keys 1 aggregates [count_rows] : (!db.column<none>, !db.column<!storage.node_id>) -> (!db.column<none>, !db.column<ui64>)
  db.output(%gkey, %n) : !db.column<none>, !db.column<ui64>
  return
}
)mlir";

// MATCH (a) RETURN toFloat(10), collect(a.score): the collect reads the same constant key.
constexpr const char* constantKeyCollectProgram = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %ten = db.constant(10 : i64)
  %key = db.to_float(%ten) : (!db.column<i64>) -> !db.column<none>
  %score = db.get_node_properties(%a, "score") : (!db.column<!storage.node_id>) -> !db.column<none>
  %gkey, %scores = db.collect(%key, %score) keys 1 : (!db.column<none>, !db.column<none>) -> (!db.column<none>, !db.column<!storage.list<none>>)
  db.output(%gkey, %scores) : !db.column<none>, !db.column<!storage.list<none>>
  return
}
)mlir";

}

// A grouping key computed from constants alone. The chunk such a computation produces
// holds one value standing for every row rather than one per row, and the group
// assignment reads a row of every key per row of the step: without a layout over the
// rows it stands for, the key serializer reads that single cell as if it were a vector.
class ConstantGroupingKeyTest : public TuringTest {
protected:
    void initialize() override {
        _jobSystem = std::make_unique<JobSystem>();
        _jobSystem->init();
    }

    void terminate() override {
        _jobSystem->terminate();
    }

    // Four nodes carrying a "score" (Int64), one of them without, so a collect over the
    // scores drops a null and a count over the nodes tallies four.
    std::unique_ptr<Graph> buildScoreGraph() {
        auto graph = Graph::create();

        auto change = graph->newChange();
        auto* commitBuilder = change->access().getTip();
        auto& builder = commitBuilder->newBuilder();
        auto& metadata = builder.getMetadata();

        metadata.getOrCreateLabel("0");
        const PropertyTypeID scoreID = metadata.getOrCreatePropertyType("score", ValueType::Int64)._id;

        const LabelSet labelset = LabelSet::fromList({0});
        const NodeID first = builder.addNode(labelset);
        const NodeID second = builder.addNode(labelset);
        const NodeID third = builder.addNode(labelset);
        builder.addNode(labelset);

        builder.addNodeProperty<types::Int64>(first, scoreID, 10);
        builder.addNodeProperty<types::Int64>(second, scoreID, 20);
        builder.addNodeProperty<types::Int64>(third, scoreID, 30);

        const auto submitResult = change->access().submit(*_jobSystem);
        EXPECT_TRUE(submitResult);

        return graph;
    }

    void runLoweredProgram(const char* programText, const GraphView& view, NLOutputSink& sink) {
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
        NLInterpreter interpreter(*nlModule, &view, &sink, &memory, ChunkConfig::CHUNK_SIZE);
        interpreter.run();
    }

    std::unique_ptr<JobSystem> _jobSystem;
};

// The single row the constants are: one group, keyed 5.0, tallying that row.
TEST_F(ConstantGroupingKeyTest, groupsOnAConstantKeyWithNoRelation) {
    auto graph = buildScoreGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    KeyedCountSink sink;
    runLoweredProgram(constantKeyAloneProgram, reader.getView(), sink);

    std::vector<KeyedCountSink::Row> rows;
    sink.sortedRows(rows);

    const std::vector<KeyedCountSink::Row> expected {{5.0, 1}};
    EXPECT_EQ(rows, expected);
}

// The key stands for every row of the scan, so the four nodes fall in one group.
TEST_F(ConstantGroupingKeyTest, groupsOnAConstantKeyOverAScan) {
    auto graph = buildScoreGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    KeyedCountSink sink;
    runLoweredProgram(constantKeyOverScanProgram, reader.getView(), sink);

    std::vector<KeyedCountSink::Row> rows;
    sink.sortedRows(rows);

    const std::vector<KeyedCountSink::Row> expected {{10.0, 4}};
    EXPECT_EQ(rows, expected);
}

// The collect groups the same way, gathering the three present scores into one list.
TEST_F(ConstantGroupingKeyTest, collectsOnAConstantKeyOverAScan) {
    auto graph = buildScoreGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    KeyedInt64ListSink sink;
    runLoweredProgram(constantKeyCollectProgram, reader.getView(), sink);

    std::vector<KeyedInt64ListSink::Row> rows;
    sink.sortedRows(rows);

    const std::vector<KeyedInt64ListSink::Row> expected {{10.0, {10, 20, 30}}};
    EXPECT_EQ(rows, expected);
}
