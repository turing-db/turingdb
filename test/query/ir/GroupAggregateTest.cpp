#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
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
#include "columns/ColumnVector.h"
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
#include "StorageDialect.h"
#include "NLInterpreter.h"
#include "NLOps.h"
#include "NLOutputSink.h"

#include "TuringTest.h"

using namespace db;
using namespace turing::test;

namespace {

// Collects the (team, count) rows a grouped count emits: a nullable string key
// chunk and a non-null !nl.chunk<ui64> count chunk. Captures every group so a
// test can assert the whole set order-independently.
class GroupCountSink : public NLOutputSink {
public:
    using Row = std::pair<std::optional<std::string>, uint64_t>;

    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 2u);

        const auto* teams = dynamic_cast<const ColumnOptVector<std::string_view>*>(chunks[0]);
        const auto* counts = dynamic_cast<const ColumnVector<uint64_t>*>(chunks[1]);
        ASSERT_NE(teams, nullptr);
        ASSERT_NE(counts, nullptr);
        ASSERT_EQ(teams->size(), counts->size());

        const auto& teamRaw = teams->getRaw();
        const auto& countRaw = counts->getRaw();
        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            std::optional<std::string> team;
            if (teamRaw[rowIndex]) {
                team = std::string(*teamRaw[rowIndex]);
            }

            _rows.push_back({team, countRaw[rowIndex]});
        }
    }

    void sortedRows(std::vector<Row>& rows) const {
        rows = _rows;
        std::sort(rows.begin(), rows.end());
    }

private:
    std::vector<Row> _rows;
};

// Collects the (team, nullable int64) rows a grouped sum / min / max emits: a
// nullable string key chunk and a !nl.chunk<!storage.nullable<i64>> value chunk.
class GroupInt64Sink : public NLOutputSink {
public:
    using Row = std::pair<std::optional<std::string>, std::optional<int64_t>>;

    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 2u);

        const auto* teams = dynamic_cast<const ColumnOptVector<std::string_view>*>(chunks[0]);
        const auto* values = dynamic_cast<const ColumnOptVector<int64_t>*>(chunks[1]);
        ASSERT_NE(teams, nullptr);
        ASSERT_NE(values, nullptr);
        ASSERT_EQ(teams->size(), values->size());

        const auto& teamRaw = teams->getRaw();
        const auto& valueRaw = values->getRaw();
        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            std::optional<std::string> team;
            if (teamRaw[rowIndex]) {
                team = std::string(*teamRaw[rowIndex]);
            }

            _rows.push_back({team, valueRaw[rowIndex]});
        }
    }

    void sortedRows(std::vector<Row>& rows) const {
        rows = _rows;
        std::sort(rows.begin(), rows.end());
    }

private:
    std::vector<Row> _rows;
};

// Collects the (team, nullable double) rows a grouped avg emits: a nullable string
// key chunk and a !nl.chunk<!storage.nullable<f64>> value chunk.
class GroupDoubleSink : public NLOutputSink {
public:
    using Row = std::pair<std::optional<std::string>, std::optional<double>>;

    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 2u);

        const auto* teams = dynamic_cast<const ColumnOptVector<std::string_view>*>(chunks[0]);
        const auto* values = dynamic_cast<const ColumnOptVector<double>*>(chunks[1]);
        ASSERT_NE(teams, nullptr);
        ASSERT_NE(values, nullptr);
        ASSERT_EQ(teams->size(), values->size());

        const auto& teamRaw = teams->getRaw();
        const auto& valueRaw = values->getRaw();
        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            std::optional<std::string> team;
            if (teamRaw[rowIndex]) {
                team = std::string(*teamRaw[rowIndex]);
            }

            _rows.push_back({team, valueRaw[rowIndex]});
        }
    }

    void sortedRows(std::vector<Row>& rows) const {
        rows = _rows;
        std::sort(rows.begin(), rows.end());
    }

private:
    std::vector<Row> _rows;
};

// Counts appendChunks calls and total rows without materializing them, so an empty
// grouped aggregation can be shown to emit nothing.
class CountingSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        _calls++;
        _totalRows += rowCount;
    }

    size_t getCalls() const { return _calls; }
    size_t getTotalRows() const { return _totalRows; }

private:
    size_t _calls {0};
    size_t _totalRows {0};
};

// MATCH (a) RETURN a.team, count(*): group the scanned nodes by their team and
// count every node in each group. count(*) is a count (kind 0) over a never-null
// column, so the node IDs are the aggregate input.
constexpr const char* groupCountStarProgram = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %team = db.get_node_properties(%a, "team") : (!db.column<!storage.node_id>) -> !db.column<none>
  %gteam, %n = db.group_aggregate(%team, %a) keys 1 aggregates [0] : (!db.column<none>, !db.column<!storage.node_id>) -> (!db.column<none>, !db.column<ui64>)
  db.output(%gteam, %n) : !db.column<none>, !db.column<ui64>
  return
}
)mlir";

// MATCH (a) RETURN a.team, count(a.score): group by team and count each group's
// non-null scores (kind 0 over the nullable score column).
constexpr const char* groupCountScoreProgram = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %team = db.get_node_properties(%a, "team") : (!db.column<!storage.node_id>) -> !db.column<none>
  %score = db.get_node_properties(%a, "score") : (!db.column<!storage.node_id>) -> !db.column<none>
  %gteam, %n = db.group_aggregate(%team, %score) keys 1 aggregates [0] : (!db.column<none>, !db.column<none>) -> (!db.column<none>, !db.column<ui64>)
  db.output(%gteam, %n) : !db.column<none>, !db.column<ui64>
  return
}
)mlir";

// MATCH (a) RETURN a.team, <agg>(a.score): group by team and reduce each group's
// scores with one value reduction. kind is 1 = sum, 2 = min, 3 = max, 4 = avg; the
// db result value type is resolved during lowering, so the db column is left none.
std::string groupScoreProgram(int64_t kind) {
    return std::string("func.func @main() {\n"
                       "  %a = db.scan_nodes() : !db.column<!storage.node_id>\n"
                       "  %team = db.get_node_properties(%a, \"team\") : (!db.column<!storage.node_id>) -> !db.column<none>\n"
                       "  %score = db.get_node_properties(%a, \"score\") : (!db.column<!storage.node_id>) -> !db.column<none>\n"
                       "  %gteam, %r = db.group_aggregate(%team, %score) keys 1 aggregates [")
           + std::to_string(kind)
           + "] : (!db.column<none>, !db.column<none>) -> (!db.column<none>, !db.column<none>)\n"
             "  db.output(%gteam, %r) : !db.column<none>, !db.column<none>\n"
             "  return\n"
             "}\n";
}

}

class GroupAggregateTest : public TuringTest {
protected:
    void initialize() override {
        _jobSystem = std::make_unique<JobSystem>();
        _jobSystem->init();
    }

    void terminate() override {
        _jobSystem->terminate();
    }

    // Four nodes grouped by a "team" (String) with a "score" (Int64): two "red"
    // nodes (scores 10, 20) and two "blue" nodes (score 100 and none, so one blue
    // score is null). This gives two groups with hand-derivable reductions and a
    // null value inside a group, exercising the per-group null handling of every
    // aggregate.
    std::unique_ptr<Graph> buildTeamGraph() {
        auto graph = Graph::create();

        auto change = graph->newChange();
        auto* commitBuilder = change->access().getTip();
        auto& builder = commitBuilder->newBuilder();
        auto& metadata = builder.getMetadata();

        metadata.getOrCreateLabel("0");
        const PropertyTypeID teamID = metadata.getOrCreatePropertyType("team", ValueType::String)._id;
        const PropertyTypeID scoreID = metadata.getOrCreatePropertyType("score", ValueType::Int64)._id;

        const LabelSet labelset = LabelSet::fromList({0});
        const NodeID red1 = builder.addNode(labelset);
        const NodeID red2 = builder.addNode(labelset);
        const NodeID blue1 = builder.addNode(labelset);
        const NodeID blue2 = builder.addNode(labelset);

        builder.addNodeProperty<types::String>(red1, teamID, "red");
        builder.addNodeProperty<types::String>(red2, teamID, "red");
        builder.addNodeProperty<types::String>(blue1, teamID, "blue");
        builder.addNodeProperty<types::String>(blue2, teamID, "blue");

        builder.addNodeProperty<types::Int64>(red1, scoreID, 10);
        builder.addNodeProperty<types::Int64>(red2, scoreID, 20);
        builder.addNodeProperty<types::Int64>(blue1, scoreID, 100);
        // blue2 carries no score, so its group has a null score value

        const auto submitResult = change->access().submit(*_jobSystem);
        EXPECT_TRUE(submitResult);

        return graph;
    }

    // The team graph's schema (the "team" and "score" properties) but no nodes, so
    // a grouped aggregation lowers (the property names resolve) yet folds no row.
    std::unique_ptr<Graph> buildEmptyTeamGraph() {
        auto graph = Graph::create();

        auto change = graph->newChange();
        auto* commitBuilder = change->access().getTip();
        auto& builder = commitBuilder->newBuilder();
        auto& metadata = builder.getMetadata();

        metadata.getOrCreateLabel("0");
        metadata.getOrCreatePropertyType("team", ValueType::String);
        metadata.getOrCreatePropertyType("score", ValueType::Int64);

        const auto submitResult = change->access().submit(*_jobSystem);
        EXPECT_TRUE(submitResult);

        return graph;
    }

    // Parses a db-dialect program, lowers it to nl with DBLowering, and runs the
    // lowered nl function against the graph view. The chunk size is exposed so a
    // test can force the group emit and the collect to span chunk boundaries.
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

TEST_F(GroupAggregateTest, countsStarPerGroup) {
    auto graph = buildTeamGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    // Two red nodes and two blue nodes, so count(*) per team is 2 and 2.
    GroupCountSink sink;
    runLoweredProgram(groupCountStarProgram, reader.getView(), sink);

    std::vector<GroupCountSink::Row> rows;
    sink.sortedRows(rows);
    const std::vector<GroupCountSink::Row> expected {{"blue", 2}, {"red", 2}};
    EXPECT_EQ(rows, expected);
}

TEST_F(GroupAggregateTest, countsNonNullScoresPerGroup) {
    auto graph = buildTeamGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    // red has scores 10 and 20 (two non-null); blue has 100 and null (one non-null),
    // so count(a.score) charges only the present values.
    GroupCountSink sink;
    runLoweredProgram(groupCountScoreProgram, reader.getView(), sink);

    std::vector<GroupCountSink::Row> rows;
    sink.sortedRows(rows);
    const std::vector<GroupCountSink::Row> expected {{"blue", 1}, {"red", 2}};
    EXPECT_EQ(rows, expected);
}

TEST_F(GroupAggregateTest, sumsScoresPerGroup) {
    auto graph = buildTeamGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    // red sums 10 + 20 = 30; blue sums 100 (+ null, ignored) = 100.
    GroupInt64Sink sink;
    runLoweredProgram(groupScoreProgram(1).c_str(), reader.getView(), sink);

    std::vector<GroupInt64Sink::Row> rows;
    sink.sortedRows(rows);
    const std::vector<GroupInt64Sink::Row> expected {{"blue", 100}, {"red", 30}};
    EXPECT_EQ(rows, expected);
}

TEST_F(GroupAggregateTest, minsAndMaxsScoresPerGroup) {
    auto graph = buildTeamGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    // red min 10 / max 20; blue has one non-null score (100), so min = max = 100.
    GroupInt64Sink minSink;
    runLoweredProgram(groupScoreProgram(2).c_str(), reader.getView(), minSink);
    std::vector<GroupInt64Sink::Row> minRows;
    minSink.sortedRows(minRows);
    EXPECT_EQ(minRows, (std::vector<GroupInt64Sink::Row> {{"blue", 100}, {"red", 10}}));

    GroupInt64Sink maxSink;
    runLoweredProgram(groupScoreProgram(3).c_str(), reader.getView(), maxSink);
    std::vector<GroupInt64Sink::Row> maxRows;
    maxSink.sortedRows(maxRows);
    EXPECT_EQ(maxRows, (std::vector<GroupInt64Sink::Row> {{"blue", 100}, {"red", 20}}));
}

TEST_F(GroupAggregateTest, averagesScoresPerGroup) {
    auto graph = buildTeamGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    // red averages (10 + 20) / 2 = 15.0; blue averages its single non-null score
    // 100 / 1 = 100.0 (the null score is not counted).
    GroupDoubleSink sink;
    runLoweredProgram(groupScoreProgram(4).c_str(), reader.getView(), sink);

    std::vector<GroupDoubleSink::Row> rows;
    sink.sortedRows(rows);
    const std::vector<GroupDoubleSink::Row> expected {{"blue", 100.0}, {"red", 15.0}};
    EXPECT_EQ(rows, expected);
}

TEST_F(GroupAggregateTest, groupsAcrossChunkBoundaries) {
    auto graph = buildTeamGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    // A chunk size of two over four nodes makes the scan feed the accumulator two
    // chunks, so a group's rows can arrive in different chunks; the group table is
    // reset once at function scope, not per chunk, so the sums still total per team.
    GroupInt64Sink sink;
    runLoweredProgram(groupScoreProgram(1).c_str(), reader.getView(), sink, /*chunkSize=*/2);

    std::vector<GroupInt64Sink::Row> rows;
    sink.sortedRows(rows);
    const std::vector<GroupInt64Sink::Row> expected {{"blue", 100}, {"red", 30}};
    EXPECT_EQ(rows, expected);
}

TEST_F(GroupAggregateTest, emptyGraphEmitsNoGroup) {
    auto graph = buildEmptyTeamGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    // No rows to fold, so no group is created and the emit loop runs zero times: a
    // grouped aggregate over an empty input emits nothing (unlike a bare aggregate,
    // which emits one default row).
    CountingSink sink;
    runLoweredProgram(groupScoreProgram(1).c_str(), reader.getView(), sink);

    EXPECT_EQ(sink.getCalls(), 0u);
    EXPECT_EQ(sink.getTotalRows(), 0u);
}

// db.group_aggregate lowers to the pipeline-breaker shape, like db.sort: a hoisted
// nl.group_aggregate_buffer, one nl.group_aggregate_update in the producing loop,
// one nl.group_aggregate source op, and two nl.for loops (the producing scan and
// the emit loop over nl.group_aggregate). Unlike db.count it does emit a loop, so
// there are two nl.for, and no nl.sort_buffer.
TEST_F(GroupAggregateTest, lowersToBufferUpdateAndEmitLoop) {
    auto graph = buildTeamGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    mlir::MLIRContext context;
    context.getOrLoadDialect<mlir::func::FuncDialect>();
    context.getOrLoadDialect<mlir::storage::Storage>();
    context.getOrLoadDialect<mlir::db::DB>();
    context.getOrLoadDialect<mlir::nl::NL>();

    const mlir::ParserConfig parserConfig(&context);
    const std::string programText = groupScoreProgram(1);
    mlir::OwningOpRef<mlir::ModuleOp> dbModule = mlir::parseSourceString<mlir::ModuleOp>(programText, parserConfig);
    ASSERT_TRUE(dbModule);

    const mlir::func::FuncOp dbFunction = dbModule->lookupSymbol<mlir::func::FuncOp>("main");
    ASSERT_TRUE(dbFunction);

    mlir::OwningOpRef<mlir::ModuleOp> nlModule = mlir::ModuleOp::create(mlir::UnknownLoc::get(&context));
    DBLowering lowering(&context, &reader.getView());
    lowering.lower(dbFunction, *nlModule);

    size_t bufferCount = 0;
    size_t updateCount = 0;
    size_t groupCount = 0;
    size_t forCount = 0;
    size_t sortBufferCount = 0;
    mlir::nl::GroupAggregateBuffer bufferOp;
    mlir::nl::GroupAggregateUpdate updateOp;
    mlir::nl::GroupAggregate groupOp;

    nlModule->walk([&](mlir::Operation* operation) {
        if (mlir::nl::GroupAggregateBuffer found = mlir::dyn_cast<mlir::nl::GroupAggregateBuffer>(operation)) {
            bufferOp = found;
            bufferCount++;
        } else if (mlir::nl::GroupAggregateUpdate found = mlir::dyn_cast<mlir::nl::GroupAggregateUpdate>(operation)) {
            updateOp = found;
            updateCount++;
        } else if (mlir::nl::GroupAggregate found = mlir::dyn_cast<mlir::nl::GroupAggregate>(operation)) {
            groupOp = found;
            groupCount++;
        } else if (mlir::isa<mlir::nl::For>(operation)) {
            forCount++;
        } else if (mlir::isa<mlir::nl::SortBuffer>(operation)) {
            sortBufferCount++;
        }
    });

    EXPECT_EQ(bufferCount, 1u);
    EXPECT_EQ(updateCount, 1u);
    EXPECT_EQ(groupCount, 1u);
    EXPECT_EQ(forCount, 2u);
    EXPECT_EQ(sortBufferCount, 0u);
    ASSERT_TRUE(bufferOp);
    ASSERT_TRUE(updateOp);
    ASSERT_TRUE(groupOp);

    // One grouping key, one aggregate (sum, kind 1); the update and the source op
    // both name the one hoisted accumulator handle.
    EXPECT_EQ(bufferOp.getKeyCount(), 1u);
    ASSERT_EQ(bufferOp.getKinds().size(), 1u);
    EXPECT_EQ(bufferOp.getKinds()[0], 1);

    const mlir::Value handle = bufferOp.getState();
    EXPECT_EQ(updateOp.getState(), handle);
    EXPECT_EQ(groupOp.getState(), handle);

    // The update runs inside an nl.for (the producing scan loop); the emit loop
    // iterates the nl.group_aggregate source op.
    EXPECT_TRUE(mlir::isa<mlir::nl::For>(updateOp->getParentOp()));
    EXPECT_TRUE(groupOp.getResult().hasOneUse());
    EXPECT_TRUE(mlir::isa<mlir::nl::For>(*groupOp.getResult().user_begin()));
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
