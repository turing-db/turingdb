#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <tuple>
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

// Collects the (team, count, sum) rows a two-aggregate group_aggregate emits: a
// nullable string key, a non-null ui64 count and a nullable int64 sum. Exercises the
// multi-aggregate path where each group binds two accumulators and two outputs.
class GroupCountSumSink : public NLOutputSink {
public:
    using Row = std::tuple<std::optional<std::string>, uint64_t, std::optional<int64_t>>;

    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 3u);

        const auto* teams = dynamic_cast<const ColumnOptVector<std::string_view>*>(chunks[0]);
        const auto* counts = dynamic_cast<const ColumnVector<uint64_t>*>(chunks[1]);
        const auto* sums = dynamic_cast<const ColumnOptVector<int64_t>*>(chunks[2]);
        ASSERT_NE(teams, nullptr);
        ASSERT_NE(counts, nullptr);
        ASSERT_NE(sums, nullptr);
        ASSERT_EQ(teams->size(), counts->size());
        ASSERT_EQ(teams->size(), sums->size());

        const auto& teamRaw = teams->getRaw();
        const auto& countRaw = counts->getRaw();
        const auto& sumRaw = sums->getRaw();
        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            std::optional<std::string> team;
            if (teamRaw[rowIndex]) {
                team = std::string(*teamRaw[rowIndex]);
            }

            _rows.emplace_back(team, countRaw[rowIndex], sumRaw[rowIndex]);
        }
    }

    void sortedRows(std::vector<Row>& rows) const {
        rows = _rows;
        std::sort(rows.begin(), rows.end());
    }

private:
    std::vector<Row> _rows;
};

// Collects the (team, city, count, sum, max) rows a multi-key, multi-aggregate
// group_aggregate emits: two nullable string keys, a non-null ui64 count and two
// nullable int64 value reductions. Exercises a composite grouping key together with
// three aggregates (count, sum, max) sharing the same groups.
class GroupTeamCitySink : public NLOutputSink {
public:
    using Row = std::tuple<std::optional<std::string>,
                           std::optional<std::string>,
                           uint64_t,
                           std::optional<int64_t>,
                           std::optional<int64_t>>;

    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 5u);

        const auto* teams = dynamic_cast<const ColumnOptVector<std::string_view>*>(chunks[0]);
        const auto* cities = dynamic_cast<const ColumnOptVector<std::string_view>*>(chunks[1]);
        const auto* counts = dynamic_cast<const ColumnVector<uint64_t>*>(chunks[2]);
        const auto* sums = dynamic_cast<const ColumnOptVector<int64_t>*>(chunks[3]);
        const auto* maxes = dynamic_cast<const ColumnOptVector<int64_t>*>(chunks[4]);
        ASSERT_NE(teams, nullptr);
        ASSERT_NE(cities, nullptr);
        ASSERT_NE(counts, nullptr);
        ASSERT_NE(sums, nullptr);
        ASSERT_NE(maxes, nullptr);
        ASSERT_EQ(teams->size(), cities->size());
        ASSERT_EQ(teams->size(), counts->size());
        ASSERT_EQ(teams->size(), sums->size());
        ASSERT_EQ(teams->size(), maxes->size());

        const auto& teamRaw = teams->getRaw();
        const auto& cityRaw = cities->getRaw();
        const auto& countRaw = counts->getRaw();
        const auto& sumRaw = sums->getRaw();
        const auto& maxRaw = maxes->getRaw();
        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            std::optional<std::string> team;
            if (teamRaw[rowIndex]) {
                team = std::string(*teamRaw[rowIndex]);
            }

            std::optional<std::string> city;
            if (cityRaw[rowIndex]) {
                city = std::string(*cityRaw[rowIndex]);
            }

            _rows.emplace_back(team, city, countRaw[rowIndex], sumRaw[rowIndex], maxRaw[rowIndex]);
        }
    }

    void sortedRows(std::vector<Row>& rows) const {
        rows = _rows;
        std::sort(rows.begin(), rows.end());
    }

private:
    std::vector<Row> _rows;
};

// Collects the (weight, count) rows a group_aggregate keyed on a Double emits: a
// nullable f64 key and a non-null ui64 count. Lets a test assert how many groups a
// set of double keys collapses into (e.g. +0.0 and -0.0 must share one group).
class GroupDoubleKeyCountSink : public NLOutputSink {
public:
    using Row = std::pair<std::optional<double>, uint64_t>;

    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 2u);

        const auto* weights = dynamic_cast<const ColumnOptVector<double>*>(chunks[0]);
        const auto* counts = dynamic_cast<const ColumnVector<uint64_t>*>(chunks[1]);
        ASSERT_NE(weights, nullptr);
        ASSERT_NE(counts, nullptr);
        ASSERT_EQ(weights->size(), counts->size());

        const auto& weightRaw = weights->getRaw();
        const auto& countRaw = counts->getRaw();
        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            _rows.push_back({weightRaw[rowIndex], countRaw[rowIndex]});
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
  %gteam, %n = db.group_aggregate(%team, %a) keys 1 aggregates [count] : (!db.column<none>, !db.column<!storage.node_id>) -> (!db.column<none>, !db.column<ui64>)
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
  %gteam, %n = db.group_aggregate(%team, %score) keys 1 aggregates [count] : (!db.column<none>, !db.column<none>) -> (!db.column<none>, !db.column<ui64>)
  db.output(%gteam, %n) : !db.column<none>, !db.column<ui64>
  return
}
)mlir";

// MATCH (a) RETURN a.team, <agg>(a.score): group by team and reduce each group's
// scores with one value reduction. kind is the GroupAggregateKind keyword - "sum",
// "min", "max" or "avg"; the db result value type is resolved during lowering, so
// the db column is left none.
std::string groupScoreProgram(const char* kind) {
    return std::string("func.func @main() {\n"
                       "  %a = db.scan_nodes() : !db.column<!storage.node_id>\n"
                       "  %team = db.get_node_properties(%a, \"team\") : (!db.column<!storage.node_id>) -> !db.column<none>\n"
                       "  %score = db.get_node_properties(%a, \"score\") : (!db.column<!storage.node_id>) -> !db.column<none>\n"
                       "  %gteam, %r = db.group_aggregate(%team, %score) keys 1 aggregates [")
           + kind
           + "] : (!db.column<none>, !db.column<none>) -> (!db.column<none>, !db.column<none>)\n"
             "  db.output(%gteam, %r) : !db.column<none>, !db.column<none>\n"
             "  return\n"
             "}\n";
}

// MATCH (a) RETURN a.team, count(*), sum(a.score): group by team and reduce each
// group two ways at once - a count (kind 0) over the node IDs and a sum (kind 1)
// over the scores - so the op takes one key column and two aggregate-input columns
// and each group binds two accumulators and two outputs.
constexpr const char* groupCountSumProgram = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %team = db.get_node_properties(%a, "team") : (!db.column<!storage.node_id>) -> !db.column<none>
  %score = db.get_node_properties(%a, "score") : (!db.column<!storage.node_id>) -> !db.column<none>
  %gteam, %n, %s = db.group_aggregate(%team, %a, %score) keys 1 aggregates [count, sum] : (!db.column<none>, !db.column<!storage.node_id>, !db.column<none>) -> (!db.column<none>, !db.column<ui64>, !db.column<none>)
  db.output(%gteam, %n, %s) : !db.column<none>, !db.column<ui64>, !db.column<none>
  return
}
)mlir";

// MATCH (a) RETURN a.team, a.city, count(*), sum(a.score), max(a.score): group by
// the (team, city) pair and reduce each group three ways at once - a count (kind 0)
// over the node IDs, a sum (kind 1) and a max (kind 3) over the scores. keys 2 makes
// the group identity the whole (team, city) tuple, so the op takes two key columns
// and three aggregate-input columns and binds one accumulator and one output per
// aggregate on top of the two passed-through keys.
constexpr const char* groupTeamCityProgram = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %team = db.get_node_properties(%a, "team") : (!db.column<!storage.node_id>) -> !db.column<none>
  %city = db.get_node_properties(%a, "city") : (!db.column<!storage.node_id>) -> !db.column<none>
  %score = db.get_node_properties(%a, "score") : (!db.column<!storage.node_id>) -> !db.column<none>
  %gteam, %gcity, %n, %s, %m = db.group_aggregate(%team, %city, %a, %score, %score) keys 2 aggregates [count, sum, max] : (!db.column<none>, !db.column<none>, !db.column<!storage.node_id>, !db.column<none>, !db.column<none>) -> (!db.column<none>, !db.column<none>, !db.column<ui64>, !db.column<none>, !db.column<none>)
  db.output(%gteam, %gcity, %n, %s, %m) : !db.column<none>, !db.column<none>, !db.column<ui64>, !db.column<none>, !db.column<none>
  return
}
)mlir";

// MATCH (a) RETURN a.team, count(*) LIMIT count: a limit over the emitted groups. The
// limit caps how many group rows come out, never how many input rows are folded, so it
// budgets the emit loop alone - the producing loop must still see every node for the
// counts to be right.
std::string groupCountStarLimitProgram(uint64_t count) {
    return std::string("func.func @main() {\n"
                       "  %a = db.scan_nodes() : !db.column<!storage.node_id>\n"
                       "  %team = db.get_node_properties(%a, \"team\") : (!db.column<!storage.node_id>) -> !db.column<none>\n"
                       "  %gteam, %n = db.group_aggregate(%team, %a) keys 1 aggregates [count] : (!db.column<none>, !db.column<!storage.node_id>) -> (!db.column<none>, !db.column<ui64>)\n"
                       "  %lteam, %ln = db.limit(%gteam, %n) count ")
           + std::to_string(count)
           + " : (!db.column<none>, !db.column<ui64>) -> (!db.column<none>, !db.column<ui64>)\n"
             "  db.output(%lteam, %ln) : !db.column<none>, !db.column<ui64>\n"
             "  return\n"
             "}\n";
}

// MATCH (a) RETURN a.weight, count(*): group by a Double property and count each
// group. The grouping key is a nullable f64, so it exercises key serialization of a
// floating-point value.
constexpr const char* groupWeightCountProgram = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %weight = db.get_node_properties(%a, "weight") : (!db.column<!storage.node_id>) -> !db.column<none>
  %gweight, %n = db.group_aggregate(%weight, %a) keys 1 aggregates [count] : (!db.column<none>, !db.column<!storage.node_id>) -> (!db.column<none>, !db.column<ui64>)
  db.output(%gweight, %n) : !db.column<none>, !db.column<ui64>
  return
}
)mlir";

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

    // buildTeamGraph plus a node with no "team" property, so its grouping key is
    // null. Two "red" nodes and one team-less node let a test assert that all
    // null-key rows collapse into a single group whose emitted key is null.
    std::unique_ptr<Graph> buildPartialTeamGraph() {
        auto graph = Graph::create();

        auto change = graph->newChange();
        auto* commitBuilder = change->access().getTip();
        auto& builder = commitBuilder->newBuilder();
        auto& metadata = builder.getMetadata();

        metadata.getOrCreateLabel("0");
        const PropertyTypeID teamID = metadata.getOrCreatePropertyType("team", ValueType::String)._id;

        const LabelSet labelset = LabelSet::fromList({0});
        const NodeID red1 = builder.addNode(labelset);
        const NodeID red2 = builder.addNode(labelset);
        builder.addNode(labelset); // a team-less node, so its grouping key is null

        builder.addNodeProperty<types::String>(red1, teamID, "red");
        builder.addNodeProperty<types::String>(red2, teamID, "red");

        const auto submitResult = change->access().submit(*_jobSystem);
        EXPECT_TRUE(submitResult);

        return graph;
    }

    // Four nodes each in their own single-node "team" (a, b, c, d) with scores 1..4,
    // so a grouped aggregate produces four groups. Run at a chunk size below four,
    // the emit loop must iterate more than once, slicing the later groups at a
    // non-zero begin offset.
    std::unique_ptr<Graph> buildManyTeamGraph() {
        auto graph = Graph::create();

        auto change = graph->newChange();
        auto* commitBuilder = change->access().getTip();
        auto& builder = commitBuilder->newBuilder();
        auto& metadata = builder.getMetadata();

        metadata.getOrCreateLabel("0");
        const PropertyTypeID teamID = metadata.getOrCreatePropertyType("team", ValueType::String)._id;
        const PropertyTypeID scoreID = metadata.getOrCreatePropertyType("score", ValueType::Int64)._id;

        const LabelSet labelset = LabelSet::fromList({0});
        const NodeID nodeA = builder.addNode(labelset);
        const NodeID nodeB = builder.addNode(labelset);
        const NodeID nodeC = builder.addNode(labelset);
        const NodeID nodeD = builder.addNode(labelset);

        builder.addNodeProperty<types::String>(nodeA, teamID, "a");
        builder.addNodeProperty<types::String>(nodeB, teamID, "b");
        builder.addNodeProperty<types::String>(nodeC, teamID, "c");
        builder.addNodeProperty<types::String>(nodeD, teamID, "d");

        builder.addNodeProperty<types::Int64>(nodeA, scoreID, 1);
        builder.addNodeProperty<types::Int64>(nodeB, scoreID, 2);
        builder.addNodeProperty<types::Int64>(nodeC, scoreID, 3);
        builder.addNodeProperty<types::Int64>(nodeD, scoreID, 4);

        const auto submitResult = change->access().submit(*_jobSystem);
        EXPECT_TRUE(submitResult);

        return graph;
    }

    // Three nodes with a Double "weight": +0.0, -0.0 and 1.5. The two zeros differ
    // only in the sign bit but are equal under Cypher value equality, so a correct
    // grouping collapses them into one group - two groups total, not three.
    std::unique_ptr<Graph> buildWeightGraph() {
        auto graph = Graph::create();

        auto change = graph->newChange();
        auto* commitBuilder = change->access().getTip();
        auto& builder = commitBuilder->newBuilder();
        auto& metadata = builder.getMetadata();

        metadata.getOrCreateLabel("0");
        const PropertyTypeID weightID = metadata.getOrCreatePropertyType("weight", ValueType::Double)._id;

        const LabelSet labelset = LabelSet::fromList({0});
        const NodeID positiveZero = builder.addNode(labelset);
        const NodeID negativeZero = builder.addNode(labelset);
        const NodeID other = builder.addNode(labelset);

        builder.addNodeProperty<types::Double>(positiveZero, weightID, 0.0);
        builder.addNodeProperty<types::Double>(negativeZero, weightID, -0.0);
        builder.addNodeProperty<types::Double>(other, weightID, 1.5);

        const auto submitResult = change->access().submit(*_jobSystem);
        EXPECT_TRUE(submitResult);

        return graph;
    }

    // Five nodes grouped by a (team, city) pair (both String) with a "score"
    // (Int64), so a composite key forms three groups: (red, paris) with scores 10
    // and 20, (red, lyon) with score 5, and (blue, paris) with score 100 plus a node
    // with no score (a null value inside the group). Lets a test hand-derive a
    // multi-key, multi-aggregate reduction where count(*) charges the null-score row
    // but sum and max ignore it.
    std::unique_ptr<Graph> buildTeamCityGraph() {
        auto graph = Graph::create();

        auto change = graph->newChange();
        auto* commitBuilder = change->access().getTip();
        auto& builder = commitBuilder->newBuilder();
        auto& metadata = builder.getMetadata();

        metadata.getOrCreateLabel("0");
        const PropertyTypeID teamID = metadata.getOrCreatePropertyType("team", ValueType::String)._id;
        const PropertyTypeID cityID = metadata.getOrCreatePropertyType("city", ValueType::String)._id;
        const PropertyTypeID scoreID = metadata.getOrCreatePropertyType("score", ValueType::Int64)._id;

        const LabelSet labelset = LabelSet::fromList({0});
        const NodeID redParis1 = builder.addNode(labelset);
        const NodeID redParis2 = builder.addNode(labelset);
        const NodeID redLyon = builder.addNode(labelset);
        const NodeID blueParis1 = builder.addNode(labelset);
        const NodeID blueParis2 = builder.addNode(labelset);

        builder.addNodeProperty<types::String>(redParis1, teamID, "red");
        builder.addNodeProperty<types::String>(redParis2, teamID, "red");
        builder.addNodeProperty<types::String>(redLyon, teamID, "red");
        builder.addNodeProperty<types::String>(blueParis1, teamID, "blue");
        builder.addNodeProperty<types::String>(blueParis2, teamID, "blue");

        builder.addNodeProperty<types::String>(redParis1, cityID, "paris");
        builder.addNodeProperty<types::String>(redParis2, cityID, "paris");
        builder.addNodeProperty<types::String>(redLyon, cityID, "lyon");
        builder.addNodeProperty<types::String>(blueParis1, cityID, "paris");
        builder.addNodeProperty<types::String>(blueParis2, cityID, "paris");

        builder.addNodeProperty<types::Int64>(redParis1, scoreID, 10);
        builder.addNodeProperty<types::Int64>(redParis2, scoreID, 20);
        builder.addNodeProperty<types::Int64>(redLyon, scoreID, 5);
        builder.addNodeProperty<types::Int64>(blueParis1, scoreID, 100);
        // blueParis2 carries no score, so its group has a null score value

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
    runLoweredProgram(groupScoreProgram("sum").c_str(), reader.getView(), sink);

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
    runLoweredProgram(groupScoreProgram("min").c_str(), reader.getView(), minSink);
    std::vector<GroupInt64Sink::Row> minRows;
    minSink.sortedRows(minRows);
    EXPECT_EQ(minRows, (std::vector<GroupInt64Sink::Row> {{"blue", 100}, {"red", 10}}));

    GroupInt64Sink maxSink;
    runLoweredProgram(groupScoreProgram("max").c_str(), reader.getView(), maxSink);
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
    runLoweredProgram(groupScoreProgram("avg").c_str(), reader.getView(), sink);

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
    runLoweredProgram(groupScoreProgram("sum").c_str(), reader.getView(), sink, /*chunkSize=*/2);

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
    runLoweredProgram(groupScoreProgram("sum").c_str(), reader.getView(), sink);

    EXPECT_EQ(sink.getCalls(), 0u);
    EXPECT_EQ(sink.getTotalRows(), 0u);
}

TEST_F(GroupAggregateTest, multipleAggregatesPerGroup) {
    auto graph = buildTeamGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    // RETURN a.team, count(*), sum(a.score): two aggregates over the same groups, so
    // each group binds two accumulators and two outputs. count(*) charges every node,
    // so both teams count 2; sum ignores the null score, so red sums 30 and blue 100.
    GroupCountSumSink sink;
    runLoweredProgram(groupCountSumProgram, reader.getView(), sink);

    std::vector<GroupCountSumSink::Row> rows;
    sink.sortedRows(rows);

    std::vector<GroupCountSumSink::Row> expected;
    expected.emplace_back(std::optional<std::string>("blue"), 2u, std::optional<int64_t>(100));
    expected.emplace_back(std::optional<std::string>("red"), 2u, std::optional<int64_t>(30));
    EXPECT_EQ(rows, expected);
}

TEST_F(GroupAggregateTest, multiKeyMultipleAggregates) {
    auto graph = buildTeamCityGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    // RETURN a.team, a.city, count(*), sum(a.score), max(a.score): a composite
    // (team, city) key with three aggregates over the same groups. count(*) charges
    // every node, so (blue, paris) counts 2 even though one score is null, while sum
    // and max ignore that null. (red, paris) sums 30 / maxes 20; (red, lyon) has the
    // lone score 5; (blue, paris) sums and maxes 100.
    GroupTeamCitySink sink;
    runLoweredProgram(groupTeamCityProgram, reader.getView(), sink);

    std::vector<GroupTeamCitySink::Row> rows;
    sink.sortedRows(rows);

    std::vector<GroupTeamCitySink::Row> expected;
    expected.emplace_back(std::optional<std::string>("blue"), std::optional<std::string>("paris"), 2u, std::optional<int64_t>(100), std::optional<int64_t>(100));
    expected.emplace_back(std::optional<std::string>("red"), std::optional<std::string>("lyon"), 1u, std::optional<int64_t>(5), std::optional<int64_t>(5));
    expected.emplace_back(std::optional<std::string>("red"), std::optional<std::string>("paris"), 2u, std::optional<int64_t>(30), std::optional<int64_t>(20));
    EXPECT_EQ(rows, expected);
}

TEST_F(GroupAggregateTest, nullGroupingKeyFormsOneGroup) {
    auto graph = buildPartialTeamGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    // The team-less node groups under a null key: all null-key rows collapse into one
    // group whose emitted key is null (nullopt). Two "red" nodes and one team-less
    // node give (null -> 1) and (red -> 2).
    GroupCountSink sink;
    runLoweredProgram(groupCountStarProgram, reader.getView(), sink);

    std::vector<GroupCountSink::Row> rows;
    sink.sortedRows(rows);
    const std::vector<GroupCountSink::Row> expected {{std::nullopt, 1}, {"red", 2}};
    EXPECT_EQ(rows, expected);
}

TEST_F(GroupAggregateTest, emitsMoreGroupsThanChunkSize) {
    auto graph = buildManyTeamGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    // Four groups with a chunk size of two, so the emit loop runs twice and its
    // second iteration slices groups [2, 4) at a non-zero begin offset - exercising
    // the count emit, the sum copy, the avg emit and the key copy each with begin > 0.
    GroupCountSink countSink;
    runLoweredProgram(groupCountStarProgram, reader.getView(), countSink, /*chunkSize=*/2);
    std::vector<GroupCountSink::Row> countRows;
    countSink.sortedRows(countRows);
    EXPECT_EQ(countRows, (std::vector<GroupCountSink::Row> {{"a", 1}, {"b", 1}, {"c", 1}, {"d", 1}}));

    GroupInt64Sink sumSink;
    runLoweredProgram(groupScoreProgram("sum").c_str(), reader.getView(), sumSink, /*chunkSize=*/2);
    std::vector<GroupInt64Sink::Row> sumRows;
    sumSink.sortedRows(sumRows);
    EXPECT_EQ(sumRows, (std::vector<GroupInt64Sink::Row> {{"a", 1}, {"b", 2}, {"c", 3}, {"d", 4}}));

    GroupDoubleSink avgSink;
    runLoweredProgram(groupScoreProgram("avg").c_str(), reader.getView(), avgSink, /*chunkSize=*/2);
    std::vector<GroupDoubleSink::Row> avgRows;
    avgSink.sortedRows(avgRows);
    EXPECT_EQ(avgRows, (std::vector<GroupDoubleSink::Row> {{"a", 1.0}, {"b", 2.0}, {"c", 3.0}, {"d", 4.0}}));
}

TEST_F(GroupAggregateTest, negativeZeroGroupsWithPositiveZero) {
    auto graph = buildWeightGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    // Grouping on a Double key must use Cypher value equality, not raw IEEE-754 bytes:
    // +0.0 and -0.0 differ only in the sign bit but are equal, so the two zero-weight
    // nodes collapse into one group (count 2) while 1.5 forms its own. A bitwise key
    // would wrongly split the zeros into two groups.
    GroupDoubleKeyCountSink sink;
    runLoweredProgram(groupWeightCountProgram, reader.getView(), sink);

    std::vector<GroupDoubleKeyCountSink::Row> rows;
    sink.sortedRows(rows);

    ASSERT_EQ(rows.size(), 2u);
    const std::vector<GroupDoubleKeyCountSink::Row> expected {{0.0, 2}, {1.5, 1}};
    EXPECT_EQ(rows, expected);
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
    const std::string programText = groupScoreProgram("sum");
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

TEST_F(GroupAggregateTest, limitCapsTheEmittedGroups) {
    auto graph = buildTeamGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    // Two groups (red and blue); LIMIT 1 emits one of them. Which one is the emit
    // order of the accumulator, so only the row count is asserted here.
    CountingSink oneSink;
    const std::string oneProgram = groupCountStarLimitProgram(1);
    runLoweredProgram(oneProgram.c_str(), reader.getView(), oneSink);

    EXPECT_EQ(oneSink.getTotalRows(), 1u);

    // A bound above the group count emits every group.
    CountingSink allSink;
    const std::string allProgram = groupCountStarLimitProgram(5);
    runLoweredProgram(allProgram.c_str(), reader.getView(), allSink);

    EXPECT_EQ(allSink.getTotalRows(), 2u);
}

TEST_F(GroupAggregateTest, lowersGroupAggregateLimitToALimitOnTheEmitLoopOnly) {
    auto graph = buildTeamGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    mlir::MLIRContext context;
    context.getOrLoadDialect<mlir::func::FuncDialect>();
    context.getOrLoadDialect<mlir::storage::Storage>();
    context.getOrLoadDialect<mlir::db::DB>();
    context.getOrLoadDialect<mlir::nl::NL>();

    const mlir::ParserConfig parserConfig(&context);
    const std::string programText = groupCountStarLimitProgram(1);
    mlir::OwningOpRef<mlir::ModuleOp> dbModule = mlir::parseSourceString<mlir::ModuleOp>(programText, parserConfig);
    ASSERT_TRUE(dbModule);

    const mlir::func::FuncOp dbFunction = dbModule->lookupSymbol<mlir::func::FuncOp>("main");
    ASSERT_TRUE(dbFunction);

    mlir::OwningOpRef<mlir::ModuleOp> nlModule = mlir::ModuleOp::create(mlir::UnknownLoc::get(&context));
    DBLowering lowering(&context, &reader.getView());
    lowering.lower(dbFunction, *nlModule);

    size_t limitCount = 0;
    mlir::nl::GroupAggregateUpdate updateOp;
    mlir::nl::GroupAggregate groupOp;
    mlir::Value handle;

    nlModule->walk([&](mlir::Operation* operation) {
        if (mlir::nl::GroupAggregateUpdate found = mlir::dyn_cast<mlir::nl::GroupAggregateUpdate>(operation)) {
            updateOp = found;
        } else if (mlir::nl::GroupAggregate found = mlir::dyn_cast<mlir::nl::GroupAggregate>(operation)) {
            groupOp = found;
        } else if (mlir::nl::Limit found = mlir::dyn_cast<mlir::nl::Limit>(operation)) {
            handle = found.getState();
            limitCount++;
        }
    });

    ASSERT_TRUE(updateOp);
    ASSERT_TRUE(groupOp);
    ASSERT_TRUE(handle);
    EXPECT_EQ(limitCount, 1u);

    // The producing loop is the one holding the update: it folds the rows into the
    // per-group state, so it must run to completion - a limit operand there would
    // group a prefix of the scan and undercount every group.
    auto producingLoop = mlir::dyn_cast<mlir::nl::For>(updateOp->getParentOp());
    ASSERT_TRUE(producingLoop);
    EXPECT_FALSE(producingLoop.getLimit());

    // The emit loop drains the finished groups, and it is what the limit budgets: it
    // carries the handle, so it stops once the budgeted groups have been emitted.
    ASSERT_TRUE(groupOp.getResult().hasOneUse());
    auto emitLoop = mlir::dyn_cast<mlir::nl::For>(*groupOp.getResult().user_begin());
    ASSERT_TRUE(emitLoop);
    EXPECT_EQ(emitLoop.getLimit(), handle);
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
