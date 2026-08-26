#include <gtest/gtest.h>

#include <stdint.h>

#include <algorithm>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <tuple>
#include <utility>
#include <vector>

#include "columns/ColumnOptVector.h"
#include "columns/ColumnVector.h"
#include "list/ListElementView.h"
#include "list/ListView.h"

#include "LocalMemory.h"
#include "NLOutputSink.h"
#include "QueryConfig.h"
#include "QueryInterpreterV3.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "TuringDB.h"
#include "dataframe/Dataframe.h"
#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"

#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

namespace {

using Names = std::vector<std::string>;
using OptInt64 = std::optional<int64_t>;
using OptDouble = std::optional<double>;

// Reads one list cell as a vector of strings.
void readStringList(const ListView& view, Names& out) {
    out.clear();
    for (const ListElementView& element : view) {
        out.push_back(std::string(element.getAs<std::string_view>()));
    }
}

class NullSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const>, size_t, size_t) override {}
};

// Collects the (team, [names], reduction) rows a grouped collect emits beside one
// reduction, reading the reduction column as the type its kind produces: a count is an
// unsigned tally, a sum or an extremum is a nullable Int64, an average a nullable Double.
template <typename ReductionColumn, typename Reduction>
class KeyedListAndReductionSink : public NLOutputSink {
public:
    using Row = std::tuple<std::optional<std::string>, Names, Reduction>;

    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 3u);

        const auto* keys = dynamic_cast<const ColumnOptVector<std::string_view>*>(chunks[0]);
        const auto* lists = dynamic_cast<const ColumnVector<ListView>*>(chunks[1]);
        const auto* reductions = dynamic_cast<const ReductionColumn*>(chunks[2]);
        ASSERT_NE(keys, nullptr);
        ASSERT_NE(lists, nullptr);
        ASSERT_NE(reductions, nullptr);
        ASSERT_EQ(keys->size(), lists->size());

        const auto& keyRaw = keys->getRaw();
        const auto& listRaw = lists->getRaw();
        const auto& reductionRaw = reductions->getRaw();
        for (size_t row = offset; row < offset + rowCount; row++) {
            std::optional<std::string> key;
            if (keyRaw[row]) {
                key = std::string(*keyRaw[row]);
            }

            Names names;
            readStringList(listRaw[row], names);

            _rows.push_back({key, names, reductionRaw[row]});
        }
    }

    void sortedRows(std::vector<Row>& rows) const {
        rows = _rows;
        std::sort(rows.begin(), rows.end());
    }

    // The rows as the sink saw them, for the ORDER BY test: the emit order is the result.
    void rowsInEmitOrder(std::vector<Row>& rows) const {
        rows = _rows;
    }

private:
    std::vector<Row> _rows;
};

using KeyedListAndCountSink = KeyedListAndReductionSink<ColumnVector<uint64_t>, uint64_t>;
using KeyedListAndInt64Sink = KeyedListAndReductionSink<ColumnOptVector<int64_t>, OptInt64>;
using KeyedListAndDoubleSink = KeyedListAndReductionSink<ColumnOptVector<double>, OptDouble>;

// Collects the (team, [names], count(*), count(score), sum, min, max, avg) rows one
// collect carrying every non-distinct reduction emits.
class KeyedListAndEveryReductionSink : public NLOutputSink {
public:
    using Row = std::tuple<std::optional<std::string>, Names, uint64_t, uint64_t, OptInt64, OptInt64, OptInt64, OptDouble>;

    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 8u);

        const auto* keys = dynamic_cast<const ColumnOptVector<std::string_view>*>(chunks[0]);
        const auto* lists = dynamic_cast<const ColumnVector<ListView>*>(chunks[1]);
        const auto* rowCounts = dynamic_cast<const ColumnVector<uint64_t>*>(chunks[2]);
        const auto* valueCounts = dynamic_cast<const ColumnVector<uint64_t>*>(chunks[3]);
        const auto* sums = dynamic_cast<const ColumnOptVector<int64_t>*>(chunks[4]);
        const auto* minima = dynamic_cast<const ColumnOptVector<int64_t>*>(chunks[5]);
        const auto* maxima = dynamic_cast<const ColumnOptVector<int64_t>*>(chunks[6]);
        const auto* averages = dynamic_cast<const ColumnOptVector<double>*>(chunks[7]);
        ASSERT_NE(keys, nullptr);
        ASSERT_NE(lists, nullptr);
        ASSERT_NE(rowCounts, nullptr);
        ASSERT_NE(valueCounts, nullptr);
        ASSERT_NE(sums, nullptr);
        ASSERT_NE(minima, nullptr);
        ASSERT_NE(maxima, nullptr);
        ASSERT_NE(averages, nullptr);

        const auto& keyRaw = keys->getRaw();
        const auto& listRaw = lists->getRaw();
        const auto& rowCountRaw = rowCounts->getRaw();
        const auto& valueCountRaw = valueCounts->getRaw();
        const auto& sumRaw = sums->getRaw();
        const auto& minimumRaw = minima->getRaw();
        const auto& maximumRaw = maxima->getRaw();
        const auto& averageRaw = averages->getRaw();
        for (size_t row = offset; row < offset + rowCount; row++) {
            std::optional<std::string> key;
            if (keyRaw[row]) {
                key = std::string(*keyRaw[row]);
            }

            Names names;
            readStringList(listRaw[row], names);

            _rows.push_back({key,
                             names,
                             rowCountRaw[row],
                             valueCountRaw[row],
                             sumRaw[row],
                             minimumRaw[row],
                             maximumRaw[row],
                             averageRaw[row]});
        }
    }

    void sortedRows(std::vector<Row>& rows) const {
        rows = _rows;
        std::sort(rows.begin(), rows.end());
    }

private:
    std::vector<Row> _rows;
};

// Collects the ([names], min, avg) row an ungrouped collect emits beside two scalar
// aggregates of its own.
class ListAndTwoReductionsSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 3u);

        const auto* lists = dynamic_cast<const ColumnVector<ListView>*>(chunks[0]);
        const auto* minima = dynamic_cast<const ColumnOptVector<int64_t>*>(chunks[1]);
        const auto* averages = dynamic_cast<const ColumnOptVector<double>*>(chunks[2]);
        ASSERT_NE(lists, nullptr);
        ASSERT_NE(minima, nullptr);
        ASSERT_NE(averages, nullptr);

        const auto& listRaw = lists->getRaw();
        for (size_t row = offset; row < offset + rowCount; row++) {
            readStringList(listRaw[row], _names);

            _min = minima->getRaw()[row];
            _average = averages->getRaw()[row];
            _rowCount++;
        }
    }

    const Names& getNames() const { return _names; }
    OptInt64 getMin() const { return _min; }
    OptDouble getAverage() const { return _average; }
    size_t getRowCount() const { return _rowCount; }

private:
    Names _names;
    OptInt64 _min;
    OptDouble _average;
    size_t _rowCount {0};
};

}

// One accumulator holds a group's collected list and its reductions, so every
// GroupAggregateKind has to come out beside the list it was folded with. This suite runs
// the whole matrix: count, sum, min, max, avg and the distinct forms of count, sum and
// avg, one per test, then all the non-distinct ones on a single collect.
class CypherCollectAggregateTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _interp3 = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager());

        SystemAccessor system = _env->getSystemManager().accessUnique();
        system.createGraph(_graphName);
    }

    // Runs a CREATE query in its own change and submits it.
    void create(std::string_view query) {
        ChangeID changeID;
        {
            SystemAccessor system = _env->getSystemManager().accessUnique();
            const auto res = system.newChange(_graphName);
            ASSERT_TRUE(res);
            changeID = res.value()->id();
        }

        NullSink discardSink;
        QueryStatus createStatus;
        _interp3->execute(createStatus, query, _graphName, CommitHash::head(), changeID, &_env->getMem(), &discardSink);
        ASSERT_TRUE(createStatus.isOk()) << "CREATE failed: " << createStatus.getError();

        QueryCallbacks callbacks;
        callbacks.setOnOutputData([](const Dataframe*) {});
        const QueryState submitState(_graphName, &_env->getMem(), &_queryConfig, &callbacks, CommitHash::head(), changeID);
        const QueryStatus submitStatus = _env->getDB().query("CHANGE SUBMIT", submitState);
        ASSERT_TRUE(submitStatus.isOk()) << "CHANGE SUBMIT failed";
    }

    // Runs a read-only MATCH query against the committed head.
    void match(std::string_view query, NLOutputSink& sink) {
        QueryStatus status;
        _interp3->execute(status, query, _graphName, CommitHash::head(), ChangeID::head(), &_env->getMem(), &sink);
        ASSERT_TRUE(status.isOk()) << "MATCH failed: " << status.getError();
    }

    // Inserts 6 nodes over three teams. red repeats the score 10 so the distinct forms
    // part from the plain ones, blue holds one score beside a node with none, and grey
    // holds no score at all - the group every reduction reads over no value.
    void buildTeamGraph() {
        create(R"(CREATE (:Node {team: "red", name: "alice", score: 10}), (:Node {team: "red", name: "carol", score: 10}), (:Node {team: "red", name: "erin", score: 40}))");
        create(R"(CREATE (:Node {team: "blue", name: "bob", score: 100}), (:Node {team: "blue", name: "dan"}), (:Node {team: "grey", name: "gus"}))");
    }

    const std::string _graphName = "teamGraph";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interp3;
    QueryConfig _queryConfig;
};

TEST_F(CypherCollectAggregateTest, groupedCollectBesideACountOfRows) {
    buildTeamGraph();

    KeyedListAndCountSink sink;
    match("MATCH (n:Node) RETURN n.team, collect(n.name), count(*)", sink);

    std::vector<KeyedListAndCountSink::Row> rows;
    sink.sortedRows(rows);

    const std::vector<KeyedListAndCountSink::Row> expected {
        {"blue", {"bob", "dan"}, 2},
        {"grey", {"gus"}, 1},
        {"red", {"alice", "carol", "erin"}, 3},
    };
    EXPECT_EQ(rows, expected);
}

TEST_F(CypherCollectAggregateTest, groupedCollectBesideACountOfValues) {
    buildTeamGraph();

    KeyedListAndCountSink sink;
    match("MATCH (n:Node) RETURN n.team, collect(n.name), count(n.score)", sink);

    std::vector<KeyedListAndCountSink::Row> rows;
    sink.sortedRows(rows);

    const std::vector<KeyedListAndCountSink::Row> expected {
        {"blue", {"bob", "dan"}, 1},
        {"grey", {"gus"}, 0},
        {"red", {"alice", "carol", "erin"}, 3},
    };
    EXPECT_EQ(rows, expected);
}

TEST_F(CypherCollectAggregateTest, groupedCollectBesideASum) {
    buildTeamGraph();

    KeyedListAndInt64Sink sink;
    match("MATCH (n:Node) RETURN n.team, collect(n.name), sum(n.score)", sink);

    std::vector<KeyedListAndInt64Sink::Row> rows;
    sink.sortedRows(rows);

    const std::vector<KeyedListAndInt64Sink::Row> expected {
        {"blue", {"bob", "dan"}, 100},
        {"grey", {"gus"}, 0},
        {"red", {"alice", "carol", "erin"}, 60},
    };
    EXPECT_EQ(rows, expected);
}

TEST_F(CypherCollectAggregateTest, groupedCollectBesideAMinimum) {
    buildTeamGraph();

    KeyedListAndInt64Sink sink;
    match("MATCH (n:Node) RETURN n.team, collect(n.name), min(n.score)", sink);

    std::vector<KeyedListAndInt64Sink::Row> rows;
    sink.sortedRows(rows);

    const std::vector<KeyedListAndInt64Sink::Row> expected {
        {"blue", {"bob", "dan"}, 100},
        {"grey", {"gus"}, std::nullopt},
        {"red", {"alice", "carol", "erin"}, 10},
    };
    EXPECT_EQ(rows, expected);
}

TEST_F(CypherCollectAggregateTest, groupedCollectBesideAMaximum) {
    buildTeamGraph();

    KeyedListAndInt64Sink sink;
    match("MATCH (n:Node) RETURN n.team, collect(n.name), max(n.score)", sink);

    std::vector<KeyedListAndInt64Sink::Row> rows;
    sink.sortedRows(rows);

    const std::vector<KeyedListAndInt64Sink::Row> expected {
        {"blue", {"bob", "dan"}, 100},
        {"grey", {"gus"}, std::nullopt},
        {"red", {"alice", "carol", "erin"}, 40},
    };
    EXPECT_EQ(rows, expected);
}

TEST_F(CypherCollectAggregateTest, groupedCollectBesideAnAverage) {
    buildTeamGraph();

    KeyedListAndDoubleSink sink;
    match("MATCH (n:Node) RETURN n.team, collect(n.name), avg(n.score)", sink);

    std::vector<KeyedListAndDoubleSink::Row> rows;
    sink.sortedRows(rows);

    const std::vector<KeyedListAndDoubleSink::Row> expected {
        {"blue", {"bob", "dan"}, 100.0},
        {"grey", {"gus"}, std::nullopt},
        {"red", {"alice", "carol", "erin"}, 20.0},
    };
    EXPECT_EQ(rows, expected);
}

TEST_F(CypherCollectAggregateTest, groupedCollectBesideACountOfDistinctValues) {
    buildTeamGraph();

    KeyedListAndCountSink sink;
    match("MATCH (n:Node) RETURN n.team, collect(n.name), count(DISTINCT n.score)", sink);

    std::vector<KeyedListAndCountSink::Row> rows;
    sink.sortedRows(rows);

    const std::vector<KeyedListAndCountSink::Row> expected {
        {"blue", {"bob", "dan"}, 1},
        {"grey", {"gus"}, 0},
        {"red", {"alice", "carol", "erin"}, 2},
    };
    EXPECT_EQ(rows, expected);
}

TEST_F(CypherCollectAggregateTest, groupedCollectBesideASumOfDistinctValues) {
    buildTeamGraph();

    KeyedListAndInt64Sink sink;
    match("MATCH (n:Node) RETURN n.team, collect(n.name), sum(DISTINCT n.score)", sink);

    std::vector<KeyedListAndInt64Sink::Row> rows;
    sink.sortedRows(rows);

    const std::vector<KeyedListAndInt64Sink::Row> expected {
        {"blue", {"bob", "dan"}, 100},
        {"grey", {"gus"}, 0},
        {"red", {"alice", "carol", "erin"}, 50},
    };
    EXPECT_EQ(rows, expected);
}

TEST_F(CypherCollectAggregateTest, groupedCollectBesideAnAverageOfDistinctValues) {
    buildTeamGraph();

    KeyedListAndDoubleSink sink;
    match("MATCH (n:Node) RETURN n.team, collect(n.name), avg(DISTINCT n.score)", sink);

    std::vector<KeyedListAndDoubleSink::Row> rows;
    sink.sortedRows(rows);

    const std::vector<KeyedListAndDoubleSink::Row> expected {
        {"blue", {"bob", "dan"}, 100.0},
        {"grey", {"gus"}, std::nullopt},
        {"red", {"alice", "carol", "erin"}, 25.0},
    };
    EXPECT_EQ(rows, expected);
}

TEST_F(CypherCollectAggregateTest, groupedCollectBesideEveryReduction) {
    buildTeamGraph();

    KeyedListAndEveryReductionSink sink;
    match("MATCH (n:Node) RETURN n.team, collect(n.name), count(*), count(n.score), sum(n.score), min(n.score), max(n.score), avg(n.score)", sink);

    std::vector<KeyedListAndEveryReductionSink::Row> rows;
    sink.sortedRows(rows);

    const std::vector<KeyedListAndEveryReductionSink::Row> expected {
        {"blue", {"bob", "dan"}, 2, 1, 100, 100, 100, 100.0},
        {"grey", {"gus"}, 1, 0, 0, std::nullopt, std::nullopt, std::nullopt},
        {"red", {"alice", "carol", "erin"}, 3, 3, 60, 10, 40, 20.0},
    };
    EXPECT_EQ(rows, expected);
}

// Ordering on a reduction the collect carries: the key of the sort is a column of the
// same drain the list comes out of. A null extremum is the largest value, so descending
// puts the group holding no score first.
TEST_F(CypherCollectAggregateTest, groupedCollectOrderedByAMinimum) {
    buildTeamGraph();

    KeyedListAndInt64Sink sink;
    match("MATCH (n:Node) RETURN n.team, collect(n.name), min(n.score) ORDER BY min(n.score) DESC", sink);

    std::vector<KeyedListAndInt64Sink::Row> rows;
    sink.rowsInEmitOrder(rows);

    const std::vector<KeyedListAndInt64Sink::Row> expected {
        {"grey", {"gus"}, std::nullopt},
        {"blue", {"bob", "dan"}, 100},
        {"red", {"alice", "carol", "erin"}, 10},
    };
    EXPECT_EQ(rows, expected);
}

TEST_F(CypherCollectAggregateTest, ungroupedCollectBesideAMinimumAndAnAverage) {
    buildTeamGraph();

    ListAndTwoReductionsSink sink;
    match("MATCH (n:Node) RETURN collect(n.name), min(n.score), avg(n.score)", sink);

    EXPECT_EQ(sink.getRowCount(), 1u);
    EXPECT_EQ(sink.getNames(), (Names {"alice", "carol", "erin", "bob", "dan", "gus"}));
    EXPECT_EQ(sink.getMin(), OptInt64 {10});
    EXPECT_EQ(sink.getAverage(), OptDouble {40.0});
}
