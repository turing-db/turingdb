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
using Values = std::vector<int64_t>;
using NodeIDs = std::vector<uint64_t>;

// Reads one list cell as a vector of strings.
void readStringList(const ListView& view, Names& out) {
    out.clear();
    for (const ListElementView& element : view) {
        out.push_back(std::string(element.getAs<std::string_view>()));
    }
}

// Reads one list cell as a vector of int64s.
void readInt64List(const ListView& view, Values& out) {
    out.clear();
    for (const ListElementView& element : view) {
        out.push_back(element.getAs<int64_t>());
    }
}

// Reads one list cell as a vector of node ids, asserting the node tag.
void readNodeIDList(const ListView& view, NodeIDs& out) {
    out.clear();
    for (const ListElementView& element : view) {
        ASSERT_EQ(element.getTag(), ListBufferTypeTag::NodeID);
        out.push_back(element.getAs<NodeID>().getValue());
    }
}

class NullSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const>, size_t, size_t) override {}
};

// Collects the (string key, [names]) rows a collect grouped on one string key emits.
class KeyedNameListSink : public NLOutputSink {
public:
    using Row = std::pair<std::optional<std::string>, Names>;

    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 2u);

        const auto* keys = dynamic_cast<const ColumnOptVector<std::string_view>*>(chunks[0]);
        const auto* lists = dynamic_cast<const ColumnVector<ListView>*>(chunks[1]);
        ASSERT_NE(keys, nullptr);
        ASSERT_NE(lists, nullptr);
        ASSERT_EQ(keys->size(), lists->size());

        const auto& keyRaw = keys->getRaw();
        const auto& listRaw = lists->getRaw();
        for (size_t row = offset; row < offset + rowCount; row++) {
            std::optional<std::string> key;
            if (keyRaw[row]) {
                key = std::string(*keyRaw[row]);
            }

            Names names;
            readStringList(listRaw[row], names);

            _rows.push_back({key, names});
        }
    }

    void sortedRows(std::vector<Row>& rows) const {
        rows = _rows;
        std::sort(rows.begin(), rows.end());
    }

private:
    std::vector<Row> _rows;
};

// Collects the (team, score, [names]) rows a collect grouped on two keys emits.
class TwoKeyNameListSink : public NLOutputSink {
public:
    using Row = std::tuple<std::optional<std::string>, std::optional<int64_t>, Names>;

    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 3u);

        const auto* teams = dynamic_cast<const ColumnOptVector<std::string_view>*>(chunks[0]);
        const auto* scores = dynamic_cast<const ColumnOptVector<int64_t>*>(chunks[1]);
        const auto* lists = dynamic_cast<const ColumnVector<ListView>*>(chunks[2]);
        ASSERT_NE(teams, nullptr);
        ASSERT_NE(scores, nullptr);
        ASSERT_NE(lists, nullptr);
        ASSERT_EQ(teams->size(), lists->size());
        ASSERT_EQ(scores->size(), lists->size());

        const auto& teamRaw = teams->getRaw();
        const auto& scoreRaw = scores->getRaw();
        const auto& listRaw = lists->getRaw();
        for (size_t row = offset; row < offset + rowCount; row++) {
            std::optional<std::string> team;
            if (teamRaw[row]) {
                team = std::string(*teamRaw[row]);
            }

            Names names;
            readStringList(listRaw[row], names);

            _rows.push_back({team, scoreRaw[row], names});
        }
    }

    void sortedRows(std::vector<Row>& rows) const {
        rows = _rows;
        std::sort(rows.begin(), rows.end());
    }

private:
    std::vector<Row> _rows;
};

// Collects the (score, [names]) rows a collect grouped on a nullable Int64 key emits.
class Int64KeyedNameListSink : public NLOutputSink {
public:
    using Row = std::pair<std::optional<int64_t>, Names>;

    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 2u);

        const auto* keys = dynamic_cast<const ColumnOptVector<int64_t>*>(chunks[0]);
        const auto* lists = dynamic_cast<const ColumnVector<ListView>*>(chunks[1]);
        ASSERT_NE(keys, nullptr);
        ASSERT_NE(lists, nullptr);
        ASSERT_EQ(keys->size(), lists->size());

        const auto& keyRaw = keys->getRaw();
        const auto& listRaw = lists->getRaw();
        for (size_t row = offset; row < offset + rowCount; row++) {
            Names names;
            readStringList(listRaw[row], names);

            _rows.push_back({keyRaw[row], names});
        }
    }

    void sortedRows(std::vector<Row>& rows) const {
        rows = _rows;
        std::sort(rows.begin(), rows.end());
    }

private:
    std::vector<Row> _rows;
};

// Collects the (team, [values]) rows a grouped collect of an Int64 expression emits.
class KeyedValueListSink : public NLOutputSink {
public:
    using Row = std::pair<std::optional<std::string>, Values>;

    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 2u);

        const auto* keys = dynamic_cast<const ColumnOptVector<std::string_view>*>(chunks[0]);
        const auto* lists = dynamic_cast<const ColumnVector<ListView>*>(chunks[1]);
        ASSERT_NE(keys, nullptr);
        ASSERT_NE(lists, nullptr);
        ASSERT_EQ(keys->size(), lists->size());

        const auto& keyRaw = keys->getRaw();
        const auto& listRaw = lists->getRaw();
        for (size_t row = offset; row < offset + rowCount; row++) {
            std::optional<std::string> key;
            if (keyRaw[row]) {
                key = std::string(*keyRaw[row]);
            }

            Values values;
            readInt64List(listRaw[row], values);

            _rows.push_back({key, values});
        }
    }

    void sortedRows(std::vector<Row>& rows) const {
        rows = _rows;
        std::sort(rows.begin(), rows.end());
    }

private:
    std::vector<Row> _rows;
};

// Collects the (name, [ids]) rows a grouped collect of nodes emits.
class KeyedNodeListSink : public NLOutputSink {
public:
    using Row = std::pair<std::optional<std::string>, NodeIDs>;

    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 2u);

        const auto* keys = dynamic_cast<const ColumnOptVector<std::string_view>*>(chunks[0]);
        const auto* lists = dynamic_cast<const ColumnVector<ListView>*>(chunks[1]);
        ASSERT_NE(keys, nullptr);
        ASSERT_NE(lists, nullptr);
        ASSERT_EQ(keys->size(), lists->size());

        const auto& keyRaw = keys->getRaw();
        const auto& listRaw = lists->getRaw();
        for (size_t row = offset; row < offset + rowCount; row++) {
            std::optional<std::string> key;
            if (keyRaw[row]) {
                key = std::string(*keyRaw[row]);
            }

            NodeIDs ids;
            readNodeIDList(listRaw[row], ids);

            _rows.push_back({key, ids});
        }
    }

    void sortedRows(std::vector<Row>& rows) const {
        rows = _rows;
        std::sort(rows.begin(), rows.end());
    }

private:
    std::vector<Row> _rows;
};

// Collects the single [names] row an ungrouped collect of strings emits.
class NameListSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 1u);

        const auto* lists = dynamic_cast<const ColumnVector<ListView>*>(chunks[0]);
        ASSERT_NE(lists, nullptr);

        const auto& listRaw = lists->getRaw();
        for (size_t row = offset; row < offset + rowCount; row++) {
            Names names;
            readStringList(listRaw[row], names);

            _rows.push_back(names);
        }
    }

    const std::vector<Names>& rows() const { return _rows; }

private:
    std::vector<Names> _rows;
};

// Collects the single [values] row an ungrouped collect of Int64s emits.
class ValueListSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 1u);

        const auto* lists = dynamic_cast<const ColumnVector<ListView>*>(chunks[0]);
        ASSERT_NE(lists, nullptr);

        const auto& listRaw = lists->getRaw();
        for (size_t row = offset; row < offset + rowCount; row++) {
            Values values;
            readInt64List(listRaw[row], values);

            _rows.push_back(values);
        }
    }

    const std::vector<Values>& rows() const { return _rows; }

private:
    std::vector<Values> _rows;
};

}

// The shapes a collect's input and grouping keys come in: several keys, a nullable key,
// an expression computed per row, a two-hop traversal, the cells of an UNWIND, and a
// filtered match.
class CypherCollectInputTest : public TuringTest {
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

    // Inserts 6 nodes over three teams: red holds the score 10 twice and 40 once, blue
    // holds 100 beside a node with no score, and grey holds no score either. The (team,
    // score) pairs make four groups over two keys, and two nodes share the null score.
    void buildTeamGraph() {
        create(R"(CREATE (:Node {team: "red", name: "alice", score: 10}), (:Node {team: "red", name: "carol", score: 10}), (:Node {team: "red", name: "erin", score: 40}))");
        create(R"(CREATE (:Node {team: "blue", name: "bob", score: 100}), (:Node {team: "blue", name: "dan"}), (:Node {team: "grey", name: "gus"}))");
    }

    // Inserts two disjoint two-hop chains out of alice (0): alice -> bob (1) -> carol (2)
    // and alice -> dave (3) -> erin (4).
    void buildChainGraph() {
        create(R"(CREATE (:Person {name: "alice"}), (:Person {name: "bob"}), (:Person {name: "carol"}), (:Person {name: "dave"}), (:Person {name: "erin"}))");
        create(R"(MATCH (a:Person {name: "alice"}), (b:Person {name: "bob"}) CREATE (a)-[:KNOWS]->(b))");
        create(R"(MATCH (b:Person {name: "bob"}), (c:Person {name: "carol"}) CREATE (b)-[:KNOWS]->(c))");
        create(R"(MATCH (a:Person {name: "alice"}), (d:Person {name: "dave"}) CREATE (a)-[:KNOWS]->(d))");
        create(R"(MATCH (d:Person {name: "dave"}), (e:Person {name: "erin"}) CREATE (d)-[:KNOWS]->(e))");
    }

    const std::string _graphName = "inputGraph";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interp3;
    QueryConfig _queryConfig;
};

// A group is the key pair, so the two nodes holding no score stay apart under the teams
// they belong to - where the single-key test below collects them together.
TEST_F(CypherCollectInputTest, groupsOnTwoKeys) {
    buildTeamGraph();

    TwoKeyNameListSink sink;
    match("MATCH (n:Node) RETURN n.team, n.score, collect(n.name)", sink);

    std::vector<TwoKeyNameListSink::Row> rows;
    sink.sortedRows(rows);

    const std::vector<TwoKeyNameListSink::Row> expected {
        {"blue", std::nullopt, {"dan"}},
        {"blue", 100, {"bob"}},
        {"grey", std::nullopt, {"gus"}},
        {"red", 10, {"alice", "carol"}},
        {"red", 40, {"erin"}},
    };
    EXPECT_EQ(rows, expected);
}

// A null key is a group of its own rather than a dropped row, and one group: the two
// nodes holding no score collect together.
TEST_F(CypherCollectInputTest, groupsOnANullableKey) {
    buildTeamGraph();

    Int64KeyedNameListSink sink;
    match("MATCH (n:Node) RETURN n.score, collect(n.name)", sink);

    std::vector<Int64KeyedNameListSink::Row> rows;
    sink.sortedRows(rows);

    const std::vector<Int64KeyedNameListSink::Row> expected {
        {std::nullopt, {"dan", "gus"}},
        {10, {"alice", "carol"}},
        {40, {"erin"}},
        {100, {"bob"}},
    };
    EXPECT_EQ(rows, expected);
}

TEST_F(CypherCollectInputTest, collectsARowVaryingExpressionPerGroup) {
    buildTeamGraph();

    KeyedValueListSink sink;
    match("MATCH (n:Node) RETURN n.team, collect(n.score * 2)", sink);

    std::vector<KeyedValueListSink::Row> rows;
    sink.sortedRows(rows);

    const std::vector<KeyedValueListSink::Row> expected {
        {"blue", {200}},
        {"grey", {}},
        {"red", {20, 20, 80}},
    };
    EXPECT_EQ(rows, expected);
}

TEST_F(CypherCollectInputTest, collectsARowVaryingExpressionOverTheWholeMatch) {
    buildTeamGraph();

    ValueListSink sink;
    match("MATCH (n:Node) RETURN collect(n.score + 1)", sink);

    ASSERT_EQ(sink.rows().size(), 1u);
    EXPECT_EQ(sink.rows().front(), (Values {11, 11, 41, 101}));
}

TEST_F(CypherCollectInputTest, collectsTheTargetOfATwoHopPattern) {
    buildChainGraph();

    KeyedNameListSink sink;
    match("MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person) RETURN a.name, collect(c.name)", sink);

    std::vector<KeyedNameListSink::Row> rows;
    sink.sortedRows(rows);

    const std::vector<KeyedNameListSink::Row> expected {{"alice", {"carol", "erin"}}};
    EXPECT_EQ(rows, expected);
}

TEST_F(CypherCollectInputTest, collectsTheMiddleNodeOfATwoHopPattern) {
    buildChainGraph();

    KeyedNodeListSink sink;
    match("MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person) RETURN a.name, collect(b)", sink);

    std::vector<KeyedNodeListSink::Row> rows;
    sink.sortedRows(rows);

    const std::vector<KeyedNodeListSink::Row> expected {{"alice", {1, 3}}};
    EXPECT_EQ(rows, expected);
}

TEST_F(CypherCollectInputTest, groupsOnTheMiddleOfATwoHopPattern) {
    buildChainGraph();

    KeyedNameListSink sink;
    match("MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person) RETURN b.name, collect(c.name)", sink);

    std::vector<KeyedNameListSink::Row> rows;
    sink.sortedRows(rows);

    const std::vector<KeyedNameListSink::Row> expected {
        {"bob", {"carol"}},
        {"dave", {"erin"}},
    };
    EXPECT_EQ(rows, expected);
}

TEST_F(CypherCollectInputTest, collectsTheCellsOfAnUnwoundList) {
    ValueListSink sink;
    match("UNWIND [10, 20, 20] AS v RETURN collect(v)", sink);

    ASSERT_EQ(sink.rows().size(), 1u);
    EXPECT_EQ(sink.rows().front(), (Values {10, 20, 20}));
}

TEST_F(CypherCollectInputTest, collectsTheDistinctCellsOfAnUnwoundList) {
    ValueListSink sink;
    match("UNWIND [10, 20, 20] AS v RETURN collect(DISTINCT v)", sink);

    ASSERT_EQ(sink.rows().size(), 1u);
    EXPECT_EQ(sink.rows().front(), (Values {10, 20}));
}

// The UNWIND crosses the match, so each group collects its own nodes' worth of cells.
TEST_F(CypherCollectInputTest, collectsAnUnwoundListPerGroup) {
    buildTeamGraph();

    KeyedValueListSink sink;
    match("MATCH (n:Node) UNWIND [1, 2] AS v RETURN n.team, collect(v)", sink);

    std::vector<KeyedValueListSink::Row> rows;
    sink.sortedRows(rows);

    const std::vector<KeyedValueListSink::Row> expected {
        {"blue", {1, 2, 1, 2}},
        {"grey", {1, 2}},
        {"red", {1, 2, 1, 2, 1, 2}},
    };
    EXPECT_EQ(rows, expected);
}

TEST_F(CypherCollectInputTest, collectsOnlyTheRowsAFilterKept) {
    buildTeamGraph();

    KeyedNameListSink sink;
    match("MATCH (n:Node) WHERE n.score > 15 RETURN n.team, collect(n.name)", sink);

    std::vector<KeyedNameListSink::Row> rows;
    sink.sortedRows(rows);

    const std::vector<KeyedNameListSink::Row> expected {
        {"blue", {"bob"}},
        {"red", {"erin"}},
    };
    EXPECT_EQ(rows, expected);
}

TEST_F(CypherCollectInputTest, collectsTheWholeMatchAFilterKept) {
    buildTeamGraph();

    NameListSink sink;
    match(R"(MATCH (n:Node) WHERE n.team = "red" RETURN collect(n.name))", sink);

    ASSERT_EQ(sink.rows().size(), 1u);
    EXPECT_EQ(sink.rows().front(), (Names {"alice", "carol", "erin"}));
}

// A grouped collect folds no row, so it emits no group - where the ungrouped form emits
// one empty list.
TEST_F(CypherCollectInputTest, emitsNoGroupWhenTheFilterKeepsNothing) {
    buildTeamGraph();

    KeyedNameListSink sink;
    match("MATCH (n:Node) WHERE n.score > 1000 RETURN n.team, collect(n.name)", sink);

    std::vector<KeyedNameListSink::Row> rows;
    sink.sortedRows(rows);
    EXPECT_TRUE(rows.empty());
}
