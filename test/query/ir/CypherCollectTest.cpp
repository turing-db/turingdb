#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
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

// Reads one list cell (a ColumnVector<ListView> row) as a vector of strings.
void readStringList(const ListView& view, std::vector<std::string>& out) {
    out.clear();
    for (const ListElementView& element : view) {
        out.push_back(std::string(element.getAs<std::string_view>()));
    }
}

// Reads one list cell as a vector of int64s.
void readInt64List(const ListView& view, std::vector<int64_t>& out) {
    out.clear();
    for (const ListElementView& element : view) {
        out.push_back(element.getAs<int64_t>());
    }
}

// Reads one list cell as a vector of node ids, asserting each element carries the node
// tag rather than a bare integer.
void readNodeIDList(const ListView& view, std::vector<uint64_t>& out) {
    out.clear();
    for (const ListElementView& element : view) {
        ASSERT_EQ(element.getTag(), ListBufferTypeTag::NodeID);
        out.push_back(element.getAs<NodeID>().getValue());
    }
}

// Reads one list cell as a vector of edge ids, asserting the edge tag.
void readEdgeIDList(const ListView& view, std::vector<uint64_t>& out) {
    out.clear();
    for (const ListElementView& element : view) {
        ASSERT_EQ(element.getTag(), ListBufferTypeTag::EdgeID);
        out.push_back(element.getAs<EdgeID>().getValue());
    }
}

// Discards output: used by the CREATE queries building the fixture and by the queries
// that are rejected before producing rows.
class NullSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const>, size_t, size_t) override {}
};

// Collects the (team, [names]) rows a grouped collect emits: a nullable string key chunk
// and a per-group list cell chunk. The list keeps its append order; the row set is
// captured for an order-independent assert.
class KeyedStringListSink : public NLOutputSink {
public:
    using Row = std::pair<std::optional<std::string>, std::vector<std::string>>;

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

            std::vector<std::string> names;
            readStringList(listRaw[row], names);

            _rows.push_back({key, names});
        }
    }

    void sortedRows(std::vector<Row>& rows) const {
        rows = _rows;
        std::sort(rows.begin(), rows.end());
    }

    // The rows as the sink saw them, for the ORDER BY tests: the emit order is the result.
    void rowsInEmitOrder(std::vector<Row>& rows) const {
        rows = _rows;
    }

private:
    std::vector<Row> _rows;
};

// Collects the (team, [scores]) rows a grouped collect over an Int64 value emits.
class KeyedInt64ListSink : public NLOutputSink {
public:
    using Row = std::pair<std::optional<std::string>, std::vector<int64_t>>;

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

            std::vector<int64_t> scores;
            readInt64List(listRaw[row], scores);

            _rows.push_back({key, scores});
        }
    }

    void sortedRows(std::vector<Row>& rows) const {
        rows = _rows;
        std::sort(rows.begin(), rows.end());
    }

private:
    std::vector<Row> _rows;
};

// Collects the (score, [scores]) rows a collect keyed on a nullable Int64 alias emits.
class Int64KeyedListSink : public NLOutputSink {
public:
    using Row = std::pair<std::optional<int64_t>, std::vector<int64_t>>;

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
            std::vector<int64_t> scores;
            readInt64List(listRaw[row], scores);

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

// Collects the (key, [ids]) rows a grouped entity collect emits, reading each list cell
// with the tag-checking reader for the collected entity kind.
template <void (*Reader)(const ListView&, std::vector<uint64_t>&)>
class KeyedIDListSink : public NLOutputSink {
public:
    using Row = std::pair<std::optional<std::string>, std::vector<uint64_t>>;

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

            std::vector<uint64_t> ids;
            Reader(listRaw[row], ids);

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

using KeyedNodeListSink = KeyedIDListSink<readNodeIDList>;
using KeyedEdgeListSink = KeyedIDListSink<readEdgeIDList>;

// Collects the (node, [nodes]) rows a collect keyed on a node alias emits.
class NodeKeyedListSink : public NLOutputSink {
public:
    using Row = std::pair<uint64_t, std::vector<uint64_t>>;

    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 2u);

        const auto* keys = dynamic_cast<const ColumnVector<NodeID>*>(chunks[0]);
        const auto* lists = dynamic_cast<const ColumnVector<ListView>*>(chunks[1]);
        ASSERT_NE(keys, nullptr);
        ASSERT_NE(lists, nullptr);
        ASSERT_EQ(keys->size(), lists->size());

        const auto& keyRaw = keys->getRaw();
        const auto& listRaw = lists->getRaw();
        for (size_t row = offset; row < offset + rowCount; row++) {
            std::vector<uint64_t> ids;
            readNodeIDList(listRaw[row], ids);

            _rows.push_back({keyRaw[row].getValue(), ids});
        }
    }

    void sortedRows(std::vector<Row>& rows) const {
        rows = _rows;
        std::sort(rows.begin(), rows.end());
    }

private:
    std::vector<Row> _rows;
};


// Collects the (node, [names]) rows a collect keyed on a node alias emits.
class NodeKeyedStringListSink : public NLOutputSink {
public:
    using Row = std::pair<uint64_t, std::vector<std::string>>;

    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 2u);

        const auto* keys = dynamic_cast<const ColumnVector<NodeID>*>(chunks[0]);
        const auto* lists = dynamic_cast<const ColumnVector<ListView>*>(chunks[1]);
        ASSERT_NE(keys, nullptr);
        ASSERT_NE(lists, nullptr);
        ASSERT_EQ(keys->size(), lists->size());

        const auto& keyRaw = keys->getRaw();
        const auto& listRaw = lists->getRaw();
        for (size_t row = offset; row < offset + rowCount; row++) {
            std::vector<std::string> names;
            readStringList(listRaw[row], names);

            _rows.push_back({keyRaw[row].getValue(), names});
        }
    }

    // The rows as the sink saw them, for the ORDER BY tests: the emit order is the result.
    void rowsInEmitOrder(std::vector<Row>& rows) const {
        rows = _rows;
    }

private:
    std::vector<Row> _rows;
};


// Collects the ([ids]) rows an ungrouped entity collect emits: a single list cell chunk.
class NodeListSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 1u);

        const auto* lists = dynamic_cast<const ColumnVector<ListView>*>(chunks[0]);
        ASSERT_NE(lists, nullptr);

        const auto& listRaw = lists->getRaw();
        for (size_t row = offset; row < offset + rowCount; row++) {
            std::vector<uint64_t> ids;
            readNodeIDList(listRaw[row], ids);
            _rows.push_back(ids);
        }
    }

    const std::vector<std::vector<uint64_t>>& rows() const { return _rows; }

private:
    std::vector<std::vector<uint64_t>> _rows;
};

// Collects the ([names]) rows an ungrouped collect emits: a single list cell chunk.
class StringListSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 1u);

        const auto* lists = dynamic_cast<const ColumnVector<ListView>*>(chunks[0]);
        ASSERT_NE(lists, nullptr);

        const auto& listRaw = lists->getRaw();
        for (size_t row = offset; row < offset + rowCount; row++) {
            std::vector<std::string> names;
            readStringList(listRaw[row], names);
            _rows.push_back(names);
        }
    }

    const std::vector<std::vector<std::string>>& rows() const { return _rows; }

private:
    std::vector<std::vector<std::string>> _rows;
};

}

// End-to-end tests driving collect() from Cypher text through parsing, analysis,
// DBProgramGenerator codegen, NL lowering and NLInterpreter execution.
class CypherCollectTest : public TuringTest {
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

    // Runs a query expected to be rejected, and hands back the status carrying the reason.
    void runQuery(std::string_view query, QueryStatus& status) {
        NullSink sink;
        _interp3->execute(status, query, _graphName, CommitHash::head(), ChangeID::head(), &_env->getMem(), &sink);
    }

    // Inserts 4 nodes: red/alice/10, red/carol/20, blue/bob/100, blue/dan (no score).
    void buildTeamGraph() {
        create(R"(CREATE (:Node {team: "red", name: "alice", score: 10}), (:Node {team: "red", name: "carol", score: 20}), (:Node {team: "blue", name: "bob", score: 100}))");
        create(R"(CREATE (:Node {team: "blue", name: "dan"}))");
    }

    // Inserts alice, who KNOWS both bob and carol.
    void buildKnowsGraph() {
        create(R"(CREATE (:Person {name: "alice"}), (:Person {name: "bob"}), (:Person {name: "carol"}))");
        create(R"(MATCH (a:Person {name: "alice"}), (b:Person {name: "bob"}) CREATE (a)-[:KNOWS]->(b))");
        create(R"(MATCH (a:Person {name: "alice"}), (c:Person {name: "carol"}) CREATE (a)-[:KNOWS]->(c))");
    }

    const std::string _graphName = "teamGraph";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interp3;
    QueryConfig _queryConfig;
};

TEST_F(CypherCollectTest, groupedCollectStrings) {
    buildTeamGraph();

    KeyedStringListSink sink;
    match("MATCH (n:Node) RETURN n.team, collect(n.name)", sink);

    std::vector<KeyedStringListSink::Row> rows;
    sink.sortedRows(rows);

    const std::vector<KeyedStringListSink::Row> expected {
        {"blue", {"bob", "dan"}},
        {"red", {"alice", "carol"}},
    };
    EXPECT_EQ(rows, expected);
}

TEST_F(CypherCollectTest, groupedCollectWithAliases) {
    buildTeamGraph();

    KeyedStringListSink sink;
    match("MATCH (n:Node) RETURN n.team AS team, collect(n.name) AS names", sink);

    std::vector<KeyedStringListSink::Row> rows;
    sink.sortedRows(rows);

    const std::vector<KeyedStringListSink::Row> expected {
        {"blue", {"bob", "dan"}},
        {"red", {"alice", "carol"}},
    };
    EXPECT_EQ(rows, expected);
}

// The grouping key is bound by the outer loop and the collected values by the inner one,
// so the per-group appends must happen where the traversal binds the target.
TEST_F(CypherCollectTest, groupedCollectOverTraversal) {
    buildKnowsGraph();

    KeyedStringListSink sink;
    match("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, collect(b.name)", sink);

    std::vector<KeyedStringListSink::Row> rows;
    sink.sortedRows(rows);

    const std::vector<KeyedStringListSink::Row> expected {{"alice", {"bob", "carol"}}};
    EXPECT_EQ(rows, expected);
}

TEST_F(CypherCollectTest, groupedCollectDropsNulls) {
    buildTeamGraph();

    KeyedInt64ListSink sink;
    match("MATCH (n:Node) RETURN n.team, collect(n.score)", sink);

    // dan has no score, so the blue group collects the one score it has: Cypher's
    // collect ignores nulls.
    std::vector<KeyedInt64ListSink::Row> rows;
    sink.sortedRows(rows);

    const std::vector<KeyedInt64ListSink::Row> expected {
        {"blue", {100}},
        {"red", {10, 20}},
    };
    EXPECT_EQ(rows, expected);
}

TEST_F(CypherCollectTest, ungroupedCollect) {
    buildTeamGraph();

    StringListSink sink;
    match("MATCH (n:Node) RETURN collect(n.name)", sink);

    const std::vector<std::vector<std::string>> expected {{"alice", "carol", "bob", "dan"}};
    EXPECT_EQ(sink.rows(), expected);
}

TEST_F(CypherCollectTest, ungroupedCollectOverNoRow) {
    buildTeamGraph();

    StringListSink sink;
    match(R"(MATCH (n:Node {team: "green"}) RETURN collect(n.name))", sink);

    const std::vector<std::vector<std::string>> expected {{}};
    EXPECT_EQ(sink.rows(), expected);
}

TEST_F(CypherCollectTest, groupedCollectWithLimit) {
    buildTeamGraph();

    KeyedStringListSink sink;
    match("MATCH (n:Node) RETURN n.team, collect(n.name) LIMIT 1", sink);

    // One group row survives the cut, and it is a whole group: the limit budgets the
    // drain, not the rows folded into it.
    std::vector<KeyedStringListSink::Row> rows;
    sink.sortedRows(rows);

    ASSERT_EQ(rows.size(), 1u);
    const KeyedStringListSink::Row& row = rows.front();
    ASSERT_TRUE(row.first);

    if (*row.first == "red") {
        EXPECT_EQ(row.second, std::vector<std::string>({"alice", "carol"}));
    } else {
        EXPECT_EQ(*row.first, "blue");
        EXPECT_EQ(row.second, std::vector<std::string>({"bob", "dan"}));
    }
}

TEST_F(CypherCollectTest, groupedCollectWithSkipAndLimit) {
    buildTeamGraph();

    KeyedStringListSink sink;
    match("MATCH (n:Node) RETURN n.team, collect(n.name) SKIP 1 LIMIT 1", sink);

    std::vector<KeyedStringListSink::Row> rows;
    sink.sortedRows(rows);

    ASSERT_EQ(rows.size(), 1u);
    const KeyedStringListSink::Row& row = rows.front();
    ASSERT_TRUE(row.first);

    if (*row.first == "red") {
        EXPECT_EQ(row.second, std::vector<std::string>({"alice", "carol"}));
    } else {
        EXPECT_EQ(*row.first, "blue");
        EXPECT_EQ(row.second, std::vector<std::string>({"bob", "dan"}));
    }
}

TEST_F(CypherCollectTest, groupedCollectWithSkip) {
    buildTeamGraph();

    KeyedStringListSink sink;
    match("MATCH (n:Node) RETURN n.team, collect(n.name) SKIP 1", sink);

    std::vector<KeyedStringListSink::Row> rows;
    sink.sortedRows(rows);

    ASSERT_EQ(rows.size(), 1u);
    const KeyedStringListSink::Row& row = rows.front();
    ASSERT_TRUE(row.first);

    if (*row.first == "red") {
        EXPECT_EQ(row.second, std::vector<std::string>({"alice", "carol"}));
    } else {
        EXPECT_EQ(*row.first, "blue");
        EXPECT_EQ(row.second, std::vector<std::string>({"bob", "dan"}));
    }
}

TEST_F(CypherCollectTest, rejectsCollectWithAnotherAggregate) {
    buildTeamGraph();

    QueryStatus status;
    runQuery("MATCH (n:Node) RETURN n.team, collect(n.name), count(*)", status);

    EXPECT_EQ(status.getStatus(), QueryStatus::Status::PLAN_ERROR);
    EXPECT_EQ(status.getError(), "collect() may not yet be combined with another aggregate.");
}

TEST_F(CypherCollectTest, rejectsUngroupedCollectWithCount) {
    buildTeamGraph();

    QueryStatus status;
    runQuery("MATCH (n:Node) RETURN collect(n.name), count(*)", status);

    EXPECT_EQ(status.getStatus(), QueryStatus::Status::PLAN_ERROR);
    EXPECT_EQ(status.getError(), "collect() may not yet be combined with another aggregate.");
}

TEST_F(CypherCollectTest, rejectsUngroupedCollectWithAWrappedAggregate) {
    buildTeamGraph();

    QueryStatus status;
    runQuery("MATCH (n:Node) RETURN collect(n.name), count(n) + 1", status);

    EXPECT_EQ(status.getStatus(), QueryStatus::Status::PLAN_ERROR);
    EXPECT_EQ(status.getError(), "collect() may not yet be combined with another aggregate.");
}

TEST_F(CypherCollectTest, rejectsGroupedCollectWithAWrappedAggregate) {
    buildTeamGraph();

    QueryStatus status;
    runQuery("MATCH (n:Node) RETURN n.team, collect(n.name), sum(n.score) + 0", status);

    EXPECT_EQ(status.getStatus(), QueryStatus::Status::PLAN_ERROR);
    EXPECT_EQ(status.getError(), "collect() may not yet be combined with another aggregate.");
}

TEST_F(CypherCollectTest, rejectsTwoCollects) {
    buildTeamGraph();

    QueryStatus status;
    runQuery("MATCH (n:Node) RETURN collect(n.name), collect(n.team)", status);

    EXPECT_EQ(status.getStatus(), QueryStatus::Status::PLAN_ERROR);
    EXPECT_EQ(status.getError(), "collect() may not yet be combined with another aggregate.");
}

// The sort orders the group rows the drain emits, carrying each group's list along with
// the key it belongs to.
TEST_F(CypherCollectTest, groupedCollectWithOrderBy) {
    buildTeamGraph();

    KeyedStringListSink sink;
    match("MATCH (n:Node) RETURN n.team, collect(n.name) ORDER BY n.team", sink);

    std::vector<KeyedStringListSink::Row> rows;
    sink.rowsInEmitOrder(rows);

    const std::vector<KeyedStringListSink::Row> expected {
        {"blue", {"bob", "dan"}},
        {"red", {"alice", "carol"}},
    };
    EXPECT_EQ(rows, expected);
}

TEST_F(CypherCollectTest, groupedCollectWithOrderByDescending) {
    buildTeamGraph();

    KeyedStringListSink sink;
    match("MATCH (n:Node) RETURN n.team, collect(n.name) ORDER BY n.team DESC", sink);

    std::vector<KeyedStringListSink::Row> rows;
    sink.rowsInEmitOrder(rows);

    const std::vector<KeyedStringListSink::Row> expected {
        {"red", {"alice", "carol"}},
        {"blue", {"bob", "dan"}},
    };
    EXPECT_EQ(rows, expected);
}

TEST_F(CypherCollectTest, groupedCollectWithOrderByOverTheReturnedNode) {
    buildTeamGraph();

    NodeKeyedStringListSink sink;
    match("MATCH (n:Node) RETURN n, collect(n.name) ORDER BY n.name", sink);

    std::vector<NodeKeyedStringListSink::Row> rows;
    sink.rowsInEmitOrder(rows);

    const std::vector<NodeKeyedStringListSink::Row> expected {
        {0, {"alice"}},
        {2, {"bob"}},
        {1, {"carol"}},
        {3, {"dan"}},
    };
    EXPECT_EQ(rows, expected);
}

// ORDER BY ... LIMIT fuses into a top-K accumulator, which trims the buffers by gathering
// the surviving rows - the list column included.
TEST_F(CypherCollectTest, groupedCollectWithOrderByAndLimit) {
    buildTeamGraph();

    KeyedStringListSink sink;
    match("MATCH (n:Node) RETURN n.team, collect(n.name) ORDER BY n.team LIMIT 1", sink);

    std::vector<KeyedStringListSink::Row> rows;
    sink.rowsInEmitOrder(rows);

    const std::vector<KeyedStringListSink::Row> expected {{"blue", {"bob", "dan"}}};
    EXPECT_EQ(rows, expected);
}

// A list is carried through the sort row-aligned with the keys but never compared, so it
// cannot be the key the rows are ordered on - alias or spelled out.
TEST_F(CypherCollectTest, rejectsOrderByOnTheCollectedList) {
    buildTeamGraph();

    QueryStatus aliasStatus;
    runQuery("MATCH (n:Node) RETURN n.team, collect(n.name) AS names ORDER BY names", aliasStatus);
    EXPECT_EQ(aliasStatus.getStatus(), QueryStatus::Status::EXEC_ERROR);
    EXPECT_EQ(aliasStatus.getError(), "a list column cannot be a sort key");

    QueryStatus spelledOutStatus;
    runQuery("MATCH (n:Node) RETURN n.team, collect(n.name) ORDER BY collect(n.name)", spelledOutStatus);
    EXPECT_EQ(spelledOutStatus.getStatus(), QueryStatus::Status::EXEC_ERROR);
    EXPECT_EQ(spelledOutStatus.getError(), "a list column cannot be a sort key");
}

TEST_F(CypherCollectTest, rejectsCollectOfConstant) {
    buildTeamGraph();

    QueryStatus status;
    runQuery("MATCH (n:Node) RETURN n.team, collect(1)", status);

    EXPECT_EQ(status.getStatus(), QueryStatus::Status::PLAN_ERROR);
    EXPECT_EQ(status.getError(), "collect() of a constant expression is not yet supported.");
}

// The alias is what makes the collected column look row-carrying: what it names is still
// the constant, bound above the loop the matched rows are read in, so both spellings must
// reach the same rejection.
TEST_F(CypherCollectTest, rejectsCollectOfAConstantAlias) {
    buildTeamGraph();

    QueryStatus groupedStatus;
    runQuery("MATCH (n:Node) RETURN n.team, 1 AS x, collect(x)", groupedStatus);

    EXPECT_EQ(groupedStatus.getStatus(), QueryStatus::Status::PLAN_ERROR);
    EXPECT_EQ(groupedStatus.getError(), "collect() of a constant expression is not yet supported.");

    QueryStatus ungroupedStatus;
    runQuery("MATCH (n:Node) RETURN 1 AS x, collect(x)", ungroupedStatus);

    EXPECT_EQ(ungroupedStatus.getStatus(), QueryStatus::Status::PLAN_ERROR);
    EXPECT_EQ(ungroupedStatus.getError(), "collect() of a constant expression is not yet supported.");
}

// An aliased item is a non-aggregate item like any other, so it groups the rows too: each
// list holds the one value its group is, not the values of the whole match.
TEST_F(CypherCollectTest, collectOverAPropertyAlias) {
    buildTeamGraph();

    KeyedStringListSink sink;
    match("MATCH (n:Node) RETURN n.name AS nm, collect(nm)", sink);

    std::vector<KeyedStringListSink::Row> rows;
    sink.sortedRows(rows);

    const std::vector<KeyedStringListSink::Row> expected {
        {"alice", {"alice"}},
        {"bob", {"bob"}},
        {"carol", {"carol"}},
        {"dan", {"dan"}},
    };
    EXPECT_EQ(rows, expected);
}

// dan has no score, so the null keys a group of its own - and collect drops the null it
// would have collected there, leaving that group's list empty.
TEST_F(CypherCollectTest, collectOverANullablePropertyAlias) {
    buildTeamGraph();

    Int64KeyedListSink sink;
    match("MATCH (n:Node) RETURN n.score AS s, collect(s)", sink);

    std::vector<Int64KeyedListSink::Row> rows;
    sink.sortedRows(rows);

    const std::vector<Int64KeyedListSink::Row> expected {
        {std::nullopt, {}},
        {10, {10}},
        {20, {20}},
        {100, {100}},
    };
    EXPECT_EQ(rows, expected);
}

TEST_F(CypherCollectTest, collectOverAnEntityAlias) {
    buildTeamGraph();

    NodeKeyedListSink sink;
    match("MATCH (n:Node) RETURN n AS m, collect(m)", sink);

    std::vector<NodeKeyedListSink::Row> rows;
    sink.sortedRows(rows);

    const std::vector<NodeKeyedListSink::Row> expected {
        {0, {0}},
        {1, {1}},
        {2, {2}},
        {3, {3}},
    };
    EXPECT_EQ(rows, expected);
}

TEST_F(CypherCollectTest, groupedCollectOfNodes) {
    buildTeamGraph();

    KeyedNodeListSink sink;
    match("MATCH (n:Node) RETURN n.team, collect(n)", sink);

    std::vector<KeyedNodeListSink::Row> rows;
    sink.sortedRows(rows);

    const std::vector<KeyedNodeListSink::Row> expected {
        {"blue", {2, 3}},
        {"red", {0, 1}},
    };
    EXPECT_EQ(rows, expected);
}

TEST_F(CypherCollectTest, ungroupedCollectOfNodes) {
    buildTeamGraph();

    NodeListSink sink;
    match("MATCH (n:Node) RETURN collect(n)", sink);

    const std::vector<std::vector<uint64_t>> expected {{0, 1, 2, 3}};
    EXPECT_EQ(sink.rows(), expected);
}

TEST_F(CypherCollectTest, collectOfNodesOnUnlabelledMatch) {
    buildTeamGraph();

    KeyedNodeListSink sink;
    match("match (n) return n.team, collect(n)", sink);

    std::vector<KeyedNodeListSink::Row> rows;
    sink.sortedRows(rows);

    const std::vector<KeyedNodeListSink::Row> expected {
        {"blue", {2, 3}},
        {"red", {0, 1}},
    };
    EXPECT_EQ(rows, expected);
}

TEST_F(CypherCollectTest, groupedCollectOfEdges) {
    buildKnowsGraph();

    KeyedEdgeListSink sink;
    match("MATCH (a:Person)-[e:KNOWS]->(b:Person) RETURN a.name, collect(e)", sink);

    std::vector<KeyedEdgeListSink::Row> rows;
    sink.sortedRows(rows);

    const std::vector<KeyedEdgeListSink::Row> expected {{"alice", {0, 1}}};
    EXPECT_EQ(rows, expected);
}

// The list carries entities through the same buffers a value list does, so the sort and
// the skip/limit copies need no entity case of their own.
TEST_F(CypherCollectTest, collectOfNodesWithOrderByAndSkipLimit) {
    buildTeamGraph();

    KeyedNodeListSink orderedSink;
    match("MATCH (n:Node) RETURN n.team, collect(n) ORDER BY n.team DESC", orderedSink);

    std::vector<KeyedNodeListSink::Row> orderedRows;
    orderedSink.sortedRows(orderedRows);

    const std::vector<KeyedNodeListSink::Row> expected {
        {"blue", {2, 3}},
        {"red", {0, 1}},
    };
    EXPECT_EQ(orderedRows, expected);

    KeyedNodeListSink cutSink;
    match("MATCH (n:Node) RETURN n.team, collect(n) SKIP 1 LIMIT 1", cutSink);

    std::vector<KeyedNodeListSink::Row> cutRows;
    cutSink.sortedRows(cutRows);

    ASSERT_EQ(cutRows.size(), 1u);
    const KeyedNodeListSink::Row& row = cutRows.front();
    ASSERT_TRUE(row.first);

    if (*row.first == "red") {
        EXPECT_EQ(row.second, std::vector<uint64_t>({0, 1}));
    } else {
        EXPECT_EQ(*row.first, "blue");
        EXPECT_EQ(row.second, std::vector<uint64_t>({2, 3}));
    }
}

TEST_F(CypherCollectTest, ungroupedCollectDistinct) {
    buildTeamGraph();

    StringListSink sink;
    match("MATCH (n:Node) RETURN collect(DISTINCT n.team)", sink);

    const std::vector<std::vector<std::string>> expected {{"red", "blue"}};
    EXPECT_EQ(sink.rows(), expected);
}

TEST_F(CypherCollectTest, groupedCollectDistinct) {
    buildTeamGraph();

    KeyedStringListSink sink;
    match("MATCH (n:Node) RETURN n.team, collect(DISTINCT n.team)", sink);

    std::vector<KeyedStringListSink::Row> rows;
    sink.sortedRows(rows);

    const std::vector<KeyedStringListSink::Row> expected {
        {"blue", {"blue"}},
        {"red", {"red"}},
    };
    EXPECT_EQ(rows, expected);
}
