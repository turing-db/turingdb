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

const std::string_view droppedKeyReason =
    "ORDER BY with an aggregate may only order by expressions over the returned columns";

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

    void rowsInEmitOrder(std::vector<Row>& rows) const {
        rows = _rows;
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
template <void (*Reader)(const ListView&, std::vector<uint64_t>&)>
class IDListSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 1u);

        const auto* lists = dynamic_cast<const ColumnVector<ListView>*>(chunks[0]);
        ASSERT_NE(lists, nullptr);

        const auto& listRaw = lists->getRaw();
        for (size_t row = offset; row < offset + rowCount; row++) {
            std::vector<uint64_t> ids;
            Reader(listRaw[row], ids);
            _rows.push_back(ids);
        }
    }

    const std::vector<std::vector<uint64_t>>& rows() const { return _rows; }

private:
    std::vector<std::vector<uint64_t>> _rows;
};

using NodeListSink = IDListSink<readNodeIDList>;
using EdgeListSink = IDListSink<readEdgeIDList>;

// Collects the ([scores]) rows an ungrouped collect over an Int64 value emits.
class Int64ListSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 1u);

        const auto* lists = dynamic_cast<const ColumnVector<ListView>*>(chunks[0]);
        ASSERT_NE(lists, nullptr);

        const auto& listRaw = lists->getRaw();
        for (size_t row = offset; row < offset + rowCount; row++) {
            std::vector<int64_t> scores;
            readInt64List(listRaw[row], scores);
            _rows.push_back(scores);
        }
    }

    const std::vector<std::vector<int64_t>>& rows() const { return _rows; }

private:
    std::vector<std::vector<int64_t>> _rows;
};

// Collects the (team, [names], count) rows a grouped collect emits beside a tally.
class KeyedListAndCountSink : public NLOutputSink {
public:
    using Row = std::tuple<std::optional<std::string>, std::vector<std::string>, uint64_t>;

    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 3u);

        const auto* keys = dynamic_cast<const ColumnOptVector<std::string_view>*>(chunks[0]);
        const auto* lists = dynamic_cast<const ColumnVector<ListView>*>(chunks[1]);
        const auto* counts = dynamic_cast<const ColumnVector<uint64_t>*>(chunks[2]);
        ASSERT_NE(keys, nullptr);
        ASSERT_NE(lists, nullptr);
        ASSERT_NE(counts, nullptr);

        const auto& keyRaw = keys->getRaw();
        const auto& listRaw = lists->getRaw();
        const auto& countRaw = counts->getRaw();
        for (size_t row = offset; row < offset + rowCount; row++) {
            std::optional<std::string> key;
            if (keyRaw[row]) {
                key = std::string(*keyRaw[row]);
            }

            std::vector<std::string> names;
            readStringList(listRaw[row], names);

            _rows.push_back({key, names, countRaw[row]});
        }
    }

    void sortedRows(std::vector<Row>& rows) const {
        rows = _rows;
        std::sort(rows.begin(), rows.end());
    }

    void rowsInEmitOrder(std::vector<Row>& rows) const {
        rows = _rows;
    }

private:
    std::vector<Row> _rows;
};

// Collects the (team, [names], sum) rows a grouped collect emits beside a reduction.
class KeyedListAndSumSink : public NLOutputSink {
public:
    using Row = std::tuple<std::optional<std::string>, std::vector<std::string>, std::optional<int64_t>>;

    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 3u);

        const auto* keys = dynamic_cast<const ColumnOptVector<std::string_view>*>(chunks[0]);
        const auto* lists = dynamic_cast<const ColumnVector<ListView>*>(chunks[1]);
        const auto* sums = dynamic_cast<const ColumnOptVector<int64_t>*>(chunks[2]);
        ASSERT_NE(keys, nullptr);
        ASSERT_NE(lists, nullptr);
        ASSERT_NE(sums, nullptr);

        const auto& keyRaw = keys->getRaw();
        const auto& listRaw = lists->getRaw();
        const auto& sumRaw = sums->getRaw();
        for (size_t row = offset; row < offset + rowCount; row++) {
            std::optional<std::string> key;
            if (keyRaw[row]) {
                key = std::string(*keyRaw[row]);
            }

            std::vector<std::string> names;
            readStringList(listRaw[row], names);

            _rows.push_back({key, names, sumRaw[row]});
        }
    }

    void sortedRows(std::vector<Row>& rows) const {
        rows = _rows;
        std::sort(rows.begin(), rows.end());
    }

private:
    std::vector<Row> _rows;
};

// Collects the ([names], count) row a collect emits beside a tally, reading each column
// by its type so both projection orders share one sink.
class ListAndCountSink : public NLOutputSink {
public:
    using Row = std::pair<std::vector<std::string>, uint64_t>;

    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 2u);

        const ColumnVector<ListView>* lists = nullptr;
        const ColumnVector<uint64_t>* counts = nullptr;
        for (const Column* chunk : chunks) {
            const ColumnVector<ListView>* list = dynamic_cast<const ColumnVector<ListView>*>(chunk);
            if (list) {
                lists = list;
                continue;
            }

            const ColumnVector<uint64_t>* count = dynamic_cast<const ColumnVector<uint64_t>*>(chunk);
            if (count) {
                counts = count;
            }
        }

        ASSERT_NE(lists, nullptr);
        ASSERT_NE(counts, nullptr);

        const auto& listRaw = lists->getRaw();
        const auto& countRaw = counts->getRaw();
        for (size_t row = offset; row < offset + rowCount; row++) {
            std::vector<std::string> names;
            readStringList(listRaw[row], names);

            _rows.push_back({names, countRaw[row]});
        }
    }

    const std::vector<Row>& rows() const { return _rows; }

private:
    std::vector<Row> _rows;
};

// Collects the ([names], sum) row a collect emits beside a value reduction.
class ListAndSumSink : public NLOutputSink {
public:
    using Row = std::pair<std::vector<std::string>, std::optional<int64_t>>;

    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 2u);

        const auto* lists = dynamic_cast<const ColumnVector<ListView>*>(chunks[0]);
        const auto* sums = dynamic_cast<const ColumnOptVector<int64_t>*>(chunks[1]);
        ASSERT_NE(lists, nullptr);
        ASSERT_NE(sums, nullptr);

        const auto& listRaw = lists->getRaw();
        const auto& sumRaw = sums->getRaw();
        for (size_t row = offset; row < offset + rowCount; row++) {
            std::vector<std::string> names;
            readStringList(listRaw[row], names);

            _rows.push_back({names, sumRaw[row]});
        }
    }

    const std::vector<Row>& rows() const { return _rows; }

private:
    std::vector<Row> _rows;
};

// Collects the list cell a projection ends with, for the shapes carrying a column beside
// the list that the assert does not read.
class TrailingInt64ListSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_FALSE(chunks.empty());

        const auto* lists = dynamic_cast<const ColumnVector<ListView>*>(chunks.back());
        ASSERT_NE(lists, nullptr);

        const auto& listRaw = lists->getRaw();
        for (size_t row = offset; row < offset + rowCount; row++) {
            std::vector<int64_t> scores;
            readInt64List(listRaw[row], scores);
            _rows.push_back(scores);
        }
    }

    const std::vector<std::vector<int64_t>>& rows() const { return _rows; }

private:
    std::vector<std::vector<int64_t>> _rows;
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

    // Inserts 3 nodes so that one group's collected list is a prefix of the other's:
    // solo collects ["ann"], duo ["ann", "bob"].
    void buildPrefixTeamGraph() {
        create(R"(CREATE (:Node {team: "duo", name: "ann"}), (:Node {team: "solo", name: "ann"}), (:Node {team: "duo", name: "bob"}))");
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

// The literal every row collects is the one collect drops, so each group's list comes out
// empty however many rows folded into it.
TEST_F(CypherCollectTest, groupedCollectOfNull) {
    buildTeamGraph();

    KeyedInt64ListSink sink;
    match("MATCH (n:Node) RETURN n.team, collect(null)", sink);

    std::vector<KeyedInt64ListSink::Row> rows;
    sink.sortedRows(rows);

    const std::vector<KeyedInt64ListSink::Row> expected {
        {"blue", {}},
        {"red", {}},
    };
    EXPECT_EQ(rows, expected);
}

TEST_F(CypherCollectTest, ungroupedCollectOfNull) {
    buildTeamGraph();

    Int64ListSink sink;
    match("MATCH (n:Node) RETURN collect(null)", sink);

    const std::vector<std::vector<int64_t>> expected {{}};
    EXPECT_EQ(sink.rows(), expected);
}

// No match drives the projection, so the collect folds the single row a bare RETURN is.
TEST_F(CypherCollectTest, collectOfNullWithoutAMatch) {
    buildTeamGraph();

    Int64ListSink sink;
    match("RETURN collect(null)", sink);

    const std::vector<std::vector<int64_t>> expected {{}};
    EXPECT_EQ(sink.rows(), expected);
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
    // drain, not the rows folded into it. Which group it is separates the limit from the
    // skip below, so it is pinned rather than taken either way.
    std::vector<KeyedStringListSink::Row> rows;
    sink.rowsInEmitOrder(rows);

    const std::vector<KeyedStringListSink::Row> expected {{"red", {"alice", "carol"}}};
    EXPECT_EQ(rows, expected);
}

TEST_F(CypherCollectTest, groupedCollectWithSkipAndLimit) {
    buildTeamGraph();

    KeyedStringListSink sink;
    match("MATCH (n:Node) RETURN n.team, collect(n.name) SKIP 1 LIMIT 1", sink);

    std::vector<KeyedStringListSink::Row> rows;
    sink.rowsInEmitOrder(rows);

    const std::vector<KeyedStringListSink::Row> expected {{"blue", {"bob", "dan"}}};
    EXPECT_EQ(rows, expected);
}

TEST_F(CypherCollectTest, groupedCollectWithSkip) {
    buildTeamGraph();

    KeyedStringListSink sink;
    match("MATCH (n:Node) RETURN n.team, collect(n.name) SKIP 1", sink);

    std::vector<KeyedStringListSink::Row> rows;
    sink.rowsInEmitOrder(rows);

    const std::vector<KeyedStringListSink::Row> expected {{"blue", {"bob", "dan"}}};
    EXPECT_EQ(rows, expected);
}

// One accumulator holds both: the group's list and the tally of the same rows, so the
// drain emits them beside the key they belong to.
TEST_F(CypherCollectTest, groupedCollectBesideACount) {
    buildTeamGraph();

    KeyedListAndCountSink sink;
    match("MATCH (n:Node) RETURN n.team, collect(n.name), count(*)", sink);

    std::vector<KeyedListAndCountSink::Row> rows;
    sink.sortedRows(rows);

    const std::vector<KeyedListAndCountSink::Row> expected {
        {"blue", {"bob", "dan"}, 2},
        {"red", {"alice", "carol"}, 2},
    };
    EXPECT_EQ(rows, expected);
}

// The tally counts non-null rows, so it need not be the length of the list beside it:
// dan has no score, and collect drops the null the count skips.
TEST_F(CypherCollectTest, groupedCollectBesideACountOfANullableProperty) {
    buildTeamGraph();

    KeyedListAndCountSink sink;
    match("MATCH (n:Node) RETURN n.team, collect(n.name), count(n.score)", sink);

    std::vector<KeyedListAndCountSink::Row> rows;
    sink.sortedRows(rows);

    const std::vector<KeyedListAndCountSink::Row> expected {
        {"blue", {"bob", "dan"}, 1},
        {"red", {"alice", "carol"}, 2},
    };
    EXPECT_EQ(rows, expected);
}

// Several reductions beside the list, each with its own per-group accumulator on the
// state the list is collected into.
TEST_F(CypherCollectTest, groupedCollectBesideTwoAggregates) {
    buildTeamGraph();

    KeyedListAndCountSink counts;
    match("MATCH (n:Node) RETURN n.team, collect(n.name), count(*)", counts);

    KeyedListAndSumSink sums;
    match("MATCH (n:Node) RETURN n.team, collect(n.name), sum(n.score)", sums);

    std::vector<KeyedListAndCountSink::Row> countRows;
    counts.sortedRows(countRows);

    const std::vector<KeyedListAndCountSink::Row> expectedCounts {
        {"blue", {"bob", "dan"}, 2},
        {"red", {"alice", "carol"}, 2},
    };
    EXPECT_EQ(countRows, expectedCounts);

    std::vector<KeyedListAndSumSink::Row> sumRows;
    sums.sortedRows(sumRows);

    const std::vector<KeyedListAndSumSink::Row> expectedSums {
        {"blue", {"bob", "dan"}, 100},
        {"red", {"alice", "carol"}, 30},
    };
    EXPECT_EQ(sumRows, expectedSums);
}

// Each dedups against a tally of its own: the collect charges a group's distinct names
// once, and the count a group's distinct scores.
TEST_F(CypherCollectTest, groupedCollectDistinctBesideACountDistinct) {
    buildTeamGraph();

    KeyedListAndCountSink sink;
    match("MATCH (n:Node) RETURN n.team, collect(DISTINCT n.team), count(DISTINCT n.score)", sink);

    std::vector<KeyedListAndCountSink::Row> rows;
    sink.sortedRows(rows);

    const std::vector<KeyedListAndCountSink::Row> expected {
        {"blue", {"blue"}, 1},
        {"red", {"red"}, 2},
    };
    EXPECT_EQ(rows, expected);
}

// The key orders the groups by an aggregate the projection does not return, which the
// same accumulator computes: blue scores once, red twice.
TEST_F(CypherCollectTest, groupedCollectOrderedByACount) {
    buildTeamGraph();

    KeyedStringListSink sink;
    match("MATCH (n:Node) RETURN n.team, collect(n.name) ORDER BY count(n.score)", sink);

    std::vector<KeyedStringListSink::Row> rows;
    sink.rowsInEmitOrder(rows);

    const std::vector<KeyedStringListSink::Row> expected {
        {"blue", {"bob", "dan"}},
        {"red", {"alice", "carol"}},
    };
    EXPECT_EQ(rows, expected);
}

TEST_F(CypherCollectTest, groupedCollectOrderedByACountDescending) {
    buildTeamGraph();

    KeyedStringListSink sink;
    match("MATCH (n:Node) RETURN n.team, collect(n.name) ORDER BY count(n.score) DESC", sink);

    std::vector<KeyedStringListSink::Row> rows;
    sink.rowsInEmitOrder(rows);

    const std::vector<KeyedStringListSink::Row> expected {
        {"red", {"alice", "carol"}},
        {"blue", {"bob", "dan"}},
    };
    EXPECT_EQ(rows, expected);
}

// Two pipeline breakers over one match, each collapsing to one row: the collect drains its
// single group in a loop of its own, and the tally is a single-row chunk bound above that
// loop - loop-invariant, as a constant is, so the emit reads it beside the list.
TEST_F(CypherCollectTest, ungroupedCollectBesideACount) {
    buildTeamGraph();

    ListAndCountSink sink;
    match("MATCH (n:Node) RETURN collect(n.name), count(*)", sink);

    const std::vector<ListAndCountSink::Row> expected {{{"alice", "carol", "bob", "dan"}, 4}};
    EXPECT_EQ(sink.rows(), expected);
}

// The tally ahead of the list: the emit is anchored in the drain loop whichever order the
// projection names them in.
TEST_F(CypherCollectTest, ungroupedCountBesideACollect) {
    buildTeamGraph();

    ListAndCountSink sink;
    match("MATCH (n:Node) RETURN count(*), collect(n.name)", sink);

    const std::vector<ListAndCountSink::Row> expected {{{"alice", "carol", "bob", "dan"}, 4}};
    EXPECT_EQ(sink.rows(), expected);
}

// The value reduction beside a collect: dan has no score, so the sum is over the three
// that have one.
TEST_F(CypherCollectTest, ungroupedCollectBesideASum) {
    buildTeamGraph();

    ListAndSumSink sink;
    match("MATCH (n:Node) RETURN collect(n.name), sum(n.score)", sink);

    const std::vector<ListAndSumSink::Row> expected {{{"alice", "carol", "bob", "dan"}, 130}};
    EXPECT_EQ(sink.rows(), expected);
}

// The aggregate wrapped in an expression is computed off the same single-row chunk.
TEST_F(CypherCollectTest, ungroupedCollectBesideAWrappedAggregate) {
    buildTeamGraph();

    ListAndSumSink sink;
    match("MATCH (n:Node) RETURN collect(n.name), sum(n.score) + 1", sink);

    const std::vector<ListAndSumSink::Row> expected {{{"alice", "carol", "bob", "dan"}, 131}};
    EXPECT_EQ(sink.rows(), expected);
}

// The reduction beside a collect, wrapped in an expression computed off its grouped
// column: red scores 30, and blue 100 - dan has none.
TEST_F(CypherCollectTest, groupedCollectBesideAWrappedAggregate) {
    buildTeamGraph();

    KeyedListAndSumSink sink;
    match("MATCH (n:Node) RETURN n.team, collect(n.name), sum(n.score) + 0", sink);

    std::vector<KeyedListAndSumSink::Row> rows;
    sink.sortedRows(rows);

    const std::vector<KeyedListAndSumSink::Row> expected {
        {"blue", {"bob", "dan"}, 100},
        {"red", {"alice", "carol"}, 30},
    };
    EXPECT_EQ(rows, expected);
}

// With no grouping key the projection is one row, and the key reducing a second aggregate
// over the same match is one value beside it: the sort has a row to order, so the query
// runs rather than needing the collect and the count to share an accumulator.
TEST_F(CypherCollectTest, ungroupedCollectOrderedByACount) {
    buildTeamGraph();

    StringListSink sink;
    match("MATCH (n:Node) RETURN collect(n.name) ORDER BY count(n)", sink);

    const std::vector<std::vector<std::string>> expected {{"alice", "carol", "bob", "dan"}};
    EXPECT_EQ(sink.rows(), expected);
}

// The value reduction of the same shape, whose result is materialized the same way
TEST_F(CypherCollectTest, ungroupedCollectOrderedByASum) {
    buildTeamGraph();

    StringListSink sink;
    match("MATCH (n:Node) RETURN collect(n.name) ORDER BY sum(n.score) DESC", sink);

    const std::vector<std::vector<std::string>> expected {{"alice", "carol", "bob", "dan"}};
    EXPECT_EQ(sink.rows(), expected);
}

// Each collect drains through a loop of its own, and the emit sits in one of them: the
// other list is bound in a loop the output is not in, so this is the pair that is still
// turned away.
TEST_F(CypherCollectTest, rejectsTwoCollects) {
    buildTeamGraph();

    QueryStatus status;
    runQuery("MATCH (n:Node) RETURN collect(n.name), collect(n.team)", status);

    EXPECT_EQ(status.getStatus(), QueryStatus::Status::PLAN_ERROR);
    EXPECT_EQ(status.getError(), "collect() may not yet be combined with another collect().");
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

// The other side of the line the test above stands on: n is returned there, so n.name is
// one name per group, while grouping on n.team drops n and leaves one name per matched
// row - four of them against two groups, lining up with no row the drain emits.
TEST_F(CypherCollectTest, rejectsOrderByOverAVariableTheGroupingDropped) {
    buildTeamGraph();

    QueryStatus status;
    runQuery("MATCH (n:Node) RETURN n.team, collect(n.name) ORDER BY n.name", status);

    EXPECT_EQ(status.getStatus(), QueryStatus::Status::ANALYZE_ERROR);
    EXPECT_NE(status.getError().find(droppedKeyReason), std::string::npos) << status.getError();
}

// An ungrouped collect keeps no variable at all, so there is not even a group for the key
// to hold one value of.
TEST_F(CypherCollectTest, rejectsOrderByOverADroppedVariableUnderAnUngroupedCollect) {
    buildTeamGraph();

    QueryStatus status;
    runQuery("MATCH (n:Node) RETURN collect(n.name) ORDER BY n.name", status);

    EXPECT_EQ(status.getStatus(), QueryStatus::Status::ANALYZE_ERROR);
    EXPECT_NE(status.getError().find(droppedKeyReason), std::string::npos) << status.getError();
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

// Two lists order lexicographically, so the collected list can be the sort key: red's
// ["alice", "carol"] sorts before blue's ["bob", "dan"] on the first element.
TEST_F(CypherCollectTest, groupedCollectOrderedByTheList) {
    buildTeamGraph();

    KeyedStringListSink sink;
    match("MATCH (n:Node) RETURN n.team, collect(n.name) AS names ORDER BY names", sink);

    std::vector<KeyedStringListSink::Row> rows;
    sink.rowsInEmitOrder(rows);

    const std::vector<KeyedStringListSink::Row> expected {
        {"red", {"alice", "carol"}},
        {"blue", {"bob", "dan"}},
    };
    EXPECT_EQ(rows, expected);
}

TEST_F(CypherCollectTest, groupedCollectOrderedByTheListDescending) {
    buildTeamGraph();

    KeyedStringListSink sink;
    match("MATCH (n:Node) RETURN n.team, collect(n.name) AS names ORDER BY names DESC", sink);

    std::vector<KeyedStringListSink::Row> rows;
    sink.rowsInEmitOrder(rows);

    const std::vector<KeyedStringListSink::Row> expected {
        {"blue", {"bob", "dan"}},
        {"red", {"alice", "carol"}},
    };
    EXPECT_EQ(rows, expected);
}

// The key need not be the alias: spelling the aggregate out again reads the same
// collected column rather than collecting a second one.
TEST_F(CypherCollectTest, groupedCollectOrderedBySpelledOutCollect) {
    buildTeamGraph();

    KeyedStringListSink sink;
    match("MATCH (n:Node) RETURN n.team, collect(n.name) ORDER BY collect(n.name)", sink);

    std::vector<KeyedStringListSink::Row> rows;
    sink.rowsInEmitOrder(rows);

    const std::vector<KeyedStringListSink::Row> expected {
        {"red", {"alice", "carol"}},
        {"blue", {"bob", "dan"}},
    };
    EXPECT_EQ(rows, expected);
}

// Element-wise comparison runs out of elements before it finds a difference, so the
// shorter list - the prefix - sorts first.
TEST_F(CypherCollectTest, groupedCollectOrderedByListPrefix) {
    buildPrefixTeamGraph();

    KeyedStringListSink sink;
    match("MATCH (n:Node) RETURN n.team, collect(n.name) AS names ORDER BY names", sink);

    std::vector<KeyedStringListSink::Row> rows;
    sink.rowsInEmitOrder(rows);

    const std::vector<KeyedStringListSink::Row> expected {
        {"solo", {"ann"}},
        {"duo", {"ann", "bob"}},
    };
    EXPECT_EQ(rows, expected);
}

// ORDER BY ... LIMIT fuses into a top-K accumulator, which trims by gathering the
// surviving rows - so the list is both the key compared and a column gathered.
TEST_F(CypherCollectTest, groupedCollectOrderedByTheListWithLimit) {
    buildTeamGraph();

    KeyedStringListSink sink;
    match("MATCH (n:Node) RETURN n.team, collect(n.name) AS names ORDER BY names LIMIT 1", sink);

    std::vector<KeyedStringListSink::Row> rows;
    sink.rowsInEmitOrder(rows);

    const std::vector<KeyedStringListSink::Row> expected {{"red", {"alice", "carol"}}};
    EXPECT_EQ(rows, expected);
}

// A list of entities orders by its elements' IDs: red collects nodes 0 and 1, blue 2
// and 3, so red sorts first.
TEST_F(CypherCollectTest, groupedCollectOfNodesOrderedByTheList) {
    buildTeamGraph();

    KeyedNodeListSink sink;
    match("MATCH (n:Node) RETURN n.team, collect(n) AS nodes ORDER BY nodes", sink);

    std::vector<KeyedNodeListSink::Row> rows;
    sink.rowsInEmitOrder(rows);

    const std::vector<KeyedNodeListSink::Row> expected {
        {"red", {0, 1}},
        {"blue", {2, 3}},
    };
    EXPECT_EQ(rows, expected);
}

// An aggregate projection emits one row per group, so no two of its rows can be equal:
// the dedup is elided and DISTINCT beside a collect returns the groups untouched.
TEST_F(CypherCollectTest, distinctOverTheCollectedList) {
    buildTeamGraph();

    KeyedStringListSink sink;
    match("MATCH (n:Node) RETURN DISTINCT n.team, collect(n.name)", sink);

    std::vector<KeyedStringListSink::Row> rows;
    sink.sortedRows(rows);

    const std::vector<KeyedStringListSink::Row> expected {
        {"blue", {"bob", "dan"}},
        {"red", {"alice", "carol"}},
    };
    EXPECT_EQ(rows, expected);
}

// A constant is bound above the loop the matched rows are read in, so it is laid out over
// those rows before the fold reads it: what is collected is one element per row of the
// group, not the single row the constant is.
TEST_F(CypherCollectTest, groupedCollectOfAConstant) {
    buildTeamGraph();

    KeyedInt64ListSink sink;
    match("MATCH (n:Node) RETURN n.team, collect(1)", sink);

    std::vector<KeyedInt64ListSink::Row> rows;
    sink.sortedRows(rows);

    const std::vector<KeyedInt64ListSink::Row> expected {
        {"blue", {1, 1}},
        {"red", {1, 1}},
    };
    EXPECT_EQ(rows, expected);
}

TEST_F(CypherCollectTest, ungroupedCollectOfAConstant) {
    buildTeamGraph();

    Int64ListSink sink;
    match("MATCH (n:Node) RETURN collect(1)", sink);

    const std::vector<std::vector<int64_t>> expected {{1, 1, 1, 1}};
    EXPECT_EQ(sink.rows(), expected);
}

// The arithmetic is computed once, above the rows, and the value it holds is what every
// row contributes.
TEST_F(CypherCollectTest, ungroupedCollectOfConstantArithmetic) {
    buildTeamGraph();

    Int64ListSink sink;
    match("MATCH (n:Node) RETURN collect(2 + 3)", sink);

    const std::vector<std::vector<int64_t>> expected {{5, 5, 5, 5}};
    EXPECT_EQ(sink.rows(), expected);
}

TEST_F(CypherCollectTest, ungroupedCollectOfAConstantString) {
    buildTeamGraph();

    StringListSink sink;
    match(R"(MATCH (n:Node) RETURN collect("x"))", sink);

    const std::vector<std::vector<std::string>> expected {{"x", "x", "x", "x"}};
    EXPECT_EQ(sink.rows(), expected);
}

// Every row holds the same value, so dropping the repeats leaves the one element.
TEST_F(CypherCollectTest, ungroupedCollectDistinctOfAConstant) {
    buildTeamGraph();

    Int64ListSink sink;
    match("MATCH (n:Node) RETURN collect(DISTINCT 1)", sink);

    const std::vector<std::vector<int64_t>> expected {{1}};
    EXPECT_EQ(sink.rows(), expected);
}

// No match drives the projection, so the rows the constant is laid out over are the single
// row a bare RETURN is.
TEST_F(CypherCollectTest, collectOfAConstantWithoutAMatch) {
    buildTeamGraph();

    Int64ListSink sink;
    match("RETURN collect(1)", sink);

    const std::vector<std::vector<int64_t>> expected {{1}};
    EXPECT_EQ(sink.rows(), expected);
}

// The rows the constant is laid out over are the rows the query left standing, so a
// filter is what the list is sized by: two of the four nodes are red.
TEST_F(CypherCollectTest, ungroupedCollectOfAConstantUnderAFilter) {
    buildTeamGraph();

    Int64ListSink sink;
    match(R"(MATCH (n:Node) WHERE n.team = "red" RETURN collect(1))", sink);

    const std::vector<std::vector<int64_t>> expected {{1, 1}};
    EXPECT_EQ(sink.rows(), expected);
}

// The driving relation need not be a match: an unwound list drives the rows here, and the
// constant is laid out over its cells.
TEST_F(CypherCollectTest, ungroupedCollectOfAConstantOverAnUnwind) {
    buildTeamGraph();

    Int64ListSink sink;
    match("UNWIND [10, 20] AS v RETURN collect(1)", sink);

    const std::vector<std::vector<int64_t>> expected {{1, 1}};
    EXPECT_EQ(sink.rows(), expected);
}

// An alias of a constant names that constant, so it is laid out the same way - grouped,
// once per row of each group, and ungrouped, once per matched row.
TEST_F(CypherCollectTest, collectOfAConstantAlias) {
    buildTeamGraph();

    TrailingInt64ListSink groupedSink;
    match("MATCH (n:Node) RETURN n.team, 1 AS x, collect(x)", groupedSink);

    const std::vector<std::vector<int64_t>> grouped {{1, 1}, {1, 1}};
    EXPECT_EQ(groupedSink.rows(), grouped);

    TrailingInt64ListSink ungroupedSink;
    match("MATCH (n:Node) RETURN 1 AS x, collect(x)", ungroupedSink);

    const std::vector<std::vector<int64_t>> ungrouped {{1, 1, 1, 1}};
    EXPECT_EQ(ungroupedSink.rows(), ungrouped);
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

// Grouping on the other end of the traversal: the key is a property of the target rather
// than of the source the two edges share, so each edge lands in a group of its own.
TEST_F(CypherCollectTest, groupedCollectOfEdgesOnTheTraversalTarget) {
    buildKnowsGraph();

    KeyedEdgeListSink sink;
    match("MATCH (a:Person)-[e:KNOWS]->(b:Person) RETURN b.name, collect(e)", sink);

    std::vector<KeyedEdgeListSink::Row> rows;
    sink.sortedRows(rows);

    const std::vector<KeyedEdgeListSink::Row> expected {
        {"bob", {0}},
        {"carol", {1}},
    };
    EXPECT_EQ(rows, expected);
}

// With no grouping key there is no key column to resolve the edge beside, so the argument
// is resolved through the edge identity's representative alone.
TEST_F(CypherCollectTest, ungroupedCollectOfEdges) {
    buildKnowsGraph();

    EdgeListSink sink;
    match("MATCH (a:Person)-[e:KNOWS]->(b:Person) RETURN collect(e)", sink);

    const std::vector<std::vector<uint64_t>> expected {{0, 1}};
    EXPECT_EQ(sink.rows(), expected);
}

// The list carries entities through the same buffers a value list does, so the sort and
// the skip/limit copies need no entity case of their own.
TEST_F(CypherCollectTest, collectOfNodesWithOrderByAndSkipLimit) {
    buildTeamGraph();

    KeyedNodeListSink orderedSink;
    match("MATCH (n:Node) RETURN n.team, collect(n) ORDER BY n.team DESC", orderedSink);

    std::vector<KeyedNodeListSink::Row> orderedRows;
    orderedSink.rowsInEmitOrder(orderedRows);

    const std::vector<KeyedNodeListSink::Row> ordered {
        {"red", {0, 1}},
        {"blue", {2, 3}},
    };
    EXPECT_EQ(orderedRows, ordered);

    KeyedNodeListSink cutSink;
    match("MATCH (n:Node) RETURN n.team, collect(n) SKIP 1 LIMIT 1", cutSink);

    std::vector<KeyedNodeListSink::Row> cutRows;
    cutSink.rowsInEmitOrder(cutRows);

    const std::vector<KeyedNodeListSink::Row> cut {{"blue", {2, 3}}};
    EXPECT_EQ(cutRows, cut);
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
