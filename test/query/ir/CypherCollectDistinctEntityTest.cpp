#include <gtest/gtest.h>

#include <stdint.h>

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

class NullSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const>, size_t, size_t) override {}
};

// Collects the single [ids] row an ungrouped entity collect emits.
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

// Collects the (name, [ids]) rows a grouped entity collect emits.
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

}

// collect(DISTINCT n) and collect(DISTINCT e) over a match that repeats an entity: the
// distinct fold keys a group on the ID it already buffered, which the plain fold beside
// each test pins as the shape it deduplicates.
class CypherCollectDistinctEntityTest : public TuringTest {
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

    // Inserts alice (0), bob (1) and carol (2), with alice reaching bob over two edges of
    // different types - KNOWS (0) and LIKES (1) - and carol over a third (2). An untyped
    // edge pattern then matches bob twice, so a group repeats both alice and bob.
    void buildFollowGraph() {
        create(R"(CREATE (:Person {name: "alice"}), (:Person {name: "bob"}), (:Person {name: "carol"}))");
        create(R"(MATCH (a:Person {name: "alice"}), (b:Person {name: "bob"}) CREATE (a)-[:KNOWS]->(b))");
        create(R"(MATCH (a:Person {name: "alice"}), (b:Person {name: "bob"}) CREATE (a)-[:LIKES]->(b))");
        create(R"(MATCH (a:Person {name: "alice"}), (c:Person {name: "carol"}) CREATE (a)-[:KNOWS]->(c))");
    }

    const std::string _graphName = "followGraph";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interp3;
    QueryConfig _queryConfig;
};

TEST_F(CypherCollectDistinctEntityTest, collectsARepeatedSourceOncePerMatch) {
    buildFollowGraph();

    NodeListSink sink;
    match("MATCH (a:Person)-[]->(b:Person) RETURN collect(a)", sink);

    ASSERT_EQ(sink.rows().size(), 1u);
    EXPECT_EQ(sink.rows().front(), (std::vector<uint64_t> {0, 0, 0}));
}

TEST_F(CypherCollectDistinctEntityTest, collectsADistinctRepeatedSourceOnce) {
    buildFollowGraph();

    NodeListSink sink;
    match("MATCH (a:Person)-[]->(b:Person) RETURN collect(DISTINCT a)", sink);

    ASSERT_EQ(sink.rows().size(), 1u);
    EXPECT_EQ(sink.rows().front(), (std::vector<uint64_t> {0}));
}

TEST_F(CypherCollectDistinctEntityTest, collectsARepeatedTargetOncePerMatch) {
    buildFollowGraph();

    NodeListSink sink;
    match("MATCH (a:Person)-[]->(b:Person) RETURN collect(b)", sink);

    ASSERT_EQ(sink.rows().size(), 1u);
    EXPECT_EQ(sink.rows().front(), (std::vector<uint64_t> {1, 1, 2}));
}

TEST_F(CypherCollectDistinctEntityTest, collectsDistinctTargets) {
    buildFollowGraph();

    NodeListSink sink;
    match("MATCH (a:Person)-[]->(b:Person) RETURN collect(DISTINCT b)", sink);

    ASSERT_EQ(sink.rows().size(), 1u);
    EXPECT_EQ(sink.rows().front(), (std::vector<uint64_t> {1, 2}));
}

TEST_F(CypherCollectDistinctEntityTest, groupedCollectKeepsRepeatedTargets) {
    buildFollowGraph();

    KeyedNodeListSink sink;
    match("MATCH (a:Person)-[]->(b:Person) RETURN a.name, collect(b)", sink);

    std::vector<KeyedNodeListSink::Row> rows;
    sink.sortedRows(rows);

    const std::vector<KeyedNodeListSink::Row> expected {{"alice", {1, 1, 2}}};
    EXPECT_EQ(rows, expected);
}

TEST_F(CypherCollectDistinctEntityTest, groupedCollectOfDistinctTargets) {
    buildFollowGraph();

    KeyedNodeListSink sink;
    match("MATCH (a:Person)-[]->(b:Person) RETURN a.name, collect(DISTINCT b)", sink);

    std::vector<KeyedNodeListSink::Row> rows;
    sink.sortedRows(rows);

    const std::vector<KeyedNodeListSink::Row> expected {{"alice", {1, 2}}};
    EXPECT_EQ(rows, expected);
}

// A cross product repeats every edge once per crossed node, which is what gives the edge
// collect a group to deduplicate: an edge is matched once per row of the pattern alone.
TEST_F(CypherCollectDistinctEntityTest, collectsARepeatedEdgeOncePerMatch) {
    buildFollowGraph();

    EdgeListSink sink;
    match("MATCH (a:Person)-[e]->(b:Person), (c:Person) RETURN collect(e)", sink);

    ASSERT_EQ(sink.rows().size(), 1u);
    EXPECT_EQ(sink.rows().front(), (std::vector<uint64_t> {0, 0, 0, 1, 1, 1, 2, 2, 2}));
}

TEST_F(CypherCollectDistinctEntityTest, collectsDistinctEdges) {
    buildFollowGraph();

    EdgeListSink sink;
    match("MATCH (a:Person)-[e]->(b:Person), (c:Person) RETURN collect(DISTINCT e)", sink);

    ASSERT_EQ(sink.rows().size(), 1u);
    EXPECT_EQ(sink.rows().front(), (std::vector<uint64_t> {0, 1, 2}));
}

TEST_F(CypherCollectDistinctEntityTest, groupedCollectOfDistinctEdges) {
    buildFollowGraph();

    KeyedEdgeListSink sink;
    match("MATCH (a:Person)-[e]->(b:Person), (c:Person) RETURN c.name, collect(DISTINCT e)", sink);

    std::vector<KeyedEdgeListSink::Row> rows;
    sink.sortedRows(rows);

    const std::vector<KeyedEdgeListSink::Row> expected {
        {"alice", {0, 1, 2}},
        {"bob", {0, 1, 2}},
        {"carol", {0, 1, 2}},
    };
    EXPECT_EQ(rows, expected);
}
