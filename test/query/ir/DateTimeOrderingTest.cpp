#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <string_view>

#include "QueryInterpreterV3.h"
#include "QueryStatus.h"

#include "Graph.h"
#include "QueryConfig.h"
#include "SimpleGraph.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "TuringDB.h"
#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"

#include "IRTestRows.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

// Comparing and ordering datetime properties: what a WHERE keeps and the order an ORDER BY
// emits, read in the order the query answered rather than sorted after the fact.
class DateTimeOrderingTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager());

        {
            SystemAccessor system = _env->getSystemManager().accessUnique();
            Graph* graph = system.createGraph(_graphName);
            SimpleGraph::createSimpleGraph(graph);
        }

        // One event with no instant at all, so a comparison and an ORDER BY each meet a
        // row whose property is absent
        write("CREATE (a:Event {name: 'a', at: datetime('2024-03-01T08:00:00Z')})");
        write("CREATE (b:Event {name: 'b', at: datetime('2026-09-23T14:05:00Z')})");
        write("CREATE (c:Event {name: 'c', at: datetime('2025-01-01T00:00:00Z')})");
        write("CREATE (d:Event {name: 'd'})");
    }

    void openChange(ChangeID& changeID) {
        SystemAccessor system = _env->getSystemManager().accessUnique();
        const auto res = system.newChange(_graphName);
        ASSERT_TRUE(res);

        changeID = res.value()->id();
    }

    void submit(const ChangeID& changeID) {
        const QueryState submitState(_graphName,
                                     &_env->getMem(),
                                     &_queryConfig,
                                     nullptr,
                                     CommitHash::head(),
                                     changeID);
        const QueryStatus status = _env->getDB().query("CHANGE SUBMIT", submitState);
        ASSERT_TRUE(status.isOk()) << "CHANGE SUBMIT failed";
    }

    void write(std::string_view query) {
        ChangeID changeID;
        openChange(changeID);

        RowSink sink;
        QueryStatus status;
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              changeID,
                              &_env->getMem(),
                              &sink);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        submit(changeID);
    }

    void expectOrderedRows(std::string_view query, const Rows& expected) {
        RowSink sink;
        QueryStatus status;
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              ChangeID::head(),
                              &_env->getMem(),
                              &sink);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        std::string actualText;
        describeRows(sink.rows(), actualText);

        EXPECT_EQ(sink.rows(), expected) << "query: " << query << "\ngot:\n" << actualText;
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
    QueryConfig _queryConfig;
};

TEST_F(DateTimeOrderingTest, ordersAscendingByTheInstant) {
    expectOrderedRows("MATCH (n:Event) WHERE n.at IS NOT NULL RETURN n.name ORDER BY n.at ASC",
                      {{"a"}, {"c"}, {"b"}});
}

TEST_F(DateTimeOrderingTest, ordersDescendingByTheInstant) {
    expectOrderedRows("MATCH (n:Event) WHERE n.at IS NOT NULL RETURN n.name ORDER BY n.at DESC",
                      {{"b"}, {"c"}, {"a"}});
}

TEST_F(DateTimeOrderingTest, keepsTheInstantsAfterABound) {
    expectOrderedRows("MATCH (n:Event) WHERE n.at > datetime('2024-06-01') "
                      "RETURN n.name ORDER BY n.at ASC",
                      {{"c"}, {"b"}});
}

TEST_F(DateTimeOrderingTest, keepsTheInstantsBeforeABound) {
    expectOrderedRows("MATCH (n:Event) WHERE n.at < datetime('2025-06-01') "
                      "RETURN n.name ORDER BY n.at ASC",
                      {{"a"}, {"c"}});
}

TEST_F(DateTimeOrderingTest, comparesAgainstABoundInclusively) {
    expectOrderedRows("MATCH (n:Event) WHERE n.at >= datetime('2025-01-01T00:00:00Z') "
                      "RETURN n.name ORDER BY n.at ASC",
                      {{"c"}, {"b"}});
}

TEST_F(DateTimeOrderingTest, matchesTheInstantAnOffsetNames) {
    expectOrderedRows("MATCH (n:Event) WHERE n.at = datetime('2026-09-23T16:05:00+02:00') "
                      "RETURN n.name",
                      {{"b"}});
}

TEST_F(DateTimeOrderingTest, keepsTheRowsAnInstantDiffersFrom) {
    expectOrderedRows("MATCH (n:Event) WHERE n.at <> datetime('2025-01-01T00:00:00Z') "
                      "RETURN n.name ORDER BY n.at ASC",
                      {{"a"}, {"b"}});
}

TEST_F(DateTimeOrderingTest, findsTheNodeCarryingNoInstant) {
    expectOrderedRows("MATCH (n:Event) WHERE n.at IS NULL RETURN n.name", {{"d"}});
}

TEST_F(DateTimeOrderingTest, ordersTheAbsentInstantLast) {
    expectOrderedRows("MATCH (n:Event) RETURN n.name ORDER BY n.at ASC",
                      {{"a"}, {"c"}, {"b"}, {"d"}});
}

TEST_F(DateTimeOrderingTest, groupsOnTheInstant) {
    write("CREATE (e:Event {name: 'e', at: datetime('2025-01-01T00:00:00Z')})");

    expectOrderedRows("MATCH (n:Event) WHERE n.at IS NOT NULL "
                      "RETURN n.at, count(n) ORDER BY n.at ASC",
                      {{"2024-03-01T08:00:00Z", "1"},
                       {"2025-01-01T00:00:00Z", "2"},
                       {"2026-09-23T14:05:00Z", "1"}});
}

TEST_F(DateTimeOrderingTest, countsTheDistinctInstants) {
    write("CREATE (e:Event {name: 'e', at: datetime('2025-01-01T00:00:00Z')})");

    expectOrderedRows("MATCH (n:Event) RETURN count(DISTINCT n.at)", {{"3"}});
}

TEST_F(DateTimeOrderingTest, takesTheEarliestAndTheLatest) {
    expectOrderedRows("MATCH (n:Event) RETURN min(n.at), max(n.at)",
                      {{"2024-03-01T08:00:00Z", "2026-09-23T14:05:00Z"}});
}
