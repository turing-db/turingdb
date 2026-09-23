#include <gtest/gtest.h>

#include <algorithm>
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

// Datetimes stored as properties: what datetime() parses out of a string, what a CREATE or
// a SET writes into a datapart, and what a later MATCH reads back. Each write runs in its
// own change and is submitted, so the read that follows sees a committed value rather than
// the column the writing query happened to build.
class DateTimePropertyTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager());

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);
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

    void expectRows(std::string_view query, const Rows& expected) {
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

        Rows actual;
        sink.sortedRows(actual);

        Rows sortedExpected = expected;
        std::sort(sortedExpected.begin(), sortedExpected.end());

        std::string actualText;
        describeRows(actual, actualText);

        EXPECT_EQ(actual, sortedExpected) << "query: " << query << "\ngot:\n" << actualText;
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
    QueryConfig _queryConfig;
};

TEST_F(DateTimePropertyTest, returnsAParsedInstantWithoutTouchingTheGraph) {
    expectRows("RETURN datetime('2026-09-23T14:05:00Z')", {{"2026-09-23T14:05:00Z"}});
}

TEST_F(DateTimePropertyTest, storesAndReadsBackAnInstant) {
    write("CREATE (n:Event {name: 'a', at: datetime('2026-09-23T14:05:00Z')})");
    expectRows("MATCH (n:Event) RETURN n.at", {{"2026-09-23T14:05:00Z"}});
}

TEST_F(DateTimePropertyTest, storesADateAsMidnightUTC) {
    write("CREATE (n:Event {name: 'a', at: datetime('2026-09-23')})");
    expectRows("MATCH (n:Event) RETURN n.at", {{"2026-09-23T00:00:00Z"}});
}

TEST_F(DateTimePropertyTest, storesTheInstantAnOffsetNames) {
    write("CREATE (n:Event {name: 'a', at: datetime('2026-09-23T16:05:00+02:00')})");
    expectRows("MATCH (n:Event) RETURN n.at", {{"2026-09-23T14:05:00Z"}});
}

TEST_F(DateTimePropertyTest, keepsTheFraction) {
    write("CREATE (n:Event {name: 'a', at: datetime('2026-09-23T14:05:00.123456Z')})");
    expectRows("MATCH (n:Event) RETURN n.at", {{"2026-09-23T14:05:00.123456Z"}});
}

TEST_F(DateTimePropertyTest, storesAnInstantBeforeTheEpoch) {
    write("CREATE (n:Event {name: 'a', at: datetime('1969-12-31T23:59:59Z')})");
    expectRows("MATCH (n:Event) RETURN n.at", {{"1969-12-31T23:59:59Z"}});
}

TEST_F(DateTimePropertyTest, setsTheInstantOfAnExistingNode) {
    write("MATCH (n:Person {name: 'Remy'}) SET n.joined = datetime('2019-04-01T09:30:00Z')");
    expectRows("MATCH (n:Person {name: 'Remy'}) RETURN n.joined",
               {{"2019-04-01T09:30:00Z"}});
}

TEST_F(DateTimePropertyTest, overwritesAnInstantWithAnother) {
    write("CREATE (n:Event {name: 'a', at: datetime('2026-09-23T14:05:00Z')})");
    write("MATCH (n:Event {name: 'a'}) SET n.at = datetime('2027-01-01T00:00:00Z')");

    expectRows("MATCH (n:Event) RETURN n.at", {{"2027-01-01T00:00:00Z"}});
}

// A string naming no instant reads as no datetime, the way toInteger answers a string
// naming no number, so the property is written as absent rather than as a wrong value
TEST_F(DateTimePropertyTest, writesNullWhereTheTextNamesNoInstant) {
    write("CREATE (n:Event {name: 'a', at: datetime('not a date')})");
    expectRows("MATCH (n:Event) RETURN n.at", {{"null"}});
}

TEST_F(DateTimePropertyTest, readsNullFromANodeCarryingNoInstant) {
    write("CREATE (a:Event {name: 'a', at: datetime('2026-09-23T14:05:00Z')})");
    write("CREATE (b:Event {name: 'b'})");

    expectRows("MATCH (n:Event) RETURN n.name, n.at",
               {{"a", "2026-09-23T14:05:00Z"}, {"b", "null"}});
}

TEST_F(DateTimePropertyTest, storesOneInstantPerNode) {
    write("CREATE (a:Event {name: 'a', at: datetime('2026-09-23T14:05:00Z')})");
    write("CREATE (b:Event {name: 'b', at: datetime('2026-09-24T14:05:00Z')})");

    expectRows("MATCH (n:Event) RETURN n.name, n.at",
               {{"a", "2026-09-23T14:05:00Z"}, {"b", "2026-09-24T14:05:00Z"}});
}

TEST_F(DateTimePropertyTest, storesAnInstantOnAnEdge) {
    write("MATCH (a:Person {name: 'Remy'}), (b:Person {name: 'Adam'}) "
          "CREATE (a)-[:MET {at: datetime('2026-09-23T14:05:00Z')}]->(b)");

    expectRows("MATCH (:Person)-[e:MET]->(:Person) RETURN e.at",
               {{"2026-09-23T14:05:00Z"}});
}

TEST_F(DateTimePropertyTest, collectsInstantsIntoAList) {
    write("CREATE (a:Event {name: 'a', at: datetime('2026-09-23T14:05:00Z')})");
    write("CREATE (b:Event {name: 'b', at: datetime('2026-09-24T14:05:00Z')})");

    expectRows("MATCH (n:Event) RETURN collect(n.at)",
               {{"[2026-09-23T14:05:00Z, 2026-09-24T14:05:00Z]"}});
}
