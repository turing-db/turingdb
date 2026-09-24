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

// The two ways of making an instant that read no text: datetime(), which answers the
// instant the query runs at, and datetime(<integer>), which reads a count of seconds since
// the Unix epoch.
class DateTimeFunctionsTest : public TuringTest {
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

TEST_F(DateTimeFunctionsTest, readsTheEpochFromACountOfSeconds) {
    expectRows("RETURN datetime(0)", {{"1970-01-01T00:00:00Z"}});
}

TEST_F(DateTimeFunctionsTest, readsAnInstantFromACountOfSeconds) {
    expectRows("RETURN datetime(1700000000)", {{"2023-11-14T22:13:20Z"}});
}

TEST_F(DateTimeFunctionsTest, readsAnInstantBeforeTheEpochFromANegativeCount) {
    expectRows("RETURN datetime(-1)", {{"1969-12-31T23:59:59Z"}});
}

// A count of seconds naming an instant format cannot spell reads as no datetime, the way
// text naming no instant does
TEST_F(DateTimeFunctionsTest, readsNullFromACountNoInstantCanSpell) {
    expectRows("RETURN datetime(253402300800)", {{"null"}});
    expectRows("RETURN datetime(9223372036854775807)", {{"null"}});
}

TEST_F(DateTimeFunctionsTest, keepsReadingAnInstantFromText) {
    expectRows("RETURN datetime('2026-09-23T14:05:00Z')", {{"2026-09-23T14:05:00Z"}});
}

TEST_F(DateTimeFunctionsTest, answersACountOfSecondsAndTheTextForItAlike) {
    expectRows("RETURN datetime(1700000000) = datetime('2023-11-14T22:13:20Z')", {{"true"}});
}

TEST_F(DateTimeFunctionsTest, readsTheClock) {
    expectRows("RETURN datetime() > datetime('2026-01-01T00:00:00Z')", {{"true"}});
    expectRows("RETURN datetime() < datetime('2100-01-01T00:00:00Z')", {{"true"}});
}

// One instant stands for the whole statement, so every row of a match reads the same one
TEST_F(DateTimeFunctionsTest, answersOneInstantForEveryRowOfAQuery) {
    expectRows("MATCH (n:Person) WITH datetime() AS now RETURN count(*), count(DISTINCT now)",
               {{"8", "1"}});
}

TEST_F(DateTimeFunctionsTest, laysTheInstantOutOverTheRowsItIsProjectedWith) {
    expectRows("MATCH (n:Person) WHERE n.name = 'Remy' RETURN n.name, datetime() = datetime()",
               {{"Remy", "true"}});
}

TEST_F(DateTimeFunctionsTest, writesTheClockIntoAProperty) {
    write("MATCH (n:Person {name: 'Remy'}) SET n.seen = datetime()");

    expectRows("MATCH (n:Person {name: 'Remy'}) RETURN n.seen > datetime('2026-01-01T00:00:00Z')",
               {{"true"}});
}

TEST_F(DateTimeFunctionsTest, writesACountOfSecondsIntoAProperty) {
    write("MATCH (n:Person {name: 'Remy'}) SET n.seen = datetime(1700000000)");

    expectRows("MATCH (n:Person {name: 'Remy'}) RETURN n.seen", {{"2023-11-14T22:13:20Z"}});
}

TEST_F(DateTimeFunctionsTest, readsAStoredCountOfSeconds) {
    write("CREATE (n:Event {name: 'a', epoch: 1700000000})");

    expectRows("MATCH (n:Event) RETURN datetime(n.epoch)", {{"2023-11-14T22:13:20Z"}});
}

TEST_F(DateTimeFunctionsTest, readsAnUnwoundCountOfSeconds) {
    expectRows("UNWIND [1700000000, 1800000000] AS x RETURN datetime(x)",
               {{"2023-11-14T22:13:20Z"}, {"2027-01-15T08:00:00Z"}});
}

TEST_F(DateTimeFunctionsTest, readsUnwoundText) {
    expectRows("UNWIND ['2026-01-15T00:00:00Z'] AS x RETURN datetime(x)",
               {{"2026-01-15T00:00:00Z"}});
}

// A list mixing types hands its elements on with no one type, and no datetime overload
// reads that, so the call is turned away rather than meeting the wrong cell at runtime
TEST_F(DateTimeFunctionsTest, rejectsACountOfSecondsUnwoundFromAMixedList) {
    RowSink sink;
    QueryStatus status;
    _interpreter->execute(status,
                          "UNWIND [1700000000, 'a'] AS x RETURN datetime(x)",
                          _graphName,
                          CommitHash::head(),
                          ChangeID::head(),
                          &_env->getMem(),
                          &sink);

    ASSERT_FALSE(status.isOk());
    EXPECT_NE(status.getError().find("Invalid arguments for function 'datetime'"), std::string::npos)
        << status.getError();
}
