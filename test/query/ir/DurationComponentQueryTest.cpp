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

// The units of a duration, read with a dot: off a variable bound to one, off a property of
// an entity holding one, and off an expression answering one.
class DurationComponentQueryTest : public TuringTest {
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

    void expectError(std::string_view query, std::string_view expectedError) {
        RowSink sink;
        QueryStatus status;
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              ChangeID::head(),
                              &_env->getMem(),
                              &sink);

        ASSERT_FALSE(status.isOk()) << "query: " << query << "\nexpected it to fail";
        EXPECT_NE(status.getError().find(expectedError), std::string::npos)
            << "query: " << query << "\nerror: " << status.getError();
    }

    void writeTasks() {
        write("CREATE (n:Task {name: 'a', took: duration(2000000)})");
        write("CREATE (n:Task {name: 'b', took: duration(90061000000)})");
        write("CREATE (n:Task {name: 'c', took: duration(-1500000)})");
        write("CREATE (n:Task {name: 'd'})");
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
    QueryConfig _queryConfig;
};

TEST_F(DurationComponentQueryTest, readsEveryUnitOfABoundDuration) {
    expectRows("WITH duration(90061123456) AS d "
               "RETURN d.years, d.quarters, d.months, d.weeks, d.days, d.hours, d.minutes, "
               "d.seconds, d.milliseconds, d.microseconds",
               {{"0", "0", "0", "0", "1", "25", "1501", "90061", "90061123", "90061123456"}});
}

TEST_F(DurationComponentQueryTest, readsTheCalendarUnitsOfAnAverageYear) {
    expectRows("WITH duration(31556952000000) AS d RETURN d.years, d.quarters, d.months, d.days",
               {{"1", "4", "12", "365"}});
}

TEST_F(DurationComponentQueryTest, readsAUnitOfAnExpression) {
    expectRows("RETURN duration(1500000).milliseconds, duration(1500000).seconds",
               {{"1500", "1"}});
}

TEST_F(DurationComponentQueryTest, truncatesANegativeDurationTowardZero) {
    expectRows("WITH duration(-90000000) AS d RETURN d.hours, d.minutes, d.seconds",
               {{"0", "-1", "-90"}});
}

TEST_F(DurationComponentQueryTest, readsNullFromANullDuration) {
    expectRows("RETURN duration(null).days", {{"null"}});
}

TEST_F(DurationComponentQueryTest, readsAUnitPerRow) {
    writeTasks();

    expectRows("MATCH (n:Task) RETURN n.name, n.took.seconds",
               {{"a", "2"}, {"b", "90061"}, {"c", "-1"}, {"d", "null"}});
}

TEST_F(DurationComponentQueryTest, filtersOnAUnit) {
    writeTasks();

    expectRows("MATCH (n:Task) WHERE n.took.hours > 0 RETURN n.name", {{"b"}});
}

TEST_F(DurationComponentQueryTest, groupsOnAUnit) {
    writeTasks();

    expectRows("MATCH (n:Task) WHERE n.took IS NOT NULL RETURN n.took.days, count(*)",
               {{"0", "2"}, {"1", "1"}});
}

TEST_F(DurationComponentQueryTest, readsAUnitOfADurationOnAnEdge) {
    write("MATCH (a:Person {name: 'Remy'}), (b:Person {name: 'Adam'}) "
          "CREATE (a)-[:WORKED_WITH {span: duration(3600000000)}]->(b)");

    expectRows("MATCH (:Person)-[e:WORKED_WITH]->(:Person) RETURN e.span.hours, e.span.minutes",
               {{"1", "60"}});
}

TEST_F(DurationComponentQueryTest, writesAUnitIntoAProperty) {
    writeTasks();
    write("MATCH (n:Task) WHERE n.name = 'b' SET n.tookSeconds = n.took.seconds");

    expectRows("MATCH (n:Task) WHERE n.name = 'b' RETURN n.tookSeconds", {{"90061"}});
}

TEST_F(DurationComponentQueryTest, readsNullFromAPropertyTheGraphDoesNotCarry) {
    writeTasks();

    expectRows("MATCH (n:Task) WHERE n.name = 'a' RETURN n.unheardOf.hours", {{"null"}});
}

TEST_F(DurationComponentQueryTest, rejectsAnUnknownUnitName) {
    expectError("WITH duration(1) AS d RETURN d.fortnight",
                "'fortnight' is not a component of a duration");
    expectError("RETURN duration(1).fortnight", "'fortnight' is not a component of a duration");
}

// The plural names are a duration's and the singular ones an instant's, as in Neo4j
TEST_F(DurationComponentQueryTest, rejectsTheComponentsOfAnInstant) {
    writeTasks();

    expectError("WITH duration(1) AS d RETURN d.year", "'year' is not a component of a duration");
    expectError("MATCH (n:Task) RETURN n.took.hour", "'hour' is not a component of a duration");
    expectError("WITH datetime() AS d RETURN d.hours", "'hours' is not a component of a datetime");
}

TEST_F(DurationComponentQueryTest, rejectsAUnitOfAPropertyThatIsNoDuration) {
    expectError("MATCH (n:Person) RETURN n.name.hours",
                "Property 'name' is 'String', only a duration has components");
}

TEST_F(DurationComponentQueryTest, rejectsAWriteToAUnit) {
    writeTasks();

    expectError("MATCH (n:Task) SET n.took.hours = 1", "A duration component cannot name a property.");
    expectError("MATCH (n:Task) REMOVE n.took.hours", "A duration component cannot name a property.");
    expectError("MATCH (n:Task) SET n.unheardOf.hours = 1", "A duration component cannot name a property.");
}

TEST_F(DurationComponentQueryTest, readsTheClockRemaindersOfABoundDuration) {
    expectRows("WITH duration(90061123456) AS d "
               "RETURN d.hours, d.minutesOfHour, d.secondsOfMinute, "
               "d.millisecondsOfSecond, d.microsecondsOfSecond, d.daysOfWeek",
               {{"25", "1", "1", "123", "123456", "1"}});
}

TEST_F(DurationComponentQueryTest, readsTheCalendarRemaindersOfAnExpression) {
    expectRows("RETURN duration(45569682000000).quartersOfYear, "
               "duration(45569682000000).monthsOfYear, "
               "duration(45569682000000).monthsOfQuarter",
               {{"1", "5", "2"}});
}

TEST_F(DurationComponentQueryTest, readsARemainderOfAStoredDuration) {
    writeTasks();

    expectRows("MATCH (n:Task) RETURN n.name, n.took.millisecondsOfSecond",
               {{"a", "0"}, {"b", "0"}, {"c", "-500"}, {"d", "null"}});
}

TEST_F(DurationComponentQueryTest, rejectsAWriteToARemainder) {
    writeTasks();

    expectError("MATCH (n:Task) SET n.took.minutesOfHour = 1", "A duration component cannot name a property.");
}
