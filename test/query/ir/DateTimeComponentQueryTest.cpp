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

// The calendar fields of an instant, read with a dot: off a variable bound to one, and off
// a property of an entity holding one.
class DateTimeComponentQueryTest : public TuringTest {
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

    void writeEvents() {
        write("CREATE (a:Event {name: 'a', at: datetime('2026-09-23T14:05:06.123456Z')})");
        write("CREATE (b:Event {name: 'b', at: datetime('2019-01-02T03:04:05Z')})");
        write("CREATE (c:Event {name: 'c'})");
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
    QueryConfig _queryConfig;
};

TEST_F(DateTimeComponentQueryTest, readsEveryFieldOfABoundInstant) {
    expectRows("WITH datetime('2026-09-23T14:05:06.123456Z') AS d "
               "RETURN d.year, d.month, d.day, d.hour, d.minute, d.second, "
               "d.millisecond, d.microsecond",
               {{"2026", "9", "23", "14", "5", "6", "123", "123456"}});
}

TEST_F(DateTimeComponentQueryTest, readsAFieldOfTheCurrentInstant) {
    expectRows("WITH datetime() AS d RETURN d.year >= 2026", {{"true"}});
}

TEST_F(DateTimeComponentQueryTest, readsAFieldOfAnInstantBeforeTheEpoch) {
    expectRows("WITH datetime('1969-12-31T23:59:59Z') AS d RETURN d.year, d.month, d.second",
               {{"1969", "12", "59"}});
}

TEST_F(DateTimeComponentQueryTest, readsAFieldOfAStoredInstant) {
    writeEvents();

    expectRows("MATCH (n:Event) WHERE n.name = 'a' RETURN n.at.year, n.at.month, n.at.day",
               {{"2026", "9", "23"}});
}

TEST_F(DateTimeComponentQueryTest, readsAFieldPerRow) {
    writeEvents();

    expectRows("MATCH (n:Event) RETURN n.name, n.at.year",
               {{"a", "2026"}, {"b", "2019"}, {"c", "null"}});
}

TEST_F(DateTimeComponentQueryTest, readsNullFromANodeCarryingNoInstant) {
    writeEvents();

    expectRows("MATCH (n:Event) WHERE n.name = 'c' RETURN n.at.hour", {{"null"}});
}

TEST_F(DateTimeComponentQueryTest, filtersOnAField) {
    writeEvents();

    expectRows("MATCH (n:Event) WHERE n.at.year > 2020 RETURN n.name", {{"a"}});
}

TEST_F(DateTimeComponentQueryTest, ordersOnAField) {
    writeEvents();

    expectRows("MATCH (n:Event) WHERE n.at IS NOT NULL RETURN n.name ORDER BY n.at.year DESC",
               {{"a"}, {"b"}});
}

TEST_F(DateTimeComponentQueryTest, groupsOnAField) {
    writeEvents();

    expectRows("MATCH (n:Event) WHERE n.at IS NOT NULL RETURN n.at.year, count(*)",
               {{"2026", "1"}, {"2019", "1"}});
}

TEST_F(DateTimeComponentQueryTest, readsAFieldOfAnInstantOnAnEdge) {
    write("MATCH (a:Person {name: 'Remy'}), (b:Person {name: 'Adam'}) "
          "CREATE (a)-[:MET {at: datetime('2026-09-23T14:05:00Z')}]->(b)");

    expectRows("MATCH (:Person)-[e:MET]->(:Person) RETURN e.at.year, e.at.minute",
               {{"2026", "5"}});
}

TEST_F(DateTimeComponentQueryTest, writesAFieldIntoAProperty) {
    writeEvents();
    write("MATCH (n:Event) WHERE n.name = 'a' SET n.atYear = n.at.year");

    expectRows("MATCH (n:Event) WHERE n.name = 'a' RETURN n.atYear", {{"2026"}});
}

// A name no property in the graph carries reads null, and so does the field taken off it
TEST_F(DateTimeComponentQueryTest, readsNullFromAPropertyTheGraphDoesNotCarry) {
    writeEvents();

    expectRows("MATCH (n:Event) WHERE n.name = 'a' RETURN n.unheardOf.year", {{"null"}});
}

TEST_F(DateTimeComponentQueryTest, rejectsAnUnknownFieldName) {
    expectError("WITH datetime() AS d RETURN d.fortnight",
                "'fortnight' is not a component of a datetime");
}

TEST_F(DateTimeComponentQueryTest, rejectsAFieldOfAPropertyThatIsNoInstant) {
    expectError("MATCH (n:Person) RETURN n.name.year",
                "Property 'name' is 'String', only a datetime has components");
}

TEST_F(DateTimeComponentQueryTest, rejectsAFieldOfAVariableThatIsNoInstant) {
    expectError("WITH 1 AS x RETURN x.year", "Variable 'x' is 'Integer' it must be a node or edge");
}

TEST_F(DateTimeComponentQueryTest, rejectsAFieldOfAField) {
    expectError("WITH datetime() AS d RETURN d.year.month",
                "Variable 'd' is 'DateTime' it must be a node or edge");
}

// A field is computed from an instant rather than stored beside it, so nothing can be
// written to one
TEST_F(DateTimeComponentQueryTest, rejectsAWriteToAField) {
    writeEvents();

    expectError("MATCH (n:Event) SET n.at.year = 2030", "A datetime component cannot name a property.");
    expectError("MATCH (n:Event) SET n.at.year = null", "A datetime component cannot name a property.");
    expectError("MATCH (n:Event) REMOVE n.at.year", "A datetime component cannot name a property.");
    expectError("MATCH (n:Event) SET n.unheardOf.year = 2030", "A datetime component cannot name a property.");
}
