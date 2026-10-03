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

class DurationPropertyTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager(), &_env->getMem(), &_env->getCompilerContext());

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
                                     &_env->getCompilerContext(),
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
                              &sink);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        submit(changeID);
    }

    void run(std::string_view query, RowSink& sink, QueryStatus& status) {
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              ChangeID::head(),
                              &sink);
    }

    void expectRows(std::string_view query, const Rows& expected) {
        RowSink sink;
        QueryStatus status;
        run(query, sink, status);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        Rows actual;
        sink.sortedRows(actual);

        Rows sortedExpected = expected;
        std::sort(sortedExpected.begin(), sortedExpected.end());

        std::string actualText;
        describeRows(actual, actualText);

        EXPECT_EQ(actual, sortedExpected) << "query: " << query << "\ngot:\n" << actualText;
    }

    void expectOrderedRows(std::string_view query, const Rows& expected) {
        RowSink sink;
        QueryStatus status;
        run(query, sink, status);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        std::string actualText;
        describeRows(sink.rows(), actualText);

        EXPECT_EQ(sink.rows(), expected) << "query: " << query << "\ngot:\n" << actualText;
    }

    void expectError(std::string_view query, std::string_view expectedError) {
        RowSink sink;
        QueryStatus status;
        run(query, sink, status);

        ASSERT_FALSE(status.isOk()) << "query: " << query << "\nexpected it to fail";
        EXPECT_NE(status.getError().find(expectedError), std::string::npos)
            << "query: " << query << "\nerror: " << status.getError();
    }

    void createThreeTasks() {
        write("CREATE (n:Task {name: 'a', took: duration(2000000)})");
        write("CREATE (n:Task {name: 'b', took: duration(90061000000)})");
        write("CREATE (n:Task {name: 'c', took: duration(-1500000)})");
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
    QueryConfig _queryConfig;
};

TEST_F(DurationPropertyTest, returnsADurationOfMicroseconds) {
    expectRows("RETURN duration(1500000)", {{"PT1.5S"}});
    expectRows("RETURN duration(0)", {{"PT0S"}});
    expectRows("RETURN duration(-90061000000)", {{"PT-25H-1M-1S"}});
}

TEST_F(DurationPropertyTest, returnsNullForANullCount) {
    expectRows("RETURN duration(null)", {{"null"}});
}

TEST_F(DurationPropertyTest, rejectsAFractionalCount) {
    expectError("RETURN duration(1.5)", "Invalid arguments for function 'duration'");
}

TEST_F(DurationPropertyTest, storesAndReadsBackADuration) {
    write("CREATE (n:Task {name: 'a', took: duration(90061000000)})");
    expectRows("MATCH (n:Task) RETURN n.took", {{"PT25H1M1S"}});
}

TEST_F(DurationPropertyTest, setsTheDurationOfAnExistingNode) {
    write("MATCH (n:Person {name: 'Remy'}) SET n.shift = duration(28800000000)");
    expectRows("MATCH (n:Person {name: 'Remy'}) RETURN n.shift", {{"PT8H"}});
}

TEST_F(DurationPropertyTest, storesADurationOnAnEdge) {
    write("MATCH (a:Person {name: 'Remy'}), (b:Person {name: 'Adam'}) "
          "CREATE (a)-[:CALLED {for: duration(300000000)}]->(b)");

    expectRows("MATCH (:Person)-[e:CALLED]->(:Person) RETURN e.for", {{"PT5M"}});
}

TEST_F(DurationPropertyTest, readsNullFromANodeCarryingNoDuration) {
    write("CREATE (a:Task {name: 'a', took: duration(1000000)})");
    write("CREATE (b:Task {name: 'b'})");

    expectRows("MATCH (n:Task) RETURN n.name, n.took", {{"a", "PT1S"}, {"b", "null"}});
}

TEST_F(DurationPropertyTest, filtersOnEquality) {
    createThreeTasks();
    expectRows("MATCH (n:Task) WHERE n.took = duration(2000000) RETURN n.name", {{"a"}});
    expectRows("MATCH (n:Task) WHERE n.took <> duration(2000000) RETURN n.name", {{"b"}, {"c"}});
}

TEST_F(DurationPropertyTest, filtersOnOrder) {
    createThreeTasks();
    expectRows("MATCH (n:Task) WHERE n.took < duration(3000000) RETURN n.name", {{"a"}, {"c"}});
}

TEST_F(DurationPropertyTest, ordersByDuration) {
    createThreeTasks();
    expectOrderedRows("MATCH (n:Task) RETURN n.name ORDER BY n.took",
                      {{"c"}, {"a"}, {"b"}});
}

TEST_F(DurationPropertyTest, takesTheShortestAndLongestDuration) {
    createThreeTasks();
    expectRows("MATCH (n:Task) RETURN min(n.took), max(n.took)", {{"PT-1.5S", "PT25H1M1S"}});
}

TEST_F(DurationPropertyTest, countsAndCollectsDurations) {
    write("CREATE (a:Task {name: 'a', took: duration(1000000)})");
    write("CREATE (b:Task {name: 'b', took: duration(1000000)})");

    expectRows("MATCH (n:Task) RETURN count(n.took), count(DISTINCT n.took)", {{"2", "1"}});
    expectRows("MATCH (n:Task) RETURN collect(n.took)", {{"[PT1S, PT1S]"}});
}

TEST_F(DurationPropertyTest, groupsByDuration) {
    write("CREATE (a:Task {name: 'a', took: duration(1000000)})");
    write("CREATE (b:Task {name: 'b', took: duration(1000000)})");
    write("CREATE (c:Task {name: 'c', took: duration(2000000)})");

    expectRows("MATCH (n:Task) RETURN n.took, count(*)", {{"PT1S", "2"}, {"PT2S", "1"}});
}

TEST_F(DurationPropertyTest, storesDurationsInAList) {
    write("CREATE (n:Task {name: 'a', laps: [duration(1000000), duration(60000000)]})");
    expectRows("MATCH (n:Task) RETURN n.laps", {{"[PT1S, PT1M]"}});
}

TEST_F(DurationPropertyTest, storesADurationInAMap) {
    write("CREATE (n:Task {name: 'a', attrs: {took: duration(1000000), n: 1}})");
    expectRows("MATCH (n:Task) RETURN n.attrs", {{"{n: 1, took: PT1S}"}});
}

TEST_F(DurationPropertyTest, unwindsAListOfDurations) {
    expectRows("UNWIND [duration(2000000), duration(1000000)] AS d RETURN d",
               {{"PT1S"}, {"PT2S"}});
}
