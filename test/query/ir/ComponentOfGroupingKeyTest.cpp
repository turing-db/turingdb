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

// A component read off a grouping key has one value per group, as any expression over the
// key has, so an ORDER BY after the aggregation may read it.
class ComponentOfGroupingKeyTest : public TuringTest {
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
        write("CREATE (n:Event {name: 'a', at: datetime('2026-09-23T14:05:06Z')})");
        write("CREATE (n:Event {name: 'b', at: datetime('2019-01-02T03:04:05Z')})");
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

TEST_F(ComponentOfGroupingKeyTest, ordersOnADurationComponentOfTheGroupingKey) {
    writeTasks();

    expectRows("MATCH (n:Task) WHERE n.took IS NOT NULL "
               "RETURN n.took, count(*) ORDER BY n.took.seconds DESC LIMIT 1",
               {{"PT25H1M1S", "1"}});
    expectRows("MATCH (n:Task) WHERE n.took IS NOT NULL "
               "RETURN n.took, count(*) ORDER BY n.took.seconds ASC LIMIT 1",
               {{"PT-1.5S", "1"}});
}

TEST_F(ComponentOfGroupingKeyTest, ordersOnADateTimeComponentOfTheGroupingKey) {
    writeEvents();

    expectRows("MATCH (n:Event) RETURN n.at, count(*) ORDER BY n.at.year ASC LIMIT 1",
               {{"2019-01-02T03:04:05Z", "1"}});
}

TEST_F(ComponentOfGroupingKeyTest, stillRejectsAComponentOfAPropertyThatIsNoKey) {
    writeTasks();

    expectError("MATCH (n:Task) RETURN n.name, count(*) ORDER BY n.took.hours",
                "ORDER BY with an aggregate may only order by expressions over the returned columns.");
}

TEST_F(ComponentOfGroupingKeyTest, ordersOnADurationComponentOfADistinctItem) {
    writeTasks();

    expectRows("MATCH (n:Task) WHERE n.took IS NOT NULL "
               "RETURN DISTINCT n.took ORDER BY n.took.seconds DESC LIMIT 1",
               {{"PT25H1M1S"}});
    expectRows("MATCH (n:Task) WHERE n.took IS NOT NULL "
               "RETURN DISTINCT n.took ORDER BY n.took.seconds ASC LIMIT 1",
               {{"PT-1.5S"}});
}
