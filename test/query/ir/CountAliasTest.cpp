#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "QueryInterpreterV3.h"
#include "QueryStatus.h"

#include "Graph.h"
#include "SimpleGraph.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"

#include "StringRowSink.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

// 'count' names the aggregate only where a call follows it. Everywhere else it is an
// ordinary identifier, so a query may alias a projection to it and read that alias back.
class CountAliasTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager());

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);
    }

    void runQuery(std::string_view query, StringRowSink& sink, QueryStatus& status) {
        _interpreter->execute(status, query, _graphName, CommitHash::head(), ChangeID::head(), &_env->getMem(), &sink);
    }

    void expectRows(std::string_view query, const std::vector<StringRowSink::Row>& expected) {
        StringRowSink sink;
        QueryStatus status;
        runQuery(query, sink, status);
        ASSERT_TRUE(status.isOk()) << query << ": " << status.getError();

        std::vector<StringRowSink::Row> actual;
        sink.sortedRows(actual);

        std::vector<StringRowSink::Row> sortedExpected = expected;
        std::sort(sortedExpected.begin(), sortedExpected.end());

        EXPECT_EQ(actual, sortedExpected) << "query: " << query;
    }

    void expectOrderedRows(std::string_view query, const std::vector<StringRowSink::Row>& expected) {
        StringRowSink sink;
        QueryStatus status;
        runQuery(query, sink, status);
        ASSERT_TRUE(status.isOk()) << query << ": " << status.getError();

        EXPECT_EQ(sink.getRows(), expected) << "query: " << query;
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

TEST_F(CountAliasTest, ordersByAnAliasNamedCount) {
    expectOrderedRows("MATCH (p:Person)-[:INTERESTED_IN]->(i:Interest) "
                      "RETURN p.name AS name, count(*) AS count ORDER BY count DESC, name ASC LIMIT 3",
                      {{"Remy", "3"}, {"Adam", "2"}, {"Cyrus", "2"}});
}

TEST_F(CountAliasTest, filtersOnAnAliasNamedCount) {
    expectRows("MATCH (p:Person)-[:INTERESTED_IN]->(i:Interest) "
               "WITH p.name AS name, count(*) AS count WHERE count > 2 RETURN name, count",
               {{"Remy", "3"}});
}

TEST_F(CountAliasTest, returnsAnAliasNamedCountThroughAWith) {
    expectRows("MATCH (p:Person) WITH count(*) AS count RETURN count", {{"8"}});
}

TEST_F(CountAliasTest, bindsAPatternVariableNamedCount) {
    expectRows("MATCH (count:Interest {name: 'Ghosts'}) RETURN count.name", {{"Ghosts"}});
}

TEST_F(CountAliasTest, stillReadsCountAsTheAggregate) {
    expectRows("MATCH (p:Person) RETURN count(*)", {{"8"}});
}

TEST_F(CountAliasTest, stillReadsCountAsTheAggregateAcrossWhitespace) {
    expectRows("MATCH (p:Person) RETURN count (*)", {{"8"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
