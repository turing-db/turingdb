#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <string>
#include <string_view>

#include "NLOutputSink.h"
#include "QueryInterpreterV3.h"
#include "QueryStatus.h"

#include "Graph.h"
#include "SimpleGraph.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"

#include "IRTestRows.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

class GroupedDistinctContainerCountTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager(), &_env->getMem(), &_env->getCompilerContext());

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);
    }

    QueryStatus runQuery(std::string_view query, NLOutputSink* sink) {
        QueryStatus status;
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              ChangeID::head(),
                              sink);

        return status;
    }

    void expectRows(std::string_view query, const Rows& expected) {
        RowSink sink;
        const QueryStatus status = runQuery(query, &sink);
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
};

TEST_F(GroupedDistinctContainerCountTest, countsEachGroupsDistinctPresentLists) {
    expectRows("MATCH (n:Person)-[:INTERESTED_IN]->(i) OPTIONAL MATCH (n)-[:KNOWS_WELL]->(m) "
               "RETURN n.name, count(DISTINCT labels(m))",
               {{"Remy", "1"},
                {"Adam", "1"},
                {"Maxime", "0"},
                {"Luc", "0"},
                {"Martina", "0"},
                {"Cyrus", "0"},
                {"Doruk", "0"},
                {"Suhas", "0"}});
}

TEST_F(GroupedDistinctContainerCountTest, countsEachGroupsDistinctPresentMaps) {
    expectRows("MATCH (n:Person)-[:INTERESTED_IN]->(i) OPTIONAL MATCH (n)-[:KNOWS_WELL]->(m) "
               "RETURN n.name, count(DISTINCT properties(m))",
               {{"Remy", "1"},
                {"Adam", "1"},
                {"Maxime", "0"},
                {"Luc", "0"},
                {"Martina", "0"},
                {"Cyrus", "0"},
                {"Doruk", "0"},
                {"Suhas", "0"}});
}

// Adam, Maxime and Martina have no real interest, so each group mixes null rows with
// present ones. Luc and Remy share Computers, and Gym's lists and maps repeat.
TEST_F(GroupedDistinctContainerCountTest, countsDistinctListsOverGroupsMixingNullRows) {
    expectRows("MATCH (n:Person) OPTIONAL MATCH (n)-[:INTERESTED_IN]->(i:Interest {isReal: true}) "
               "RETURN n.isFrench, count(DISTINCT labels(i))",
               {{"true", "3"},
                {"false", "1"}});
}

TEST_F(GroupedDistinctContainerCountTest, countsDistinctMapsOverGroupsMixingNullRows) {
    expectRows("MATCH (n:Person) OPTIONAL MATCH (n)-[:INTERESTED_IN]->(i:Interest {isReal: true}) "
               "RETURN n.isFrench, count(DISTINCT properties(i))",
               {{"true", "3"},
                {"false", "3"}});
}
