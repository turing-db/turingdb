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

// collect() taken at a WITH barrier: the list a part publishes, what the part after it
// reads back, and the ORDER BY, cut, filter and reductions that sit beside it.
class WithCollectTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager());

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
                              &_env->getMem(),
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

    // The rows in the order the query emits them, which is what a barrier's ORDER BY is
    void expectRowsInOrder(std::string_view query, const Rows& expected) {
        RowSink sink;
        const QueryStatus status = runQuery(query, &sink);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        std::string actualText;
        describeRows(sink.rows(), actualText);

        EXPECT_EQ(sink.rows(), expected) << "query: " << query << "\ngot:\n" << actualText;
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

TEST_F(WithCollectTest, publishesAGroupedListAtTheBarrier) {
    expectRows("MATCH (p:Person)-[:INTERESTED_IN]->(i:Interest) "
               "WITH p.name AS person, collect(i.name) AS interests "
               "RETURN person, interests",
               {{"Remy", "[Ghosts, Computers, Eighties]"},
                {"Adam", "[Bio, Cooking]"},
                {"Maxime", "[Bio, Padel]"},
                {"Luc", "[Animals, Computers]"},
                {"Martina", "[Cooking]"},
                {"Suhas", "[Gym, JiuJitsu]"},
                {"Cyrus", "[Gym, Travel]"},
                {"Doruk", "[Gym]"}});
}

TEST_F(WithCollectTest, publishesTheWholeMatchAsOneListAtTheBarrier) {
    expectRows("MATCH (i:Interest) WITH collect(i.name) AS names RETURN names",
               {{"[Computers, Eighties, Bio, Cooking, Ghosts, Padel, Animals, Gym, Travel, JiuJitsu]"}});
}

// The list rides past the barrier untouched while the key beside it is filtered on, the
// HAVING a WITH reads as
TEST_F(WithCollectTest, filtersTheGroupedRowsAfterTheBarrier) {
    expectRows("MATCH (p:Person)-[:INTERESTED_IN]->(i:Interest) "
               "WITH p.name AS person, collect(i.name) AS interests "
               "WHERE person = 'Adam' "
               "RETURN person, interests",
               {{"Adam", "[Bio, Cooking]"}});
}

TEST_F(WithCollectTest, ordersTheGroupedRowsAtTheBarrier) {
    expectRowsInOrder("MATCH (p:Person)-[:INTERESTED_IN]->(i:Interest) "
                      "WITH p.name AS person, collect(i.name) AS interests ORDER BY person "
                      "RETURN person, interests",
                      {{"Adam", "[Bio, Cooking]"},
                       {"Cyrus", "[Gym, Travel]"},
                       {"Doruk", "[Gym]"},
                       {"Luc", "[Animals, Computers]"},
                       {"Martina", "[Cooking]"},
                       {"Maxime", "[Bio, Padel]"},
                       {"Remy", "[Ghosts, Computers, Eighties]"},
                       {"Suhas", "[Gym, JiuJitsu]"}});
}

TEST_F(WithCollectTest, cutsTheGroupedRowsAtTheBarrier) {
    expectRowsInOrder("MATCH (p:Person)-[:INTERESTED_IN]->(i:Interest) "
                      "WITH p.name AS person, collect(i.name) AS interests "
                      "ORDER BY person SKIP 1 LIMIT 2 "
                      "RETURN person, interests",
                      {{"Cyrus", "[Gym, Travel]"}, {"Doruk", "[Gym]"}});
}

TEST_F(WithCollectTest, reducesBesideTheListAtTheBarrier) {
    expectRows("MATCH (p:Person)-[:INTERESTED_IN]->(i:Interest) "
               "WITH p.name AS person, collect(i.name) AS interests, count(i) AS interestCount "
               "RETURN person, interests, interestCount",
               {{"Remy", "[Ghosts, Computers, Eighties]", "3"},
                {"Adam", "[Bio, Cooking]", "2"},
                {"Maxime", "[Bio, Padel]", "2"},
                {"Luc", "[Animals, Computers]", "2"},
                {"Martina", "[Cooking]", "1"},
                {"Suhas", "[Gym, JiuJitsu]", "2"},
                {"Cyrus", "[Gym, Travel]", "2"},
                {"Doruk", "[Gym]", "1"}});
}

// One person contributes as many rows as they have interests, so the barrier collects a
// name per row unless the DISTINCT drops the repeats
TEST_F(WithCollectTest, collectsDistinctValuesAtTheBarrier) {
    expectRows("MATCH (p:Person)-[:INTERESTED_IN]->(i:Interest) "
               "WITH collect(DISTINCT p.name) AS people "
               "RETURN people",
               {{"[Remy, Adam, Maxime, Luc, Martina, Cyrus, Suhas, Doruk]"}});
}

// The collected column is one an earlier barrier published, not one the traversal bound
TEST_F(WithCollectTest, collectsWhatAnEarlierBarrierPublished) {
    expectRows("MATCH (p:Person) WITH p.name AS person "
               "WITH collect(person) AS people "
               "RETURN people",
               {{"[Remy, Adam, Maxime, Luc, Martina, Suhas, Cyrus, Doruk]"}});
}

// A list of node identities rather than of values: Computers is 2, Eighties 3, Bio 4,
// Cooking 5, Ghosts 6, Padel 7, Animals 10, Gym 13, Travel 14, JiuJitsu 16
TEST_F(WithCollectTest, collectsTheEntitiesTheBarrierGroups) {
    expectRows("MATCH (p:Person)-[:INTERESTED_IN]->(i:Interest) "
               "WITH p.name AS person, collect(i) AS interests "
               "RETURN person, interests",
               {{"Remy", "[6, 2, 3]"},
                {"Adam", "[4, 5]"},
                {"Maxime", "[4, 7]"},
                {"Luc", "[10, 2]"},
                {"Martina", "[5]"},
                {"Suhas", "[13, 16]"},
                {"Cyrus", "[13, 14]"},
                {"Doruk", "[13]"}});
}

// Grouping on the node itself leaves the part after the barrier reading a property off
// the grouped column, not off the traversal variable the barrier dropped
TEST_F(WithCollectTest, groupsOnTheNodeTheBarrierPublishes) {
    expectRows("MATCH (p:Person)-[:INTERESTED_IN]->(i:Interest) "
               "WITH p, collect(i.name) AS interests "
               "RETURN p.name, interests",
               {{"Remy", "[Ghosts, Computers, Eighties]"},
                {"Adam", "[Bio, Cooking]"},
                {"Maxime", "[Bio, Padel]"},
                {"Luc", "[Animals, Computers]"},
                {"Martina", "[Cooking]"},
                {"Suhas", "[Gym, JiuJitsu]"},
                {"Cyrus", "[Gym, Travel]"},
                {"Doruk", "[Gym]"}});
}

// The collect is taken in the part after the barrier, over a traversal rooted on the node
// the barrier published
TEST_F(WithCollectTest, collectsAfterTraversingFromWhatTheBarrierPublished) {
    expectRows("MATCH (p:Person {name: 'Remy'}) WITH p AS remy "
               "MATCH (remy)-[:INTERESTED_IN]->(i:Interest) "
               "RETURN collect(i.name)",
               {{"[Ghosts, Computers, Eighties]"}});
}

// The barrier's sort key is the list itself, ordered element by element with the shorter
// of two common prefixes first
TEST_F(WithCollectTest, ordersTheBarrierRowsByTheCollectedList) {
    expectRowsInOrder("MATCH (p:Person)-[:INTERESTED_IN]->(i:Interest) "
                      "WITH p.name AS person, collect(i.name) AS interests ORDER BY interests "
                      "RETURN person, interests",
                      {{"Luc", "[Animals, Computers]"},
                       {"Adam", "[Bio, Cooking]"},
                       {"Maxime", "[Bio, Padel]"},
                       {"Martina", "[Cooking]"},
                       {"Remy", "[Ghosts, Computers, Eighties]"},
                       {"Doruk", "[Gym]"},
                       {"Suhas", "[Gym, JiuJitsu]"},
                       {"Cyrus", "[Gym, Travel]"}});
}

// The collect folds the rows the barrier published, not the ones its cut dropped
TEST_F(WithCollectTest, collectsOnlyTheRowsTheBarrierLeft) {
    expectRows("MATCH (p:Person) WITH p.name AS name ORDER BY name LIMIT 3 "
               "RETURN collect(name)",
               {{"[Adam, Cyrus, Doruk]"}});
}

// One accumulator carries both lists, so a barrier publishes them side by side
TEST_F(WithCollectTest, publishesTwoListsAtTheBarrier) {
    expectRows("MATCH (p:Person)-[:INTERESTED_IN]->(i:Interest) "
               "WITH p.name AS person, collect(i.name) AS interests, collect(i) AS nodes "
               "RETURN person, interests, nodes",
               {{"Remy", "[Ghosts, Computers, Eighties]", "[6, 2, 3]"},
                {"Adam", "[Bio, Cooking]", "[4, 5]"},
                {"Maxime", "[Bio, Padel]", "[4, 7]"},
                {"Luc", "[Animals, Computers]", "[10, 2]"},
                {"Martina", "[Cooking]", "[5]"},
                {"Suhas", "[Gym, JiuJitsu]", "[13, 16]"},
                {"Cyrus", "[Gym, Travel]", "[13, 14]"},
                {"Doruk", "[Gym]", "[13]"}});
}
