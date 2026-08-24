#include <gtest/gtest.h>

#include <stdint.h>

#include <algorithm>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

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

// A chain of WITH barriers where each part matches over what the one before published:
// rank then follow up, filter an aggregate then aggregate again, narrow then fan back out.
class WithMultiPartTest : public TuringTest {
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

    // The rows the query emits, compared order-independently
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

    // The rows in the order the query emits them, for the ORDER BY tests
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

// Gym is reached three times and Bio, Computers and Cooking twice each, so the tie is
// broken by name and Bio takes the second place
TEST_F(WithMultiPartTest, ranksThenFansBackOut) {
    expectRows("MATCH (p:Person)-[:INTERESTED_IN]->(i) "
               "WITH i, count(p) AS fans ORDER BY fans DESC, i.name LIMIT 2 "
               "MATCH (i)<-[:INTERESTED_IN]-(q:Person) "
               "RETURN i.name, count(q)",
               {{"Bio", "2"}, {"Gym", "3"}});
}

// Remy has the most out-edges, four, three of which are interests
TEST_F(WithMultiPartTest, followsUpOnTheTopRankedRow) {
    expectRows("MATCH (p:Person)-->(x) "
               "WITH p, count(x) AS degree ORDER BY degree DESC LIMIT 1 "
               "MATCH (p)-[:INTERESTED_IN]->(i) "
               "RETURN i.name",
               {{"Computers"}, {"Eighties"}, {"Ghosts"}});
}

// The first HAVING drops Martina and Doruk, who have one interest each, which changes the
// second tally: Cooking falls to one sharer and Gym from three to two
TEST_F(WithMultiPartTest, chainsTwoHavings) {
    expectRows("MATCH (p:Person)-[:INTERESTED_IN]->(i) "
               "WITH p, count(i) AS interests WHERE interests > 1 "
               "MATCH (p)-[:INTERESTED_IN]->(j) "
               "WITH j.name AS interest, count(p) AS sharers WHERE sharers > 1 "
               "RETURN interest, sharers",
               {{"Bio", "2"}, {"Computers", "2"}, {"Gym", "2"}});
}

// The four French Persons reach seven distinct interests, reached ten times in all
TEST_F(WithMultiPartTest, narrowsThenFansOutOverDedupedRows) {
    expectRows("MATCH (p:Person) "
               "WITH p WHERE p.isFrench = true "
               "MATCH (p)-[:INTERESTED_IN]->(i) "
               "WITH DISTINCT i "
               "MATCH (i)<-[:INTERESTED_IN]-(q:Person) "
               "WITH q.name AS person "
               "RETURN count(person)",
               {{"10"}});
}

// Three INTERESTED_IN edges are expert-held, and only Ghosts - Remy's - knows anybody well
TEST_F(WithMultiPartTest, carriesAnEdgePropertyAcrossAFurtherHop) {
    expectRows("MATCH (p:Person)-[e:INTERESTED_IN]->(i) "
               "WITH p.name AS person, e.proficiency AS level, i WHERE level = 'expert' "
               "MATCH (i)-[:KNOWS_WELL]->(k) "
               "RETURN person, level, k.name",
               {{"Remy", "expert", "Remy"}});
}

// The four Persons with a PhD, and how many interests each of them holds
TEST_F(WithMultiPartTest, groupsTheLastHopAfterThreeBarriers) {
    expectRows("MATCH (p:Person) WITH p WHERE p.hasPhD = true "
               "MATCH (p)-[:INTERESTED_IN]->(i) WITH DISTINCT p, i "
               "WITH p.name AS person, count(i) AS interests "
               "RETURN person, interests",
               {{"Adam", "2"}, {"Luc", "2"}, {"Martina", "1"}, {"Remy", "3"}});
}

TEST_F(WithMultiPartTest, republishesAnAggregateThroughASecondBarrier) {
    expectRowsInOrder("MATCH (p:Person)-[:INTERESTED_IN]->(i) "
                      "WITH p.name AS person, count(i) AS interests "
                      "WITH person, interests WHERE interests > 1 "
                      "RETURN person, interests ORDER BY interests DESC, person",
                      {{"Remy", "3"},
                       {"Adam", "2"},
                       {"Cyrus", "2"},
                       {"Luc", "2"},
                       {"Maxime", "2"},
                       {"Suhas", "2"}});
}

// Gym leads with three followers, so skipping it leaves the three interests reached twice
TEST_F(WithMultiPartTest, pagesARankingOnTheBarrier) {
    expectRowsInOrder("MATCH (p:Person)-[:INTERESTED_IN]->(i) "
                      "WITH i.name AS interest, count(p) AS fans "
                      "ORDER BY fans DESC, interest SKIP 1 LIMIT 3 "
                      "RETURN interest, fans",
                      {{"Bio", "2"}, {"Computers", "2"}, {"Cooking", "2"}});
}

// The common-neighbour shape, whose two ends are both variables the barrier bound:
// Computers is the one interest Remy and Luc share.
TEST_F(WithMultiPartTest, joinsTwoBoundVariablesThroughANewNode) {
    expectRows("MATCH (a:Person), (b:Person) "
               "WITH a, b WHERE a.name = 'Remy' AND b.name = 'Luc' "
               "MATCH (a)-[:INTERESTED_IN]->(i)<-[:INTERESTED_IN]-(b) "
               "RETURN i.name",
               {{"Computers"}});
}

// Of the four nodes Remy points at, one is the one he knows well
TEST_F(WithMultiPartTest, joinsTwoBoundVariablesDirectly) {
    expectRows("MATCH (a:Person {name: 'Remy'})-->(b) WITH a, b "
               "MATCH (a)-[:KNOWS_WELL]->(b) "
               "RETURN b.name",
               {{"Adam"}});
}

// Every Person-to-Person edge of simpledb is a KNOWS_WELL; the seventeen out of a Person
// are not
TEST_F(WithMultiPartTest, constrainsTheEdgeTypeOfAClosingJoin) {
    expectRows("MATCH (a:Person)-->(b) WITH a, b "
               "MATCH (a)-[:KNOWS_WELL]->(b) "
               "RETURN a.name, b.name",
               {{"Adam", "Remy"}, {"Remy", "Adam"}});
}

// An undirected closing join reads the edge either way round: Remy knows Adam well in
// both directions, and Ghosts knows him well the other way
TEST_F(WithMultiPartTest, joinsTwoBoundVariablesUndirected) {
    expectRows("MATCH (a:Person {name: 'Remy'})-->(b) WITH a, b "
               "MATCH (a)-[:KNOWS_WELL]-(b) "
               "RETURN b.name",
               {{"Adam"}, {"Adam"}, {"Ghosts"}});
}

// Each of the three KNOWS_WELL pairs has exactly one edge each way, and the single-part
// spelling of the query is what the barrier has to agree with
TEST_F(WithMultiPartTest, closesACycleThroughTwoBoundVariables) {
    expectRows("MATCH (a)-[:KNOWS_WELL]->(b), (a)-[e1]->(b), (b)-[e2]->(a) RETURN count(*)",
               {{"3"}});
    expectRows("MATCH (a)-[:KNOWS_WELL]->(b) WITH a, b "
               "MATCH (a)-[e1]->(b), (b)-[e2]->(a) "
               "RETURN count(*)",
               {{"3"}});
}

// One edge runs between the two ends of each KNOWS_WELL pair, so e1 and e2 are that edge
TEST_F(WithMultiPartTest, closesTwoParallelHopsBetweenBoundVariables) {
    expectRows("MATCH (a)-[:KNOWS_WELL]->(b) WITH a, b "
               "MATCH (a)-[e1]->(b), (a)-[e2]->(b) "
               "RETURN count(*)",
               {{"3"}});
}

// Remy and Adam each know the other well; nobody else knows a Person well both ways
TEST_F(WithMultiPartTest, closesACycleThroughOneBoundVariable) {
    expectRows("MATCH (a:Person) WITH a "
               "MATCH (a)-[:KNOWS_WELL]->(b)-[:KNOWS_WELL]->(a) "
               "RETURN a.name, b.name",
               {{"Adam", "Remy"}, {"Remy", "Adam"}});
}

// The middle leg closes a cycle between two ends that both reach the bound variable. No
// two Persons who know somebody well are interested in each other, so no row - as in the
// single-part spelling
TEST_F(WithMultiPartTest, walksACycleAmongTheEndsOfABoundVariable) {
    expectRows("MATCH (a:Person), (b)-[:KNOWS_WELL]->(a), (c)-[:KNOWS_WELL]->(a), "
               "      (b)-[:INTERESTED_IN]->(c) "
               "RETURN a.name",
               {});
    expectRows("MATCH (a:Person) WITH a "
               "MATCH (b)-[:KNOWS_WELL]->(a), (c)-[:KNOWS_WELL]->(a), "
               "      (b)-[:INTERESTED_IN]->(c) "
               "RETURN a.name",
               {});
}

// The barrier published one row per Person, so the pattern after it is crossed with
// those eight
TEST_F(WithMultiPartTest, crossesAConstantsOnlyBarrierWithTheNextPattern) {
    expectRows("MATCH (p:Person) WITH 1 AS one MATCH (q:Person) RETURN count(*)", {{"64"}});
    expectRows("MATCH (p:Person) WITH 1 AS one MATCH (q:Person {name: 'Remy'}) "
               "RETURN one, q.name",
               Rows(8, Row {"1", "Remy"}));
}

// An UNWIND after a barrier is a list crossed with the rows the barrier published
TEST_F(WithMultiPartTest, unwindsBesideABoundVariable) {
    expectRows("MATCH (n:Person {name: 'Remy'}) WITH n UNWIND [1, 2] AS y RETURN n.name, y",
               {{"Remy", "1"}, {"Remy", "2"}});
}

// The same the other way round: the barrier publishes the unwound rows
TEST_F(WithMultiPartTest, matchesBesideAnUnwoundBoundVariable) {
    expectRows("UNWIND [1, 2] AS x WITH x MATCH (n:Person {name: 'Remy'}) RETURN x, n.name",
               {{"1", "Remy"}, {"2", "Remy"}});
}

// A scalar aggregate is the one row of its part, crossed with the pattern after it
TEST_F(WithMultiPartTest, crossesAScalarAggregateWithTheNextPattern) {
    expectRows("MATCH (n:Person) WITH count(*) AS total "
               "MATCH (q:Person {name: 'Remy'}) "
               "RETURN total, q.name",
               {{"8", "Remy"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
