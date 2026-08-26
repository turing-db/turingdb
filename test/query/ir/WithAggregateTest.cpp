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

// The aggregates a WITH publishes beyond count: the sums, extrema and averages a barrier
// reduces its part to, and the keys it groups them behind.
class WithAggregateTest : public TuringTest {
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

    void expectCounts(std::string_view query, const Counts& expected) {
        CountSink sink;
        const QueryStatus status = runQuery(query, &sink);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        Counts actual;
        sink.sortedCounts(actual);

        EXPECT_EQ(actual, expected) << "query: " << query;
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

// Remy and Adam are the two Persons carrying an age, both 32
TEST_F(WithAggregateTest, sumsAValueOverEveryRow) {
    expectRows("MATCH (n:Person) WITH sum(n.age) AS total RETURN total", {{"64"}});
}

TEST_F(WithAggregateTest, publishesTheExtremaOfAValue) {
    expectRows("MATCH (n:Person) WITH min(n.age) AS lo, max(n.age) AS hi RETURN lo, hi",
               {{"32", "32"}});
}

TEST_F(WithAggregateTest, averagesAValueOverEveryRow) {
    expectRows("MATCH (n:Person) WITH avg(n.age) AS mean RETURN mean", {{"32.000000"}});
}

// Ghosts, Eighties and Animals are each reached by one twenty-minute edge; every other
// interest is reached for a shorter time or by edges carrying no duration at all
TEST_F(WithAggregateTest, sumsAnEdgePropertyPerGroupPastItsFilter) {
    expectRows("MATCH (p:Person)-[e:INTERESTED_IN]->(i) "
               "WITH i.name AS interest, sum(e.duration) AS total WHERE total > 15 "
               "RETURN interest, total",
               {{"Animals", "20"}, {"Eighties", "20"}, {"Ghosts", "20"}});
}

// Two aggregates behind one grouping key, one counting rows and one summing a property
// the edges of most groups do not carry: those sum to nothing
TEST_F(WithAggregateTest, publishesTwoAggregatesBehindOneGroupingKey) {
    expectRows("MATCH (p:Person)-[e:INTERESTED_IN]->(i) "
               "WITH p.name AS name, count(i) AS interests, sum(e.duration) AS total "
               "RETURN name, interests, total",
               {{"Adam", "2", "0"},
                {"Cyrus", "2", "0"},
                {"Doruk", "1", "0"},
                {"Luc", "2", "35"},
                {"Martina", "1", "10"},
                {"Maxime", "2", "0"},
                {"Remy", "3", "40"},
                {"Suhas", "2", "0"}});
}

// Nobody is interested in the same thing twice, so the distinct tally is the plain one:
// every Person bar Martina and Doruk reaches more than one interest
TEST_F(WithAggregateTest, countsDistinctValuesPerGroup) {
    expectRows("MATCH (p:Person)-[:INTERESTED_IN]->(i) "
               "WITH p.name AS name, count(DISTINCT i.name) AS kinds WHERE kinds > 1 "
               "RETURN name, kinds",
               {{"Adam", "2"}, {"Cyrus", "2"}, {"Luc", "2"},
                {"Maxime", "2"}, {"Remy", "3"}, {"Suhas", "2"}});
}

// Fifteen INTERESTED_IN edges reach ten distinct interests, whichever side of the barrier
// the deduplication happens on
TEST_F(WithAggregateTest, countsDistinctValuesOfAPublishedColumn) {
    expectCounts("MATCH (p:Person)-[:INTERESTED_IN]->(i) WITH i.name AS interest "
                 "WITH count(DISTINCT interest) AS kinds RETURN kinds",
                 {10});
}

TEST_F(WithAggregateTest, countsDistinctBoundNodes) {
    expectCounts("MATCH (p:Person) WITH p RETURN count(DISTINCT p)", {8});
}

// The key is computed rather than read: the two aged Persons group under 33 and the six
// without an age under the null the arithmetic leaves
TEST_F(WithAggregateTest, groupsOnAnExpressionKey) {
    expectRows("MATCH (n:Person) WITH n.age + 1 AS next, count(*) AS c RETURN next, c",
               {{"33", "2"}, {"null", "6"}});
}

// Four Persons hold a PhD and four do not
TEST_F(WithAggregateTest, groupsOnABooleanKey) {
    expectCounts("MATCH (n:Person) WITH n.hasPhD AS phd, count(*) AS c RETURN c", {4, 4});
}

// Remy is an expert on two of his interests and Maxime on one of his
TEST_F(WithAggregateTest, groupsOnAnEdgeProperty) {
    expectRows("MATCH (p:Person)-[e:INTERESTED_IN]->(i) "
               "WITH e.proficiency AS level, count(*) AS c WHERE level = 'expert' "
               "RETURN level, c",
               {{"expert", "3"}});
}

// The column this barrier reduces is the count the one before it published: the fifteen
// INTERESTED_IN edges spread over ten interests
TEST_F(WithAggregateTest, sumsAnAggregateABarrierPublished) {
    expectRows("MATCH (p:Person)-[:INTERESTED_IN]->(i) "
               "WITH i.name AS interest, count(p) AS fans "
               "WITH sum(fans) AS total "
               "RETURN total",
               {{"15"}});
}

// Six interests are reached once and Gym three times
TEST_F(WithAggregateTest, publishesTheExtremaOfAnAggregateABarrierPublished) {
    expectRows("MATCH (p:Person)-[:INTERESTED_IN]->(i) "
               "WITH i.name AS interest, count(p) AS fans "
               "WITH min(fans) AS fewest, max(fans) AS most "
               "RETURN fewest, most",
               {{"1", "3"}});
}

TEST_F(WithAggregateTest, averagesAnAggregateABarrierPublished) {
    expectRows("MATCH (p:Person)-[:INTERESTED_IN]->(i) "
               "WITH i.name AS interest, count(p) AS fans "
               "WITH avg(fans) AS mean "
               "RETURN mean",
               {{"1.500000"}});
}

// The grouped reduction reads the same published count: each interest is a group of one
// row here, so its sum is its own tally
TEST_F(WithAggregateTest, groupsASumOfAnAggregateABarrierPublished) {
    expectRows("MATCH (p:Person)-[:INTERESTED_IN]->(i) WITH i, count(p) AS fans "
               "WITH i.name AS interest, sum(fans) AS total WHERE total > 1 "
               "RETURN interest, total",
               {{"Bio", "2"}, {"Computers", "2"}, {"Cooking", "2"}, {"Gym", "3"}});
}

// The group is a node the barrier before it published, not one this part matched
TEST_F(WithAggregateTest, groupsOnABoundEntity) {
    expectRows("MATCH (p:Person) WITH p "
               "MATCH (p)-->(x) "
               "WITH p, count(x) AS c WHERE c > 2 "
               "RETURN p.name, c",
               {{"Adam", "3"}, {"Remy", "4"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
