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

// What the scope a WITH opens holds: the columns its filter can read, the names it can
// republish, and what a part after it can reach from them.
class WithScopeTest : public TuringTest {
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

    // A rejection has to be a rejection: turned away at @param stage with a message naming
    // what is wrong with the query, not by a tripped assertion or invalid IR
    void expectRejected(std::string_view query, QueryStatus::Status stage) {
        RowSink sink;
        const QueryStatus status = runQuery(query, &sink);
        ASSERT_FALSE(status.isOk()) << "query accepted: " << query;

        const std::string& error = status.getError();

        EXPECT_EQ(status.getStatus(), stage)
            << "query: " << query
            << "\nstage: " << QueryStatusDescription::value(status.getStatus())
            << "\nerror: " << error;

        EXPECT_EQ(error.find("Unexpected exception"), std::string::npos)
            << "query: " << query << "\nerror: " << error;
        EXPECT_EQ(error.find("Internal Error"), std::string::npos)
            << "query: " << query << "\nerror: " << error;
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

TEST_F(WithScopeTest, filtersThePublishedRowsOnADisjunction) {
    expectRows("MATCH (n:Person) WITH n.name AS name WHERE name = 'Remy' OR name = 'Adam' "
               "RETURN name",
               {{"Adam"}, {"Remy"}});
}

TEST_F(WithScopeTest, filtersThePublishedRowsOnANegation) {
    expectRows("MATCH (n:Person) WITH n.name AS name, n.hasPhD AS phd WHERE NOT phd "
               "RETURN name",
               {{"Cyrus"}, {"Doruk"}, {"Maxime"}, {"Suhas"}});
}

// Six of the eight Persons carry no age, and the rows the barrier published for them hold
// a null the filter reads like any other value
TEST_F(WithScopeTest, filtersThePublishedRowsOnAMissingValue) {
    expectRows("MATCH (n:Person) WITH n.name AS name, n.age AS age WHERE age IS NULL "
               "RETURN name",
               {{"Cyrus"}, {"Doruk"}, {"Luc"}, {"Martina"}, {"Maxime"}, {"Suhas"}});
}

// The filter reads a property off the node the barrier published rather than a column the
// projection spelled out
TEST_F(WithScopeTest, filtersOnAPropertyOfABoundNode) {
    expectRows("MATCH (p:Person) WITH p WHERE p.name = 'Remy' RETURN p.name", {{"Remy"}});
}

// The one KNOWS_WELL edge lasting longer than twenty minutes
TEST_F(WithScopeTest, filtersOnAPropertyOfABoundEdge) {
    expectRows("MATCH (p)-[e:KNOWS_WELL]->(x) WITH e WHERE e.duration > 20 RETURN e.name",
               {{"Ghosts -> Remy"}});
}

// The filter reads the barrier's scope, so the match variable it dropped is out of reach
TEST_F(WithScopeTest, rejectsAVariableTheBarrierDroppedInItsFilter) {
    expectRejected("MATCH (n:Person) WITH n.name AS name WHERE n.age > 30 RETURN name",
                   QueryStatus::Status::ANALYZE_ERROR);
}

// A wildcard publishes the variables in scope, and a barrier opening the query has none
TEST_F(WithScopeTest, rejectsAWildcardWithNothingInScope) {
    expectRejected("WITH * RETURN 1", QueryStatus::Status::ANALYZE_ERROR);
}

// The wildcard of the second barrier expands to what the first one published, which is an
// alias rather than a match variable
TEST_F(WithScopeTest, republishesThePublishedScopeUnderAWildcard) {
    expectRows("MATCH (n:Person) WITH n.name AS name WITH * RETURN name",
               {{"Adam"}, {"Cyrus"}, {"Doruk"}, {"Luc"},
                {"Martina"}, {"Maxime"}, {"Remy"}, {"Suhas"}});
}

// Both names hold the same node, so the hop one of them walks reads the other's rows
TEST_F(WithScopeTest, publishesOneNodeUnderTwoNames) {
    expectRows("MATCH (p:Person {name: 'Remy'}) WITH p AS a, p AS b "
               "MATCH (a)-[:KNOWS_WELL]->(x) "
               "RETURN b.name, x.name",
               {{"Remy", "Adam"}});
}

// A name renamed at every barrier of a chain still holds the rows the first one published
TEST_F(WithScopeTest, renamesThroughAChainOfBarriers) {
    expectCounts("MATCH (p:Person) WITH p.name AS name WITH name AS person "
                 "WITH person AS who WITH who AS someone "
                 "RETURN count(someone)",
                 {8});
}

// The hop leaves the second of the two bound variables and fans out: Remy and Adam each
// know the other well, so each row carries the other's interests
TEST_F(WithScopeTest, fansOutFromTheSecondOfTwoBoundVariables) {
    expectRows("MATCH (a:Person)-[:KNOWS_WELL]->(b) WITH a, b "
               "MATCH (b)-[:INTERESTED_IN]->(i) "
               "RETURN a.name, i.name",
               {{"Adam", "Computers"}, {"Adam", "Eighties"}, {"Adam", "Ghosts"},
                {"Remy", "Bio"}, {"Remy", "Cooking"}});
}

// Read either way round, the hop finds Adam on both sides of his pair with Remy and Ghosts
// on the edge pointing at Remy
TEST_F(WithScopeTest, hopsUndirectedFromABoundVariable) {
    expectRows("MATCH (p:Person {name: 'Remy'}) WITH p "
               "MATCH (p)-[:KNOWS_WELL]-(x) "
               "RETURN x.name",
               {{"Adam"}, {"Adam"}, {"Ghosts"}});
}

// The predicate of the part after the barrier reads a column the barrier published, and
// cuts the rows of a hop the barrier never saw
TEST_F(WithScopeTest, filtersAHopOnAPublishedAlias) {
    expectRows("MATCH (n:Person) WITH n, n.name AS name "
               "MATCH (n)-[:INTERESTED_IN]->(x) WHERE name = 'Remy' "
               "RETURN x.name",
               {{"Computers"}, {"Eighties"}, {"Ghosts"}});
}

// Another name for a column the projection already publishes, which is what the same
// reference inside a wider expression has always been
TEST_F(WithScopeTest, republishesASiblingAliasUnderASecondName) {
    expectRows("MATCH (n:Person {name: 'Remy'}) WITH n.name AS name, name AS person "
               "RETURN name, person",
               {{"Remy", "Remy"}});
    expectRows("MATCH (n:Person {name: 'Remy'}) RETURN n.name AS name, name AS person",
               {{"Remy", "Remy"}});
}

// The ORDER BY of a barrier reads the scope the barrier opens, so a match variable its
// projection dropped is out of reach there just as it is in the filter
TEST_F(WithScopeTest, rejectsAnOrderByKeyTheBarrierDropped) {
    expectRejected("MATCH (p:Person) WITH p.name AS name ORDER BY p.age RETURN name",
                   QueryStatus::Status::ANALYZE_ERROR);
    expectRejected("MATCH (p:Person) WITH p.name AS name ORDER BY name, p.age RETURN name",
                   QueryStatus::Status::ANALYZE_ERROR);
}

// A key over a variable the projection did publish orders on it as before: Adam is the
// first Person in name order
TEST_F(WithScopeTest, ordersOnAPropertyOfAPublishedVariable) {
    expectRows("MATCH (p:Person) WITH p.name AS name, p AS person ORDER BY person.name LIMIT 1 "
               "RETURN name",
               {{"Adam"}});
}

// The scope a barrier opens replaces the one before it, so an alias may take the name of
// the variable whose property it publishes
TEST_F(WithScopeTest, publishesAPropertyUnderTheNameOfItsVariable) {
    expectRows("MATCH (n:Person {name: 'Remy'}) WITH n.name AS n RETURN n", {{"Remy"}});
    expectRows("MATCH (n:Person {name: 'Remy'}) RETURN n.name AS n", {{"Remy"}});
    expectRows("MATCH (n:Person) WITH n.name AS n ORDER BY n LIMIT 2 RETURN n",
               {{"Adam"}, {"Cyrus"}});
}

// A barrier leaves behind the traversal an edge's column was published under, so a pattern
// matching that edge again is turned away rather than silently matching something else
TEST_F(WithScopeTest, rejectsRematchingABoundEdge) {
    expectRejected("MATCH (a:Person)-[e:KNOWS_WELL]->(b) WITH e MATCH (c)-[e]->(d) "
                   "RETURN c.name",
                   QueryStatus::Status::PLAN_ERROR);
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
