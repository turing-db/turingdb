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

namespace {

// The eight simpledb nodes carrying the Person label
const Rows people = {
    {"Remy"}, {"Adam"}, {"Maxime"}, {"Luc"}, {"Martina"}, {"Suhas"}, {"Cyrus"}, {"Doruk"},
};

// The ten carrying Interest, which is every simpledb node that is not a person
const Rows interests = {
    {"Computers"}, {"Eighties"}, {"Bio"}, {"Cooking"}, {"Ghosts"},
    {"Padel"}, {"Animals"}, {"Gym"}, {"Travel"}, {"JiuJitsu"},
};

}

// `WHERE n:Person` tests the label set of a matched node, and `WHERE e:KNOWS_WELL` the type
// of a matched edge. The predicate is an expression of the language like any other: it
// reads a variable already in flight and answers a boolean per row, so it composes with
// AND, OR and NOT and holds anywhere a boolean does.
class LabelPredicateTest : public TuringTest {
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

    void expectError(std::string_view query, std::string_view message) {
        RowSink sink;
        const QueryStatus status = runQuery(query, &sink);

        ASSERT_FALSE(status.isOk()) << "query: " << query;
        EXPECT_NE(status.getError().find(message), std::string::npos)
            << "query: " << query << "\nerror: " << status.getError();
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

TEST_F(LabelPredicateTest, keepsTheNodesCarryingTheLabel) {
    expectRows("MATCH (n) WHERE n:Person RETURN n.name", people);
}

// Every label of the predicate must be on the node, so the answer is the nodes whose
// label set is a superset of the two, not the union of the two labels' nodes.
TEST_F(LabelPredicateTest, requiresEveryLabelOfAChain) {
    expectRows("MATCH (n) WHERE n:Person:SoftwareEngineering RETURN n.name",
               {{"Remy"}, {"Luc"}, {"Suhas"}, {"Cyrus"}});
}

TEST_F(LabelPredicateTest, keepsNoNodeWhenTheChainIsCarriedByNone) {
    expectRows("MATCH (n) WHERE n:Person:Interest RETURN n.name", {});
}

// A label no node in the graph carries is one no node can match, so the predicate answers
// false for every row rather than failing the query.
TEST_F(LabelPredicateTest, keepsNoNodeForALabelTheGraphDoesNotHave) {
    expectRows("MATCH (n) WHERE n:Sasquatch RETURN n.name", {});
}

TEST_F(LabelPredicateTest, keepsTheNodesEitherLabelHolds) {
    expectRows("MATCH (n) WHERE n:Founder OR n:Sales RETURN n.name",
               {{"Remy"}, {"Adam"}, {"Doruk"}});
}

TEST_F(LabelPredicateTest, keepsTheNodesTheLabelDoesNotHold) {
    expectRows("MATCH (n) WHERE NOT n:Person RETURN n.name", interests);
}

TEST_F(LabelPredicateTest, testsTheLabelOfATraversedNode) {
    expectRows("MATCH (n)-->(m) WHERE n:Person AND m:Interest RETURN m.name ORDER BY m.name",
               {{"Animals"}, {"Bio"}, {"Bio"}, {"Computers"}, {"Computers"}, {"Cooking"},
                {"Cooking"}, {"Eighties"}, {"Ghosts"}, {"Gym"}, {"Gym"}, {"Gym"},
                {"JiuJitsu"}, {"Padel"}, {"Travel"}});
}

TEST_F(LabelPredicateTest, testsTheTypeOfAMatchedEdge) {
    expectRows("MATCH (n)-[e]-(m) WHERE e:KNOWS_WELL RETURN n.name",
               {{"Remy"}, {"Remy"}, {"Remy"}, {"Adam"}, {"Adam"}, {"Ghosts"}});
}

TEST_F(LabelPredicateTest, keepsNoEdgeForATypeTheGraphDoesNotHave) {
    expectRows("MATCH (n)-[e]-(m) WHERE e:ARM_WRESTLES RETURN n.name", {});
}

// The suite's success-reads-where-0: two label predicates and a property comparison over
// three variables of one pattern, every conjunct of the same WHERE.
TEST_F(LabelPredicateTest, composesWithAPropertyComparison) {
    expectRows("MATCH (a { name: \"Remy\" })-[e:KNOWS_WELL]-(m { name: \"Adam\" }) "
               "WHERE a:Person AND m:Person AND e.duration > 10 "
               "RETURN a.name",
               {{"Remy"}, {"Remy"}});
}

// The same label predicate the filters above test, read as the boolean it is: one row per
// matched node carrying whether the label holds.
TEST_F(LabelPredicateTest, projectsTheLabelTestAsABoolean) {
    expectRows("MATCH (n) WHERE n.name = 'Remy' OR n.name = 'Ghosts' "
               "RETURN n.name, n:Person ORDER BY n.name",
               {{"Ghosts", "false"}, {"Remy", "true"}});
}


// A barrier republishes the node under a declaration of its own, and the predicate below
// reads the label set of whatever that declaration is now bound to.
TEST_F(LabelPredicateTest, testsTheLabelOfANodeAWithPublished) {
    expectRows("MATCH (n) WITH n WHERE n:Person RETURN n.name", people);
}

TEST_F(LabelPredicateTest, testsTheLabelOfARenamedNode) {
    expectRows("MATCH (n) WITH n AS p WHERE p:Person RETURN p.name", people);
}

// The boolean a WITH publishes is a column like any other, so the part below reads it
// without recomputing the test.
TEST_F(LabelPredicateTest, publishesTheLabelTestAsAColumn) {
    expectRows("MATCH (n) WITH n.name AS name, n:Person AS isPerson "
               "WHERE name = 'Remy' OR name = 'Ghosts' RETURN name, isPerson ORDER BY name",
               {{"Ghosts", "false"}, {"Remy", "true"}});
}

TEST_F(LabelPredicateTest, groupsOnTheLabelTest) {
    expectRows("MATCH (n) RETURN n:Person, count(*)", {{"false", "10"}, {"true", "8"}});
}

// The key is the very expression the projection returns, so it orders the groups the
// grouping made rather than needing a column of its own.
TEST_F(LabelPredicateTest, ordersTheGroupsTheLabelTestKeyed) {
    expectRows("MATCH (n) RETURN count(*), n:Person ORDER BY n:Person",
               {{"10", "false"}, {"8", "true"}});
}

TEST_F(LabelPredicateTest, dedupsOnTheLabelTest) {
    expectRows("MATCH (n) RETURN DISTINCT n:Person", {{"false"}, {"true"}});
}

// A barrier republishes only the columns its projection names, so the type column a
// traversal published is gone below one. The test reads the type of the edge the row
// holds, which the edge ID is enough to answer.
TEST_F(LabelPredicateTest, testsTheTypeOfAnEdgeAWithPublished) {
    expectRows("MATCH (n)-[e]-(m) WITH e, n WHERE e:KNOWS_WELL RETURN n.name",
               {{"Remy"}, {"Remy"}, {"Remy"}, {"Adam"}, {"Adam"}, {"Ghosts"}});
}

TEST_F(LabelPredicateTest, testsTheTypeOfARenamedEdge) {
    expectRows("MATCH (n)-[e]-(m) WITH e AS r, n WHERE r:KNOWS_WELL RETURN n.name",
               {{"Remy"}, {"Remy"}, {"Remy"}, {"Adam"}, {"Adam"}, {"Ghosts"}});
}

// A dedup drops rows, so a type column carried across it would answer for the wrong edge.
// The three KNOWS_WELL edges of simpledb are what the count reports.
TEST_F(LabelPredicateTest, testsTheTypeOfADedupedEdge) {
    expectRows("MATCH (n)-[e]-(m) WITH DISTINCT e WHERE e:KNOWS_WELL RETURN count(*)", {{"3"}});
}

TEST_F(LabelPredicateTest, testsTheTypeOfASortedEdge) {
    expectRows("MATCH (n)-[e]-(m) WITH e, n ORDER BY n.name WHERE e:KNOWS_WELL RETURN n.name",
               {{"Remy"}, {"Remy"}, {"Remy"}, {"Adam"}, {"Adam"}, {"Ghosts"}});
}

// A grouping key is one row per group below the barrier, and the type test reads the edge
// that key holds: each of the eighteen edges keys a group of its own.
TEST_F(LabelPredicateTest, testsTheTypeOfAGroupingKeyEdge) {
    expectRows("MATCH (n)-[e]->(m) WITH e, count(*) AS hops WHERE e:KNOWS_WELL RETURN count(*)",
               {{"3"}});
}

TEST_F(LabelPredicateTest, projectsTheTypeTestOfAPublishedEdgeAsABoolean) {
    expectRows("MATCH (n)-[e]->(m) WHERE n.name = 'Remy' WITH e, m "
               "RETURN m.name, e:KNOWS_WELL ORDER BY m.name",
               {{"Adam", "true"}, {"Computers", "false"}, {"Eighties", "false"}, {"Ghosts", "false"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
