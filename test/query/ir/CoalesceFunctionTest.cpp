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

// Only Remy and Adam carry an age in simpledb, and both are 32
const Rows agesOrZero = {
    {"Remy", "32"}, {"Adam", "32"}, {"Maxime", "0"}, {"Luc", "0"},
    {"Martina", "0"}, {"Suhas", "0"}, {"Cyrus", "0"}, {"Doruk", "0"},
};

// The four people simpledb gives a date of birth, and the four it does not
const Rows datesOfBirth = {
    {"Remy", "18/01"}, {"Adam", "18/08"}, {"Maxime", "24/07"}, {"Luc", "28/05"},
    {"Martina", "unknown"}, {"Suhas", "unknown"}, {"Cyrus", "unknown"}, {"Doruk", "unknown"},
};

}

// coalesce answers the first of its arguments that is not null, and null when they all are.
class CoalesceFunctionTest : public TuringTest {
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

    void expectOrderedRows(std::string_view query, const Rows& expected) {
        RowSink sink;
        const QueryStatus status = runQuery(query, &sink);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        std::string actualText;
        describeRows(sink.rows(), actualText);

        EXPECT_EQ(sink.rows(), expected) << "query: " << query << "\ngot:\n" << actualText;
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

TEST_F(CoalesceFunctionTest, fallsBackToTheDefaultOnAnAbsentProperty) {
    expectRows("MATCH (n:Person) RETURN n.name, coalesce(n.age, 0)", agesOrZero);
}

TEST_F(CoalesceFunctionTest, fallsBackToAStringDefault) {
    expectRows("MATCH (n:Person) RETURN n.name, coalesce(n.dob, 'unknown')", datesOfBirth);
}

// The first argument that has a value wins, so the second is read only where the first is
// absent and the third only where both are
TEST_F(CoalesceFunctionTest, takesTheFirstArgumentThatHasAValue) {
    expectRows("MATCH (n:Person) RETURN n.name, coalesce(n.dob, n.name, 'none')",
               {
                   {"Remy", "18/01"}, {"Adam", "18/08"}, {"Maxime", "24/07"}, {"Luc", "28/05"},
                   {"Martina", "Martina"}, {"Suhas", "Suhas"},
                   {"Cyrus", "Cyrus"}, {"Doruk", "Doruk"},
               });
}

// A name no property in the graph carries is null on every row, so it never wins
TEST_F(CoalesceFunctionTest, skipsAnAbsentPropertyName) {
    expectRows("MATCH (n:Person) RETURN n.name, coalesce(n.nickname, n.name)",
               {
                   {"Remy", "Remy"}, {"Adam", "Adam"}, {"Maxime", "Maxime"}, {"Luc", "Luc"},
                   {"Martina", "Martina"}, {"Suhas", "Suhas"},
                   {"Cyrus", "Cyrus"}, {"Doruk", "Doruk"},
               });
}

// With nothing but nulls to choose from there is no value to answer, so the answer is null
TEST_F(CoalesceFunctionTest, answersNullWhenEveryArgumentIsNull) {
    expectRows("MATCH (n:Person) WHERE n.name = 'Remy' RETURN coalesce(n.nickname, null)",
               {{"null"}});
}

TEST_F(CoalesceFunctionTest, answersItsOnlyArgument) {
    expectRows("RETURN coalesce(1)", {{"1"}});
}

// A null literal is absent on every row, so the literal behind it is what answers
TEST_F(CoalesceFunctionTest, skipsANullLiteral) {
    expectRows("RETURN coalesce(null, 42)", {{"42"}});
}

// An integer argument beside a double one widens the column, exactly as a CASE's branches
// promote against each other
TEST_F(CoalesceFunctionTest, promotesNumericArgumentsAgainstEachOther) {
    expectRows("MATCH (n:Person) WHERE n.hasPhD RETURN n.name, coalesce(n.age, 0.5)",
               {
                   {"Remy", "32.000000"}, {"Adam", "32.000000"},
                   {"Luc", "0.500000"}, {"Martina", "0.500000"},
               });
}

// Boolean arguments give a boolean column, which reads back as one. Bio, Cooking and Padel
// are the three simpledb interests that carry no isReal
TEST_F(CoalesceFunctionTest, coalescesBooleans) {
    expectRows("MATCH (n:Interest) RETURN n.name, coalesce(n.isReal, false)",
               {
                   {"Computers", "true"}, {"Eighties", "false"}, {"Ghosts", "true"},
                   {"Animals", "true"}, {"Gym", "true"}, {"Travel", "true"},
                   {"JiuJitsu", "true"}, {"Bio", "false"}, {"Cooking", "false"},
                   {"Padel", "false"},
               });
}

// The answer is a value like any other, so it holds where a predicate reads it
TEST_F(CoalesceFunctionTest, filtersOnTheAnswer) {
    expectRows("MATCH (n:Person) WHERE coalesce(n.age, 0) > 30 RETURN n.name",
               {{"Remy"}, {"Adam"}});
}

// What coalesce returns feeds the operator around it
TEST_F(CoalesceFunctionTest, feedsTheExpressionAroundIt) {
    expectRows("MATCH (n:Person) WHERE n.hasPhD RETURN n.name, coalesce(n.age, 0) + 1",
               {{"Remy", "33"}, {"Adam", "33"}, {"Luc", "1"}, {"Martina", "1"}});
}

// The answer splits the rows into groups, so it is a grouping key like any other expression
TEST_F(CoalesceFunctionTest, groupsOnTheAnswer) {
    expectRows("MATCH (n:Person) RETURN coalesce(n.age, 0) AS age, count(n)",
               {{"32", "2"}, {"0", "6"}});
}

TEST_F(CoalesceFunctionTest, ordersOnTheAnswer) {
    expectOrderedRows("MATCH (n:Person) WHERE n.hasPhD "
                      "RETURN n.name ORDER BY coalesce(n.age, 0) DESC, n.name",
                      {{"Adam"}, {"Remy"}, {"Luc"}, {"Martina"}});
}

// A row the fallback filled is a row the tally charges, unlike the absent value it replaced
TEST_F(CoalesceFunctionTest, countsEveryFilledRow) {
    expectRows("MATCH (n:Person) RETURN count(coalesce(n.age, 0))", {{"8"}});
}

// An edge property reads per matched edge, so the fallback is applied where the hop binds it
TEST_F(CoalesceFunctionTest, coalescesAnEdgeProperty) {
    expectRows("MATCH (a:Person)-[e:INTERESTED_IN]->(b) WHERE a.name = 'Adam' "
               "RETURN b.name, coalesce(e.duration, 0)",
               {{"Bio", "0"}, {"Cooking", "0"}});
}

// The answer a WITH publishes is a column the rest of the query reads by its alias
TEST_F(CoalesceFunctionTest, carriesTheAnswerThroughAWith) {
    expectRows("MATCH (n:Person) WITH coalesce(n.age, 0) AS age RETURN DISTINCT age",
               {{"32"}, {"0"}});
}

// An OPTIONAL MATCH that missed leaves a null node, which is fallen through like any other
// null. What the selection answers is still a node, so the query reads its properties on.
// Only Remy and Adam know someone well in simpledb; the other six fall back to themselves
TEST_F(CoalesceFunctionTest, coalescesNodes) {
    expectRows("MATCH (n:Person) OPTIONAL MATCH (n)-[:KNOWS_WELL]->(f) "
               "WITH n, coalesce(f, n) AS someone RETURN n.name, someone.name",
               {
                   {"Remy", "Adam"}, {"Adam", "Remy"}, {"Maxime", "Maxime"}, {"Luc", "Luc"},
                   {"Martina", "Martina"}, {"Suhas", "Suhas"}, {"Cyrus", "Cyrus"},
                   {"Doruk", "Doruk"},
               });
}

// Edges coalesce as nodes do, and what comes out is still an edge the query reads a
// property from. Remy's KNOWS_WELL hop wins on each of his three interests; Martina has
// no such hop, so her row falls back to the interest one
TEST_F(CoalesceFunctionTest, coalescesEdges) {
    expectRows("MATCH (n:Person) WHERE n.name = 'Remy' OR n.name = 'Martina' "
               "OPTIONAL MATCH (n)-[k:KNOWS_WELL]->() "
               "OPTIONAL MATCH (n)-[i:INTERESTED_IN]->() "
               "WITH coalesce(k, i) AS link RETURN link.name",
               {
                   {"Remy -> Adam"}, {"Remy -> Adam"}, {"Remy -> Adam"},
                   {"Martina -> Cooking"},
               });
}

// With nothing but hops that missed to choose from, the answer is the null an entity
// column spells as an invalid ID
TEST_F(CoalesceFunctionTest, answersANullEntityWhenNoArgumentHasOne) {
    expectRows("MATCH (n:Person) WHERE n.name = 'Martina' "
               "OPTIONAL MATCH (n)-[k:KNOWS_WELL]->() "
               "WITH coalesce(k, null) AS link RETURN link.name",
               {{"null"}});
}

// A node and an edge are no more one column than a node and a number, so neither pair is
// held together
TEST_F(CoalesceFunctionTest, rejectsANodeBesideAnEdge) {
    expectError("MATCH (n:Person)-[e]->() RETURN coalesce(n, e)", "must share a type");
    expectError("MATCH (n:Person) RETURN coalesce(n, 1)", "must share a type");
}

// A reduction answers one row, and the fallback stands in when that row is null. None of
// the four people without a PhD carries an age, so their extremum reduces nothing
TEST_F(CoalesceFunctionTest, coalescesAReduction) {
    expectRows("MATCH (n:Person) RETURN coalesce(max(n.age), 0)", {{"32"}});
    expectRows("MATCH (n:Person) WHERE NOT n.hasPhD RETURN coalesce(max(n.age), 0)", {{"0"}});
    expectRows("MATCH (n:Person) RETURN coalesce(max(n.shoeSize), 0)", {{"0"}});
}

// coalesce answers one column, so arguments no column type can hold together are reported
TEST_F(CoalesceFunctionTest, rejectsArgumentsOfUnrelatedTypes) {
    expectError("MATCH (n:Person) RETURN coalesce(n.age, 'none')", "must share a type");
}

// There is no first non-null argument to answer with when there is no argument at all
TEST_F(CoalesceFunctionTest, rejectsACallWithNoArgument) {
    expectError("RETURN coalesce()", "Invalid arguments for function 'coalesce'");
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
