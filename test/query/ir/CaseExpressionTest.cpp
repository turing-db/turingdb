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

// The four simpledb people holding a PhD, and the four who do not
const Rows doctors = {
    {"Remy", "doctor"}, {"Adam", "doctor"}, {"Luc", "doctor"}, {"Martina", "doctor"},
    {"Maxime", "not a doctor"}, {"Suhas", "not a doctor"},
    {"Cyrus", "not a doctor"}, {"Doruk", "not a doctor"},
};

// Only Remy and Adam carry an age in simpledb, and both are 32
const Rows agedThirtyTwo = {
    {"Remy", "thirty-two"}, {"Adam", "thirty-two"}, {"Maxime", "other"}, {"Luc", "other"},
    {"Martina", "other"}, {"Suhas", "other"}, {"Cyrus", "other"}, {"Doruk", "other"},
};

}

// CASE in both of its forms: the generic one testing a predicate per branch, and the
// simple one comparing a subject against each branch's value. A row takes the first branch
// whose condition holds, the ELSE when none does, and null when there is no ELSE.
class CaseExpressionTest : public TuringTest {
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

    // The rows in the order the query emitted them, for an ORDER BY whose whole point is
    // that order
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

TEST_F(CaseExpressionTest, selectsABranchOnAPredicate) {
    expectRows("MATCH (n:Person) "
               "RETURN n.name, CASE WHEN n.hasPhD THEN 'doctor' ELSE 'not a doctor' END",
               doctors);
}

// With no ELSE, a row matching no branch is null rather than absent from the result
TEST_F(CaseExpressionTest, leavesUnmatchedRowsNullWithoutAnElse) {
    expectRows("MATCH (n:Person) RETURN n.name, CASE WHEN n.hasPhD THEN 'doctor' END",
               {
                   {"Remy", "doctor"}, {"Adam", "doctor"}, {"Luc", "doctor"}, {"Martina", "doctor"},
                   {"Maxime", "null"}, {"Suhas", "null"}, {"Cyrus", "null"}, {"Doruk", "null"},
               });
}

// The first branch whose condition holds wins, so a PhD-holding French person reads as a
// doctor and never falls through to the branch behind it
TEST_F(CaseExpressionTest, takesTheFirstBranchThatHolds) {
    expectRows("MATCH (n:Person) "
               "RETURN n.name, CASE WHEN n.hasPhD THEN 'phd' WHEN n.isFrench THEN 'french' "
               "ELSE 'other' END",
               {
                   {"Remy", "phd"}, {"Adam", "phd"}, {"Luc", "phd"}, {"Martina", "phd"},
                   {"Maxime", "french"},
                   {"Suhas", "other"}, {"Cyrus", "other"}, {"Doruk", "other"},
               });
}

TEST_F(CaseExpressionTest, comparesASubjectAgainstEachBranch) {
    expectRows("MATCH (n:Person) "
               "RETURN n.name, CASE n.name WHEN 'Remy' THEN 1 WHEN 'Adam' THEN 2 ELSE 0 END",
               {
                   {"Remy", "1"}, {"Adam", "2"}, {"Maxime", "0"}, {"Luc", "0"},
                   {"Martina", "0"}, {"Suhas", "0"}, {"Cyrus", "0"}, {"Doruk", "0"},
               });
}

// A null subject equals nothing, so a person with no age takes the ELSE
TEST_F(CaseExpressionTest, matchesNoBranchOnANullSubject) {
    expectRows("MATCH (n:Person) "
               "RETURN n.name, CASE n.age WHEN 32 THEN 'thirty-two' ELSE 'other' END",
               agedThirtyTwo);
}

// A comparison against an absent property is null, which is not true, so the branch does
// not fire for the six people simpledb gives no age
TEST_F(CaseExpressionTest, treatsANullConditionAsNotTaken) {
    expectRows("MATCH (n:Person) "
               "RETURN n.name, CASE WHEN n.age > 30 THEN 'old' ELSE 'young' END",
               {
                   {"Remy", "old"}, {"Adam", "old"}, {"Maxime", "young"}, {"Luc", "young"},
                   {"Martina", "young"}, {"Suhas", "young"}, {"Cyrus", "young"}, {"Doruk", "young"},
               });
}

// An integer branch beside a double one widens the column the CASE returns, exactly as
// the arithmetic operators promote
TEST_F(CaseExpressionTest, promotesNumericBranchesAgainstEachOther) {
    expectRows("MATCH (n:Person) WHERE n.hasPhD RETURN n.name, CASE WHEN n.isFrench THEN 1 ELSE 0.5 END",
               {
                   {"Remy", "1.000000"}, {"Adam", "1.000000"},
                   {"Luc", "1.000000"}, {"Martina", "0.500000"},
               });
}

// The selection is a boolean expression like any other, so it holds where a predicate does
TEST_F(CaseExpressionTest, filtersOnASelection) {
    expectRows("MATCH (n:Person) WHERE CASE WHEN n.hasPhD THEN true ELSE false END RETURN n.name",
               {{"Remy"}, {"Adam"}, {"Luc"}, {"Martina"}});
}

// Every branch is a constant, so the CASE carries no rows of its own: the match is what
// says how many rows it stands for
TEST_F(CaseExpressionTest, laysAConstantSelectionOverTheMatch) {
    expectRows("MATCH (n:Person) RETURN CASE WHEN true THEN 'yes' ELSE 'no' END",
               Rows(8, {"yes"}));
}

// With no relation driving it, the selection is the single row it computes
TEST_F(CaseExpressionTest, selectsWithoutAMatch) {
    expectRows("RETURN CASE WHEN 1 < 2 THEN 'less' ELSE 'more' END", {{"less"}});
}

// A branch of null names no type, so the column is the one the other branches share
TEST_F(CaseExpressionTest, takesItsTypeFromTheBranchesThatHaveOne) {
    expectRows("MATCH (n:Person) RETURN n.name, CASE WHEN n.hasPhD THEN null ELSE 'no phd' END",
               {
                   {"Remy", "null"}, {"Adam", "null"}, {"Luc", "null"}, {"Martina", "null"},
                   {"Maxime", "no phd"}, {"Suhas", "no phd"},
                   {"Cyrus", "no phd"}, {"Doruk", "no phd"},
               });
}

// A selection groups the rows it splits, so it is a grouping key like any other expression
TEST_F(CaseExpressionTest, groupsOnASelection) {
    expectRows("MATCH (n:Person) "
               "RETURN CASE WHEN n.hasPhD THEN 'phd' ELSE 'no phd' END AS holds, count(n)",
               {{"phd", "4"}, {"no phd", "4"}});
}

// Boolean branches give a boolean column, which reads back as one
TEST_F(CaseExpressionTest, selectsBooleanBranches) {
    expectRows("MATCH (n:Person) RETURN n.name, CASE WHEN n.hasPhD THEN true ELSE false END",
               {
                   {"Remy", "true"}, {"Adam", "true"}, {"Luc", "true"}, {"Martina", "true"},
                   {"Maxime", "false"}, {"Suhas", "false"},
                   {"Cyrus", "false"}, {"Doruk", "false"},
               });
}

// The selection is an expression, so it composes: what it returns feeds the operator around it
TEST_F(CaseExpressionTest, feedsTheExpressionAroundIt) {
    expectRows("MATCH (n:Person) RETURN n.name, 1 + CASE WHEN n.hasPhD THEN 1 ELSE 2 END",
               {
                   {"Remy", "2"}, {"Adam", "2"}, {"Luc", "2"}, {"Martina", "2"},
                   {"Maxime", "3"}, {"Suhas", "3"}, {"Cyrus", "3"}, {"Doruk", "3"},
               });
}

// A tally is unsigned and a literal signed, so the two branches promote to the one integer
// column Cypher has
TEST_F(CaseExpressionTest, promotesATallyAgainstAnInteger) {
    expectRows("MATCH (n:Person) RETURN CASE WHEN true THEN count(n) ELSE 0 END", {{"8"}});
}

// A row the CASE left null is a row the tally skips, as any other absent value is
TEST_F(CaseExpressionTest, countsOnlyTheMatchedRows) {
    expectRows("MATCH (n:Person) RETURN count(CASE WHEN n.hasPhD THEN 1 END)", {{"4"}});
}

// The selection sorts the rows it splits, so it is an ORDER BY key like any other expression
TEST_F(CaseExpressionTest, ordersOnASelection) {
    expectOrderedRows("MATCH (n:Person) "
                      "RETURN n.name ORDER BY CASE WHEN n.hasPhD THEN 0 ELSE 1 END, n.name",
                      {
                          {"Adam"}, {"Luc"}, {"Martina"}, {"Remy"},
                          {"Cyrus"}, {"Doruk"}, {"Maxime"}, {"Suhas"},
                      });
}

// A selection a WITH publishes is a column the rest of the query reads by its alias
TEST_F(CaseExpressionTest, carriesASelectionThroughAWith) {
    expectRows("MATCH (n:Person) "
               "WITH CASE WHEN n.hasPhD THEN 'phd' ELSE 'no phd' END AS holds "
               "RETURN DISTINCT holds",
               {{"phd"}, {"no phd"}});
}

// The simple form has the same defaultless behaviour as the generic one
TEST_F(CaseExpressionTest, leavesTheSubjectFormNullWithoutAnElse) {
    expectRows("MATCH (n:Person) RETURN n.name, CASE n.name WHEN 'Remy' THEN 'me' END",
               {
                   {"Remy", "me"}, {"Adam", "null"}, {"Maxime", "null"}, {"Luc", "null"},
                   {"Martina", "null"}, {"Suhas", "null"}, {"Cyrus", "null"}, {"Doruk", "null"},
               });
}

// An edge property reads per matched edge, so a selection over one is evaluated where the
// hop binds it rather than over the nodes it started from
TEST_F(CaseExpressionTest, selectsOnAnEdgeProperty) {
    expectRows("MATCH (a:Person)-[e:INTERESTED_IN]->(b) WHERE a.name = 'Luc' "
               "RETURN b.name, CASE WHEN e.duration > 15 THEN 'long' ELSE 'short' END",
               {{"Animals", "long"}, {"Computers", "short"}});
}

// A branch may select an entity. The column that comes out is still one of nodes, so the
// query reads their properties on. simpledb's two Person-to-Person KNOWS_WELL hops are
// Remy's and Adam's, and both select Remy
TEST_F(CaseExpressionTest, selectsANodeBranch) {
    expectRows("MATCH (a:Person)-[:KNOWS_WELL]->(b:Person) "
               "WITH CASE WHEN a.name = 'Remy' THEN a ELSE b END AS chosen "
               "RETURN chosen.name",
               {{"Remy"}, {"Remy"}});
}

// A row matching no branch is null, which an entity column spells as an invalid ID rather
// than as an absent optional
TEST_F(CaseExpressionTest, leavesAnUnmatchedEntityRowNull) {
    expectRows("MATCH (a:Person)-[:INTERESTED_IN]->(b) "
               "WHERE a.name = 'Martina' OR a.name = 'Doruk' "
               "WITH a, CASE WHEN a.name = 'Martina' THEN b END AS chosen "
               "RETURN a.name, chosen.name",
               {{"Martina", "Cooking"}, {"Doruk", "null"}});
}

// A CASE returns one column, so branches no column type can hold together are reported
// rather than silently taking one of the two
TEST_F(CaseExpressionTest, rejectsBranchesOfUnrelatedTypes) {
    expectError("MATCH (n:Person) RETURN CASE WHEN n.hasPhD THEN 1 ELSE 'none' END",
                "must share a type");
}

// The generic form tests predicates, so a branch condition that is not one is reported
TEST_F(CaseExpressionTest, rejectsANonBooleanCondition) {
    expectError("MATCH (n:Person) RETURN CASE WHEN n.name THEN 1 ELSE 0 END",
                "must be a boolean");
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
