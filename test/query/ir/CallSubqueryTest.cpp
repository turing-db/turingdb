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

// CALL { ... } subqueries whose body carries the rows it is given through to its RETURN:
// the scope clause and the importing WITH, what the body may read and what it hands back
class CallSubqueryTest : public TuringTest {
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

    void expectRejected(std::string_view query, std::string_view message) {
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

// The 8 Persons hold 15 INTERESTED_IN edges between them
TEST_F(CallSubqueryTest, scopeClauseImportsTheRowsInFlight) {
    expectRows("MATCH (p:Person) "
               "CALL (p) { MATCH (p)-[:INTERESTED_IN]->(i) RETURN i } "
               "RETURN count(i)",
               {{"15"}});
}

// Each Person comes back once per row the body yielded for it: Remy has 4 out-edges,
// Adam 3, Martina and Doruk 1
TEST_F(CallSubqueryTest, returnedColumnsJoinTheirInputRow) {
    expectRows("MATCH (p:Person) "
               "CALL (p) { MATCH (p)-->(x) RETURN x } "
               "RETURN p.name, count(x)",
               {{"Remy", "4"},
                {"Adam", "3"},
                {"Maxime", "2"},
                {"Luc", "2"},
                {"Martina", "1"},
                {"Suhas", "2"},
                {"Cyrus", "2"},
                {"Doruk", "1"}});
}

// Only Remy and Adam know anybody well, so the six other Persons are dropped
TEST_F(CallSubqueryTest, dropsTheRowsTheBodyYieldsNothingFor) {
    expectRows("MATCH (p:Person) "
               "CALL (p) { MATCH (p)-[:KNOWS_WELL]->(k) RETURN k } "
               "RETURN p.name, k.name",
               {{"Remy", "Adam"}, {"Adam", "Remy"}});
}

// The interest is not imported, and is still in scope after the CALL beside what the body
// returned: each interest tallies the out-degree of every Person holding it
TEST_F(CallSubqueryTest, keepsTheColumnsTheBodyDoesNotImport) {
    expectRows("MATCH (p:Person)-[:INTERESTED_IN]->(i) "
               "CALL (p) { MATCH (p)-->(x) RETURN x } "
               "RETURN i.name, count(x)",
               {{"Ghosts", "4"},
                {"Computers", "6"},
                {"Eighties", "4"},
                {"Bio", "5"},
                {"Cooking", "4"},
                {"Padel", "2"},
                {"Animals", "2"},
                {"Gym", "5"},
                {"Travel", "2"},
                {"JiuJitsu", "2"}});
}

TEST_F(CallSubqueryTest, importingWithNamesTheImports) {
    expectRows("MATCH (p:Person) "
               "CALL { WITH p MATCH (p)-[:INTERESTED_IN]->(i) RETURN i.name AS interest } "
               "RETURN p.name, interest",
               {{"Remy", "Ghosts"},
                {"Remy", "Computers"},
                {"Remy", "Eighties"},
                {"Adam", "Bio"},
                {"Adam", "Cooking"},
                {"Maxime", "Bio"},
                {"Maxime", "Padel"},
                {"Luc", "Animals"},
                {"Luc", "Computers"},
                {"Martina", "Cooking"},
                {"Suhas", "Gym"},
                {"Suhas", "JiuJitsu"},
                {"Cyrus", "Gym"},
                {"Cyrus", "Travel"},
                {"Doruk", "Gym"}});
}

// A barrier inside the body keeps the input rows beside what it publishes: each Person
// tallies the Persons sharing each of their interests, themselves included
TEST_F(CallSubqueryTest, carriesTheInputsPastABarrierInTheBody) {
    expectRows("MATCH (p:Person) "
               "CALL (p) { MATCH (p)-[:INTERESTED_IN]->(i) WITH i MATCH (i)<-[:INTERESTED_IN]-(q:Person) RETURN q } "
               "RETURN p.name, count(q)",
               {{"Remy", "4"},
                {"Adam", "4"},
                {"Maxime", "3"},
                {"Luc", "3"},
                {"Martina", "2"},
                {"Suhas", "4"},
                {"Cyrus", "4"},
                {"Doruk", "3"}});
}

TEST_F(CallSubqueryTest, nestsASubqueryInTheBody) {
    expectRows("MATCH (p:Person) "
               "CALL (p) { "
               "  MATCH (p)-[:INTERESTED_IN]->(i) "
               "  CALL (i) { MATCH (i)<-[:INTERESTED_IN]-(q:Person) RETURN q } "
               "  RETURN q "
               "} "
               "RETURN p.name, count(q)",
               {{"Remy", "4"},
                {"Adam", "4"},
                {"Maxime", "3"},
                {"Luc", "3"},
                {"Martina", "2"},
                {"Suhas", "4"},
                {"Cyrus", "4"},
                {"Doruk", "3"}});
}

// A body importing nothing yields the same rows for every input row: 8 Persons times 10
// Interests
TEST_F(CallSubqueryTest, crossesAnUncorrelatedBodyWithTheRowsInFlight) {
    expectRows("MATCH (p:Person) "
               "CALL () { MATCH (i:Interest) RETURN i } "
               "RETURN count(i)",
               {{"80"}});
}

TEST_F(CallSubqueryTest, aBodyOpeningOnNoWithImportsNothing) {
    expectRows("MATCH (p:Person) "
               "CALL { MATCH (i:Interest) RETURN i } "
               "RETURN count(i)",
               {{"80"}});
}

TEST_F(CallSubqueryTest, readsAnImportedPropertyAfterAHop) {
    expectRows("MATCH (p:Person) "
               "CALL (p) { MATCH (p)-[:KNOWS_WELL]->(k) RETURN p.name AS knower, k.name AS known } "
               "RETURN knower, known",
               {{"Remy", "Adam"}, {"Adam", "Remy"}});
}

TEST_F(CallSubqueryTest, rejectsAVariableTheBodyDidNotImport) {
    expectRejected("MATCH (p:Person)-[:INTERESTED_IN]->(i) "
                   "CALL (p) { MATCH (p)-->(x) RETURN i.name AS interest } "
                   "RETURN interest",
                   "Variable 'i' not found");
}

TEST_F(CallSubqueryTest, rejectsReturningANameTheOuterScopeHolds) {
    expectRejected("MATCH (p:Person)-->(y) "
                   "CALL (p) { MATCH (p)-->(x) RETURN x AS y } "
                   "RETURN y",
                   "already declared in the outer scope");
}

TEST_F(CallSubqueryTest, hidesTheBodysOwnVariables) {
    expectRejected("MATCH (p:Person) "
                   "CALL (p) { MATCH (p)-->(x)-->(y) RETURN y } "
                   "RETURN x",
                   "Variable 'x' not found");
}

TEST_F(CallSubqueryTest, rejectsAnImportingWithThatFilters) {
    expectRejected("MATCH (p:Person) "
                   "CALL { WITH p WHERE p.age > 3 MATCH (p)-->(x) RETURN x } "
                   "RETURN x",
                   "An importing WITH holds plain variable references only");
}

TEST_F(CallSubqueryTest, rejectsAnImportingWithOfAnExpression) {
    expectRejected("MATCH (p:Person) "
                   "CALL { WITH p.name AS name RETURN name } "
                   "RETURN name",
                   "An importing WITH holds plain variable references only");
}

TEST_F(CallSubqueryTest, rejectsAnImportingWithStar) {
    expectRejected("MATCH (p:Person) "
                   "CALL { WITH * MATCH (p)-->(x) RETURN x } "
                   "RETURN x",
                   "WITH * names none of them");
}

TEST_F(CallSubqueryTest, rejectsAnImportOfAnUnknownVariable) {
    expectRejected("MATCH (p:Person) "
                   "CALL (q) { RETURN 1 AS one } "
                   "RETURN one",
                   "Variable 'q' not found");
}

TEST_F(CallSubqueryTest, rejectsAReadingBodyWithoutAReturn) {
    expectRejected("MATCH (p:Person) "
                   "CALL (p) { MATCH (p)-->(x) } "
                   "RETURN p",
                   "Return statement is missing");
}

// A WITH inside the body does not drop what the scope clause imported: the barrier
// publishes i, and p is still readable below it
TEST_F(CallSubqueryTest, anImportSurvivesABarrierInTheBody) {
    expectRows("MATCH (p:Person) "
               "CALL (p) { MATCH (p)-[:INTERESTED_IN]->(i) WITH i RETURN p.name AS person, i.name AS interest } "
               "RETURN person, interest",
               {{"Remy", "Ghosts"},
                {"Remy", "Computers"},
                {"Remy", "Eighties"},
                {"Adam", "Bio"},
                {"Adam", "Cooking"},
                {"Maxime", "Bio"},
                {"Maxime", "Padel"},
                {"Luc", "Animals"},
                {"Luc", "Computers"},
                {"Martina", "Cooking"},
                {"Suhas", "Gym"},
                {"Suhas", "JiuJitsu"},
                {"Cyrus", "Gym"},
                {"Cyrus", "Travel"},
                {"Doruk", "Gym"}});
}

// The barrier the Neo4j manual opens its import example with: one that binds a value and
// names nothing of the rows in flight
TEST_F(CallSubqueryTest, anImportSurvivesABarrierBindingAValue) {
    expectRows("MATCH (p:Person {name: 'Remy'}) "
               "CALL (p) { WITH 1 AS one MATCH (p)-[:INTERESTED_IN]->(i) RETURN p.name AS person, i.name AS interest, one } "
               "RETURN person, interest, one",
               {{"Remy", "Ghosts", "1"}, {"Remy", "Computers", "1"}, {"Remy", "Eighties", "1"}});
}

TEST_F(CallSubqueryTest, anImportSurvivesAChainOfBarriers) {
    expectRows("MATCH (p:Person {name: 'Remy'}) "
               "CALL (p) { "
               "  MATCH (p)-[:INTERESTED_IN]->(i) WITH i WITH i.name AS interest "
               "  RETURN p.name AS person, interest "
               "} "
               "RETURN person, interest",
               {{"Remy", "Ghosts"}, {"Remy", "Computers"}, {"Remy", "Eighties"}});
}

// A barrier projecting the import publishes it as its own item, which is no redeclaration
TEST_F(CallSubqueryTest, aBarrierMayProjectTheImportItself) {
    expectRows("MATCH (p:Person {name: 'Remy'}) "
               "CALL (p) { MATCH (p)-[:INTERESTED_IN]->(i) WITH p, i RETURN p.name AS person, i.name AS interest } "
               "RETURN person, interest",
               {{"Remy", "Ghosts"}, {"Remy", "Computers"}, {"Remy", "Eighties"}});
}

TEST_F(CallSubqueryTest, rejectsABarrierRedeclaringAnImport) {
    expectRejected("MATCH (p:Person) "
                   "CALL (p) { WITH 'Remy' AS p RETURN p AS named } "
                   "RETURN named",
                   "a clause of the subquery cannot declare it again");
}

// The deprecated form imports through an ordinary projection, so an ordinary WITH below
// it descopes the name as it would anywhere else
TEST_F(CallSubqueryTest, aBarrierDropsWhatALeadingWithImported) {
    expectRejected("MATCH (p:Person) "
                   "CALL { WITH p MATCH (p)-[:INTERESTED_IN]->(i) WITH i RETURN p.name AS person } "
                   "RETURN person",
                   "Variable 'p' not found");
}
