#include <gtest/gtest.h>

#include <string>
#include <string_view>

#include "NLOutputSink.h"
#include "QueryStatus.h"

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// A CALL subquery whose body is a UNION: each input row gets the rows of every branch,
// and a UNION dedups the rows of that input row alone
class SubqueryUnionBodyTest : public WriteQueryTest {
protected:
    void expectError(std::string_view query, std::string_view reason) {
        RowSink sink;
        const QueryStatus status = runQuery(query, &sink);
        ASSERT_FALSE(status.isOk()) << "query: " << query;
        EXPECT_NE(status.getError().find(reason), std::string::npos) << "error: " << status.getError();
    }
};

TEST_F(SubqueryUnionBodyTest, appendsTheBranchesOfAnUncorrelatedBody) {
    expectRows("CALL { MATCH (i:Interest) RETURN i.name AS name "
               "UNION ALL MATCH (p:Person) RETURN p.name AS name } "
               "RETURN name",
               {{"Computers"}, {"Eighties"}, {"Bio"}, {"Cooking"}, {"Ghosts"},
                {"Padel"}, {"Animals"}, {"Gym"}, {"Travel"}, {"JiuJitsu"},
                {"Remy"}, {"Adam"}, {"Maxime"}, {"Luc"}, {"Martina"},
                {"Suhas"}, {"Cyrus"}, {"Doruk"}});
}

// Gym is reached three times by the first branch and once by the second
TEST_F(SubqueryUnionBodyTest, dedupsWithinAndAcrossTheBranches) {
    expectRows("CALL { MATCH (:Person)-[:INTERESTED_IN]->(i) RETURN i.name AS name "
               "UNION MATCH (i:Interest {name: 'Gym'}) RETURN i.name AS name } "
               "RETURN name",
               {{"Computers"}, {"Eighties"}, {"Bio"}, {"Cooking"}, {"Ghosts"},
                {"Padel"}, {"Animals"}, {"Gym"}, {"Travel"}, {"JiuJitsu"}});
}

TEST_F(SubqueryUnionBodyTest, runsEveryBranchForEachImportedRow) {
    expectRows("MATCH (p:Person) "
               "CALL (p) { MATCH (p)-[:INTERESTED_IN]->(i) RETURN i.name AS z "
               "UNION ALL MATCH (p)-[:KNOWS_WELL]->(k) RETURN k.name AS z } "
               "RETURN p.name, z",
               {{"Remy", "Ghosts"}, {"Remy", "Computers"}, {"Remy", "Eighties"}, {"Remy", "Adam"},
                {"Adam", "Bio"}, {"Adam", "Cooking"}, {"Adam", "Remy"},
                {"Maxime", "Bio"}, {"Maxime", "Padel"},
                {"Luc", "Animals"}, {"Luc", "Computers"},
                {"Martina", "Cooking"},
                {"Cyrus", "Gym"}, {"Cyrus", "Travel"},
                {"Suhas", "Gym"}, {"Suhas", "JiuJitsu"},
                {"Doruk", "Gym"}});
}

TEST_F(SubqueryUnionBodyTest, importsThroughTheLeadingWithOfEachBranch) {
    expectRows("MATCH (p:Person) "
               "CALL { WITH p MATCH (p)-[:INTERESTED_IN]->(i) RETURN i.name AS z "
               "UNION ALL WITH p MATCH (p)-[:KNOWS_WELL]->(k) RETURN k.name AS z } "
               "RETURN p.name, z",
               {{"Remy", "Ghosts"}, {"Remy", "Computers"}, {"Remy", "Eighties"}, {"Remy", "Adam"},
                {"Adam", "Bio"}, {"Adam", "Cooking"}, {"Adam", "Remy"},
                {"Maxime", "Bio"}, {"Maxime", "Padel"},
                {"Luc", "Animals"}, {"Luc", "Computers"},
                {"Martina", "Cooking"},
                {"Cyrus", "Gym"}, {"Cyrus", "Travel"},
                {"Suhas", "Gym"}, {"Suhas", "JiuJitsu"},
                {"Doruk", "Gym"}});
}

// The second branch imports nothing: 2 KNOWS_WELL rows, then one Gym row for each of the
// 8 people
TEST_F(SubqueryUnionBodyTest, importsIntoTheBranchWhoseWithNamesTheVariableOnly) {
    expectRows("MATCH (p:Person) "
               "CALL { WITH p MATCH (p)-[:KNOWS_WELL]->(k) RETURN k.name AS z "
               "UNION ALL MATCH (g:Interest {name: 'Gym'}) RETURN g.name AS z } "
               "RETURN count(*)",
               {{"10"}});
}

// Both input rows are 1: a dedup spanning the input rows would drop the second one's row
TEST_F(SubqueryUnionBodyTest, dedupsTheRowsOfOneInputRowAtATime) {
    expectRows("UNWIND [1, 1] AS x CALL { RETURN 1 AS y UNION RETURN 1 AS y } RETURN x, y",
               {{"1", "1"}, {"1", "1"}});

    expectRows("UNWIND [1, 1] AS x CALL { RETURN 1 AS y UNION ALL RETURN 1 AS y } RETURN x, y",
               {{"1", "1"}, {"1", "1"}, {"1", "1"}, {"1", "1"}});
}

TEST_F(SubqueryUnionBodyTest, dedupsTwoBranchesYieldingTheSameRows) {
    expectRows("MATCH (p:Person) "
               "CALL (p) { MATCH (p)-[:INTERESTED_IN]->(i) RETURN i.name AS z "
               "UNION MATCH (p)-[:INTERESTED_IN]->(i) RETURN i.name AS z } "
               "RETURN count(*)",
               {{"15"}});
}

// A UNION ALL before the last UNION is deduped with it; one after it appends
TEST_F(SubqueryUnionBodyTest, collapsesAChainOfMixedOperators) {
    expectRows("CALL { RETURN 1 AS x UNION ALL RETURN 1 AS x UNION RETURN 2 AS x } RETURN x",
               {{"1"}, {"2"}});

    expectRows("CALL { RETURN 1 AS x UNION RETURN 1 AS x UNION ALL RETURN 1 AS x } RETURN x",
               {{"1"}, {"1"}});
}

TEST_F(SubqueryUnionBodyTest, unionsBranchesThatAggregate) {
    expectRows("MATCH (p:Person) "
               "CALL (p) { MATCH (p)-[:INTERESTED_IN]->(i) RETURN count(i) AS n "
               "UNION ALL MATCH (p)-[:KNOWS_WELL]->(k) RETURN count(k) AS n } "
               "RETURN p.name, n",
               {{"Remy", "3"}, {"Remy", "1"},
                {"Adam", "2"}, {"Adam", "1"},
                {"Maxime", "2"}, {"Maxime", "0"},
                {"Luc", "2"}, {"Luc", "0"},
                {"Martina", "1"}, {"Martina", "0"},
                {"Cyrus", "2"}, {"Cyrus", "0"},
                {"Suhas", "2"}, {"Suhas", "0"},
                {"Doruk", "1"}, {"Doruk", "0"}});
}

TEST_F(SubqueryUnionBodyTest, aggregatesTheNodesTheBranchesReturn) {
    expectRows("MATCH (p:Person) "
               "CALL (p) { MATCH (p)-[:INTERESTED_IN]->(i) RETURN i AS z "
               "UNION MATCH (p)-[:KNOWS_WELL]->(k) RETURN k AS z } "
               "RETURN p.name, count(z)",
               {{"Remy", "4"}, {"Adam", "3"}, {"Maxime", "2"}, {"Luc", "2"},
                {"Martina", "1"}, {"Cyrus", "2"}, {"Suhas", "2"}, {"Doruk", "1"}});
}

TEST_F(SubqueryUnionBodyTest, matchesFromTheNodesTheBranchesReturn) {
    expectRows("CALL { MATCH (p:Person {name: 'Remy'}) RETURN p "
               "UNION MATCH (p:Person {name: 'Adam'}) RETURN p } "
               "MATCH (p)-[:INTERESTED_IN]->(i) "
               "RETURN p.name, i.name",
               {{"Remy", "Ghosts"}, {"Remy", "Computers"}, {"Remy", "Eighties"},
                {"Adam", "Bio"}, {"Adam", "Cooking"}});
}

TEST_F(SubqueryUnionBodyTest, padsTheRowsNoBranchYieldsForUnderOptional) {
    expectRows("MATCH (p:Person) "
               "OPTIONAL CALL (p) { MATCH (p)-[:KNOWS_WELL]->(k) RETURN k.name AS z "
               "UNION MATCH (p)<-[:KNOWS_WELL]-(k:Interest) RETURN k.name AS z } "
               "RETURN p.name, z",
               {{"Remy", "Adam"}, {"Remy", "Ghosts"}, {"Adam", "Remy"},
                {"Maxime", "null"}, {"Luc", "null"}, {"Martina", "null"},
                {"Cyrus", "null"}, {"Suhas", "null"}, {"Doruk", "null"}});
}

TEST_F(SubqueryUnionBodyTest, unionsAPropertyWithANullLiteral) {
    expectRows("MATCH (p:Person {name: 'Remy'}) "
               "CALL (p) { RETURN p.name AS z UNION ALL RETURN null AS z } "
               "RETURN z",
               {{"Remy"}, {"null"}});
}

TEST_F(SubqueryUnionBodyTest, unionsAPropertyWithAConstant) {
    expectRows("MATCH (p:Person) "
               "CALL (p) { RETURN p.name AS z UNION ALL RETURN 'x' AS z } "
               "RETURN count(z)",
               {{"16"}});
}

TEST_F(SubqueryUnionBodyTest, keepsAnImportedConstantABranchReturnsAfterTheCall) {
    expectRows("WITH 5 AS c MATCH (p:Person) "
               "CALL (c) { RETURN 7 AS v UNION ALL RETURN c AS v } "
               "RETURN count(c)",
               {{"16"}});

    expectRows("WITH 5 AS c MATCH (p:Person {name: 'Remy'}) "
               "CALL (c) { RETURN c AS v UNION ALL RETURN 7 AS v } "
               "RETURN p.name, c, v",
               {{"Remy", "5", "5"}, {"Remy", "5", "7"}});
}

TEST_F(SubqueryUnionBodyTest, writesInEveryBranchForEachRow) {
    expectWriteRows("MATCH (p:Person) "
                    "CALL (p) { CREATE (a:Audit) RETURN a AS x UNION ALL CREATE (b:Audit) RETURN b AS x } "
                    "RETURN count(x)",
                    {{"16"}});

    expectRows("MATCH (a:Audit) RETURN count(a)", {{"16"}});
}

TEST_F(SubqueryUnionBodyTest, rejectsBranchesNamingTheirColumnsDifferently) {
    expectError("MATCH (p:Person) "
                "CALL (p) { MATCH (p)-->(x) RETURN x AS z UNION MATCH (p)-->(y) RETURN y AS w } "
                "RETURN count(*)",
                "same column names");
}

TEST_F(SubqueryUnionBodyTest, rejectsABranchEndingWithoutAReturn) {
    expectError("MATCH (p:Person) "
                "CALL (p) { MATCH (p)-->(x) RETURN x AS z UNION CREATE (:Audit) } "
                "RETURN count(*)",
                "must end with a RETURN clause");
}

TEST_F(SubqueryUnionBodyTest, rejectsAReturnedNameTheOuterScopeDeclares) {
    expectError("MATCH (p:Person), (q:Interest) "
                "CALL (p) { MATCH (p)-->(x) RETURN x AS q UNION MATCH (p)<--(y) RETURN y AS q } "
                "RETURN count(*)",
                "already declared in the outer scope");
}
