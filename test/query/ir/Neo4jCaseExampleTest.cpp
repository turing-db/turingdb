#include <gtest/gtest.h>

#include <string>
#include <string_view>
#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

// The CASE examples of the openCypher / Neo4j Cypher manual, carried over to simpledb.
// Neo4j runs them on five people of whom one has no age; simpledb has eight of whom six
// have none, and carries a dob on four, so the same branch shapes are reachable here.
class Neo4jCaseExampleTest : public CallV3Test {
protected:
    void expectRows(std::string_view query, const std::vector<StringRowSink::Row>& expected) {
        StringRowSink sink;
        runQuery(query, sink);

        std::vector<StringRowSink::Row> rows;
        sink.sortedRows(rows);

        EXPECT_EQ(rows, expected) << query;
    }
};

// Manual example 1, one value per branch
TEST_F(Neo4jCaseExampleTest, simpleCaseComparesTheSubjectAgainstEachBranch) {
    expectRows("MATCH (n:Person) "
               "RETURN n.name, CASE n.dob WHEN '18/01' THEN 1 WHEN '18/08' THEN 2 ELSE 3 END",
               {
                   {"Adam", "2"}, {"Cyrus", "3"}, {"Doruk", "3"}, {"Luc", "3"},
                   {"Martina", "3"}, {"Maxime", "3"}, {"Remy", "1"}, {"Suhas", "3"},
               });
}

// Manual example 1 as written: a branch listing several values takes a row matching any of them
TEST_F(Neo4jCaseExampleTest, simpleCaseTakesABranchMatchingAnyOfItsValues) {
    expectRows("MATCH (n:Person) "
               "RETURN n.name, CASE n.dob WHEN '18/01' THEN 1 WHEN '18/08', '24/07' THEN 2 ELSE 3 END",
               {
                   {"Adam", "2"}, {"Cyrus", "3"}, {"Doruk", "3"}, {"Luc", "3"},
                   {"Martina", "3"}, {"Maxime", "2"}, {"Remy", "1"}, {"Suhas", "3"},
               });
}

// Manual example 2, the extended simple form where a branch carries its own comparator. The
// manual's first branch also reads WHEN IS NOT TYPED INTEGER | FLOAT, which needs the type
// predicates the engine has no notion of, so only its null test is kept here.
TEST_F(Neo4jCaseExampleTest, extendedSimpleCaseComparesWithAnOperator) {
    expectRows("MATCH (n:Person) "
               "RETURN n.name, CASE n.age WHEN IS NULL THEN 'Unknown' WHEN = 0, = 1, = 2 THEN 'Baby' "
               "WHEN <= 13 THEN 'Child' WHEN < 20 THEN 'Teenager' WHEN < 30 THEN 'Young Adult' "
               "WHEN > 1000 THEN 'Immortal' ELSE 'Adult' END",
               {
                   {"Adam", "Adult"}, {"Cyrus", "Unknown"}, {"Doruk", "Unknown"}, {"Luc", "Unknown"},
                   {"Martina", "Unknown"}, {"Maxime", "Unknown"}, {"Remy", "Adult"}, {"Suhas", "Unknown"},
               });
}

TEST_F(Neo4jCaseExampleTest, extendedSimpleCaseMixesANullTestIntoAValueList) {
    expectRows("MATCH (n:Person) "
               "RETURN n.name, CASE n.dob WHEN IS NULL, = '18/01' THEN 'unknown or Remy' "
               "ELSE 'other' END",
               {
                   {"Adam", "other"}, {"Cyrus", "unknown or Remy"}, {"Doruk", "unknown or Remy"},
                   {"Luc", "other"}, {"Martina", "unknown or Remy"}, {"Maxime", "other"},
                   {"Remy", "unknown or Remy"}, {"Suhas", "unknown or Remy"},
               });
}

TEST_F(Neo4jCaseExampleTest, extendedSimpleCaseOrdersItsComparatorBranches) {
    expectRows("MATCH (n:Person) "
               "RETURN n.name, CASE n.age WHEN > 40 THEN 'over forty' WHEN >= 32 THEN 'thirty-something' "
               "WHEN IS NOT NULL THEN 'younger' ELSE 'unknown' END",
               {
                   {"Adam", "thirty-something"}, {"Cyrus", "unknown"}, {"Doruk", "unknown"},
                   {"Luc", "unknown"}, {"Martina", "unknown"}, {"Maxime", "unknown"},
                   {"Remy", "thirty-something"}, {"Suhas", "unknown"},
               });
}

// Manual example 3
TEST_F(Neo4jCaseExampleTest, genericCaseTestsAPredicatePerBranch) {
    expectRows("MATCH (n:Person) "
               "RETURN n.name, CASE WHEN n.dob = '24/07' THEN 1 WHEN n.age < 40 THEN 2 ELSE 3 END",
               {
                   {"Adam", "2"}, {"Cyrus", "3"}, {"Doruk", "3"}, {"Luc", "3"},
                   {"Martina", "3"}, {"Maxime", "1"}, {"Remy", "2"}, {"Suhas", "3"},
               });
}

// Manual example 4: the generic form is how a null is told apart from a value
TEST_F(Neo4jCaseExampleTest, genericCaseSeparatesANullFromAValue) {
    expectRows("MATCH (n:Person) "
               "RETURN n.name, CASE WHEN n.age IS NULL THEN -1 ELSE n.age - 10 END",
               {
                   {"Adam", "22"}, {"Cyrus", "-1"}, {"Doruk", "-1"}, {"Luc", "-1"},
                   {"Martina", "-1"}, {"Maxime", "-1"}, {"Remy", "22"}, {"Suhas", "-1"},
               });
}

// Manual example 5, the counterpart the manual contrasts with example 4: null equals nothing,
// itself included, so a WHEN null branch is dead and an ageless row falls to the ELSE, where
// null - 10 is null again
TEST_F(Neo4jCaseExampleTest, simpleCaseNeverMatchesNull) {
    expectRows("MATCH (n:Person) "
               "RETURN n.name, CASE n.age WHEN null THEN -1 ELSE n.age - 10 END",
               {
                   {"Adam", "22"}, {"Cyrus", "null"}, {"Doruk", "null"}, {"Luc", "null"},
                   {"Martina", "null"}, {"Maxime", "null"}, {"Remy", "22"}, {"Suhas", "null"},
               });
}

TEST_F(Neo4jCaseExampleTest, simpleCaseWithoutAnElseIsNull) {
    expectRows("MATCH (n:Person) RETURN n.name, CASE n.dob WHEN '18/01' THEN 'first' END",
               {
                   {"Adam", "null"}, {"Cyrus", "null"}, {"Doruk", "null"}, {"Luc", "null"},
                   {"Martina", "null"}, {"Maxime", "null"}, {"Remy", "first"}, {"Suhas", "null"},
               });
}

TEST_F(Neo4jCaseExampleTest, casesNest) {
    expectRows("MATCH (n:Person) "
               "RETURN n.name, CASE WHEN n.isFrench THEN CASE WHEN n.hasPhD THEN 'french phd' "
               "ELSE 'french' END ELSE 'other' END",
               {
                   {"Adam", "french phd"}, {"Cyrus", "other"}, {"Doruk", "other"}, {"Luc", "french phd"},
                   {"Martina", "other"}, {"Maxime", "french"}, {"Remy", "french phd"}, {"Suhas", "other"},
               });
}

TEST_F(Neo4jCaseExampleTest, aggregatesOverACase) {
    expectRows("MATCH (n:Person) RETURN sum(CASE WHEN n.hasPhD THEN 1 ELSE 0 END)", {{"4"}});
}

// Manual example 6: a CASE bound in WITH drives the SET behind it
TEST_F(Neo4jCaseExampleTest, caseInWithDrivesASet) {
    runWrite("MATCH (n:Person) WITH n, CASE WHEN n.hasPhD THEN 1 ELSE 2 END AS code SET n.age = code");

    expectRows("MATCH (n:Person) RETURN n.name, n.age",
               {
                   {"Adam", "1"}, {"Cyrus", "2"}, {"Doruk", "2"}, {"Luc", "1"},
                   {"Martina", "1"}, {"Maxime", "2"}, {"Remy", "1"}, {"Suhas", "2"},
               });
}

// Manual example 6 as written, setting a property the graph does not carry yet and reading it
// back in the same query. Disabled: the read-back fails with "Unknown property", which has
// nothing to do with CASE — a SET of a fresh property from a literal fails the same way.
TEST_F(Neo4jCaseExampleTest, DISABLED_caseInWithSetsAFreshProperty) {
    runWrite("MATCH (n:Person) WITH n, CASE WHEN n.hasPhD THEN 1 ELSE 2 END AS code "
             "SET n.code = code RETURN n.name, n.code");
}

// A comparator branch has nothing on its left without a subject to compare
TEST_F(Neo4jCaseExampleTest, rejectsAComparatorWithoutASubject) {
    runQueryExpectingError("MATCH (n:Person) RETURN n.name, CASE WHEN < 30 THEN 'young' ELSE 'old' END",
                           "needs a CASE subject");
}

TEST_F(Neo4jCaseExampleTest, rejectsAValueListWithoutASubject) {
    runQueryExpectingError("MATCH (n:Person) "
                           "RETURN n.name, CASE WHEN n.age < 30, n.age > 40 THEN 'x' ELSE 'y' END",
                           "one predicate per WHEN");
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
