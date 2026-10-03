#include <gtest/gtest.h>

#include <string>
#include <string_view>

#include "NLOutputSink.h"
#include "QueryStatus.h"

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// An EXISTS whose body is a UNION holds for a row when any branch produces a row for it
class ExistsUnionBodyTest : public WriteQueryTest {
protected:
    void expectError(std::string_view query, std::string_view reason) {
        RowSink sink;
        const QueryStatus status = runQuery(query, &sink);
        ASSERT_FALSE(status.isOk()) << "query: " << query;
        EXPECT_NE(status.getError().find(reason), std::string::npos) << "error: " << status.getError();
    }
};

// Remy and Adam know each other well, and Ghosts knows Remy well
TEST_F(ExistsUnionBodyTest, holdsForARowAnyBranchMatches) {
    expectRows("MATCH (p:Person) "
               "WHERE EXISTS { MATCH (p)-[:KNOWS_WELL]->() UNION MATCH (p)<-[:KNOWS_WELL]-(:Interest) } "
               "RETURN p.name",
               {{"Remy"}, {"Adam"}});
}

TEST_F(ExistsUnionBodyTest, holdsForTheRowsOfEveryBranch) {
    expectRows("MATCH (p:Person) "
               "WHERE EXISTS { MATCH (p)-[:INTERESTED_IN]->(:Interest {name: 'Gym'}) "
               "UNION ALL MATCH (p)-[:INTERESTED_IN]->(:Interest {name: 'Bio'}) } "
               "RETURN p.name",
               {{"Cyrus"}, {"Suhas"}, {"Doruk"}, {"Adam"}, {"Maxime"}});
}

TEST_F(ExistsUnionBodyTest, answersEveryRowAsAValue) {
    expectRows("MATCH (p:Person) "
               "RETURN p.name, EXISTS { MATCH (p)-[:KNOWS_WELL]->() "
               "UNION MATCH (p)-[:INTERESTED_IN]->(:Interest {name: 'Gym'}) } AS x",
               {{"Remy", "true"}, {"Adam", "true"}, {"Maxime", "false"}, {"Luc", "false"},
                {"Martina", "false"}, {"Cyrus", "true"}, {"Suhas", "true"}, {"Doruk", "true"}});
}

// The SKIP empties the first branch for everyone but Remy, the one person with 3 interests
TEST_F(ExistsUnionBodyTest, cutsEachBranchOnItsOwnReturn) {
    expectRows("MATCH (p:Person) "
               "WHERE EXISTS { MATCH (p)-[:INTERESTED_IN]->(i) RETURN i SKIP 2 "
               "UNION MATCH (p)-[:INTERESTED_IN]->(k:Interest {name: 'Cooking'}) RETURN k AS i } "
               "RETURN p.name",
               {{"Remy"}, {"Adam"}, {"Martina"}});
}

TEST_F(ExistsUnionBodyTest, holdsThroughABranchReadingNothingOfTheRow) {
    expectRows("MATCH (p:Person) "
               "WHERE EXISTS { MATCH (p)-[:KNOWS_WELL]->() UNION MATCH (n:Interest {name: 'Padel'}) } "
               "RETURN count(p)",
               {{"8"}});
}

TEST_F(ExistsUnionBodyTest, rejectsABranchThatWrites) {
    expectError("MATCH (p:Person) "
                "WHERE EXISTS { MATCH (p)-->() UNION CREATE (:Audit) } "
                "RETURN p.name",
                "read-only");
}

TEST_F(ExistsUnionBodyTest, rejectsBranchesNamingTheirColumnsDifferently) {
    expectError("MATCH (p:Person) "
                "WHERE EXISTS { MATCH (p)-->(x) RETURN x UNION MATCH (p)<--(y) RETURN y } "
                "RETURN p.name",
                "same column names");
}

TEST_F(ExistsUnionBodyTest, rejectsABranchEndingWithoutAReturnBesideOneWithIt) {
    expectError("MATCH (p:Person) "
                "WHERE EXISTS { MATCH (p)-->(x) RETURN x UNION MATCH (p)<--(y) } "
                "RETURN p.name",
                "must end with a RETURN clause");
}
