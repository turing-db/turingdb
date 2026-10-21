#include <gtest/gtest.h>

#include <string>
#include <string_view>

#include "NLOutputSink.h"
#include "QueryStatus.h"

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// COLLECT { ... }: one list per row in flight, the values of the one column the body returns
// for it. Remy is interested in Ghosts, Computers and Eighties, Adam in Bio and Cooking,
// Maxime in Bio and Padel, Luc in Animals and Computers, Martina in Cooking, Suhas in Gym
// and JiuJitsu, Cyrus in Gym and Travel and Doruk in Gym. KNOWS_WELL leaves Remy for Adam and
// Adam for Remy.
class CollectSubqueryTest : public WriteQueryTest {
protected:
    void expectError(std::string_view query, std::string_view message) {
        RowSink sink;
        const QueryStatus status = runQuery(query, &sink);

        ASSERT_FALSE(status.isOk()) << "query: " << query << " was expected to fail";
        EXPECT_NE(status.getError().find(message), std::string::npos)
            << "query: " << query << "\nerror: " << status.getError();
    }
};

TEST_F(CollectSubqueryTest, collectsWhatTheBodyReturnsForEachRow) {
    expectRows("MATCH (p:Person) RETURN p.name, COLLECT { MATCH (p)-[:INTERESTED_IN]->(i) RETURN i.name }",
               {{"Remy", "[Ghosts, Computers, Eighties]"},
                {"Adam", "[Bio, Cooking]"},
                {"Maxime", "[Bio, Padel]"},
                {"Luc", "[Animals, Computers]"},
                {"Martina", "[Cooking]"},
                {"Suhas", "[Gym, JiuJitsu]"},
                {"Cyrus", "[Gym, Travel]"},
                {"Doruk", "[Gym]"}});
}

TEST_F(CollectSubqueryTest, collectsTheEmptyListForARowTheBodyReturnsNothingFor) {
    expectRows("MATCH (p:Person) RETURN p.name, COLLECT { MATCH (p)-[:KNOWS_WELL]->(k) RETURN k.name }",
               {{"Remy", "[Adam]"}, {"Adam", "[Remy]"}, {"Maxime", "[]"}, {"Luc", "[]"},
                {"Martina", "[]"}, {"Suhas", "[]"}, {"Cyrus", "[]"}, {"Doruk", "[]"}});
}

TEST_F(CollectSubqueryTest, isCaseInsensitive) {
    expectRows("MATCH (p:Person {name: 'Adam'}) RETURN collect { MATCH (p)-[:INTERESTED_IN]->(i) RETURN i.name }",
               {{"[Bio, Cooking]"}});
}

TEST_F(CollectSubqueryTest, filtersTheBodyRowsWithAWhere) {
    expectRows("MATCH (p:Person) "
               "RETURN p.name, COLLECT { MATCH (p)-[:INTERESTED_IN]->(i) WHERE i.isReal = true RETURN i.name }",
               {{"Remy", "[Ghosts, Computers]"}, {"Adam", "[]"}, {"Maxime", "[]"}, {"Luc", "[Animals, Computers]"},
                {"Martina", "[]"}, {"Suhas", "[Gym, JiuJitsu]"}, {"Cyrus", "[Gym, Travel]"}, {"Doruk", "[Gym]"}});
}

TEST_F(CollectSubqueryTest, filtersTheRowsInFlightOnTheList) {
    expectRows("MATCH (p:Person) WHERE 'Gym' IN COLLECT { MATCH (p)-[:INTERESTED_IN]->(i) RETURN i.name } "
               "RETURN p.name",
               {{"Suhas"}, {"Cyrus"}, {"Doruk"}});
}

TEST_F(CollectSubqueryTest, keepsTheNullsTheBodyReturns) {
    expectRows("MATCH (p:Person {name: 'Remy'}) RETURN COLLECT { MATCH (p)-[e:INTERESTED_IN]->() RETURN e.duration }",
               {{"[20, null, 20]"}});
}

TEST_F(CollectSubqueryTest, readsThePropertiesOfTheRowItCollectsFor) {
    expectRows("MATCH (p:Person) WHERE p.name = 'Adam' OR p.name = 'Doruk' "
               "RETURN p.name, COLLECT { MATCH (p)-[:INTERESTED_IN]->(i) RETURN p.name + ' likes ' + i.name }",
               {{"Adam", "[Adam likes Bio, Adam likes Cooking]"}, {"Doruk", "[Doruk likes Gym]"}});
}

TEST_F(CollectSubqueryTest, collectsAConstantOncePerBodyRow) {
    expectRows("MATCH (p:Person) WHERE p.name = 'Remy' OR p.name = 'Martina' "
               "RETURN p.name, COLLECT { MATCH (p)-[:INTERESTED_IN]->(i) RETURN 1 }",
               {{"Remy", "[1, 1, 1]"}, {"Martina", "[1]"}});
}

TEST_F(CollectSubqueryTest, collectsTheNodesTheBodyReturns) {
    expectRows("MATCH (p:Person {name: 'Martina'}) RETURN COLLECT { MATCH (p)-[:INTERESTED_IN]->(i) RETURN i }",
               {{"[5]"}});
}

TEST_F(CollectSubqueryTest, collectsTheListsTheBodyReturns) {
    expectRows("MATCH (p:Person {name: 'Adam'}) "
               "RETURN COLLECT { MATCH (p)-[:INTERESTED_IN]->(i) RETURN [i.name, p.name] }",
               {{"[[Bio, Adam], [Cooking, Adam]]"}});
}

TEST_F(CollectSubqueryTest, collectsOverNoRowInFlight) {
    expectRows("RETURN COLLECT { MATCH (a:Person)-[:KNOWS_WELL]->(b:Person) RETURN b.name }", {{"[Adam, Remy]"}});
    expectRows("RETURN COLLECT { MATCH (p:Person {name: 'Nobody'}) RETURN p.name }", {{"[]"}});
}

TEST_F(CollectSubqueryTest, anUncorrelatedBodyCollectsTheSameForEveryRow) {
    expectRows("MATCH (p:Person) WHERE p.name = 'Remy' OR p.name = 'Doruk' "
               "RETURN p.name, COLLECT { MATCH (a)-[:KNOWS_WELL]->(b:Person) RETURN a.name }",
               {{"Remy", "[Remy, Adam, Ghosts]"}, {"Doruk", "[Remy, Adam, Ghosts]"}});
}

TEST_F(CollectSubqueryTest, collectsABodyReturningOnlyAConstantOfTheScope) {
    expectRows("WITH 5 AS k RETURN COLLECT { RETURN k }", {{"[5]"}});
    expectRows("WITH 5 AS k RETURN COLLECT { WITH k RETURN k + 1 }", {{"[6]"}});
}

TEST_F(CollectSubqueryTest, aSortingBodyCollectsInItsOrder) {
    expectRows("MATCH (p:Person) "
               "RETURN p.name, COLLECT { MATCH (p)-[:INTERESTED_IN]->(i) RETURN i.name AS name ORDER BY name }",
               {{"Remy", "[Computers, Eighties, Ghosts]"},
                {"Adam", "[Bio, Cooking]"},
                {"Maxime", "[Bio, Padel]"},
                {"Luc", "[Animals, Computers]"},
                {"Martina", "[Cooking]"},
                {"Suhas", "[Gym, JiuJitsu]"},
                {"Cyrus", "[Gym, Travel]"},
                {"Doruk", "[Gym]"}});

    expectRows("MATCH (p:Person) WHERE p.name = 'Remy' OR p.name = 'Cyrus' "
               "RETURN p.name, COLLECT { MATCH (p)-[:INTERESTED_IN]->(i) RETURN i.name AS name ORDER BY name DESC }",
               {{"Remy", "[Ghosts, Eighties, Computers]"}, {"Cyrus", "[Travel, Gym]"}});
}

// The bare LIMIT streams its budget; the one under an ORDER BY is fused into the sort's
// top-K. Either budgets each row alone.
TEST_F(CollectSubqueryTest, budgetsALimitInTheBodyPerRow) {
    expectRows("MATCH (p:Person) WHERE p.name = 'Remy' OR p.name = 'Martina' "
               "RETURN p.name, COLLECT { MATCH (p)-[:INTERESTED_IN]->(i) RETURN i.name LIMIT 2 }",
               {{"Remy", "[Ghosts, Computers]"}, {"Martina", "[Cooking]"}});

    expectRows("MATCH (p:Person) WHERE p.name = 'Remy' OR p.name = 'Martina' "
               "RETURN p.name, COLLECT { MATCH (p)-[:INTERESTED_IN]->(i) RETURN i.name AS name ORDER BY name LIMIT 2 }",
               {{"Remy", "[Computers, Eighties]"}, {"Martina", "[Cooking]"}});
}

TEST_F(CollectSubqueryTest, aSkippingBodyCollectsTheRowsPastTheSkip) {
    expectRows("MATCH (p:Person) WHERE p.name = 'Remy' OR p.name = 'Martina' "
               "RETURN p.name, COLLECT { MATCH (p)-[:INTERESTED_IN]->(i) RETURN i.name AS name ORDER BY name SKIP 1 }",
               {{"Remy", "[Eighties, Ghosts]"}, {"Martina", "[]"}});
}

TEST_F(CollectSubqueryTest, aDedupingBodyCollectsDistinctValues) {
    expectRows("MATCH (p:Person) WHERE p.name = 'Remy' OR p.name = 'Adam' "
               "RETURN p.name, COLLECT { MATCH (p)-[:INTERESTED_IN]->(i) RETURN DISTINCT p.name }",
               {{"Remy", "[Remy]"}, {"Adam", "[Adam]"}});
}

TEST_F(CollectSubqueryTest, aKeylessAggregateCollectsItsOneRow) {
    expectRows("MATCH (p:Person) WHERE p.name = 'Remy' OR p.name = 'Maxime' "
               "RETURN p.name, COLLECT { MATCH (p)-[:KNOWS_WELL]->(k) RETURN count(k) }",
               {{"Remy", "[1]"}, {"Maxime", "[0]"}});
}

TEST_F(CollectSubqueryTest, aBarrierInTheBodyKeepsTheRowItCollectsFor) {
    expectRows("MATCH (p:Person) "
               "RETURN p.name, COLLECT { MATCH (p)-[:INTERESTED_IN]->(i) WITH i WHERE i.isReal = true "
               "                         MATCH (p)-[:KNOWS_WELL]->(k) RETURN i.name + '/' + k.name }",
               {{"Remy", "[Ghosts/Adam, Computers/Adam]"}, {"Adam", "[]"}, {"Maxime", "[]"}, {"Luc", "[]"},
                {"Martina", "[]"}, {"Suhas", "[]"}, {"Cyrus", "[]"}, {"Doruk", "[]"}});
}

TEST_F(CollectSubqueryTest, aConstantBarrierInTheBodyKeepsTheRowItCollectsFor) {
    expectRows("MATCH (p:Person) "
               "RETURN p.name, COLLECT { WITH 20 AS wanted MATCH (p)-[e:INTERESTED_IN]->(i) "
               "                         WHERE e.duration = wanted RETURN i.name }",
               {{"Remy", "[Ghosts, Eighties]"}, {"Adam", "[]"}, {"Maxime", "[]"}, {"Luc", "[Animals]"},
                {"Martina", "[]"}, {"Suhas", "[]"}, {"Cyrus", "[]"}, {"Doruk", "[]"}});
}

TEST_F(CollectSubqueryTest, aUnionCollectsTheDistinctValuesOfItsBranches) {
    expectRows("MATCH (p:Person) WHERE p.name = 'Remy' OR p.name = 'Adam' OR p.name = 'Doruk' "
               "RETURN p.name, COLLECT { MATCH (p)-[:INTERESTED_IN]->(i) RETURN i.name AS name "
               "                         UNION MATCH (p)-[:INTERESTED_IN]->(i) RETURN i.name AS name "
               "                         UNION MATCH (p)-[:KNOWS_WELL]->(k) RETURN k.name AS name }",
               {{"Remy", "[Ghosts, Computers, Eighties, Adam]"},
                {"Adam", "[Bio, Cooking, Remy]"},
                {"Doruk", "[Gym]"}});
}

TEST_F(CollectSubqueryTest, aUnionAllCollectsEveryValueOfItsBranchesInBranchOrder) {
    expectRows("MATCH (p:Person) WHERE p.name = 'Remy' OR p.name = 'Doruk' "
               "RETURN p.name, COLLECT { MATCH (p)-[:KNOWS_WELL]->(k) RETURN k.name AS name "
               "                         UNION ALL MATCH (p)-[:INTERESTED_IN]->(i) RETURN i.name AS name "
               "                         UNION ALL MATCH (p)-[:KNOWS_WELL]->(k) RETURN k.name AS name }",
               {{"Remy", "[Adam, Ghosts, Computers, Eighties, Adam]"}, {"Doruk", "[Gym]"}});
}

// UNION is left associative: A UNION B UNION ALL C is distinct(A ++ B) ++ C
TEST_F(CollectSubqueryTest, aMixedUnionDedupsUpToItsLastUnion) {
    expectRows("MATCH (p:Person {name: 'Doruk'}) "
               "RETURN COLLECT { MATCH (p)-[:INTERESTED_IN]->(i) RETURN i.name AS name "
               "                 UNION MATCH (p)-[:INTERESTED_IN]->(i) RETURN i.name AS name "
               "                 UNION ALL MATCH (p)-[:INTERESTED_IN]->(i) RETURN i.name AS name }",
               {{"[Gym, Gym]"}});
}

TEST_F(CollectSubqueryTest, aPerRowBodyStandsBesideOtherColumns) {
    expectRows("MATCH (p:Person) WHERE p.name = 'Remy' OR p.name = 'Adam' OR p.name = 'Luc' "
               "RETURN p.name, COLLECT { MATCH (p)-[:INTERESTED_IN]->(i) RETURN i.name AS name ORDER BY name LIMIT 1 }, "
               "       COLLECT { MATCH (p)-[:INTERESTED_IN]->(i) RETURN i.name }",
               {{"Remy", "[Computers]", "[Ghosts, Computers, Eighties]"},
                {"Adam", "[Bio]", "[Bio, Cooking]"},
                {"Luc", "[Animals]", "[Animals, Computers]"}});
}

// The scan meets Remy, Adam and Maxime first
TEST_F(CollectSubqueryTest, anOuterLimitStopsThePerRowBody) {
    expectRows("MATCH (p:Person) RETURN p.name, COLLECT { MATCH (p)-[:INTERESTED_IN]->(i) RETURN i.name LIMIT 1 } LIMIT 3",
               {{"Remy", "[Ghosts]"}, {"Adam", "[Bio]"}, {"Maxime", "[Bio]"}});
}

TEST_F(CollectSubqueryTest, nestsACollectInTheBody) {
    expectRows("MATCH (p:Person {name: 'Remy'}) "
               "RETURN COLLECT { MATCH (p)-[:KNOWS_WELL]->(k) "
               "                 RETURN COLLECT { MATCH (k)-[:INTERESTED_IN]->(i) RETURN i.name } }",
               {{"[[Bio, Cooking]]"}});
}

TEST_F(CollectSubqueryTest, nestsACountInTheBody) {
    expectRows("MATCH (p:Person {name: 'Adam'}) "
               "RETURN COLLECT { MATCH (p)-[:INTERESTED_IN]->(i) RETURN COUNT { (i)<-[:INTERESTED_IN]-() } }",
               {{"[2, 2]"}});
}

TEST_F(CollectSubqueryTest, sizesTheList) {
    expectRows("MATCH (p:Person) RETURN p.name, size(COLLECT { MATCH (p)-[:INTERESTED_IN]->(i) RETURN i.name })",
               {{"Remy", "3"}, {"Adam", "2"}, {"Maxime", "2"}, {"Luc", "2"},
                {"Martina", "1"}, {"Suhas", "2"}, {"Cyrus", "2"}, {"Doruk", "1"}});
}

TEST_F(CollectSubqueryTest, unwindsTheList) {
    expectRows("MATCH (p:Person {name: 'Adam'}) "
               "UNWIND COLLECT { MATCH (p)-[:INTERESTED_IN]->(i) RETURN i.name } AS interest "
               "RETURN p.name, interest",
               {{"Adam", "Bio"}, {"Adam", "Cooking"}});
}

TEST_F(CollectSubqueryTest, comparesTheList) {
    expectRows("MATCH (p:Person) WHERE COLLECT { MATCH (p)-[:KNOWS_WELL]->(k) RETURN k.name } = [] RETURN p.name",
               {{"Maxime"}, {"Luc"}, {"Martina"}, {"Suhas"}, {"Cyrus"}, {"Doruk"}});
}

TEST_F(CollectSubqueryTest, publishesThroughAWith) {
    expectRows("MATCH (p:Person) "
               "WITH p, COLLECT { MATCH (p)-[:INTERESTED_IN]->(i) RETURN i.name } AS interests "
               "WHERE size(interests) = 1 "
               "RETURN p.name, interests",
               {{"Martina", "[Cooking]"}, {"Doruk", "[Gym]"}});
}

TEST_F(CollectSubqueryTest, carriesTheListThroughALaterMatch) {
    expectRows("MATCH (p:Person) "
               "WITH p, COLLECT { MATCH (p)-[:INTERESTED_IN]->(i) RETURN i.name AS name ORDER BY name LIMIT 1 } AS first "
               "MATCH (p)-[:KNOWS_WELL]->(k) "
               "RETURN p.name, first, k.name",
               {{"Remy", "[Computers]", "Adam"}, {"Adam", "[Bio]", "Remy"}});
}

TEST_F(CollectSubqueryTest, standsInACaseExpression) {
    expectRows("MATCH (p:Person) WHERE p.name = 'Remy' OR p.name = 'Doruk' "
               "RETURN CASE WHEN COLLECT { MATCH (p)-[:KNOWS_WELL]->(k) RETURN k.name } = [] "
               "            THEN 'Nobody for ' + p.name ELSE p.name END",
               {{"Remy"}, {"Nobody for Doruk"}});
}

TEST_F(CollectSubqueryTest, groupsOnTheList) {
    expectRows("MATCH (p:Person) "
               "RETURN COLLECT { MATCH (p)-[:KNOWS_WELL]->(k) RETURN k.name } AS known, count(p) AS people",
               {{"[]", "6"}, {"[Adam]", "1"}, {"[Remy]", "1"}});
}

TEST_F(CollectSubqueryTest, ordersOnTheList) {
    expectRowsInOrder("MATCH (p:Person) WHERE p.name = 'Remy' OR p.name = 'Adam' OR p.name = 'Doruk' "
                      "RETURN p.name ORDER BY COLLECT { MATCH (p)-[:INTERESTED_IN]->(i) RETURN i.name }",
                      {{"Adam"}, {"Remy"}, {"Doruk"}});
}

TEST_F(CollectSubqueryTest, collectsForEachFactorOfACrossProduct) {
    expectRows("MATCH (p:Person), (i:Interest {name: 'Gym'}) WHERE p.name = 'Suhas' OR p.name = 'Remy' "
               "RETURN p.name, COLLECT { MATCH (p)-[e:INTERESTED_IN]->(i) RETURN e.name }",
               {{"Remy", "[]"}, {"Suhas", "[Suhas -> Gym]"}});
}

// A null node matches no pattern, so the six people nobody knows well collect nothing
TEST_F(CollectSubqueryTest, collectsOnTheRowsAnOptionalMatchLeft) {
    expectRows("MATCH (p:Person) WHERE p.name = 'Remy' OR p.name = 'Doruk' "
               "OPTIONAL MATCH (p)-[:KNOWS_WELL]->(k) "
               "RETURN p.name, COLLECT { MATCH (k)-[:INTERESTED_IN]->(i) RETURN i.name }",
               {{"Remy", "[Bio, Cooking]"}, {"Doruk", "[]"}});
}

TEST_F(CollectSubqueryTest, collectsOverUnwoundRows) {
    expectRows("UNWIND ['Adam', 'Doruk'] AS name "
               "RETURN name, COLLECT { MATCH (p:Person)-[:INTERESTED_IN]->(i) WHERE p.name = name RETURN i.name }",
               {{"Adam", "[Bio, Cooking]"}, {"Doruk", "[Gym]"}});
}

TEST_F(CollectSubqueryTest, setsAPropertyToTheList) {
    expectWriteRows("MATCH (p:Person {name: 'Adam'}) "
                    "SET p.interests = COLLECT { MATCH (p)-[:INTERESTED_IN]->(i) RETURN i.name } "
                    "RETURN p.interests",
                    {{"[Bio, Cooking]"}});

    expectRows("MATCH (p:Person {name: 'Adam'}) RETURN p.interests", {{"[Bio, Cooking]"}});
}

TEST_F(CollectSubqueryTest, dropsTheVariablesOfTheBody) {
    expectError("MATCH (p:Person) RETURN COLLECT { MATCH (p)-[:INTERESTED_IN]->(i) RETURN i.name }, i.name",
                "not found");
}

TEST_F(CollectSubqueryTest, needsAReturn) {
    expectError("MATCH (p:Person) RETURN COLLECT { MATCH (p)-[:INTERESTED_IN]->(i) }",
                "A COLLECT subquery must end with a RETURN clause");
}

TEST_F(CollectSubqueryTest, needsExactlyOneColumn) {
    expectError("MATCH (p:Person) RETURN COLLECT { MATCH (p)-[:INTERESTED_IN]->(i) RETURN i.name, p.name }",
                "A COLLECT subquery must return exactly one column, not 2");
}

TEST_F(CollectSubqueryTest, needsTheSameColumnInEveryBranch) {
    expectError("MATCH (p:Person) "
                "RETURN COLLECT { MATCH (p)-[:INTERESTED_IN]->(i) RETURN i.name AS a "
                "                 UNION MATCH (p)-[:KNOWS_WELL]->(k) RETURN k.name AS b }",
                "All sub-queries of a UNION must return the same column names");
}

TEST_F(CollectSubqueryTest, collectsTheListAnAggregateInTheBodyBuilt) {
    expectRows("MATCH (p:Person {name: 'Remy'}) RETURN COLLECT { MATCH (p)-[:INTERESTED_IN]->(i) RETURN collect(i.name) }",
               {{"[[Ghosts, Computers, Eighties]]"}});
}

TEST_F(CollectSubqueryTest, rejectsABodyRedeclaringAVariableInFlight) {
    expectError("WITH 'Remy' AS name MATCH (p:Person {name: name}) "
                "RETURN COLLECT { WITH 'Ghosts' AS name MATCH (p)-[:INTERESTED_IN]->(i {name: name}) RETURN i.name }",
                "Variable 'name' is imported by the subquery");
}

TEST_F(CollectSubqueryTest, isReadOnly) {
    expectError("MATCH (p:Person) RETURN COLLECT { CREATE (n:Person) RETURN n }",
                "A COLLECT subquery is read-only");
}
