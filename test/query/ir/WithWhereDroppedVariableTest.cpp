#include <gtest/gtest.h>

#include <string>
#include <string_view>

#include "QueryStatus.h"

#include "IRTestRows.h"
#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// The WHERE of a WITH that neither aggregates nor dedups reads the variables before the
// WITH as well as the ones it publishes, as in Neo4j
class WithWhereDroppedVariableTest : public WriteQueryTest {
protected:
    void expectRejected(std::string_view query) {
        RowSink sink;
        const QueryStatus status = runQuery(query, &sink);
        ASSERT_FALSE(status.isOk()) << "query accepted: " << query;

        const std::string& error = status.getError();

        EXPECT_EQ(status.getStatus(), QueryStatus::Status::ANALYZE_ERROR)
            << "query: " << query << "\nerror: " << error;
        EXPECT_EQ(error.find("Internal Error"), std::string::npos)
            << "query: " << query << "\nerror: " << error;
    }

    void expectWriteRejected(std::string_view query, std::string_view message) {
        ChangeID changeID;
        openChange(changeID);

        const QueryStatus status = runWrite(query, changeID);
        ASSERT_FALSE(status.isOk()) << "query accepted: " << query;

        EXPECT_NE(status.getError().find(message), std::string::npos)
            << "query: " << query << "\nerror: " << status.getError();
    }
};

TEST_F(WithWhereDroppedVariableTest, readsADroppedConstant) {
    expectRows("WITH 1 AS a, 5 AS k WITH a WHERE a < k RETURN a", {{"1"}});
    expectRows("WITH 1 AS a, 5 AS k WITH a WHERE a > k RETURN a", {});
}

TEST_F(WithWhereDroppedVariableTest, readsAConstantBoundBeforeTheMatch) {
    expectRows("WITH 30 AS k MATCH (p:Person) WITH p WHERE p.age > k RETURN p.name",
               {{"Adam"}, {"Remy"}});
}

TEST_F(WithWhereDroppedVariableTest, readsADroppedNode) {
    expectRows("MATCH (n:Person) WITH n.name AS name WHERE n.age > 30 RETURN name",
               {{"Adam"}, {"Remy"}});
    expectRows("MATCH (n:Person) WITH 1 AS one WHERE n.age > 30 RETURN one", {{"1"}, {"1"}});
}

TEST_F(WithWhereDroppedVariableTest, readsADroppedEdge) {
    expectRows("MATCH (p)-[e:KNOWS_WELL]->(x) WITH x WHERE e.duration > 20 RETURN x.name",
               {{"Remy"}});
}

TEST_F(WithWhereDroppedVariableTest, readsADroppedCollectedList) {
    expectRows("MATCH (i:SleepDisturber) WITH collect(i) AS disturbers "
               "MATCH (p:Person)-[:INTERESTED_IN]->(interest) "
               "WITH p WHERE interest IN disturbers "
               "RETURN p.name",
               {{"Luc"}, {"Remy"}});
    expectCounts("MATCH (i:SleepDisturber) WITH collect(i) AS disturbers "
                 "MATCH (p:Person)-[:INTERESTED_IN]->(interest) "
                 "WITH p, interest WHERE interest IN disturbers "
                 "RETURN count(p)",
                 {2});
}

TEST_F(WithWhereDroppedVariableTest, readsADroppedNodeInAnExistsSubquery) {
    expectRows("MATCH (p:Person) WITH p.name AS name WHERE EXISTS { (p)-[:KNOWS_WELL]->() } "
               "RETURN name",
               {{"Adam"}, {"Remy"}});
}

TEST_F(WithWhereDroppedVariableTest, readsADroppedNodeInAPatternPredicate) {
    expectRows("MATCH (p:Person) WITH p.name AS name WHERE (p)-[:KNOWS_WELL]->() RETURN name",
               {{"Adam"}, {"Remy"}});
}

// Remy is the one person interested in more than two things
TEST_F(WithWhereDroppedVariableTest, readsADroppedNodeInAPatternComprehension) {
    expectRows("MATCH (p:Person) WITH p.name AS name "
               "WHERE size([(p)-[:INTERESTED_IN]->(x) | x]) > 2 RETURN name",
               {{"Remy"}});
}

TEST_F(WithWhereDroppedVariableTest, readsADroppedEntityACreateWrote) {
    expectWriteRows("CREATE (n:Person {name: 'Wu', age: 40}) WITH 1 AS one WHERE n.age > 30 RETURN one",
                    {{"1"}});
    expectWriteRows("CREATE (n:Person {name: 'Yan', age: 20}) WITH 1 AS one WHERE n.age > 30 RETURN one",
                    {});
    expectWriteRows("CREATE (n:Person {name: 'Yan', age: 20}) WITH 1 AS one WHERE n.age > 30 RETURN count(*)",
                    {{"0"}});
}

TEST_F(WithWhereDroppedVariableTest, readsADroppedPath) {
    expectRows("MATCH (a:Person)-[r:KNOWS_WELL*1..2]->(b) WITH a WHERE size(r) > 1 RETURN a.name",
               {{"Adam"}, {"Remy"}});
    expectRows("MATCH (a:Person)-[r:KNOWS_WELL*1..2]->(b) WITH a ORDER BY a.name WHERE size(r) > 1 "
               "RETURN a.name",
               {{"Adam"}, {"Remy"}});
    expectRows("MATCH p = (a:Person)-[:KNOWS_WELL*1..2]->(b) WITH a ORDER BY a.name WHERE p IS NOT NULL "
               "RETURN a.name",
               {{"Adam"}, {"Adam"}, {"Remy"}, {"Remy"}});
    expectRows("MATCH p = (a:Person)-[:KNOWS_WELL]->(b) WITH a ORDER BY a.name WHERE p IS NOT NULL "
               "RETURN a.name",
               {{"Adam"}, {"Remy"}});
}

TEST_F(WithWhereDroppedVariableTest, rejectsADroppedMergedEntity) {
    expectWriteRejected("MERGE (n:Person {name: 'Remy'}) WITH 1 AS one WHERE n.age > 30 RETURN one",
                        "The WHERE of a WITH cannot read 'n'");
    expectWriteRejected("MERGE (n:Person {name: 'Remy'}) WITH 1 AS one "
                        "WHERE EXISTS { (n)-[:KNOWS_WELL]->() } RETURN one",
                        "The WHERE of a WITH cannot read 'n'");
}

TEST_F(WithWhereDroppedVariableTest, dropsAMergedEntityTheFilterDoesNotRead) {
    expectWriteRows("MERGE (n:Person {name: 'Remy'}) WITH 1 AS one WHERE one > 0 RETURN one", {{"1"}});
    expectWriteRows("MERGE (n:Person {name: 'Remy'}) WITH n.age AS age WHERE age > 30 RETURN age", {{"32"}});
}

TEST_F(WithWhereDroppedVariableTest, readsThePublishedNameOverTheOneItShadows) {
    expectRows("WITH 1 AS a, 5 AS k WITH k + 0 AS a WHERE a > 3 RETURN a", {{"5"}});
    expectRows("UNWIND [1, 2, 3, 4] AS x WITH x * 2 AS x WHERE x > 4 RETURN x", {{"6"}, {"8"}});
}

// Adam, Cyrus and Doruk are the first three names, and only Adam has a PhD
TEST_F(WithWhereDroppedVariableTest, filtersTheRowsTheCutKept) {
    expectRows("MATCH (p:Person) WITH p.name AS name ORDER BY name LIMIT 3 WHERE p.hasPhD "
               "RETURN name",
               {{"Adam"}});
    expectRows("UNWIND [1, 2, 3, 4] AS x WITH x AS y ORDER BY y DESC LIMIT 2 WHERE x > 3 RETURN y",
               {{"4"}});
    expectRows("UNWIND [1, 2, 3, 4] AS x WITH x AS y SKIP 1 WHERE x < 4 RETURN y",
               {{"2"}, {"3"}});
}

TEST_F(WithWhereDroppedVariableTest, dropsTheVariableAfterTheFilter) {
    expectRejected("WITH 1 AS a, 5 AS k WITH a WHERE a < k RETURN k");
    expectRejected("MATCH (n:Person) WITH n.name AS name WHERE n.age > 30 RETURN n");
}

TEST_F(WithWhereDroppedVariableTest, rejectsAVariableDroppedByAnEarlierWith) {
    expectRejected("UNWIND [1, 2, 3, 4] AS x WITH x AS y WITH y WHERE x > 2 RETURN y");
}

TEST_F(WithWhereDroppedVariableTest, rejectsADroppedVariableUnderAnAggregate) {
    expectRejected("UNWIND [1, 2, 3, 4] AS x WITH count(*) AS c WHERE x > 2 RETURN c");
    expectRejected("MATCH (n:Person) WITH n.name AS name, count(*) AS c WHERE n.age > 30 RETURN name");
}

TEST_F(WithWhereDroppedVariableTest, rejectsADroppedVariableUnderDistinct) {
    expectRejected("UNWIND [1, 2, 3, 4] AS x WITH DISTINCT x % 2 AS y WHERE x > 2 RETURN y");
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
