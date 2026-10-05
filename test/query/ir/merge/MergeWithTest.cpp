#include <gtest/gtest.h>

#include <string>
#include <string_view>

#include "QueryStatus.h"

#include "IRTestRows.h"
#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// A MERGE's rows mix the entities it wrote with the ones it matched, and a mask beside them
// says which is which. The mask crosses a WITH with them, so the part below reads each row
// where its entity lives: Remy off the graph, Nia out of the write buffer.
class MergeWithTest : public WriteQueryTest {
protected:
    void expectWriteRejected(std::string_view query, std::string_view message) {
        ChangeID changeID;
        openChange(changeID);

        const QueryStatus status = runWrite(query, changeID);
        ASSERT_FALSE(status.isOk()) << "query accepted: " << query;

        EXPECT_NE(status.getError().find(message), std::string::npos)
            << "query: " << query << "\nerror: " << status.getError();
    }
};

TEST_F(MergeWithTest, publishesAMergedEntity) {
    expectWriteRows("UNWIND ['Remy', 'Nia'] AS name MERGE (n:Person {name: name}) "
                    "WITH n RETURN n.name, n.age",
                    {{"Nia", "null"}, {"Remy", "32"}});
    expectWriteRows("MERGE (n:Person {name: 'Nia'}) WITH n RETURN n.name", {{"Nia"}});
}

TEST_F(MergeWithTest, publishesAMergedEntityUnderAnAlias) {
    expectWriteRows("UNWIND ['Remy', 'Nia'] AS name MERGE (n:Person {name: name}) "
                    "WITH n AS m RETURN m.name, m.age",
                    {{"Nia", "null"}, {"Remy", "32"}});
}

TEST_F(MergeWithTest, publishesAMergedEntityUnderTwoNames) {
    expectWriteRows("UNWIND ['Remy', 'Nia'] AS name MERGE (n:Person {name: name}) "
                    "WITH n, n AS m RETURN n.name, m.age",
                    {{"Nia", "null"}, {"Remy", "32"}});
}

TEST_F(MergeWithTest, publishesAMergedEntityThroughAWildcard) {
    expectWriteRows("UNWIND ['Remy', 'Nia'] AS name MERGE (n:Person {name: name}) "
                    "WITH * RETURN n.name, n.age",
                    {{"Nia", "null"}, {"Remy", "32"}});
}

TEST_F(MergeWithTest, publishesAMergedEntityPastTwoBarriers) {
    expectWriteRows("UNWIND ['Remy', 'Nia'] AS name MERGE (n:Person {name: name}) "
                    "WITH n WITH n WHERE n.age IS NULL RETURN count(*)",
                    {{"1"}});
}

TEST_F(MergeWithTest, cutsTheRowsOfAMergedEntity) {
    expectWriteRows("UNWIND ['Remy', 'Nia'] AS name MERGE (n:Person {name: name}) "
                    "WITH n ORDER BY n.name LIMIT 1 RETURN n.name, n.age",
                    {{"Nia", "null"}});
}

TEST_F(MergeWithTest, dedupsAMergedEntity) {
    expectWriteRows("UNWIND ['Remy', 'Nia', 'Remy'] AS name MERGE (n:Person {name: name}) "
                    "WITH DISTINCT n RETURN n.name, n.age",
                    {{"Nia", "null"}, {"Remy", "32"}});
}

TEST_F(MergeWithTest, filtersAPublishedMergedEntity) {
    expectWriteRows("UNWIND ['Remy', 'Nia'] AS name MERGE (n:Person {name: name}) "
                    "WITH n WHERE n.age IS NULL RETURN n.name",
                    {{"Nia"}});
}

TEST_F(MergeWithTest, filtersADroppedMergedEntity) {
    expectWriteRows("UNWIND ['Remy', 'Nia'] AS name MERGE (n:Person {name: name}) "
                    "WITH n.name AS name WHERE n.age > 30 RETURN name",
                    {{"Remy"}});
}

TEST_F(MergeWithTest, filtersAMergedEntityOnAnExistsSubquery) {
    expectWriteRows("UNWIND ['Remy', 'Nia'] AS name MERGE (n:Person {name: name}) "
                    "WITH n WHERE EXISTS { (n)-[:KNOWS_WELL]->() } RETURN n.name",
                    {{"Remy"}});
}

TEST_F(MergeWithTest, fansOutAMergedEntity) {
    expectWriteRows("UNWIND ['Remy', 'Nia'] AS name MERGE (n:Person {name: name}) "
                    "WITH n UNWIND [1, 2] AS x RETURN n.name, n.age, x",
                    {{"Nia", "null", "1"}, {"Nia", "null", "2"}, {"Remy", "32", "1"}, {"Remy", "32", "2"}});
}

TEST_F(MergeWithTest, crossesAMergedEntityWithAMatch) {
    expectWriteRows("UNWIND ['Remy', 'Nia'] AS name MERGE (n:Person {name: name}) "
                    "WITH n MATCH (p:Person {name: 'Adam'}) RETURN n.name, n.age, p.name",
                    {{"Nia", "null", "Adam"}, {"Remy", "32", "Adam"}});
}

TEST_F(MergeWithTest, testsTheLabelsOfAMergedEntity) {
    expectWriteRows("UNWIND ['Remy', 'Nia'] AS name MERGE (n:Person {name: name}) "
                    "WITH n RETURN n.name, n:Person",
                    {{"Nia", "true"}, {"Remy", "true"}});
}

TEST_F(MergeWithTest, setsAPropertyOfAMergedEntity) {
    expectWriteRows("UNWIND ['Remy', 'Nia'] AS name MERGE (n:Person {name: name}) "
                    "WITH n SET n.age = 40 RETURN n.name, n.age",
                    {{"Nia", "40"}, {"Remy", "40"}});
}

TEST_F(MergeWithTest, createsAnEdgeFromAMergedEntity) {
    applyWrite("UNWIND ['Remy', 'Nia'] AS name MERGE (n:Person {name: name}) "
               "WITH n CREATE (n)-[:LIKES]->(:Dish {name: 'Pho'})");

    expectRows("MATCH (p:Person)-[:LIKES]->(d:Dish) RETURN p.name, d.name",
               {{"Nia", "Pho"}, {"Remy", "Pho"}});
}

TEST_F(MergeWithTest, mergesAnEdgeFromAMergedEntity) {
    expectWriteRows("UNWIND ['Remy', 'Nia'] AS name MERGE (n:Person {name: name}) "
                    "WITH n MERGE (n)-[:LIKES]->(d:Dish {name: 'Pho'}) RETURN n.name, d.name",
                    {{"Nia", "Pho"}, {"Remy", "Pho"}});
}

TEST_F(MergeWithTest, deletesAMergedEntity) {
    applyWrite("UNWIND ['Remy', 'Nia'] AS name MERGE (n:Person {name: name}) "
               "WITH n DETACH DELETE n");

    expectRows("MATCH (p:Person) WHERE p.name = 'Remy' OR p.name = 'Nia' RETURN p.name", {});
}

TEST_F(MergeWithTest, importsAMergedEntityIntoACallSubquery) {
    expectWriteRows("UNWIND ['Remy', 'Nia'] AS name MERGE (n:Person {name: name}) "
                    "WITH n CALL (n) { RETURN n.age AS age } RETURN n.name, age",
                    {{"Nia", "null"}, {"Remy", "32"}});
}

// The graph the pattern reads holds none of what this change wrote until the commit, as
// for what a CREATE wrote
TEST_F(MergeWithTest, rejectsAnOptionalMatchOverAMergedEntity) {
    expectWriteRejected("MERGE (n:Person {name: 'Nia'}) WITH n "
                        "OPTIONAL MATCH (n)-[:KNOWS_WELL]->(m) RETURN m.name",
                        "An OPTIONAL MATCH cannot read what a MERGE in the same query wrote");
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
