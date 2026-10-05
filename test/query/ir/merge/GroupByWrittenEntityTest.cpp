#include <gtest/gtest.h>

#include "IRTestRows.h"
#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

class GroupByWrittenEntityTest : public WriteQueryTest {
};

TEST_F(GroupByWrittenEntityTest, withGroupsByAMergedEntity) {
    expectWriteRows("UNWIND ['Remy', 'Nia', 'Nia'] AS name MERGE (n:Person {name: name}) "
                    "WITH n, count(*) AS c RETURN n.name, n.age, c",
                    {{"Nia", "null", "2"}, {"Remy", "32", "1"}});
}

TEST_F(GroupByWrittenEntityTest, withGroupsByAMergedEntityUnderAnAlias) {
    expectWriteRows("UNWIND ['Remy', 'Nia', 'Nia'] AS name MERGE (n:Person {name: name}) "
                    "WITH n AS m, count(*) AS c RETURN m.name, m.age, c",
                    {{"Nia", "null", "2"}, {"Remy", "32", "1"}});
}

TEST_F(GroupByWrittenEntityTest, withCollectsPerMergedEntity) {
    expectWriteRows("UNWIND ['Remy', 'Nia', 'Nia'] AS name MERGE (n:Person {name: name}) "
                    "WITH n, collect(name) AS names RETURN n.name, size(names)",
                    {{"Nia", "2"}, {"Remy", "1"}});
}

TEST_F(GroupByWrittenEntityTest, withGroupsByAMergedEntityAcrossACut) {
    expectWriteRows("UNWIND ['Remy', 'Nia', 'Nia'] AS name MERGE (n:Person {name: name}) "
                    "WITH n, name WITH n, count(*) AS c RETURN n.name, n.age, c",
                    {{"Nia", "null", "2"}, {"Remy", "32", "1"}});
}

TEST_F(GroupByWrittenEntityTest, setsAPropertyOfAGroupedMergedEntity) {
    expectWriteRows("UNWIND ['Remy', 'Nia', 'Nia'] AS name MERGE (n:Person {name: name}) "
                    "WITH n, count(*) AS c SET n.age = c RETURN n.name, n.age",
                    {{"Nia", "2"}, {"Remy", "1"}});
}

TEST_F(GroupByWrittenEntityTest, returnGroupsByAMergedEntity) {
    expectWriteRowCount("UNWIND ['Remy', 'Nia', 'Nia'] AS name MERGE (n:Person {name: name}) "
                        "RETURN n, count(*)",
                        2);
}

TEST_F(GroupByWrittenEntityTest, callReturnGroupsByAnEntityTheBodyMerged) {
    expectWriteRows("UNWIND ['Remy', 'Nia'] AS name "
                    "CALL (name) { MERGE (n:Person {name: name}) RETURN n, count(*) AS c } "
                    "RETURN n.name, n.age, c",
                    {{"Nia", "null", "1"}, {"Remy", "32", "1"}});
}

TEST_F(GroupByWrittenEntityTest, withGroupsByAMergedEdge) {
    expectWriteRows("UNWIND [1, 2] AS x MERGE (a:Person {name: 'Kai'})-[e:KNOWS_WELL]->(b:Person {name: 'Lea'}) "
                    "WITH e, count(*) AS c RETURN c",
                    {{"2"}});
}

TEST_F(GroupByWrittenEntityTest, withGroupsByACreatedEntity) {
    expectWriteRows("UNWIND ['Remy', 'Nia', 'Nia'] AS name CREATE (n:Person {name: name}) "
                    "WITH n, count(*) AS c RETURN n.name, c",
                    {{"Nia", "1"}, {"Nia", "1"}, {"Remy", "1"}});
}

TEST_F(GroupByWrittenEntityTest, returnGroupsByACreatedEntity) {
    expectWriteRowCount("UNWIND ['Remy', 'Nia', 'Nia'] AS name CREATE (n:Person {name: name}) "
                        "RETURN n, count(*)",
                        3);
}

TEST_F(GroupByWrittenEntityTest, setsAPropertyOfAGroupedCreatedEntity) {
    expectWriteRows("UNWIND ['Remy', 'Nia', 'Nia'] AS name CREATE (n:Person {name: name}) "
                    "WITH n, count(*) AS c SET n.age = c + 10 RETURN n.name, n.age",
                    {{"Nia", "11"}, {"Nia", "11"}, {"Remy", "11"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
