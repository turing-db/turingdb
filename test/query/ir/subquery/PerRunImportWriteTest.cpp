#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// A body run once per input row writes from its import past a keyless WITH, and a MERGE
// reads back the properties of the rows it wrote in the same query.
class PerRunImportWriteTest : public WriteQueryTest {
};

TEST_F(PerRunImportWriteTest, createsFromTheImportPastAKeylessWith) {
    expectWriteRows("MATCH (p:Person) "
                    "CALL (p) { WITH count(*) AS c CREATE (p)-[:LIKES]->(:Tag {n: p.name}) } "
                    "RETURN count(*)",
                    {{"8"}});

    expectRows("MATCH (p:Person)-[:LIKES]->(t:Tag) RETURN p.name, t.n",
               {{"Remy", "Remy"},
                {"Adam", "Adam"},
                {"Maxime", "Maxime"},
                {"Luc", "Luc"},
                {"Martina", "Martina"},
                {"Suhas", "Suhas"},
                {"Cyrus", "Cyrus"},
                {"Doruk", "Doruk"}});
}

TEST_F(PerRunImportWriteTest, mergesFromTheImportPastAKeylessWith) {
    expectWriteRows("MATCH (p:Person) "
                    "CALL (p) { WITH count(*) AS c MERGE (p)-[:OWNS]->(t:Box {n: p.name}) RETURN t.n AS n } "
                    "RETURN n",
                    {{"Remy"},
                     {"Adam"},
                     {"Maxime"},
                     {"Luc"},
                     {"Martina"},
                     {"Suhas"},
                     {"Cyrus"},
                     {"Doruk"}});
}

TEST_F(PerRunImportWriteTest, setsAPropertyOfTheImportPastAKeylessWith) {
    applyWrite("MATCH (p:Person) CALL (p) { WITH count(*) AS c SET p.c = c + size(p.name) } RETURN count(*)");

    expectRows("MATCH (p:Person) RETURN p.name, p.c",
               {{"Remy", "5"},
                {"Adam", "5"},
                {"Maxime", "7"},
                {"Luc", "4"},
                {"Martina", "8"},
                {"Suhas", "6"},
                {"Cyrus", "6"},
                {"Doruk", "6"}});
}

TEST_F(PerRunImportWriteTest, readsWhatAMergeCreatedOnEachRow) {
    expectWriteRows("UNWIND ['Remy', 'Zed', 'Zed'] AS nm MERGE (n:Person {name: nm}) ON CREATE SET n.age = 40 RETURN nm, n.age",
                    {{"Remy", "32"}, {"Zed", "40"}, {"Zed", "40"}});
}

TEST_F(PerRunImportWriteTest, readsTheEdgeAndNodeAMergeCreated) {
    expectWriteRows("MATCH (r:Person {name: 'Remy'}) "
                    "MERGE (r)-[e:KNOWS_WELL]->(z:Person {name: 'Yan'}) ON CREATE SET e.duration = 5, z.age = 7 "
                    "RETURN e.duration, z.age, z.name",
                    {{"5", "7", "Yan"}});
}

TEST_F(PerRunImportWriteTest, readsAMergedNodeInASubqueryAndAComprehension) {
    expectWriteRows("MERGE (n:Person {name: 'Xi'}) RETURN n.name, COUNT { MATCH (n)-->() RETURN 1 }, [x IN [1] | n.name]",
                    {{"Xi", "0", "[Xi]"}});
}

TEST_F(PerRunImportWriteTest, readsAMergedImportPastAKeylessWith) {
    expectWriteRows("MERGE (n:Person {name: 'Wu'}) WITH n CALL (n) { WITH count(*) AS c RETURN n.name AS k } RETURN k",
                    {{"Wu"}});
}
