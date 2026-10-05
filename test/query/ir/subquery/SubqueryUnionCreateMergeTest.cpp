#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// A UNION in a CALL body returning an entity its branches created, merged or matched: Remy
// is in the graph, Nia and Kai are not
class SubqueryUnionCreateMergeTest : public WriteQueryTest {
};

TEST_F(SubqueryUnionCreateMergeTest, returnsAnEntityEveryBranchMerged) {
    expectWriteRows("UNWIND ['Remy', 'Nia'] AS name "
                    "CALL (name) { "
                    "MERGE (n:Person {name: name}) RETURN n "
                    "UNION ALL "
                    "MERGE (n:Person {name: name}) RETURN n "
                    "} "
                    "RETURN n.name, n.age",
                    {{"Nia", "null"}, {"Nia", "null"}, {"Remy", "32"}, {"Remy", "32"}});

    expectRows("MATCH (p:Person {name: 'Nia'}) RETURN count(p)", {{"1"}});
}

TEST_F(SubqueryUnionCreateMergeTest, dedupsAnEntityOneBranchMergedAndAnotherMatched) {
    expectWriteRows("CALL { "
                    "MERGE (n:Person {name: 'Remy'}) RETURN n "
                    "UNION "
                    "MATCH (n:Person {name: 'Remy'}) RETURN n "
                    "} "
                    "RETURN n.name, n.age",
                    {{"Remy", "32"}});
}

TEST_F(SubqueryUnionCreateMergeTest, dedupsAnEntityEveryBranchMergedPending) {
    expectWriteRows("CALL { "
                    "MERGE (n:Person {name: 'Nia'}) RETURN n "
                    "UNION "
                    "MERGE (n:Person {name: 'Nia'}) RETURN n "
                    "} "
                    "RETURN n.name",
                    {{"Nia"}});
}

TEST_F(SubqueryUnionCreateMergeTest, returnsAnEntityOneBranchCreatedAndAnotherMerged) {
    expectWriteRows("UNWIND ['Nia', 'Kai'] AS name "
                    "CALL (name) { "
                    "CREATE (n:Robot {name: name + 'R'}) RETURN n "
                    "UNION "
                    "MERGE (n:Person {name: name}) RETURN n "
                    "} "
                    "RETURN n.name, n:Robot",
                    {{"NiaR", "true"}, {"Nia", "false"}, {"KaiR", "true"}, {"Kai", "false"}});
}

TEST_F(SubqueryUnionCreateMergeTest, returnsAnEntityOneBranchCreatedAndAnotherMatched) {
    expectWriteRows("CALL { "
                    "CREATE (n:Person {name: 'Kai'}) RETURN n "
                    "UNION "
                    "MATCH (n:Person {name: 'Remy'}) RETURN n "
                    "} "
                    "SET n.age = 9 RETURN n.name, n.age",
                    {{"Kai", "9"}, {"Remy", "9"}});

    expectRows("MATCH (p:Person) WHERE p.age = 9 RETURN p.name", {{"Kai"}, {"Remy"}});
}

TEST_F(SubqueryUnionCreateMergeTest, returnsAnImportedMergedEntity) {
    expectWriteRows("UNWIND ['Remy', 'Nia'] AS name MERGE (n:Person {name: name}) "
                    "WITH n CALL (n) { RETURN n AS m UNION RETURN n AS m } "
                    "RETURN m.name, m.age",
                    {{"Nia", "null"}, {"Remy", "32"}});
}

TEST_F(SubqueryUnionCreateMergeTest, createsAnEdgeFromAnEntityMergedOrMatched) {
    expectWriteRows("CALL { "
                    "MERGE (n:Person {name: 'Nia'}) RETURN n "
                    "UNION "
                    "MATCH (n:Person {name: 'Remy'}) RETURN n "
                    "} "
                    "CREATE (n)-[:HOLDS]->(:Badge) "
                    "RETURN count(*)",
                    {{"2"}});

    expectRows("MATCH (p:Person)-[:HOLDS]->(:Badge) RETURN p.name", {{"Nia"}, {"Remy"}});
}
