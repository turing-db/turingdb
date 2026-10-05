#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// WHEN branches returning one entity column that one branch created, another merged and
// another matched: Remy is in the graph, Nia and Kai are not
class ConditionalCreateMergeTest : public WriteQueryTest {
};

TEST_F(ConditionalCreateMergeTest, returnsAnEntityOneBranchCreatedAndAnotherMerged) {
    expectWriteRows("UNWIND ['Remy', 'Nia', 'Kai'] AS name "
                    "CALL (name) { "
                    "WHEN name = 'Kai' THEN { CREATE (n:Person {name: name}) RETURN n } "
                    "ELSE { MERGE (n:Person {name: name}) RETURN n } "
                    "} "
                    "RETURN n.name, n.age",
                    {{"Kai", "null"}, {"Nia", "null"}, {"Remy", "32"}});

    expectRows("MATCH (p:Person) WHERE p.name IN ['Remy', 'Nia', 'Kai'] RETURN p.name",
               {{"Kai"}, {"Nia"}, {"Remy"}});
}

TEST_F(ConditionalCreateMergeTest, returnsAnEntityOneBranchCreatedAndAnotherMatched) {
    expectWriteRows("UNWIND ['Remy', 'Kai'] AS name "
                    "CALL (name) { "
                    "WHEN name = 'Remy' THEN { MATCH (n:Person {name: name}) RETURN n } "
                    "ELSE { CREATE (n:Person {name: name}) RETURN n } "
                    "} "
                    "SET n.age = 40 RETURN n.name, n.age",
                    {{"Kai", "40"}, {"Remy", "40"}});

    expectRows("MATCH (p:Person) WHERE p.age = 40 RETURN p.name", {{"Kai"}, {"Remy"}});
}

TEST_F(ConditionalCreateMergeTest, writesFromAnEntityCreatedMergedOrMatched) {
    expectWriteRows("UNWIND ['Remy', 'Nia', 'Kai'] AS name "
                    "CALL (name) { "
                    "WHEN name = 'Kai' THEN { CREATE (n:Person {name: name}) RETURN n } "
                    "WHEN name = 'Nia' THEN { MERGE (n:Person {name: name}) RETURN n } "
                    "ELSE { MATCH (n:Person {name: name}) RETURN n } "
                    "} "
                    "CREATE (n)-[:HOLDS]->(:Badge) "
                    "RETURN n.name, n:Person",
                    {{"Kai", "true"}, {"Nia", "true"}, {"Remy", "true"}});

    expectRows("MATCH (p:Person)-[:HOLDS]->(:Badge) RETURN p.name", {{"Kai"}, {"Nia"}, {"Remy"}});
}

TEST_F(ConditionalCreateMergeTest, checksTheLabelsOfAnEntityCreatedOrMerged) {
    expectWriteRows("UNWIND ['Nia', 'Kai'] AS name "
                    "CALL (name) { "
                    "WHEN name = 'Kai' THEN { CREATE (n:Robot {name: name}) RETURN n } "
                    "ELSE { MERGE (n:Person {name: name}) RETURN n } "
                    "} "
                    "RETURN n.name, n:Robot, n:Person",
                    {{"Kai", "true", "false"}, {"Nia", "false", "true"}});
}

TEST_F(ConditionalCreateMergeTest, returnsAnEntityEveryBranchCreated) {
    expectWriteRows("UNWIND ['Nia', 'Kai'] AS name "
                    "CALL (name) { "
                    "WHEN name = 'Kai' THEN { CREATE (n:Robot {name: name}) RETURN n } "
                    "ELSE { CREATE (n:Person {name: name}) RETURN n } "
                    "} "
                    "RETURN n.name, n:Robot",
                    {{"Kai", "true"}, {"Nia", "false"}});
}

TEST_F(ConditionalCreateMergeTest, returnsAnImportedCreatedEntityOrAMatchedOne) {
    expectWriteRows("UNWIND ['Remy', 'Kai'] AS name CREATE (k:Person {name: name + '2'}) "
                    "WITH name, k "
                    "CALL (name, k) { "
                    "WHEN name = 'Kai' THEN RETURN k AS n "
                    "ELSE { MATCH (n:Person {name: name}) RETURN n } "
                    "} "
                    "SET n.age = 9 RETURN n.name, n.age",
                    {{"Kai2", "9"}, {"Remy", "9"}});
}
