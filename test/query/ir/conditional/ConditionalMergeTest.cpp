#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// A WHEN branch returning an entity a MERGE wrote: Remy is in the graph, Nia is not
class ConditionalMergeTest : public WriteQueryTest {
};

TEST_F(ConditionalMergeTest, returnsAnEntityEveryBranchMerged) {
    expectWriteRows("UNWIND ['Remy', 'Nia'] AS name "
                    "CALL (name) { "
                    "WHEN name = 'Remy' THEN { MERGE (n:Person {name: name}) RETURN n } "
                    "ELSE { MERGE (n:Person {name: name}) RETURN n } "
                    "} "
                    "RETURN n.name, n.age",
                    {{"Nia", "null"}, {"Remy", "32"}});
}

TEST_F(ConditionalMergeTest, returnsAnEntityOneBranchMergedAndAnotherMatched) {
    expectWriteRows("UNWIND ['Remy', 'Nia'] AS name "
                    "CALL (name) { "
                    "WHEN name = 'Nia' THEN { MERGE (n:Person {name: name}) RETURN n } "
                    "ELSE { MATCH (n:Person {name: name}) RETURN n } "
                    "} "
                    "RETURN n.name, n.age",
                    {{"Nia", "null"}, {"Remy", "32"}});

    expectWriteRows("UNWIND ['Remy', 'Nia'] AS name "
                    "CALL (name) { "
                    "WHEN name = 'Remy' THEN { MATCH (n:Person {name: name}) RETURN n } "
                    "ELSE { MERGE (n:Person {name: name}) RETURN n } "
                    "} "
                    "RETURN n.name, n.age",
                    {{"Nia", "null"}, {"Remy", "32"}});
}

TEST_F(ConditionalMergeTest, setsAPropertyOfAReturnedMergedEntity) {
    expectWriteRows("UNWIND ['Remy', 'Nia'] AS name "
                    "CALL (name) { "
                    "WHEN name = 'Remy' THEN { MATCH (n:Person {name: name}) RETURN n } "
                    "ELSE { MERGE (n:Person {name: name}) RETURN n } "
                    "} "
                    "SET n.age = 50 RETURN n.name, n.age",
                    {{"Nia", "50"}, {"Remy", "50"}});
}

TEST_F(ConditionalMergeTest, createsAnEdgeFromAReturnedMergedEntity) {
    expectWriteRows("UNWIND ['Remy', 'Nia'] AS name "
                    "CALL (name) { "
                    "WHEN name = 'Nia' THEN { MERGE (n:Person {name: name}) RETURN n } "
                    "ELSE { MATCH (n:Person {name: name}) RETURN n } "
                    "} "
                    "CREATE (n)-[:HOLDS]->(:Badge) "
                    "RETURN count(*)",
                    {{"2"}});

    expectRows("MATCH (p:Person)-[:HOLDS]->(:Badge) RETURN p.name", {{"Nia"}, {"Remy"}});
}

TEST_F(ConditionalMergeTest, returnsAnImportedMergedEntity) {
    expectWriteRows("UNWIND ['Remy', 'Nia'] AS name MERGE (n:Person {name: name}) "
                    "WITH n CALL (n) { WHEN n.age IS NULL THEN RETURN n AS m ELSE RETURN n AS m } "
                    "RETURN m.name, m.age",
                    {{"Nia", "null"}, {"Remy", "32"}});
}

TEST_F(ConditionalMergeTest, padsAnOptionalCallReturningAMergedEntity) {
    expectWriteRows("UNWIND ['Remy', 'Nia'] AS name "
                    "OPTIONAL CALL (name) { WHEN name = 'Nia' THEN { MERGE (n:Person {name: name}) RETURN n } } "
                    "SET n.age = 7 RETURN name, n.age",
                    {{"Nia", "7"}, {"Remy", "null"}});
}

TEST_F(ConditionalMergeTest, mergesInTheBranchOfAStandaloneConditional) {
    expectWriteRows("WHEN true THEN { MERGE (n:Person {name: 'Nia'}) RETURN n.name AS name } "
                    "ELSE { MATCH (n:Person {name: 'Remy'}) RETURN n.name AS name }",
                    {{"Nia"}});

    expectRows("MATCH (p:Person {name: 'Nia'}) RETURN count(p)", {{"1"}});
}

TEST_F(ConditionalMergeTest, mergesNothingInABranchNotTaken) {
    expectWriteRows("WHEN false THEN { MERGE (:Person {name: 'Nia'}) } ELSE { CREATE (:Badge) }", {});

    expectRows("MATCH (p:Person {name: 'Nia'}) RETURN count(p)", {{"0"}});
    expectRows("MATCH (b:Badge) RETURN count(b)", {{"1"}});
}

TEST_F(ConditionalMergeTest, mergesInTheBranchOfACallWithNoInput) {
    expectWriteRows("CALL () { "
                    "WHEN false THEN { MATCH (n:Person {name: 'Remy'}) RETURN n } "
                    "ELSE { MERGE (n:Person {name: 'Nia'}) RETURN n } "
                    "} "
                    "RETURN n.name",
                    {{"Nia"}});
}
