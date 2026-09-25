#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// Two MERGEs of one pattern, with a SET or a DELETE between them of what the first one
// read. The second MERGE has to find the entities as the write left them, not as the
// first one saw them.
//
// simpledb holds 8 Person nodes, no Tag node and no MENTORS edge.
class MergeAfterIndexedWriteTest : public WriteQueryTest {
};

TEST_F(MergeAfterIndexedWriteTest, matchesTheNodeASetRenamed) {
    expectWriteRows("MERGE (p:Person {name: 'Remy'}) WITH count(*) AS merged "
                    "MATCH (q:Person {name: 'Remy'}) SET q.name = 'Remi' "
                    "WITH count(*) AS renamed MERGE (r:Person {name: 'Remi'}) RETURN r.name",
                    {{"Remi"}});

    expectRows("MATCH (q:Person) RETURN count(q)", {{"8"}});
}

TEST_F(MergeAfterIndexedWriteTest, createsANodeForTheNameASetTookOff) {
    expectWriteRows("MERGE (p:Person {name: 'Remy'}) WITH count(*) AS merged "
                    "MATCH (q:Person {name: 'Remy'}) SET q.name = 'Remi' "
                    "WITH count(*) AS renamed MERGE (r:Person {name: 'Remy'}) RETURN r.name",
                    {{"Remy"}});

    expectRows("MATCH (q:Person) RETURN count(q)", {{"9"}});
}

TEST_F(MergeAfterIndexedWriteTest, createsANodeInPlaceOfTheOneADeleteTookOff) {
    expectWriteRows("MERGE (p:Person {name: 'Remy'}) WITH count(*) AS merged "
                    "MATCH (q:Person {name: 'Remy'}) DETACH DELETE q "
                    "WITH count(*) AS deleted MERGE (r:Person {name: 'Remy'}) RETURN r.name",
                    {{"Remy"}});

    expectRows("MATCH (q:Person) RETURN count(q)", {{"8"}});
}

TEST_F(MergeAfterIndexedWriteTest, matchesTheCreatedNodeASetRenamed) {
    expectWriteRows("CREATE (t:Tag {name: 'x'}) WITH count(*) AS created "
                    "MERGE (u:Tag {name: 'x'}) WITH count(*) AS merged "
                    "MATCH (w:Tag {name: 'x'}) SET w.name = 'y' "
                    "WITH count(*) AS renamed MERGE (v:Tag {name: 'y'}) RETURN v.name",
                    {{"y"}});

    expectRows("MATCH (t:Tag) RETURN t.name", {{"y"}});
}

TEST_F(MergeAfterIndexedWriteTest, createsANodeForTheNameASetTookOffTheCreatedNode) {
    expectWriteRows("CREATE (t:Tag {name: 'x'}) WITH count(*) AS created "
                    "MERGE (u:Tag {name: 'x'}) WITH count(*) AS merged "
                    "MATCH (w:Tag {name: 'x'}) SET w.name = 'y' "
                    "WITH count(*) AS renamed MERGE (v:Tag {name: 'x'}) RETURN v.name",
                    {{"x"}});

    expectRows("MATCH (t:Tag) RETURN t.name", {{"x"}, {"y"}});
}

TEST_F(MergeAfterIndexedWriteTest, createsANodeInPlaceOfTheCreatedOneADeleteTookOff) {
    expectWriteRows("CREATE (t:Tag {name: 'x'}) WITH count(*) AS created "
                    "MERGE (u:Tag {name: 'x'}) WITH count(*) AS merged "
                    "MATCH (w:Tag {name: 'x'}) DELETE w "
                    "WITH count(*) AS deleted MERGE (v:Tag {name: 'x'}) RETURN v.name",
                    {{"x"}});

    expectRows("MATCH (t:Tag) RETURN t.name", {{"x"}});
}

TEST_F(MergeAfterIndexedWriteTest, createsAnEdgeInPlaceOfTheCreatedOneADeleteTookOff) {
    expectWriteRows("MATCH (a:Person {name: 'Remy'}), (b:Person {name: 'Luc'}) CREATE (a)-[:MENTORS]->(b) "
                    "WITH a, b MERGE (a)-[:MENTORS]->(b) "
                    "WITH a, b MATCH (a)-[m:MENTORS]->(b) DELETE m "
                    "WITH a, b MERGE (a)-[n:MENTORS]->(b) RETURN count(n)",
                    {{"1"}});

    expectRows("MATCH ()-[m:MENTORS]->() RETURN count(m)", {{"1"}});
}

TEST_F(MergeAfterIndexedWriteTest, matchesTheNodeASetLeftTheNameOn) {
    expectWriteRows("MERGE (p:Person {name: 'Remy'}) WITH count(*) AS merged "
                    "MATCH (q:Person {name: 'Remy'}) SET q.age = 40 "
                    "WITH count(*) AS aged MERGE (r:Person {name: 'Remy'}) RETURN r.age",
                    {{"40"}});

    expectRows("MATCH (q:Person) RETURN count(q)", {{"8"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
