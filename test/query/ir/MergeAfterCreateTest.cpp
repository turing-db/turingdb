#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// A MERGE behind a write of the same query that created what its pattern asks for. The
// graph holds nothing the query wrote until the commit, and the MERGE has to match it
// there rather than write a second copy, whichever clause wrote it.
//
// simpledb holds no Tag node and no MENTORS edge.
class MergeAfterCreateTest : public WriteQueryTest {
};

TEST_F(MergeAfterCreateTest, matchesTheNodeACreateWrote) {
    expectWriteRows("CREATE (t:Tag {name: 'x'}) WITH count(*) AS created MERGE (u:Tag {name: 'x'}) RETURN u.name",
                    {{"x"}});

    expectRows("MATCH (t:Tag) RETURN count(t)", {{"1"}});
}

TEST_F(MergeAfterCreateTest, matchesTheNodeACreateWroteInTheSamePart) {
    expectWriteRows("CREATE (t:Tag {name: 'x'}) MERGE (u:Tag {name: 'x'}) RETURN u.name", {{"x"}});

    expectRows("MATCH (t:Tag) RETURN count(t)", {{"1"}});
}

TEST_F(MergeAfterCreateTest, matchesEveryNodeACreateWroteWithTheValue) {
    expectWriteRows("UNWIND ['x', 'y', 'x'] AS n CREATE (t:Tag {name: n}) "
                    "WITH count(*) AS created MERGE (u:Tag {name: 'x'}) RETURN count(u)",
                    {{"2"}});

    expectRows("MATCH (t:Tag) RETURN count(t)", {{"3"}});
}

TEST_F(MergeAfterCreateTest, matchesANodeCarryingMoreLabels) {
    expectWriteRows("CREATE (t:Tag:Other {name: 'x'}) WITH count(*) AS created MERGE (u:Tag {name: 'x'}) RETURN u.name",
                    {{"x"}});

    expectRows("MATCH (t:Tag) RETURN count(t)", {{"1"}});
}

TEST_F(MergeAfterCreateTest, matchesTheNodeAMergeOfOtherLabelsWrote) {
    expectWriteRows("MERGE (t:Tag:Other {name: 'x'}) WITH count(*) AS merged MERGE (u:Tag {name: 'x'}) RETURN u.name",
                    {{"x"}});

    expectRows("MATCH (t:Tag) RETURN count(t)", {{"1"}});
}

TEST_F(MergeAfterCreateTest, createsANodeForAnotherValue) {
    expectWriteRows("CREATE (t:Tag {name: 'x'}) WITH count(*) AS created MERGE (u:Tag {name: 'y'}) RETURN u.name",
                    {{"y"}});

    expectRows("MATCH (t:Tag) RETURN t.name", {{"x"}, {"y"}});
}

TEST_F(MergeAfterCreateTest, matchesTheCreatedNodeByTheValueASetWrote) {
    expectWriteRows("CREATE (t:Tag {name: 'x'}) WITH t SET t.name = 'y' "
                    "WITH count(*) AS renamed MERGE (u:Tag {name: 'y'}) RETURN u.name",
                    {{"y"}});

    expectRows("MATCH (t:Tag) RETURN count(t)", {{"1"}});
}

TEST_F(MergeAfterCreateTest, createsANodeInPlaceOfTheCreatedOneADeleteTookOff) {
    expectWriteRows("CREATE (t:Tag {name: 'x'}) WITH t DELETE t "
                    "WITH count(*) AS deleted MERGE (u:Tag {name: 'x'}) RETURN u.name",
                    {{"x"}});

    expectRows("MATCH (t:Tag) RETURN count(t)", {{"1"}});
}

TEST_F(MergeAfterCreateTest, bindsTheNodeAnEarlierRowOfTheMergeWrote) {
    expectWriteRows("UNWIND ['x', 'x'] AS n MERGE (t:Tag {name: n}) RETURN t.name", {{"x"}, {"x"}});

    expectRows("MATCH (t:Tag) RETURN count(t)", {{"1"}});
}

TEST_F(MergeAfterCreateTest, matchesTheEdgeACreateWrote) {
    expectWriteRows("MATCH (a:Person {name: 'Remy'}), (b:Person {name: 'Luc'}) CREATE (a)-[:MENTORS]->(b) "
                    "WITH a, b MERGE (a)-[m:MENTORS]->(b) RETURN count(m)",
                    {{"1"}});

    expectRows("MATCH ()-[m:MENTORS]->() RETURN count(m)", {{"1"}});
}

TEST_F(MergeAfterCreateTest, matchesTheEdgeACreateWroteWithTheValue) {
    expectWriteRows("MATCH (a:Person {name: 'Remy'}), (b:Person {name: 'Luc'}) CREATE (a)-[:MENTORS {since: 2020}]->(b) "
                    "WITH a, b MERGE (a)-[m:MENTORS {since: 2020}]->(b) RETURN m.since",
                    {{"2020"}});

    expectRows("MATCH ()-[m:MENTORS]->() RETURN count(m)", {{"1"}});
}

TEST_F(MergeAfterCreateTest, createsAnEdgeForAnotherValue) {
    expectWriteRows("MATCH (a:Person {name: 'Remy'}), (b:Person {name: 'Luc'}) CREATE (a)-[:MENTORS {since: 2020}]->(b) "
                    "WITH a, b MERGE (a)-[m:MENTORS {since: 2021}]->(b) RETURN m.since",
                    {{"2021"}});

    expectRows("MATCH ()-[m:MENTORS]->() RETURN m.since", {{"2020"}, {"2021"}});
}

TEST_F(MergeAfterCreateTest, matchesThePathACreateWrote) {
    expectWriteRows("CREATE (a:Tag {name: 'a'})-[:TAGS]->(b:Tag {name: 'b'}) "
                    "WITH count(*) AS created MERGE (x:Tag {name: 'a'})-[:TAGS]->(y:Tag {name: 'b'}) RETURN x.name, y.name",
                    {{"a", "b"}});
    expectWriteRows("CREATE (a:Tag {name: 'c'})-[:TAGS]->(b:Tag {name: 'd'}) "
                    "WITH count(*) AS created MERGE (y:Tag {name: 'd'})<-[:TAGS]-(x:Tag {name: 'c'}) RETURN x.name, y.name",
                    {{"c", "d"}});

    expectRows("MATCH (t:Tag) RETURN count(t)", {{"4"}});
    expectRows("MATCH ()-[e:TAGS]->() RETURN count(e)", {{"2"}});
}

TEST_F(MergeAfterCreateTest, bindsTheEdgeAnEarlierRowOfTheMergeWrote) {
    expectWriteRows("MATCH (a:Person {name: 'Remy'}), (b:Person {name: 'Luc'}) UNWIND [1, 2] AS i "
                    "MERGE (a)-[m:MENTORS]->(b) RETURN i",
                    {{"1"}, {"2"}});

    expectRows("MATCH ()-[m:MENTORS]->() RETURN count(m)", {{"1"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
