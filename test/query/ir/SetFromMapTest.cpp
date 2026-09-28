#include <gtest/gtest.h>

#include "QueryStatus.h"
#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// SET n = m and SET n += m, m a map computed at run time: its keys are only known row by
// row. Remy is 32 in simpledb.
class SetFromMapTest : public WriteQueryTest {
protected:
    void expectWriteError(std::string_view query, std::string_view error) {
        ChangeID changeID;
        openChange(changeID);

        const QueryStatus status = runWrite(query, changeID);
        ASSERT_FALSE(status.isOk());
        EXPECT_NE(status.getError().find(error), std::string::npos) << status.getError();
    }
};

TEST_F(SetFromMapTest, createsNodesFromUnwoundMaps) {
    applyWrite("UNWIND [{k: 1, v: 'a'}, {k: 2, v: 'b'}] AS m CREATE (n:X) SET n = m");

    expectRows("MATCH (n:X) RETURN n.k, n.v", {{"1", "a"}, {"2", "b"}});
}

TEST_F(SetFromMapTest, readsBackWhatAMapWrote) {
    expectWriteRows("UNWIND [{name: 'Zed', age: 40}] AS m CREATE (n:X) SET n = m RETURN n.name, n.age",
                    {{"Zed", "40"}});
}

TEST_F(SetFromMapTest, addsTheEntriesOfAMap) {
    expectWriteRows("MATCH (n:Person {name: 'Remy'}) UNWIND [{age: 33}] AS m SET n += m RETURN n.name, n.age",
                    {{"Remy", "33"}});
}

TEST_F(SetFromMapTest, replacesThePropertiesWithTheEntriesOfAMap) {
    expectWriteRows("MATCH (n:Person {name: 'Remy'}) UNWIND [{age: 33}] AS m SET n = m RETURN n.name, n.age",
                    {{"null", "33"}});
}

TEST_F(SetFromMapTest, removesAPropertyANullEntryNames) {
    expectWriteRows("MATCH (n:Person {name: 'Remy'}) UNWIND [{age: null}] AS m SET n += m RETURN n.name, n.age",
                    {{"Remy", "null"}});
}

TEST_F(SetFromMapTest, addsNothingFromANullMap) {
    expectWriteRows("MATCH (n:Person {name: 'Remy'}) SET n += null RETURN n.name, n.age",
                    {{"Remy", "32"}});
}

TEST_F(SetFromMapTest, replacesThePropertiesWithANullMap) {
    expectWriteRows("MATCH (n:Person {name: 'Remy'}) SET n = null RETURN n.name, n.age",
                    {{"null", "null"}});
}

TEST_F(SetFromMapTest, replacesRowByRow) {
    applyWrite("MATCH (n:Person {name: 'Remy'}) UNWIND [{a1: 1}, {a2: 2}] AS m SET n = m");

    expectRows("MATCH (n:Person {a2: 2}) RETURN n.a1, n.a2, n.name", {{"null", "2", "null"}});
}

TEST_F(SetFromMapTest, replacesACreatedNodeRowByRow) {
    applyWrite("CREATE (n:X) WITH n UNWIND [{a1: 1}, {a2: 2}] AS m SET n = m");

    expectRows("MATCH (n:X) RETURN n.a1, n.a2", {{"null", "2"}});
}

TEST_F(SetFromMapTest, writesTheEntriesOfAMapToAnEdge) {
    applyWrite("CREATE (:X)-[e:R {w: 1}]->(:X) WITH e UNWIND [{w: 3, z: 'z'}] AS m SET e += m");

    expectRows("MATCH ()-[e:R]->() RETURN e.w, e.z", {{"3", "z"}});
}

TEST_F(SetFromMapTest, writesListAndMapEntries) {
    applyWrite("UNWIND [{tags: [1, 2], meta: {x: 1}}] AS m CREATE (n:X) SET n = m");

    expectRows("MATCH (n:X) RETURN size(n.tags), n.meta", {{"2", "{x: 1}"}});
}

TEST_F(SetFromMapTest, mergesAndSetsFromAMap) {
    expectWriteRows("UNWIND [{name: 'Zed', age: 50}] AS m MERGE (n:Person {name: 'Zed'}) ON CREATE SET n += m RETURN n.name, n.age",
                    {{"Zed", "50"}});
}

TEST_F(SetFromMapTest, mergeReadsTheKeyAMapRewrote) {
    applyWrite("UNWIND [{name: 'Zed'}, {name: 'Zed'}] AS m MERGE (n:Person {name: 'Zed1'}) ON CREATE SET n += m");

    expectRows("MATCH (n:Person {name: 'Zed'}) RETURN count(n)", {{"2"}});
}

TEST_F(SetFromMapTest, laterMatchReadsWhatAMapWrote) {
    expectWriteRows("MATCH (n:Person {name: 'Remy'}) UNWIND [{age: 99}] AS m SET n += m "
                    "WITH count(*) AS c MATCH (p:Person {age: 99}) RETURN p.name",
                    {{"Remy"}});
}

TEST_F(SetFromMapTest, valueReadsWhatAnEarlierRowWrote) {
    expectWriteRows("UNWIND [1, 2] AS i MATCH (n:Person {name: 'Remy'}) SET n += [{age: n.age + 1}][0] RETURN n.age",
                    {{"34"}, {"34"}});
}

TEST_F(SetFromMapTest, writesMapsAmongOtherValues) {
    applyWrite("UNWIND [{k: 1}, null, {k: 2}] AS m CREATE (n:X) SET n = m");

    expectRows("MATCH (n:X) RETURN n.k", {{"1"}, {"null"}, {"2"}});
}

TEST_F(SetFromMapTest, rejectsAValueHoldingNoMap) {
    expectWriteError("MATCH (n:Person {name: 'Remy'}) UNWIND [{age: 1}, 5] AS m SET n += m", "holds no map");
}

TEST_F(SetFromMapTest, rejectsAnEntryOfAnotherType) {
    expectWriteError("MATCH (n:Person {name: 'Remy'}) UNWIND [{age: 'old'}] AS m SET n += m", "age");
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
