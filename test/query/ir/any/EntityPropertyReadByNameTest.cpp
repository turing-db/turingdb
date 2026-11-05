#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// A property read by name off a cell holding a node or an edge answers the value the
// newest write gave it: a write of the query itself, else of the newest commit.
class EntityPropertyReadByNameTest : public WriteQueryTest {
};

TEST_F(EntityPropertyReadByNameTest, readsWhatTheNewestCommitWrote) {
    applyWrite("MATCH (n:Person {name: 'Remy'}) SET n.age = 40, n.nickname = 'R'");
    applyWrite("MATCH (n:Person {name: 'Adam'}) SET n.age = null");

    expectRows("MATCH (n:Person) WHERE n.name IN ['Remy', 'Adam'] WITH [n, 1][0] AS x RETURN x.name, x.age, x.nickname",
               {{"Remy", "40", "R"}, {"Adam", "null", "null"}});
}

TEST_F(EntityPropertyReadByNameTest, readsANodeANewerCommitCreated) {
    applyWrite("CREATE (:T {v: 1})");

    expectRows("MATCH (t:T) WITH [t, 1][0] AS x RETURN x.v, x.name", {{"1", "null"}});
}

TEST_F(EntityPropertyReadByNameTest, readsWhatTheQueryWroteToANode) {
    expectWriteRows("MATCH (n:Person {name: 'Remy'}) SET n.age = 40, n.nickname = 'R' "
                    "WITH [n, 1][0] AS x RETURN x.name, x.age, x.nickname",
                    {{"Remy", "40", "R"}});
    expectWriteRows("MATCH (n:Person {name: 'Remy'}) SET n.age = null WITH [n, 1][0] AS x RETURN x.age",
                    {{"null"}});
}

TEST_F(EntityPropertyReadByNameTest, readsANodeTheQueryCreated) {
    expectWriteRows("CREATE (n:T {v: 1, w: 2}) SET n.v = 3, n.w = null WITH [n, 1][0] AS x RETURN x.v, x.w, x.name",
                    {{"3", "null", "null"}});
}

TEST_F(EntityPropertyReadByNameTest, readsAnEdgeEveryWriteReached) {
    applyWrite("MATCH (:Person {name: 'Remy'})-[e:KNOWS_WELL]->(:Person {name: 'Adam'}) SET e.duration = 30");

    expectRows("MATCH (:Person {name: 'Remy'})-[e:KNOWS_WELL]->(:Person {name: 'Adam'}) "
               "WITH [e, 1][0] AS x RETURN x.name, x.duration",
               {{"Remy -> Adam", "30"}});
    expectWriteRows("MATCH (:Person {name: 'Remy'})-[e:KNOWS_WELL]->(:Person {name: 'Adam'}) SET e.duration = null "
                    "WITH [e, 1][0] AS x RETURN x.duration",
                    {{"null"}});
    expectWriteRows("CREATE (:T)-[e:R {w: 1}]->(:T) WITH [e, 1][0] AS x RETURN x.w, x.name", {{"1", "null"}});
}

TEST_F(EntityPropertyReadByNameTest, readsNullForANameNoPropertyHas) {
    expectRows("MATCH (n:Person {name: 'Remy'}) WITH [n, 1][0] AS x RETURN x.nope", {{"null"}});
    expectRows("MATCH (n:Person {name: 'Remy'}) WITH [n, 1] AS l, ['nope', 1][0] AS k RETURN l[0][k]", {{"null"}});
}

TEST_F(EntityPropertyReadByNameTest, readsByAKeyEachRowHolds) {
    expectRows("MATCH (n:Person {name: 'Remy'}) UNWIND [[n, 'name'], [n, 'age'], [{age: 1}, 'age']] AS p RETURN p[0][p[1]]",
               {{"Remy"}, {"32"}, {"1"}});
}
