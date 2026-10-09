#include <gtest/gtest.h>

#include <string_view>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// FOREACH (x IN list | updates) runs its updates once per element of the list, for each
// row in flight, and leaves those rows as they were.
//
// simpledb holds 8 Persons and no Tag, Place, Leaf or Pair node.
class ForeachTest : public WriteQueryTest {
protected:
    void expectRejected(std::string_view query, std::string_view message) {
        ChangeID changeID;
        openChange(changeID);

        const QueryStatus status = runWrite(query, changeID);
        ASSERT_FALSE(status.isOk()) << "accepted: " << query;

        EXPECT_NE(status.getError().find(message), std::string::npos)
            << "query: " << query << "\nerror: " << status.getError();
    }
};

TEST_F(ForeachTest, writesOncePerElementOfEachRowAndPassesTheRowsThrough) {
    expectWriteRows("MATCH (p:Person) "
                    "FOREACH (x IN [1, 2] | CREATE (p)-[:VISITED]->(:Place {n: x})) "
                    "RETURN count(p)",
                    {{"8"}});

    expectRows("MATCH (:Person)-[:VISITED]->(place:Place) RETURN place.n, count(place)",
               {{"1", "8"}, {"2", "8"}});
}

TEST_F(ForeachTest, opensTheQuery) {
    applyWrite("FOREACH (x IN [1, 2, 3] | CREATE (:Tag {v: x}))");

    expectRows("MATCH (t:Tag) RETURN t.v", {{"1"}, {"2"}, {"3"}});
}

TEST_F(ForeachTest, walksAListCollectedFromTheRows) {
    applyWrite("MATCH (:Person)-[:INTERESTED_IN]->(i) "
               "WITH collect(i) AS interests "
               "FOREACH (i IN interests | SET i.liked = true)");

    expectRows("MATCH (i:Interest) WHERE i.liked = true RETURN count(i)", {{"10"}});
}

TEST_F(ForeachTest, walksTheNodesOfANamedPath) {
    applyWrite("MATCH path = (:Person {name: 'Remy'})-[:KNOWS_WELL]->(:Person) "
               "FOREACH (n IN nodes(path) | SET n.marked = true)");

    expectRows("MATCH (n) WHERE n.marked = true RETURN n.name", {{"Adam"}, {"Remy"}});
}

TEST_F(ForeachTest, deletesTheRelationshipsOfAPath) {
    applyWrite("MATCH path = (:Person {name: 'Remy'})-[:INTERESTED_IN]->() "
               "FOREACH (r IN relationships(path) | DELETE r)");

    expectRows("MATCH (:Person {name: 'Remy'})-[r:INTERESTED_IN]->() RETURN count(r)", {{"0"}});
    expectRows("MATCH (:Person)-[r:INTERESTED_IN]->() RETURN count(r)", {{"12"}});
}

TEST_F(ForeachTest, aClauseOfTheBodyReadsWhatAnEarlierOneCreated) {
    applyWrite("FOREACH (name IN ['a', 'b'] | "
               "CREATE (t:Tag {name: name}) "
               "CREATE (t)-[:NEXT]->(:Tag {name: name + '2'}))");

    expectRows("MATCH (a:Tag)-[:NEXT]->(b:Tag) RETURN a.name, b.name", {{"a", "a2"}, {"b", "b2"}});
}

TEST_F(ForeachTest, nestsAForeachInAForeach) {
    applyWrite("FOREACH (x IN [1, 2] | FOREACH (y IN [10, 20] | CREATE (:Pair {x: x, y: y})))");

    expectRows("MATCH (p:Pair) RETURN p.x, p.y", {{"1", "10"}, {"1", "20"}, {"2", "10"}, {"2", "20"}});
}

TEST_F(ForeachTest, writesUnderANodeCreatedBeforeIt) {
    applyWrite("CREATE (t:Tag {name: 'root'}) FOREACH (x IN [1, 2] | CREATE (t)-[:HAS]->(:Leaf {v: x}))");

    expectRows("MATCH (:Tag {name: 'root'})-[:HAS]->(l:Leaf) RETURN l.v", {{"1"}, {"2"}});
}

TEST_F(ForeachTest, runsForEveryRowEvenADuplicate) {
    applyWrite("UNWIND [1, 1] AS k FOREACH (x IN [k] | CREATE (:Tag {v: x}))");

    expectRows("MATCH (t:Tag) RETURN count(t)", {{"2"}});
}

TEST_F(ForeachTest, writesNothingOverANullList) {
    expectWriteRows("MATCH (p:Person) FOREACH (x IN null | CREATE (:Tag)) RETURN count(p)", {{"8"}});

    expectRows("MATCH (t:Tag) RETURN count(t)", {{"0"}});
}

TEST_F(ForeachTest, mergesOncePerDistinctElement) {
    applyWrite("FOREACH (x IN ['a', 'a', 'b'] | MERGE (:Tag {name: x}))");

    expectRows("MATCH (t:Tag) RETURN t.name", {{"a"}, {"b"}});
}

TEST_F(ForeachTest, mergesOntoANodeACreateBeforeItWrote) {
    applyWrite("CREATE (:Tag {name: 'a'}) FOREACH (x IN ['a', 'b'] | MERGE (:Tag {name: x}))");

    expectRows("MATCH (t:Tag) RETURN t.name", {{"a"}, {"b"}});
}

TEST_F(ForeachTest, setsThePropertyToTheLastElement) {
    applyWrite("MATCH (p:Person {name: 'Remy'}) FOREACH (x IN [1, 2, 3] | SET p.v = x)");

    expectRows("MATCH (p:Person {name: 'Remy'}) RETURN p.v", {{"3"}});
}

TEST_F(ForeachTest, removesAProperty) {
    applyWrite("MATCH (p:Person) FOREACH (x IN [1] | REMOVE p.age)");

    expectRows("MATCH (p:Person) WHERE p.age IS NOT NULL RETURN count(p)", {{"0"}});
}

TEST_F(ForeachTest, theBodyVariablesStayInsideIt) {
    expectRejected("FOREACH (x IN [1] | CREATE (t:Tag)) RETURN t", "'t'");
    expectRejected("FOREACH (x IN [1] | CREATE (:Tag)) RETURN x", "'x'");
}

TEST_F(ForeachTest, rejectsAVariableNamedLikeOneInScope) {
    expectRejected("WITH 1 AS x FOREACH (x IN [1, 2] | CREATE (:Tag {v: x}))",
                   "Variable 'x' is already declared");
}

TEST_F(ForeachTest, rejectsAReadingClauseAfterIt) {
    expectRejected("FOREACH (x IN [1] | CREATE (:Tag)) MATCH (n) RETURN count(n)",
                   "A reading clause cannot follow an updating clause");
}

TEST_F(ForeachTest, rejectsANonListLiteral) {
    expectRejected("FOREACH (x IN 5 | CREATE (:Tag))", "requires a list");
}

TEST_F(ForeachTest, rejectsAReadingClauseInTheBody) {
    expectRejected("FOREACH (x IN [1] | MATCH (n) CREATE (:Tag))", "syntax error");
}

TEST_F(ForeachTest, mergesOntoAnEdgeACreateBeforeItWrote) {
    applyWrite("MATCH (p:Person {name: 'Remy'}) CREATE (p)-[:LINK]->(:Tag {name: 't'}) "
               "FOREACH (x IN [1, 2] | MERGE (p)-[:LINK]->(:Tag {name: 't'}))");

    expectRows("MATCH (:Person)-[l:LINK]->(:Tag) RETURN count(l)", {{"1"}});
    expectRows("MATCH (t:Tag) RETURN count(t)", {{"1"}});
}

TEST_F(ForeachTest, mergesOntoAPropertyASetBeforeItWrote) {
    applyWrite("MATCH (p:Person {name: 'Remy'}) SET p.tag = 'z' "
               "FOREACH (x IN ['z'] | MERGE (:Person {tag: x}))");

    expectRows("MATCH (p:Person) WHERE p.tag = 'z' RETURN p.name", {{"Remy"}});
}
