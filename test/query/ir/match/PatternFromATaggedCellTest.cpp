#include <gtest/gtest.h>

#include <string>
#include <string_view>
#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

// A pattern may name a variable whose type is known per row only: a value read out of a map,
// a mixed list or a CASE. Each row is checked to hold a node, or a relationship where the
// pattern names one: a null matches nothing and any other value is the type error the row
// raises, as Neo4j does.
class PatternFromATaggedCellTest : public CallV3Test {
protected:
    void expectRows(std::string_view query, const std::vector<StringRowSink::Row>& expected) {
        StringRowSink sink;
        runQuery(query, sink);

        std::vector<StringRowSink::Row> rows;
        sink.sortedRows(rows);

        EXPECT_EQ(rows, expected) << query;
    }

    void expectWrittenRows(std::string_view write,
                           std::string_view read,
                           const std::vector<StringRowSink::Row>& expected) {
        runWrite(write);
        expectRows(read, expected);
    }
};

TEST_F(PatternFromATaggedCellTest, startsAMatchFromANodeReadByAComputedKey) {
    expectRows("MATCH (n:Person {name: 'Remy'}) WITH {person: n} AS m, 'person' AS k WITH m[k] AS p "
               "MATCH (p)-[:KNOWS_WELL]->(f) RETURN f.name",
               {{"Adam"}});
}

TEST_F(PatternFromATaggedCellTest, startsAMatchFromTheNodeAMixedListHolds) {
    expectRows("MATCH (n:Person {name: 'Remy'}) WITH [n, 1] AS l WITH l[0] AS p "
               "MATCH (p)-[:KNOWS_WELL]->(f) RETURN f.name",
               {{"Adam"}});
}

TEST_F(PatternFromATaggedCellTest, startsAMatchFromTheNodeEachRowHolds) {
    expectRows("MATCH (n:Person) WITH {person: n} AS m WITH m.person AS p "
               "MATCH (p)-[:KNOWS_WELL]->(f) RETURN p.name, f.name, p",
               {{"Adam", "Remy", "1"}, {"Remy", "Adam", "0"}});
}

TEST_F(PatternFromATaggedCellTest, matchesNothingFromANull) {
    expectRows("WITH null AS p MATCH (p)-[:KNOWS_WELL]->(f) RETURN f.name", {});
}

TEST_F(PatternFromATaggedCellTest, matchesNothingFromAKeyTheMapDoesNotHold) {
    expectRows("MATCH (n:Person {name: 'Remy'}) WITH {person: n} AS m WITH m.nobody AS p "
               "MATCH (p)-[:KNOWS_WELL]->(f) RETURN f.name",
               {});
}

TEST_F(PatternFromATaggedCellTest, matchesNothingFromTheRowsHoldingANull) {
    expectRows("MATCH (n:Person) WITH {person: CASE WHEN n.name = 'Remy' THEN n ELSE null END} AS m "
               "WITH m.person AS p MATCH (p)-[:KNOWS_WELL]->(f) RETURN f.name",
               {{"Adam"}});
}

TEST_F(PatternFromATaggedCellTest, raisesOnAValueThatIsNotANode) {
    runQueryExpectingError("WITH {person: 'Remy'} AS m WITH m.person AS p MATCH (p)-[:KNOWS_WELL]->(f) RETURN f.name",
                           "A pattern reads a node");
}

TEST_F(PatternFromATaggedCellTest, raisesOnARelationshipWhereANodeIsExpected) {
    runQueryExpectingError("MATCH (:Person {name: 'Remy'})-[r:KNOWS_WELL]->() WITH {rel: r} AS m WITH m.rel AS p "
                           "MATCH (p)-[:KNOWS_WELL]->(f) RETURN f.name",
                           "A pattern reads a node");
}

TEST_F(PatternFromATaggedCellTest, raisesOnTheRowsACaseFillsWithAString) {
    runQueryExpectingError("MATCH (n:Person) WITH CASE WHEN n.name = 'Remy' THEN n ELSE 'none' END AS p "
                           "MATCH (p)-[:KNOWS_WELL]->(f) RETURN f.name",
                           "A pattern reads a node");
}

TEST_F(PatternFromATaggedCellTest, rejectsAVariableHoldingAString) {
    runQueryExpectingError("MATCH (n:Person) WITH n.name AS p MATCH (p)-[:KNOWS_WELL]->(f) RETURN f.name",
                           "Variable 'p' is already declared with type 'String'");
}

TEST_F(PatternFromATaggedCellTest, checksTheLabelsOfTheNode) {
    expectRows("MATCH (n:Person {name: 'Remy'}) WITH {person: n} AS m WITH m.person AS p "
               "MATCH (p:Founder)-[:KNOWS_WELL]->(f) RETURN f.name",
               {{"Adam"}});
}

TEST_F(PatternFromATaggedCellTest, matchesNothingFromANodeMissingTheLabel) {
    expectRows("MATCH (n:Person {name: 'Remy'}) WITH {person: n} AS m WITH m.person AS p "
               "MATCH (p:Bioinformatics)-[:KNOWS_WELL]->(f) RETURN f.name",
               {});
}

TEST_F(PatternFromATaggedCellTest, checksAPropertyOfTheNode) {
    expectRows("MATCH (n:Person) WITH {person: n} AS m WITH m.person AS p "
               "MATCH (p {name: 'Adam'})-[:KNOWS_WELL]->(f) RETURN f.name",
               {{"Remy"}});
}

TEST_F(PatternFromATaggedCellTest, endsAMatchAtTheNode) {
    expectRows("MATCH (n:Person {name: 'Remy'}) WITH {person: n} AS m WITH m.person AS p "
               "MATCH (f)-[:KNOWS_WELL]->(p) RETURN f.name",
               {{"Adam"}, {"Ghosts"}});
}

TEST_F(PatternFromATaggedCellTest, matchesTheNodeAlone) {
    expectRows("MATCH (n:Person {name: 'Remy'}) WITH {person: n} AS m WITH m.person AS p "
               "MATCH (p) RETURN p.name",
               {{"Remy"}});
}

TEST_F(PatternFromATaggedCellTest, closesACycleOnTheNode) {
    expectRows("MATCH (n:Person {name: 'Remy'}) WITH {person: n} AS m WITH m.person AS p "
               "MATCH (p)-[:KNOWS_WELL]->(f)-[:KNOWS_WELL]->(p) RETURN f.name",
               {{"Adam"}});
}

TEST_F(PatternFromATaggedCellTest, readsTheNodeInTheWhereOfTheWithThatPublishedIt) {
    expectRows("MATCH (n:Person) WITH {person: n} AS m WITH m.person AS p WHERE p.name = 'Remy' "
               "MATCH (p)-[:KNOWS_WELL]->(f) RETURN f.name, p.name",
               {{"Adam", "Remy"}});
}

TEST_F(PatternFromATaggedCellTest, joinsASecondMatchOnTheNode) {
    expectRows("MATCH (n:Person {name: 'Remy'}) WITH {person: n} AS m WITH m.person AS p "
               "MATCH (p)-[:KNOWS_WELL]->(f) MATCH (p)-[:INTERESTED_IN]->(i) RETURN count(i)",
               {{"3"}});
}

TEST_F(PatternFromATaggedCellTest, carriesTheNodeThroughAWith) {
    expectRows("MATCH (n:Person {name: 'Remy'}) WITH {person: n} AS m WITH m.person AS p "
               "MATCH (p)-[:KNOWS_WELL]->(f) WITH p MATCH (p)-[:INTERESTED_IN]->(i) RETURN count(i)",
               {{"3"}});
}

TEST_F(PatternFromATaggedCellTest, returnsTheNodeOnceUnderAWildcard) {
    StringRowSink sink;
    runQuery("MATCH (n:Person {name: 'Remy'}) WITH {person: n} AS m WITH m.person AS p "
             "MATCH (p)-[:KNOWS_WELL]->(f) RETURN *",
             sink);

    EXPECT_EQ(sink.getNames(), (std::vector<std::string> {"p", "f"}));
    EXPECT_EQ(sink.getRows(), (std::vector<StringRowSink::Row> {{"0", "1"}}));
}

TEST_F(PatternFromATaggedCellTest, startsAMatchFromTheNodeAnUnwoundMapHolds) {
    expectRows("MATCH (n:Person {name: 'Remy'}) UNWIND [{person: n}] AS m WITH m.person AS p "
               "MATCH (p)-[:KNOWS_WELL]->(f) RETURN f.name",
               {{"Adam"}});
}

TEST_F(PatternFromATaggedCellTest, startsAMatchFromANodeUnwoundOutOfAListAMapHolds) {
    expectRows("MATCH (n:Person {name: 'Remy'}) WITH {people: [n]} AS m UNWIND m.people AS x "
               "MATCH (x)-[:KNOWS_WELL]->(y) RETURN y.name",
               {{"Adam"}});
}

TEST_F(PatternFromATaggedCellTest, startsAMatchFromANodeACallSubqueryReturns) {
    expectRows("CALL { MATCH (n:Person {name: 'Remy'}) WITH {person: n} AS m RETURN m.person AS p } "
               "MATCH (p)-[:KNOWS_WELL]->(y) RETURN y.name",
               {{"Adam"}});
}

TEST_F(PatternFromATaggedCellTest, matchesNothingFromAnUnwoundNull) {
    expectRows("UNWIND [null] AS p MATCH (p)-[:KNOWS_WELL]->(f) RETURN f.name", {});
}

TEST_F(PatternFromATaggedCellTest, keepsTheRowsAnOptionalMatchMisses) {
    expectRows("MATCH (n:Person) WHERE n.name IN ['Remy', 'Luc'] WITH {person: n} AS m WITH m.person AS p "
               "OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f) RETURN p.name, f.name",
               {{"Luc", "null"}, {"Remy", "Adam"}});
}

TEST_F(PatternFromATaggedCellTest, keepsANullThroughAnOptionalMatch) {
    expectRows("WITH null AS p OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f) RETURN p, f", {{"null", "null"}});
}

TEST_F(PatternFromATaggedCellTest, testsAnExistsSubqueryFromTheNode) {
    expectRows("MATCH (n:Person) WHERE n.name IN ['Remy', 'Luc'] WITH {person: n} AS m WITH m.person AS p "
               "RETURN p.name, EXISTS { (p)-[:KNOWS_WELL]->() }",
               {{"Luc", "false"}, {"Remy", "true"}});
}

TEST_F(PatternFromATaggedCellTest, countsFromTheNode) {
    expectRows("MATCH (n:Person) WHERE n.name IN ['Remy', 'Luc'] WITH {person: n} AS m WITH m.person AS p "
               "RETURN p.name, COUNT { (p)-[:INTERESTED_IN]->() }",
               {{"Luc", "2"}, {"Remy", "3"}});
}

TEST_F(PatternFromATaggedCellTest, collectsAPatternComprehensionFromTheNode) {
    expectRows("MATCH (n:Person {name: 'Remy'}) WITH {person: n} AS m WITH m.person AS p "
               "RETURN [(p)-[:KNOWS_WELL]->(f) | f.name]",
               {{"Adam"}});
}

TEST_F(PatternFromATaggedCellTest, filtersOnAPatternFromTheNode) {
    expectRows("MATCH (n:Person) WHERE n.name IN ['Remy', 'Luc'] WITH {person: n} AS m WITH m.person AS p "
               "WHERE (p)-[:KNOWS_WELL]->() RETURN p.name",
               {{"Remy"}});
}

TEST_F(PatternFromATaggedCellTest, importsTheNodeIntoACallSubquery) {
    expectRows("MATCH (n:Person {name: 'Remy'}) WITH {person: n} AS m WITH m.person AS p "
               "CALL (p) { MATCH (p)-[:KNOWS_WELL]->(f) RETURN f } RETURN f.name",
               {{"Adam"}});
}

TEST_F(PatternFromATaggedCellTest, matchesTheRelationshipAMapHolds) {
    expectRows("MATCH (:Person {name: 'Remy'})-[r:KNOWS_WELL]->() WITH {rel: r} AS m WITH m.rel AS r "
               "MATCH (x)-[r]->(y) RETURN x.name, y.name",
               {{"Remy", "Adam"}});
}

TEST_F(PatternFromATaggedCellTest, matchesNothingFromANullRelationship) {
    expectRows("WITH null AS r MATCH (x)-[r]->(y) RETURN x.name", {});
}

TEST_F(PatternFromATaggedCellTest, raisesOnANodeWhereARelationshipIsExpected) {
    runQueryExpectingError("MATCH (n:Person {name: 'Remy'}) WITH {rel: n} AS m WITH m.rel AS r "
                           "MATCH (x)-[r]->(y) RETURN x.name",
                           "A pattern reads a relationship");
}

TEST_F(PatternFromATaggedCellTest, createsARelationshipFromTheNode) {
    expectWrittenRows("MATCH (n:Person {name: 'Remy'}) WITH {person: n} AS m WITH m.person AS p "
                      "CREATE (p)-[:LIKES]->(:Thing {name: 'Kite'})",
                      "MATCH (p)-[:LIKES]->(t:Thing) RETURN p.name, t.name",
                      {{"Remy", "Kite"}});
}

TEST_F(PatternFromATaggedCellTest, createsARelationshipToTheNode) {
    expectWrittenRows("MATCH (n:Person {name: 'Remy'}) WITH {person: n} AS m WITH m.person AS p "
                      "CREATE (:Thing {name: 'Kite'})-[:LIKES]->(p)",
                      "MATCH (t:Thing)-[:LIKES]->(p) RETURN t.name, p.name",
                      {{"Kite", "Remy"}});
}

TEST_F(PatternFromATaggedCellTest, createsARelationshipFromANodeUnwoundOutOfAListAMapHolds) {
    expectWrittenRows("MATCH (n:Person {name: 'Remy'}) WITH {people: [n]} AS m UNWIND m.people AS x "
                      "CREATE (x)-[:LIKES]->(:Thing {name: 'Kite'})",
                      "MATCH (p)-[:LIKES]->(t:Thing) RETURN p.name, t.name",
                      {{"Remy", "Kite"}});
}

TEST_F(PatternFromATaggedCellTest, raisesOnCreatingARelationshipFromANull) {
    runWriteExpectingError("WITH null AS p CREATE (p)-[:LIKES]->(:Thing {name: 'Kite'})",
                           "Cannot create an edge from a null node");
}

TEST_F(PatternFromATaggedCellTest, raisesOnCreatingARelationshipFromAValueThatIsNotANode) {
    runWriteExpectingError("WITH {person: 'Remy'} AS m WITH m.person AS p CREATE (p)-[:LIKES]->(:Thing {name: 'Kite'})",
                           "A pattern reads a node");
}

TEST_F(PatternFromATaggedCellTest, mergesARelationshipFromTheNode) {
    expectWrittenRows("MATCH (n:Person {name: 'Remy'}) WITH {person: n} AS m WITH m.person AS p "
                      "MERGE (p)-[:LIKES]->(:Thing {name: 'Kite'})",
                      "MATCH (p)-[:LIKES]->(t:Thing) RETURN p.name, t.name",
                      {{"Remy", "Kite"}});
}

TEST_F(PatternFromATaggedCellTest, mergesTheNodeAlone) {
    expectWrittenRows("MATCH (n:Person) WITH {person: n} AS m WITH m.person AS p MERGE (p)",
                      "MATCH (p:Person) RETURN count(p)",
                      {{"8"}});
}

TEST_F(PatternFromATaggedCellTest, raisesOnMergingARelationshipFromANull) {
    runWriteExpectingError("WITH null AS p MERGE (p)-[:LIKES]->(:Thing {name: 'Kite'})",
                           "Cannot merge a pattern on a null node");
}

TEST_F(PatternFromATaggedCellTest, testsASubqueryReadingTheValueBeforeTheMatchCastingIt) {
    expectRows("MATCH (n:Person {name: 'Remy'}) WITH {person: n} AS m WITH m.person AS p "
               "MATCH (a:Person {name: 'Luc'}) WHERE EXISTS { (p)-[:KNOWS_WELL]->() } "
               "MATCH (p)-[:KNOWS_WELL]->(f) RETURN a.name, f.name",
               {{"Luc", "Adam"}});
}

TEST_F(PatternFromATaggedCellTest, pairsANodeUnwoundBesideABoundNodeWithTheMatch) {
    expectRows("MATCH (a:Person {name: 'Remy'}) WITH a, {people: [a]} AS m UNWIND m.people AS x "
               "MATCH (x)-[:KNOWS_WELL]->(y) RETURN a.name, y.name",
               {{"Remy", "Adam"}});
}

TEST_F(PatternFromATaggedCellTest, castsTheValueInTwoSubqueries) {
    expectRows("MATCH (n:Person {name: 'Remy'}) WITH {person: n} AS m WITH m.person AS p "
               "RETURN EXISTS { (p)-[:KNOWS_WELL]->() }, COUNT { (p)-[:INTERESTED_IN]->() }",
               {{"true", "3"}});
}

TEST_F(PatternFromATaggedCellTest, startsTwoPatternsOfOneMatchFromTheNode) {
    expectRows("MATCH (n:Person {name: 'Remy'}) WITH {person: n} AS m WITH m.person AS p "
               "MATCH (p)-[:KNOWS_WELL]->(f), (p)-[:INTERESTED_IN]->(i) RETURN count(i)",
               {{"3"}});
}

TEST_F(PatternFromATaggedCellTest, matchesARelationshipUnwoundOutOfAListAMapHolds) {
    expectRows("MATCH (:Person {name: 'Remy'})-[r:KNOWS_WELL]->() WITH {rels: [r]} AS m UNWIND m.rels AS e "
               "MATCH (x)-[e]->(y) RETURN x.name, y.name",
               {{"Remy", "Adam"}});
}

TEST_F(PatternFromATaggedCellTest, matchesNothingFromARelationshipUnwoundOutOfANull) {
    expectRows("UNWIND [null] AS r MATCH (x)-[r]->(y) RETURN x.name", {});
}

TEST_F(PatternFromATaggedCellTest, collectsAPatternComprehensionFromEachElementOfAMixedList) {
    expectRows("MATCH (n:Person {name: 'Remy'}) WITH [n, null] AS l "
               "RETURN [x IN l | size([(x)-[:KNOWS_WELL]->(f) | f])]",
               {{"1, 0"}});
}

TEST_F(PatternFromATaggedCellTest, setsAPropertyFromAPatternComprehensionOverTheNode) {
    expectWrittenRows("MATCH (n:Person {name: 'Remy'}) WITH {person: n} AS m WITH m.person AS p "
                      "MATCH (q:Person {name: 'Luc'}) SET q.friends = size([(p)-[:KNOWS_WELL]->(f) | f.name])",
                      "MATCH (q:Person {name: 'Luc'}) RETURN q.friends",
                      {{"1"}});
}

TEST_F(PatternFromATaggedCellTest, searchesTheShortestPathFromTheNode) {
    expectRows("MATCH (n:Person {name: 'Remy'}) WITH {person: n} AS m WITH m.person AS a "
               "MATCH (b:Person {name: 'Adam'}) SHORTESTPATH(a, b, duration, d, path) RETURN d",
               {{"20"}});
}

// Disabled: db.shortest_path takes a node column alone, and turns away the column of none a
// list index leaves for lowering to resolve.
TEST_F(PatternFromATaggedCellTest, DISABLED_searchesTheShortestPathFromANodeAListHolds) {
    expectRows("MATCH (n:Person {name: 'Remy'}) WITH [n] AS l WITH l[0] AS a "
               "MATCH (b:Person {name: 'Adam'}) SHORTESTPATH(a, b, duration, d, path) RETURN d",
               {{"20"}});
}

TEST_F(PatternFromATaggedCellTest, setsAPropertyOfTheNodeAMatchReadsOutOfTheValue) {
    expectWrittenRows("MATCH (n:Person {name: 'Remy'}) WITH {person: n} AS m WITH m.person AS p "
                      "MATCH (p) SET p.mood = 'happy'",
                      "MATCH (p:Person {mood: 'happy'}) RETURN p.name",
                      {{"Remy"}});
}

// Disabled: SET, REMOVE and DELETE name no pattern, so nothing says whether the row holds a
// node or a relationship, and the analyzer turns the variable away (TUR-370).
TEST_F(PatternFromATaggedCellTest, DISABLED_setsAPropertyOfTheNodeTheValueHolds) {
    expectWrittenRows("MATCH (n:Person {name: 'Remy'}) WITH {person: n} AS m WITH m.person AS p SET p.mood = 'happy'",
                      "MATCH (p:Person {mood: 'happy'}) RETURN p.name",
                      {{"Remy"}});
}

TEST_F(PatternFromATaggedCellTest, DISABLED_removesAPropertyOfTheNodeTheValueHolds) {
    expectWrittenRows("MATCH (n:Person {name: 'Remy'}) WITH {person: n} AS m WITH m.person AS p REMOVE p.age",
                      "MATCH (p:Person {name: 'Remy'}) RETURN p.age",
                      {{"null"}});
}

TEST_F(PatternFromATaggedCellTest, DISABLED_deletesTheNodeTheValueHolds) {
    expectWrittenRows("MATCH (n:Person {name: 'Remy'}) WITH {person: n} AS m WITH m.person AS p DETACH DELETE p",
                      "MATCH (p:Person) RETURN count(p)",
                      {{"7"}});
}

TEST_F(PatternFromATaggedCellTest, DISABLED_setsAPropertyOfTheRelationshipTheValueHolds) {
    expectWrittenRows("MATCH (:Person {name: 'Remy'})-[r:KNOWS_WELL]->() WITH {rel: r} AS m WITH m.rel AS r "
                      "SET r.mood = 'happy'",
                      "MATCH ()-[r:KNOWS_WELL {mood: 'happy'}]->(b) RETURN b.name",
                      {{"Adam"}});
}

TEST_F(PatternFromATaggedCellTest, startsAMatchFromANodeReadByALiteralIndex) {
    expectRows("MATCH (n:Person {name: 'Remy'}) WITH {person: n} AS m WITH m['person'] AS p "
               "MATCH (p)-[:KNOWS_WELL]->(f) RETURN f.name",
               {{"Adam"}});
}
