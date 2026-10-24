#include <gtest/gtest.h>

#include <string>
#include <string_view>
#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

// A node or an edge equals the integer its ID is, so an inline constraint naming one
// compares an integer property against that ID, as the WHERE it spells does. Adam is node 1,
// Remy -> Ghosts edge 1, and Luc -> Computers and Martina -> Cooking last 15 and 10, the IDs
// of Cyrus and Animals.
class PropertyConstraintOnAnEntityTest : public CallV3Test {
protected:
    void expectRows(std::string_view query, const std::vector<StringRowSink::Row>& expected) {
        StringRowSink sink;
        runQuery(query, sink);

        std::vector<StringRowSink::Row> rows;
        sink.sortedRows(rows);

        EXPECT_EQ(rows, expected) << query;
    }
};

TEST_F(PropertyConstraintOnAnEntityTest, matchesANodePropertyHoldingTheIDOfANode) {
    runWrite("MATCH (n:Person {name: 'Maxime'}) SET n.age = 1");

    expectRows("MATCH (m) MATCH (n {age: m}) RETURN n.name, m.name", {{"Maxime", "Adam"}});
}

TEST_F(PropertyConstraintOnAnEntityTest, matchesANodePropertyHoldingTheIDOfAnEdge) {
    runWrite("MATCH (n:Person {name: 'Maxime'}) SET n.age = 1");

    expectRows("MATCH (a)-[e]->(b) MATCH (n {age: e}) RETURN n.name, a.name, b.name", {{"Maxime", "Remy", "Ghosts"}});
}

TEST_F(PropertyConstraintOnAnEntityTest, matchesAnEdgePropertyHoldingTheIDOfANode) {
    runWrite("MATCH (:Person {name: 'Remy'})-[r:KNOWS_WELL]->() SET r.duration = 1");

    expectRows("MATCH (m) MATCH (a)-[r {duration: m}]->(b) RETURN a.name, b.name, m.name",
               {
                   {"Luc", "Computers", "Cyrus"}, {"Martina", "Cooking", "Animals"}, {"Remy", "Adam", "Adam"},
               });
}
