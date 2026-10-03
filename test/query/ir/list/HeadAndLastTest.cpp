#include <gtest/gtest.h>

#include <string_view>

#include "CallV3Test.h"
#include "IRTestRows.h"

using namespace turing::test;

// head() and last() read the first and the last element of a list as the type the list
// holds: a node of nodes(p) or of collect(n) has its properties and compares with a node,
// and an empty or absent list gives null
class HeadAndLastTest : public CallV3Test {
protected:
    void expectRows(std::string_view query, const Rows& expected) {
        RowSink sink;
        runQuery(query, sink);

        Rows rows;
        sink.sortedRows(rows);

        EXPECT_EQ(rows, expected) << query;
    }
};

TEST_F(HeadAndLastTest, readsAPropertyOfTheEndsOfAPath) {
    expectRows("MATCH p = (n:Person)-[e]->(m:Person) RETURN head(nodes(p)).name, last(nodes(p)).name",
               {{"Adam", "Remy"}, {"Remy", "Adam"}});
}

TEST_F(HeadAndLastTest, comparesTheEndsOfAPathWithItsNodes) {
    expectRows("MATCH p = (n:Person)-[e]->(m:Person) RETURN last(nodes(p)) = m, head(nodes(p)) = m",
               {{"true", "false"}, {"true", "false"}});
}

TEST_F(HeadAndLastTest, readsAPropertyOfTheFirstRelationshipOfAWalk) {
    expectRows("MATCH p = (n:Person)-[e]->+(m:Person) WHERE length(p) = 2 RETURN head(relationships(p)).name, last(relationships(p)).name", {
        {"Adam -> Remy", "Remy -> Adam"},
        {"Remy -> Adam", "Adam -> Remy"},
        {"Remy -> Ghosts", "Ghosts -> Remy"},
    });
}

TEST_F(HeadAndLastTest, readsAPropertyOfACollectedNode) {
    expectRows("MATCH (n:Person {name: 'Luc'}) WITH collect(n) AS ns RETURN head(ns).name, last(ns).name", {{"Luc", "Luc"}});
}

TEST_F(HeadAndLastTest, readsNullWhereAnOptionalMatchMissed) {
    expectRows("MATCH (n:Person) OPTIONAL MATCH p = (n)-[e]->(m:Person) RETURN n.name, head(nodes(p)).name", {
        {"Adam", "Adam"}, {"Cyrus", "null"}, {"Doruk", "null"}, {"Luc", "null"},
        {"Martina", "null"}, {"Maxime", "null"}, {"Remy", "Remy"}, {"Suhas", "null"},
    });
}

TEST_F(HeadAndLastTest, readsNullOverAnEmptyList) {
    expectRows("RETURN head([]), last([])", {{"null", "null"}});
}

TEST_F(HeadAndLastTest, testsANullElementForNull) {
    expectRows("RETURN head([null, 2]) IS NULL, last([2, null]) IS NULL, head([null, 2]) IS NOT NULL", {{"true", "true", "false"}});
}

TEST_F(HeadAndLastTest, readsTheScalarsOfATypedList) {
    expectRows("RETURN head([1, 2]) + 1, last(['a', 'b']), head([1, 2]) = 1", {{"2", "b", "true"}});
}

TEST_F(HeadAndLastTest, readsANestedListAsAList) {
    expectRows("RETURN head([[1, 2], [3]])[1], size(last([[1, 2], [3]]))", {{"2", "1"}});
}

TEST_F(HeadAndLastTest, readsTheElementsOfAStoredList) {
    runWrite("MATCH (n:Person {name: 'Luc'}) SET n.tags = [1, 'a']");

    expectRows("MATCH (n:Person {name: 'Luc'}) RETURN head(n.tags), last(n.tags)", {{"1", "a"}});
}
