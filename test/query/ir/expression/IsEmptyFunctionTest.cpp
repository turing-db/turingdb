#include <gtest/gtest.h>

#include <algorithm>
#include <string>
#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

namespace {

using Rows = std::vector<StringRowSink::Row>;

}

// Expected values are Neo4j 5.26's on the same simpledb graph. Its Person nodes are Remy,
// Adam, Maxime, Luc, Martina, Suhas, Cyrus and Doruk; the first four have a dob, and none
// is over 100. Its KNOWS_WELL edges are Remy -> Adam, Adam -> Remy and Ghosts -> Remy.
class IsEmptyFunctionTest : public CallV3Test {
protected:
    void expectSortedRows(const std::string& query, Rows expected) {
        StringRowSink sink;
        runQuery(query, sink);

        Rows rows = sink.getRows();
        std::sort(rows.begin(), rows.end());
        std::sort(expected.begin(), expected.end());

        EXPECT_EQ(rows, expected) << query;
    }
};

TEST_F(IsEmptyFunctionTest, readsAListAStringAMapAndANull) {
    expectSortedRows("RETURN isEmpty([]), isEmpty(''), isEmpty({}), isEmpty(null), isEmpty([1])",
                     {{"true", "true", "true", "null", "false"}});
}

TEST_F(IsEmptyFunctionTest, readsANullElementOrASpaceAsContent) {
    expectSortedRows("RETURN isEmpty(' '), isEmpty([null]), isEmpty({a: null}), isEmpty(trim(' ')), ISEMPTY([])",
                     {{"false", "false", "false", "true", "true"}});
}

TEST_F(IsEmptyFunctionTest, readsACollectedListOfNodes) {
    expectSortedRows("MATCH (a:Person) WITH collect(a) AS l RETURN isEmpty(l), isEmpty(l[1..1])",
                     {{"false", "true"}});
}

TEST_F(IsEmptyFunctionTest, readsACollectOfNoNode) {
    expectSortedRows("MATCH (a:Person) WHERE a.age > 100 WITH collect(a) AS l RETURN isEmpty(l)",
                     {{"true"}});
}

TEST_F(IsEmptyFunctionTest, readsAGroupThatMayBeEmpty) {
    expectSortedRows("MATCH (n:Person) ((x)-[:KNOWS_WELL]->(y)){0,1} (c) RETURN n.name, isEmpty(x)",
                     {{"Remy", "true"},
                      {"Remy", "false"},
                      {"Adam", "true"},
                      {"Adam", "false"},
                      {"Maxime", "true"},
                      {"Luc", "true"},
                      {"Martina", "true"},
                      {"Suhas", "true"},
                      {"Cyrus", "true"},
                      {"Doruk", "true"}});
}

TEST_F(IsEmptyFunctionTest, readsAGroupAndItsTail) {
    expectSortedRows("MATCH (n) ((x)-[:KNOWS_WELL]->(y)){1,2} (c) RETURN n.name, isEmpty(x), isEmpty(tail(x))",
                     {{"Remy", "false", "true"},
                      {"Remy", "false", "false"},
                      {"Adam", "false", "true"},
                      {"Adam", "false", "false"},
                      {"Ghosts", "false", "true"},
                      {"Ghosts", "false", "false"}});
}

TEST_F(IsEmptyFunctionTest, readsTheRelationshipsOfAWalk) {
    expectSortedRows("MATCH (n:Person {name: 'Remy'})-[r:KNOWS_WELL*0..1]->(c) RETURN c.name, isEmpty(r)",
                     {{"Remy", "true"},
                      {"Adam", "false"}});
}

TEST_F(IsEmptyFunctionTest, readsAPropertyThatMayBeNull) {
    expectSortedRows("MATCH (n:Person) RETURN n.name, isEmpty(n.dob)",
                     {{"Remy", "false"},
                      {"Adam", "false"},
                      {"Maxime", "false"},
                      {"Luc", "false"},
                      {"Martina", "null"},
                      {"Suhas", "null"},
                      {"Cyrus", "null"},
                      {"Doruk", "null"}});
}

TEST_F(IsEmptyFunctionTest, filtersOnAPropertyThatIsNotEmpty) {
    expectSortedRows("MATCH (a:Person) WHERE NOT isEmpty(a.dob) RETURN a.name",
                     {{"Remy"},
                      {"Adam"},
                      {"Maxime"},
                      {"Luc"}});
}

TEST_F(IsEmptyFunctionTest, readsTheMapAndTheKeysOfANode) {
    expectSortedRows("MATCH (n:Person {name: 'Remy'}) RETURN isEmpty(properties(n)), isEmpty(keys(n))",
                     {{"false", "false"}});
}

TEST_F(IsEmptyFunctionTest, readsTheMapAndTheKeysOfAnAbsentNode) {
    expectSortedRows("MATCH (n:Person {name: 'Luc'}) OPTIONAL MATCH (n)-[:KNOWS_WELL]->(m) "
                     "RETURN isEmpty(properties(m)), isEmpty(keys(m))",
                     {{"null", "null"}});
}

TEST_F(IsEmptyFunctionTest, readsTaggedCells) {
    expectSortedRows("UNWIND [[], 'a', {}, [1], null, ''] AS x RETURN isEmpty(x)",
                     {{"true"},
                      {"false"},
                      {"true"},
                      {"false"},
                      {"null"},
                      {"true"}});
}

TEST_F(IsEmptyFunctionTest, readsTheValuesOfAMap) {
    expectSortedRows("WITH {a: [], b: 'x', c: {}, e: null} AS m "
                     "RETURN isEmpty(m.a), isEmpty(m.b), isEmpty(m.c), isEmpty(m.d), isEmpty(m.e)",
                     {{"true", "false", "true", "null", "null"}});
}

TEST_F(IsEmptyFunctionTest, rejectsAnArgumentThatIsNoListStringOrMap) {
    runQueryExpectingError("RETURN isEmpty(1)", "Invalid arguments for function 'isEmpty'");
    runQueryExpectingError("RETURN isEmpty(1.5)", "Invalid arguments for function 'isEmpty'");
    runQueryExpectingError("RETURN isEmpty(true)", "Invalid arguments for function 'isEmpty'");
    runQueryExpectingError("RETURN isEmpty(duration(1))", "Invalid arguments for function 'isEmpty'");
    runQueryExpectingError("MATCH (n) RETURN isEmpty(n)", "Invalid arguments for function 'isEmpty'");
    runQueryExpectingError("MATCH ()-[r]->() RETURN isEmpty(r)", "Invalid arguments for function 'isEmpty'");
}

TEST_F(IsEmptyFunctionTest, rejectsACellHoldingNoListStringOrMap) {
    runQueryExpectingError("UNWIND [1, 'a'] AS x RETURN isEmpty(x)", "isEmpty() reads a list, a string or a map");
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
