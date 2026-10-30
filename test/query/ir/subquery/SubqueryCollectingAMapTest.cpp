#include <gtest/gtest.h>

#include <string_view>

#include "CallV3Test.h"
#include "IRTestRows.h"

using namespace turing::test;

// A COLLECT subquery and a pattern comprehension gather one value per row of their body into
// a list, so a body that returns a map gathers a list of maps
class SubqueryCollectingAMapTest : public CallV3Test {
protected:
    void expectRows(std::string_view query, const Rows& expected) {
        RowSink sink;
        runQuery(query, sink);

        EXPECT_EQ(sink.rows(), expected) << query;
    }
};

TEST_F(SubqueryCollectingAMapTest, collectsAScalarPerRowOfACollectSubquery) {
    expectRows("RETURN COLLECT { UNWIND [1, 2] AS x RETURN x }", {{"[1, 2]"}});
}

TEST_F(SubqueryCollectingAMapTest, collectsANodePerMatchOfACollectSubquery) {
    expectRows("MATCH (n:Person {name: 'Remy'}) RETURN COLLECT { MATCH (n)-[:KNOWS_WELL]->(f) RETURN f }", {{"[1]"}});
}

TEST_F(SubqueryCollectingAMapTest, collectsAScalarPerMatchOfAPatternComprehension) {
    expectRows("MATCH (n:Person {name: 'Remy'}) RETURN [(n)-[:KNOWS_WELL]->(f) | f.name]", {{"[Adam]"}});
}

TEST_F(SubqueryCollectingAMapTest, collectsAListPerRowOfACollectSubquery) {
    expectRows("RETURN COLLECT { UNWIND [1, 2] AS x RETURN [x] }", {{"[[1], [2]]"}});
}

TEST_F(SubqueryCollectingAMapTest, collectsAListPerMatchOfAPatternComprehension) {
    expectRows("MATCH (n:Person {name: 'Remy'}) RETURN [(n)-[:KNOWS_WELL]->(f) | [f.name]]", {{"[[Adam]]"}});
}

// Disabled: lowering reads the body's map column as a nullable value column, which only a
// scalar column can be read as. collect() of a map is turned away earlier (TUR-100).
TEST_F(SubqueryCollectingAMapTest, DISABLED_collectsAMapPerRowOfACollectSubquery) {
    expectRows("RETURN COLLECT { UNWIND [1, 2] AS x RETURN {a: x} }", {{"[{a: 1}, {a: 2}]"}});
}

TEST_F(SubqueryCollectingAMapTest, DISABLED_collectsAMapPerMatchOfACollectSubquery) {
    expectRows("MATCH (n:Person {name: 'Remy'}) RETURN COLLECT { MATCH (n)-[:KNOWS_WELL]->(f) RETURN {name: f.name} }",
               {{"[{name: Adam}]"}});
}

TEST_F(SubqueryCollectingAMapTest, DISABLED_collectsAMapPerMatchOfAPatternComprehension) {
    expectRows("MATCH (n:Person {name: 'Remy'}) RETURN [(n)-[:KNOWS_WELL]->(f) | {name: f.name}]", {{"[{name: Adam}]"}});
}
