#include <gtest/gtest.h>

#include "ExpectedASTTest.h"

using namespace db;
using namespace turing::test;

// VECTOR SEARCH writes its neighbour count and its query vector side by side. The count is
// a primary - a literal, a parameter, a variable, a property read of one, or a
// parenthesized expression - and the vector is any expression, so a bracket or a sign
// after the count opens the vector.
class VectorSearchNeighbourCountTest : public ExpectedASTTest {
};

TEST_F(VectorSearchNeighbourCountTest, opensTheVectorAtABracketAfterTheCount) {
    expectParse("VECTOR SEARCH IN v FOR k [w][0] YIELD ids RETURN ids",
                {query({vectorSearch({._index = string("v"),
                                      ._neighbours = symbol("k"),
                                      ._queryVector = index(list({symbol("w")}), integer(0)),
                                      ._yield = yield({symbol("ids")})})},
                       returning({symbol("ids")}))});

    expectParse("VECTOR SEARCH IN v FOR 3 [1, 2] YIELD ids RETURN ids",
                {query({vectorSearch({._index = string("v"),
                                      ._neighbours = integer(3),
                                      ._queryVector = list({integer(1), integer(2)}),
                                      ._yield = yield({symbol("ids")})})},
                       returning({symbol("ids")}))});
}

TEST_F(VectorSearchNeighbourCountTest, opensTheVectorAtASignAfterTheCount) {
    expectParse("VECTOR SEARCH IN v FOR 2 -x YIELD ids RETURN ids",
                {query({vectorSearch({._index = string("v"),
                                      ._neighbours = integer(2),
                                      ._queryVector = unary(UnaryOperator::Minus, symbol("x")),
                                      ._yield = yield({symbol("ids")})})},
                       returning({symbol("ids")}))});
}

TEST_F(VectorSearchNeighbourCountTest, opensTheVectorAtAParenthesisAfterAVariableCount) {
    expectParse("VECTOR SEARCH IN v FOR k (1, 2) YIELD ids RETURN ids",
                {query({vectorSearch({._index = string("v"),
                                      ._neighbours = symbol("k"),
                                      ._queryVector = literal(embedding({1.0f, 2.0f})),
                                      ._yield = yield({symbol("ids")})})},
                       returning({symbol("ids")}))});
}

TEST_F(VectorSearchNeighbourCountTest, readsAPropertyOfTheCount) {
    expectParse("VECTOR SEARCH IN v FOR n.k (1, 2) YIELD ids RETURN ids",
                {query({vectorSearch({._index = string("v"),
                                      ._neighbours = property({"n", "k"}),
                                      ._queryVector = literal(embedding({1.0f, 2.0f})),
                                      ._yield = yield({symbol("ids")})})},
                       returning({symbol("ids")}))});

    expectParse("VECTOR SEARCH IN v FOR $m.k [w][0] YIELD ids RETURN ids",
                {query({vectorSearch({._index = string("v"),
                                      ._neighbours = lookup(parameter("m"), "k"),
                                      ._queryVector = index(list({symbol("w")}), integer(0)),
                                      ._yield = yield({symbol("ids")})})},
                       returning({symbol("ids")}))});
}

TEST_F(VectorSearchNeighbourCountTest, readsAComputedCountInParentheses) {
    expectParse("VECTOR SEARCH IN v FOR ($k + 1) $m.v YIELD ids RETURN ids",
                {query({vectorSearch({._index = string("v"),
                                      ._neighbours = parenthesized(binary(BinaryOperator::Add, parameter("k"), integer(1))),
                                      ._queryVector = lookup(parameter("m"), "v"),
                                      ._yield = yield({symbol("ids")})})},
                       returning({symbol("ids")}))});

    expectParse("VECTOR SEARCH IN v FOR (toInteger(k)) (1, 2) YIELD ids RETURN ids",
                {query({vectorSearch({._index = string("v"),
                                      ._neighbours = parenthesized(invocation({"toInteger"}, {symbol("k")})),
                                      ._queryVector = literal(embedding({1.0f, 2.0f})),
                                      ._yield = yield({symbol("ids")})})},
                       returning({symbol("ids")}))});
}

TEST_F(VectorSearchNeighbourCountTest, rejectsAnOperatorAfterABareCount) {
    expectError("VECTOR SEARCH IN v FOR k + 1 w YIELD ids",
                "     1 | VECTOR SEARCH IN v FOR k + 1 w YIELD ids\n"
                "       |                              ^\n"
                "-------* syntax error, unexpected ID");
}
