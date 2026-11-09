#include <gtest/gtest.h>

#include <string_view>
#include <vector>

#include "ParameterMap.h"
#include "ParameterValue.h"

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace db;
using namespace turing::test;

namespace {

using Rows = std::vector<StringRowSink::Row>;

constexpr std::string_view COSINE_TWO_AND_THREE =
    "cosine_similarity() reads two embeddings of the same dimension, not of dimensions 2 and 3";
constexpr std::string_view COSINE_THREE_AND_TWO =
    "cosine_similarity() reads two embeddings of the same dimension, not of dimensions 3 and 2";
constexpr std::string_view EUCLIDEAN_TWO_AND_THREE =
    "euclidean_distance() reads two embeddings of the same dimension, not of dimensions 2 and 3";
constexpr std::string_view EUCLIDEAN_THREE_AND_TWO =
    "euclidean_distance() reads two embeddings of the same dimension, not of dimensions 3 and 2";

}

class EmbeddingDimensionMismatchTest : public CallV3Test {
protected:
    void expectRows(std::string_view query, const Rows& expected) {
        StringRowSink sink;
        runQuery(query, sink);
        EXPECT_EQ(sink.getRows(), expected) << query;
    }
};

TEST_F(EmbeddingDimensionMismatchTest, raisesOnTwoLiteralsOfDifferentDimensions) {
    runQueryExpectingError("RETURN cosine_similarity((1.0, 0.0), (1.0, 0.0, 0.0))", COSINE_TWO_AND_THREE);
    runQueryExpectingError("RETURN euclidean_distance((1.0, 0.0, 0.0), (1.0, 0.0))", EUCLIDEAN_THREE_AND_TWO);
}

TEST_F(EmbeddingDimensionMismatchTest, raisesOnAParameterOfAnotherDimension) {
    ParameterMap parameters;
    parameters.insert("e").setEmbedding(std::vector<float> {1.0f, 0.0f});

    runQueryExpectingError("RETURN cosine_similarity($e, (1.0, 0.0, 0.0))", parameters, COSINE_TWO_AND_THREE);
    runQueryExpectingError("RETURN euclidean_distance((1.0, 0.0, 0.0), $e)", parameters, EUCLIDEAN_THREE_AND_TWO);
}

TEST_F(EmbeddingDimensionMismatchTest, raisesOnAnElementOfAnotherDimensionOnEitherSide) {
    runQueryExpectingError("UNWIND [(1.0, 0.0)] AS v RETURN cosine_similarity(v, (1.0, 0.0, 0.0))", COSINE_TWO_AND_THREE);
    runQueryExpectingError("UNWIND [(1.0, 0.0)] AS v RETURN euclidean_distance((1.0, 0.0, 0.0), v)", EUCLIDEAN_THREE_AND_TWO);
    runQueryExpectingError("UNWIND [[(1.0, 0.0), (1.0, 0.0, 0.0)]] AS p RETURN cosine_similarity(p[0], p[1])", COSINE_TWO_AND_THREE);
    runQueryExpectingError("RETURN euclidean_distance({at: (1.0, 0.0)}.at, (1.0, 0.0, 0.0))", EUCLIDEAN_TWO_AND_THREE);
}

TEST_F(EmbeddingDimensionMismatchTest, raisesOnTheRowWhosePropertyHasAnotherDimension) {
    runWrite("CREATE (:Vec {name: 'a', e: (1.0, 0.0)})");
    runWrite("CREATE (:Vec {name: 'b', e: (1.0, 0.0, 0.0)})");

    expectRows("MATCH (n:Vec) WHERE n.name = 'a' RETURN cosine_similarity(n.e, (1.0, 0.0)), euclidean_distance((1.0, 0.0), n.e)",
               {{"1", "0"}});
    runQueryExpectingError("MATCH (n:Vec) RETURN cosine_similarity(n.e, (1.0, 0.0))", COSINE_THREE_AND_TWO);
    runQueryExpectingError("MATCH (a:Vec {name: 'a'}), (b:Vec {name: 'b'}) RETURN euclidean_distance(a.e, b.e)",
                           EUCLIDEAN_TWO_AND_THREE);
}

TEST_F(EmbeddingDimensionMismatchTest, raisesOnARowPastTheFirstChunk) {
    runQueryExpectingError("UNWIND range(0, 70000) AS i WITH CASE WHEN i = 69999 THEN (1.0, 0.0, 0.0) ELSE (1.0, 0.0) END AS v "
                           "RETURN count(cosine_similarity(v, (1.0, 0.0)))",
                           COSINE_THREE_AND_TWO);
}

TEST_F(EmbeddingDimensionMismatchTest, comparesEmbeddingsOfDifferentDimensionsAsUnequal) {
    expectRows("RETURN (1.0, 0.0) = (1.0, 0.0, 0.0), (1.0, 0.0) <> (1.0, 0.0, 0.0)", {{"false", "true"}});
    expectRows("UNWIND [(1.0, 0.0)] AS v RETURN v = (1.0, 0.0, 0.0), (1.0, 0.0, 0.0) <> v", {{"false", "true"}});
    expectRows("RETURN (1.0, 0.0) IN [(1.0, 0.0, 0.0)], [(1.0, 0.0)] = [(1.0, 0.0, 0.0)], {at: (1.0, 0.0)} = {at: (1.0, 0.0, 0.0)}",
               {{"false", "false", "false"}});

    runWrite("CREATE (:Vec {name: 'a', e: (1.0, 0.0)})");
    runWrite("CREATE (:Vec {name: 'b', e: (1.0, 0.0, 0.0)})");

    expectRows("MATCH (n:Vec) WHERE n.e = (1.0, 0.0, 0.0) RETURN n.name", {{"b"}});
    expectRows("MATCH (a:Vec), (b:Vec) WHERE a.e = b.e RETURN a.name, b.name", {{"a", "a"}, {"b", "b"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
