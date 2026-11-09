#include <gtest/gtest.h>

#include <string>
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

const std::string SQRT_TWO = "1.4142135381698608";

void setEmbeddings(ParameterMap& parameters, std::string_view name) {
    const std::vector<float> first {1.0f, 0.0f};
    const std::vector<float> second {0.0f, 1.0f};

    ParameterValue::List& vectors = parameters.insert(name).setList();
    vectors.emplace_back().setEmbedding(first);
    vectors.emplace_back().setEmbedding(second);
}

}

// An embedding read out of a list or a map is a type-erased cell. cosine_similarity and
// euclidean_distance read the embedding it holds, answer null for a null, and raise on any
// other value, as the string functions do over a cell.
class EmbeddingFunctionOverCellTest : public CallV3Test {
protected:
    void expectRows(std::string_view query, const Rows& expected) {
        StringRowSink sink;
        runQuery(query, sink);
        EXPECT_EQ(sink.getRows(), expected) << query;
    }

    void expectRows(std::string_view query, const ParameterMap& parameters, const Rows& expected) {
        StringRowSink sink;
        runQuery(query, parameters, sink);
        EXPECT_EQ(sink.getRows(), expected) << query;
    }
};

TEST_F(EmbeddingFunctionOverCellTest, scoresAnUnwoundEmbeddingOnEitherSide) {
    expectRows("UNWIND [(1.0, 0.0), (0.0, 1.0)] AS v "
               "RETURN cosine_similarity(v, (1.0, 0.0)), cosine_similarity((1.0, 0.0), v), cosine_similarity(v, v)",
               {{"1", "1", "1"}, {"0", "0", "1"}});
}

TEST_F(EmbeddingFunctionOverCellTest, measuresAnUnwoundEmbeddingOnEitherSide) {
    expectRows("RETURN euclidean_distance((0.0, 1.0), (1.0, 0.0))", {{SQRT_TWO}});

    expectRows("UNWIND [(1.0, 0.0), (0.0, 1.0)] AS v "
               "RETURN euclidean_distance(v, (1.0, 0.0)), euclidean_distance((1.0, 0.0), v), euclidean_distance(v, v)",
               {{"0", "0", "0"}, {SQRT_TWO, SQRT_TWO, "0"}});
}

TEST_F(EmbeddingFunctionOverCellTest, scoresAnUnwoundListParameterOfEmbeddings) {
    ParameterMap parameters;
    setEmbeddings(parameters, "vs");

    expectRows("UNWIND $vs AS v RETURN cosine_similarity(v, (1.0, 0.0)), euclidean_distance((1.0, 0.0), v)",
               parameters,
               {{"1", "0"}, {"0", SQRT_TWO}});
    expectRows("RETURN cosine_similarity($vs[0], $vs[1]), euclidean_distance($vs[0], $vs[1])", parameters, {{"0", SQRT_TWO}});
}

TEST_F(EmbeddingFunctionOverCellTest, scoresAnEmbeddingIndexedOutOfAList) {
    expectRows("RETURN cosine_similarity([(1.0, 0.0)][0], (1.0, 0.0)), euclidean_distance((1.0, 0.0), [(0.0, 1.0)][0])",
               {{"1", SQRT_TWO}});
    expectRows("RETURN cosine_similarity([(1.0, 0.0)][1], (1.0, 0.0))", {{"null"}});

    expectRows("UNWIND [[(1.0, 0.0), 1], [(0.0, 1.0), 2]] AS pair RETURN cosine_similarity(pair[0], (1.0, 0.0))",
               {{"1"}, {"0"}});
}

TEST_F(EmbeddingFunctionOverCellTest, scoresAnEmbeddingReadOutOfAMap) {
    expectRows("RETURN cosine_similarity({at: (0.0, 1.0)}.at, (1.0, 0.0))", {{"0"}});

    expectRows("UNWIND [{at: (1.0, 0.0)}, {at: (0.0, 1.0)}] AS m RETURN cosine_similarity(m.at, (1.0, 0.0))",
               {{"1"}, {"0"}});
}

TEST_F(EmbeddingFunctionOverCellTest, scoresACollectedEmbedding) {
    expectRows("UNWIND [(1.0, 0.0), (0.0, 1.0), 'a'] AS v WITH collect(v) AS vs "
               "RETURN cosine_similarity(vs[0], vs[1]), euclidean_distance(vs[0], vs[0])",
               {{"0", "0"}});
    expectRows("UNWIND [(1.0, 0.0), (0.0, 1.0)] AS v WITH collect({at: v}) AS ms "
               "RETURN cosine_similarity(ms[1].at, (1.0, 0.0))",
               {{"0"}});
}

TEST_F(EmbeddingFunctionOverCellTest, scoresAnEmbeddingOfAListProperty) {
    runWrite("CREATE (:Vec {vs: [(1.0, 0.0), (0.0, 1.0)]})");

    expectRows("MATCH (n:Vec) UNWIND n.vs AS v RETURN cosine_similarity(v, (1.0, 0.0))", {{"1"}, {"0"}});
    expectRows("MATCH (n:Vec) RETURN cosine_similarity(n.vs[1], (1.0, 0.0)), euclidean_distance(n.vs[0], n.vs[1])",
               {{"0", SQRT_TWO}});
    expectRows("MATCH (n:Vec) RETURN [x IN n.vs | cosine_similarity(x, (1.0, 0.0))]", {{"1, 0"}});
}

TEST_F(EmbeddingFunctionOverCellTest, scoresAnEmbeddingAComprehensionOrACoalesceAnswers) {
    expectRows("RETURN [x IN [(1.0, 0.0), (0.0, 1.0)] | cosine_similarity(x, (1.0, 0.0))]", {{"1, 0"}});
    expectRows("UNWIND [(1.0, 0.0), null] AS v RETURN cosine_similarity(coalesce(v, (0.0, 1.0)), (1.0, 0.0))",
               {{"1"}, {"0"}});
    expectRows("RETURN cosine_similarity(CASE WHEN true THEN (1.0, 0.0) ELSE 'a' END, (1.0, 0.0))", {{"1"}});
}

TEST_F(EmbeddingFunctionOverCellTest, scoresAnAbsentPropertyAsNull) {
    runWrite("CREATE (:Vec {name: 'Vec', e: (0.0, 1.0)})");

    expectRows("MATCH (n) WHERE n.name = 'Remy' OR n.name = 'Vec' "
               "RETURN n.name, cosine_similarity(n.e, (1.0, 0.0)), euclidean_distance((1.0, 0.0), n.e), cosine_similarity(n.e, n.e)",
               {{"Remy", "null", "null", "null"}, {"Vec", "0", SQRT_TWO, "1"}});
}

TEST_F(EmbeddingFunctionOverCellTest, scoresANullElementAsNull) {
    expectRows("UNWIND [(1.0, 0.0), null] AS v RETURN cosine_similarity(v, (1.0, 0.0)), euclidean_distance((1.0, 0.0), v)",
               {{"1", "0"}, {"null", "null"}});
}

TEST_F(EmbeddingFunctionOverCellTest, raisesOnAnElementThatIsNoEmbedding) {
    runQueryExpectingError("UNWIND [(1.0, 0.0), 'a'] AS v RETURN cosine_similarity(v, (1.0, 0.0))",
                           "cosine_similarity() reads an embedding, not 'String'");
    runQueryExpectingError("UNWIND [(1.0, 0.0), 1] AS v RETURN cosine_similarity((1.0, 0.0), v)",
                           "cosine_similarity() reads an embedding, not 'Integer'");
    runQueryExpectingError("UNWIND [(1.0, 0.0), [1.0, 0.0]] AS v RETURN euclidean_distance(v, v)",
                           "euclidean_distance() reads an embedding, not 'List'");
}

TEST_F(EmbeddingFunctionOverCellTest, raisesOnARowPastTheFirstChunk) {
    expectRows("UNWIND range(0, 70000) AS i WITH CASE WHEN i = -1 THEN 'a' ELSE (1.0, 0.0) END AS v "
               "RETURN count(cosine_similarity(v, (1.0, 0.0)))",
               {{"70001"}});
    runQueryExpectingError("UNWIND range(0, 70000) AS i WITH CASE WHEN i = 69999 THEN 'a' ELSE (1.0, 0.0) END AS v "
                           "RETURN count(cosine_similarity(v, (1.0, 0.0)))",
                           "cosine_similarity() reads an embedding, not 'String'");
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
