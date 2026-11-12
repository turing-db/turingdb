#include <gtest/gtest.h>

#include <fstream>
#include <initializer_list>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "NLOutputSink.h"
#include "ParameterMap.h"
#include "ParameterValue.h"
#include "QueryConfig.h"
#include "QueryInterpreterV3.h"
#include "QueryState.h"
#include "QueryStatus.h"

#include "Graph.h"
#include "SimpleGraph.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "TuringDB.h"
#include "versioning/Change.h"
#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"

#include "StringRowSink.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

namespace {

using Rows = std::vector<StringRowSink::Row>;

// The squared L2 distances from (1, 0) are 0 for node 0, 2 for node 1 and 1 for node 2
constexpr std::string_view VECTOR_FILE = "0,1,0\n1,0,1\n2,1,1\n";

bool contains(std::string_view text, std::string_view needle) {
    return text.find(needle) != std::string_view::npos;
}

void setEmbedding(ParameterMap& parameters, std::string_view name, std::initializer_list<float> floats) {
    const std::vector<float> embedding(floats);
    parameters.insert(name).setEmbedding(embedding);
}

void setList(ParameterMap& parameters, std::string_view name, std::initializer_list<double> numbers) {
    ParameterValue::List& list = parameters.insert(name).setList();
    for (const double number : numbers) {
        list.emplace_back().setDouble(number);
    }
}

}

class EmbeddingParameterTest : public TuringTest {
public:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);

        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager(), &_env->getMem(), &_env->getCompilerContext());

        const fs::Path path = _env->getConfig().getDataDir() / "vectors.csv";
        std::ofstream file(path.get());
        file << VECTOR_FILE;
    }

protected:
    void runQuery(std::string_view query, const ParameterMap& parameters, QueryStatus& status, NLOutputSink& sink) {
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              ChangeID::head(),
                              &sink,
                              &parameters);
    }

    void createVectorIndex() {
        const ParameterMap noParameters;

        for (const std::string_view command : {"CREATE VECTOR INDEX idx WITH DIMENSION 2 METRIC EUCLID",
                                               "LOAD VECTOR FROM \"vectors.csv\" IN idx"}) {
            QueryStatus status;
            StringRowSink sink;
            runQuery(command, noParameters, status, sink);
            ASSERT_TRUE(status.isOk()) << command << ": " << status.getError();
        }
    }

    void expectRows(std::string_view query, const ParameterMap& parameters, const Rows& expected) {
        QueryStatus status;
        StringRowSink sink;
        runQuery(query, parameters, status, sink);

        ASSERT_TRUE(status.isOk()) << query << ": " << status.getError();
        EXPECT_EQ(sink.getRows(), expected) << query;
    }

    void expectError(std::string_view query, const ParameterMap& parameters, std::string_view reason, QueryStatus::Status kind) {
        QueryStatus status;
        StringRowSink sink;
        runQuery(query, parameters, status, sink);

        ASSERT_FALSE(status.isOk()) << "accepted: " << query;
        EXPECT_EQ(status.getStatus(), kind) << query << ": " << status.getError();
        EXPECT_TRUE(contains(status.getError(), reason)) << query << ": " << status.getError();
    }

    void runWrite(std::string_view query, const ParameterMap& parameters) {
        ChangeID change;
        {
            SystemAccessor system = _env->getSystemManager().accessUnique();
            const auto opened = system.newChange(_graphName);
            ASSERT_TRUE(opened);
            change = opened.value()->id();
        }

        QueryStatus status;
        StringRowSink sink;
        _interpreter->execute(status, query, _graphName, CommitHash::head(), change, &sink, &parameters);
        ASSERT_TRUE(status.isOk()) << query << ": " << status.getError();

        const QueryState submitState(_graphName, &_env->getMem(), &_env->getCompilerContext(), &_queryConfig, nullptr, CommitHash::head(), change);
        const QueryStatus submitStatus = _env->getDB().query("CHANGE SUBMIT", submitState);
        ASSERT_TRUE(submitStatus.isOk()) << submitStatus.getError();
    }

    void dbProgramOf(std::string_view query, const ParameterMap& parameters, std::string& program) {
        QueryStatus status;
        StringRowSink sink;
        runQuery(std::string("EXPLAIN (db) ") + std::string(query), parameters, status, sink);

        ASSERT_TRUE(status.isOk()) << query << ": " << status.getError();
        ASSERT_EQ(sink.getRows().size(), 1u);
        program = sink.getRows().front().back();
    }

    const std::string _graphName {"simpledb"};
    QueryConfig _queryConfig;
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

TEST_F(EmbeddingParameterTest, searchesForTheQueryVectorAParameterHolds) {
    createVectorIndex();

    const ParameterMap noParameters;
    expectRows("VECTOR SEARCH IN idx FOR 2 (1.0, 0.0) YIELD ids, score RETURN ids, score", noParameters, {{"0", "0"}, {"2", "1"}});

    ParameterMap parameters;
    setEmbedding(parameters, "v", {1.0f, 0.0f});
    expectRows("VECTOR SEARCH IN idx FOR 2 $v YIELD ids, score RETURN ids, score", parameters, {{"0", "0"}, {"2", "1"}});

    setEmbedding(parameters, "v", {0.0f, 1.0f});
    expectRows("VECTOR SEARCH IN idx FOR 1 $v YIELD ids RETURN ids", parameters, {{"1"}});
}

TEST_F(EmbeddingParameterTest, searchesForANeighbourCountAndAQueryVectorParameter) {
    createVectorIndex();

    ParameterMap parameters;
    parameters.insert("k").setInt64(2);
    setEmbedding(parameters, "v", {1.0f, 0.0f});

    expectRows("VECTOR SEARCH IN idx FOR $k $v YIELD ids RETURN ids", parameters, {{"0"}, {"2"}});
}

TEST_F(EmbeddingParameterTest, searchesForAQueryVectorParameterForEveryMatchedRow) {
    createVectorIndex();

    ParameterMap parameters;
    setEmbedding(parameters, "v", {1.0f, 0.0f});

    expectRows("MATCH (n:Person {name: 'Remy'}) VECTOR SEARCH IN idx FOR 2 $v YIELD ids RETURN n.name, ids",
               parameters,
               {{"Remy", "0"}, {"Remy", "2"}});
}

TEST_F(EmbeddingParameterTest, readsTheQueryVectorParameterWhenTheSearchRuns) {
    ParameterMap parameters;
    setEmbedding(parameters, "v", {1.0f, 0.0f});

    std::string program;
    dbProgramOf("VECTOR SEARCH IN idx FOR 2 $v YIELD ids RETURN ids", parameters, program);

    EXPECT_TRUE(contains(program, "db.vector_search(\"idx\", 2, %arg0 : !db.column<!storage.embedding>)")) << program;
}

TEST_F(EmbeddingParameterTest, refusesAQueryVectorParameterOfAnotherDimensionAsALiteral) {
    createVectorIndex();

    const ParameterMap noParameters;
    expectError("VECTOR SEARCH IN idx FOR 2 (1.0, 0.0, 0.0) YIELD ids RETURN ids",
                noParameters,
                "holds vectors of dimension 2, but the query searches for one of dimension 3",
                QueryStatus::Status::EXEC_ERROR);

    ParameterMap parameters;
    setEmbedding(parameters, "v", {1.0f, 0.0f, 0.0f});
    expectError("VECTOR SEARCH IN idx FOR 2 $v YIELD ids RETURN ids",
                parameters,
                "holds vectors of dimension 2, but the query searches for one of dimension 3",
                QueryStatus::Status::EXEC_ERROR);
}

TEST_F(EmbeddingParameterTest, refusesAQueryVectorParameterThatIsNotAnEmbedding) {
    createVectorIndex();

    constexpr std::string_view QUERY = "VECTOR SEARCH IN idx FOR 2 $v YIELD ids RETURN ids";
    constexpr std::string_view REASON = "VECTOR SEARCH query vector must be an Embedding";

    ParameterMap parameters;
    setList(parameters, "v", {1.0, 0.0});
    expectError(QUERY, parameters, REASON, QueryStatus::Status::ANALYZE_ERROR);

    ParameterValue::List& withAString = parameters.insert("v").setList();
    withAString.emplace_back().setString("1.0");
    withAString.emplace_back().setDouble(0.0);
    expectError(QUERY, parameters, REASON, QueryStatus::Status::ANALYZE_ERROR);

    ParameterValue::List& withANull = parameters.insert("v").setList();
    withANull.emplace_back().setDouble(1.0);
    withANull.emplace_back();
    expectError(QUERY, parameters, REASON, QueryStatus::Status::ANALYZE_ERROR);

    parameters.insert("v").setString("(1.0, 0.0)");
    expectError(QUERY, parameters, REASON, QueryStatus::Status::ANALYZE_ERROR);

    parameters.set("v", ParameterValue());
    expectError(QUERY, parameters, REASON, QueryStatus::Status::ANALYZE_ERROR);
}

TEST_F(EmbeddingParameterTest, scoresAnEmbeddingParameterAgainstALiteral) {
    ParameterMap parameters;
    setEmbedding(parameters, "v", {1.0f, 0.0f});

    expectRows("RETURN cosine_similarity($v, (1.0, 0.0)), cosine_similarity($v, (0.0, 1.0)), cosine_similarity($v, (-1.0, 0.0))",
               parameters,
               {{"1", "0", "-1"}});

    setEmbedding(parameters, "v", {4.0f, 3.0f});
    expectRows("RETURN euclidean_distance($v, (4.0, 3.0)), euclidean_distance($v, (1.0, -1.0))", parameters, {{"0", "5"}});
}

TEST_F(EmbeddingParameterTest, scoresTwoEmbeddingParameters) {
    ParameterMap parameters;
    setEmbedding(parameters, "v", {1.0f, 0.0f});
    setEmbedding(parameters, "w", {0.0f, 2.0f});

    expectRows("RETURN cosine_similarity($v, $w), euclidean_distance($w, $w)", parameters, {{"0", "0"}});
}

TEST_F(EmbeddingParameterTest, scoresANullParameterAsANullLiteral) {
    const ParameterMap noParameters;
    expectRows("RETURN cosine_similarity(null, (1.0, 0.0)) AS s", noParameters, {{"null"}});

    ParameterMap parameters;
    parameters.set("v", ParameterValue());
    expectRows("RETURN cosine_similarity($v, (1.0, 0.0)) AS s", parameters, {{"null"}});
    expectRows("RETURN euclidean_distance($v, (1.0, 0.0)) AS s", parameters, {{"null"}});
}

TEST_F(EmbeddingParameterTest, refusesAFunctionArgumentParameterThatIsNotAnEmbedding) {
    const ParameterMap noParameters;
    expectError("RETURN cosine_similarity([1.0, 0.0], (1.0, 0.0))",
                noParameters,
                "Invalid arguments for function 'cosine_similarity'",
                QueryStatus::Status::ANALYZE_ERROR);

    ParameterMap parameters;
    setList(parameters, "v", {1.0, 0.0});
    expectError("RETURN cosine_similarity($v, (1.0, 0.0))",
                parameters,
                "Invalid arguments for function 'cosine_similarity'",
                QueryStatus::Status::ANALYZE_ERROR);
    expectError("RETURN euclidean_distance($v, (1.0, 0.0))",
                parameters,
                "Invalid arguments for function 'euclidean_distance'",
                QueryStatus::Status::ANALYZE_ERROR);

    parameters.insert("v").setString("(1.0, 0.0)");
    expectError("RETURN cosine_similarity($v, (1.0, 0.0))",
                parameters,
                "Invalid arguments for function 'cosine_similarity'",
                QueryStatus::Status::ANALYZE_ERROR);
}

TEST_F(EmbeddingParameterTest, returnsAnEmbeddingParameterAsALiteral) {
    const ParameterMap noParameters;
    expectRows("RETURN (1.0, 0.5, -2.0)", noParameters, {{"1, 0.5, -2"}});

    ParameterMap parameters;
    setEmbedding(parameters, "v", {1.0f, 0.5f, -2.0f});
    expectRows("RETURN $v", parameters, {{"1, 0.5, -2"}});
}

TEST_F(EmbeddingParameterTest, readsAnEmbeddingInsideAListOrAMapParameterAsALiteral) {
    const ParameterMap noParameters;
    expectRows("UNWIND [(1.0, 0.0), (0.0, 1.0)] AS v RETURN v", noParameters, {{"1, 0"}, {"0, 1"}});
    expectRows("RETURN {at: (0.0, 1.0)}.at", noParameters, {{"[0, 1]"}});

    const std::vector<float> first {1.0f, 0.0f};
    const std::vector<float> second {0.0f, 1.0f};

    ParameterMap parameters;
    ParameterValue::List& vectors = parameters.insert("vs").setList();
    vectors.emplace_back().setEmbedding(first);
    vectors.emplace_back().setEmbedding(second);
    expectRows("UNWIND $vs AS v RETURN v", parameters, {{"1, 0"}, {"0, 1"}});

    parameters.insert("m").setMap()["at"].setEmbedding(second);
    expectRows("RETURN $m.at", parameters, {{"[0, 1]"}});
}

TEST_F(EmbeddingParameterTest, writesAnEmbeddingParameterToAProperty) {
    ParameterMap parameters;
    setEmbedding(parameters, "v", {1.0f, 0.5f});
    runWrite("CREATE (:Vec {name: 'created', e: $v})", parameters);

    setEmbedding(parameters, "v", {0.0f, 2.0f});
    runWrite("MATCH (n:Vec {name: 'created'}) SET n.f = $v", parameters);

    const ParameterMap noParameters;
    expectRows("MATCH (n:Vec) RETURN n.e, n.f", noParameters, {{"1, 0.5", "0, 2"}});
}

TEST_F(EmbeddingParameterTest, matchesAnEmbeddingPropertyAgainstAParameterAsALiteral) {
    ParameterMap parameters;
    setEmbedding(parameters, "v", {1.0f, 0.5f});
    runWrite("CREATE (:Vec {name: 'created', e: $v})", parameters);

    const ParameterMap noParameters;
    expectRows("MATCH (n:Vec {e: (1.0, 0.5)}) RETURN n.name", noParameters, {{"created"}});
    expectRows("MATCH (n:Vec) WHERE n.e = (1.0, 0.5) RETURN n.name", noParameters, {{"created"}});
    expectRows("MATCH (n:Vec {e: $v}) RETURN n.name", parameters, {{"created"}});
    expectRows("MATCH (n:Vec) WHERE n.e = $v RETURN n.name", parameters, {{"created"}});
    expectRows("MATCH (n {e: $v}) RETURN n.name", parameters, {{"created"}});
}
