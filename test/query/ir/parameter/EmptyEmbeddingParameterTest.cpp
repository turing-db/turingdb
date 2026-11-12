#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <string_view>
#include <vector>

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

constexpr std::string_view EMPTY_V = "An embedding cannot be empty, and the value bound to $v holds one";

bool contains(std::string_view text, std::string_view needle) {
    return text.find(needle) != std::string_view::npos;
}

void setEmbedding(ParameterValue& value, const std::vector<float>& floats) {
    value.setEmbedding(floats);
}

}

class EmptyEmbeddingParameterTest : public TuringTest {
public:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);

        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager(), &_env->getMem(), &_env->getCompilerContext());
    }

protected:
    void runQuery(std::string_view query, const ParameterMap& parameters, ChangeID change, QueryStatus& status, StringRowSink& sink) {
        _interpreter->execute(status, query, _graphName, CommitHash::head(), change, &sink, &parameters);
    }

    void expectRows(std::string_view query, const ParameterMap& parameters, const Rows& expected) {
        QueryStatus status;
        StringRowSink sink;
        runQuery(query, parameters, ChangeID::head(), status, sink);

        ASSERT_TRUE(status.isOk()) << query << ": " << status.getError();
        EXPECT_EQ(sink.getRows(), expected) << query;
    }

    void expectRefused(std::string_view query, const ParameterMap& parameters, ChangeID change, std::string_view reason) {
        QueryStatus status;
        StringRowSink sink;
        runQuery(query, parameters, change, status, sink);

        ASSERT_FALSE(status.isOk()) << "accepted: " << query;
        EXPECT_EQ(status.getStatus(), QueryStatus::Status::ANALYZE_ERROR) << query << ": " << status.getError();
        EXPECT_TRUE(contains(status.getError(), reason)) << query << ": " << status.getError();
    }

    void expectRefused(std::string_view query, const ParameterMap& parameters, std::string_view reason) {
        expectRefused(query, parameters, ChangeID::head(), reason);
    }

    void expectWriteRefused(std::string_view query, const ParameterMap& parameters, std::string_view reason) {
        ChangeID change;
        newChange(change);

        expectRefused(query, parameters, change, reason);
        submitChange(change);

        const ParameterMap noParameters;
        expectRows("MATCH (n) WHERE n.e IS NOT NULL RETURN count(n)", noParameters, {{"0"}});
    }

    void runWrite(std::string_view query, const ParameterMap& parameters) {
        ChangeID change;
        newChange(change);

        QueryStatus status;
        StringRowSink sink;
        runQuery(query, parameters, change, status, sink);
        ASSERT_TRUE(status.isOk()) << query << ": " << status.getError();

        submitChange(change);
    }

    void newChange(ChangeID& change) {
        SystemAccessor system = _env->getSystemManager().accessUnique();
        const auto opened = system.newChange(_graphName);
        ASSERT_TRUE(opened);

        change = opened.value()->id();
    }

    void submitChange(ChangeID change) {
        const QueryState submitState(_graphName, &_env->getMem(), &_env->getCompilerContext(), &_queryConfig, nullptr, CommitHash::head(), change);
        const QueryStatus submitStatus = _env->getDB().query("CHANGE SUBMIT", submitState);
        ASSERT_TRUE(submitStatus.isOk()) << submitStatus.getError();
    }

    const std::string _graphName {"simpledb"};
    QueryConfig _queryConfig;
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

TEST_F(EmptyEmbeddingParameterTest, refusesAnEmptyEmbeddingParameterInAnExpression) {
    ParameterMap parameters;
    setEmbedding(parameters.insert("v"), {});

    expectRefused("RETURN $v", parameters, EMPTY_V);
    expectRefused("RETURN cosine_similarity($v, $v)", parameters, EMPTY_V);
    expectRefused("RETURN euclidean_distance($v, (1.0, 0.0))", parameters, EMPTY_V);
    expectRefused("MATCH (n:Person {name: 'Remy'}) RETURN cosine_similarity((1.0, 0.0), $v)", parameters, EMPTY_V);
}

TEST_F(EmptyEmbeddingParameterTest, refusesAnEmptyEmbeddingParameterToMatchOn) {
    const ParameterMap noParameters;
    runWrite("CREATE (:Vec {name: 'a', e: (1.0, 0.0)})", noParameters);

    ParameterMap parameters;
    setEmbedding(parameters.insert("v"), {});

    expectRefused("MATCH (n {e: $v}) RETURN n.name", parameters, EMPTY_V);
    expectRefused("MATCH (n:Vec {e: $v}) RETURN n.name", parameters, EMPTY_V);
    expectRefused("MATCH (n:Vec) WHERE n.e = $v RETURN n.name", parameters, EMPTY_V);
}

TEST_F(EmptyEmbeddingParameterTest, refusesAnEmptyEmbeddingParameterToSearchFor) {
    const ParameterMap noParameters;
    QueryStatus status;
    StringRowSink sink;
    runQuery("CREATE VECTOR INDEX idx WITH DIMENSION 2 METRIC EUCLID", noParameters, ChangeID::head(), status, sink);
    ASSERT_TRUE(status.isOk()) << status.getError();

    ParameterMap parameters;
    setEmbedding(parameters.insert("v"), {});

    expectRefused("VECTOR SEARCH IN idx FOR 2 $v YIELD ids RETURN ids", parameters, EMPTY_V);
}

TEST_F(EmptyEmbeddingParameterTest, refusesToWriteAnEmptyEmbeddingParameter) {
    ParameterMap parameters;
    setEmbedding(parameters.insert("v"), {});
    setEmbedding(parameters.insert("props").setMap()["e"], {});

    constexpr std::string_view EMPTY_PROPS = "An embedding cannot be empty, and the value bound to $props holds one";

    expectWriteRefused("CREATE (:Vec {name: 'z', e: $v})", parameters, EMPTY_V);
    expectWriteRefused("MATCH (n:Person {name: 'Remy'}) SET n.e = $v", parameters, EMPTY_V);
    expectWriteRefused("CREATE (n:Vec) SET n += $props", parameters, EMPTY_PROPS);
    expectWriteRefused("MATCH (n:Person {name: 'Remy'}) SET n += $props", parameters, EMPTY_PROPS);

    const ParameterMap noParameters;
    runWrite("CREATE (:Vec {name: 'a', e: (1.0, 0.0)})", noParameters);
    expectRows("MATCH (n:Vec) RETURN n.name, n.e", noParameters, {{"a", "1, 0"}});
}

TEST_F(EmptyEmbeddingParameterTest, refusesAnEmptyEmbeddingInsideAListOrAMapParameter) {
    constexpr std::string_view EMPTY_VS = "An embedding cannot be empty, and the value bound to $vs holds one";
    constexpr std::string_view EMPTY_M = "An embedding cannot be empty, and the value bound to $m holds one";
    constexpr std::string_view EMPTY_DEEP = "An embedding cannot be empty, and the value bound to $deep holds one";

    ParameterMap parameters;
    ParameterValue::List& vectors = parameters.insert("vs").setList();
    setEmbedding(vectors.emplace_back(), {1.0f, 0.0f});
    setEmbedding(vectors.emplace_back(), {});

    setEmbedding(parameters.insert("m").setMap()["at"], {});

    ParameterValue::List& outer = parameters.insert("deep").setList();
    ParameterValue::Map& inner = outer.emplace_back().setMap();
    setEmbedding(inner["at"].setList().emplace_back(), {});

    expectRefused("UNWIND $vs AS v RETURN v", parameters, EMPTY_VS);
    expectRefused("RETURN $vs[0]", parameters, EMPTY_VS);
    expectRefused("RETURN $m.at", parameters, EMPTY_M);
    expectRefused("RETURN size($deep)", parameters, EMPTY_DEEP);
    expectWriteRefused("UNWIND $vs AS v CREATE (:Vec {e: v})", parameters, EMPTY_VS);
}

TEST_F(EmptyEmbeddingParameterTest, takesAnEmbeddingOfOneFloat) {
    ParameterMap parameters;
    setEmbedding(parameters.insert("v"), {0.5f});

    expectRows("RETURN $v", parameters, {{"0.5"}});

    runWrite("CREATE (:Vec {name: 'one', e: $v})", parameters);

    const ParameterMap noParameters;
    expectRows("MATCH (n:Vec) RETURN n.name, n.e", noParameters, {{"one", "0.5"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
