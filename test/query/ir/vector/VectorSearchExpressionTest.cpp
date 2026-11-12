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

// Node 0 at (1, 0), node 1 at (0, 1), node 2 at (1, 1) and node 3 at (-1, 0). The squared
// L2 distances are 0, 2, 1, 4 from (1, 0), 2, 0, 1, 2 from (0, 1), and 4, 2, 5, 0 from (-1, 0).
constexpr std::string_view VECTOR_FILE = "0,1,0\n1,0,1\n2,1,1\n3,-1,0\n";

bool contains(std::string_view text, std::string_view needle) {
    return text.find(needle) != std::string_view::npos;
}

void setEmbedding(ParameterValue& value, std::initializer_list<float> floats) {
    const std::vector<float> embedding(floats);
    value.setEmbedding(embedding);
}

}

class VectorSearchExpressionTest : public TuringTest {
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

    // Three :X nodes, each holding one of the vectors the searches look around, and a :Y
    // node holding none
    void createSearchedNodes() {
        ChangeID change;
        {
            SystemAccessor system = _env->getSystemManager().accessUnique();
            const auto opened = system.newChange(_graphName);
            ASSERT_TRUE(opened);
            change = opened.value()->id();
        }

        const ParameterMap noParameters;
        QueryStatus status;
        StringRowSink sink;
        _interpreter->execute(status,
                              "CREATE (:X {name: 'a', e: (1.0, 0.0)}), (:X {name: 'b', e: (0.0, 1.0)}), "
                              "(:X {name: 'c', e: (-1.0, 0.0)}), (:Y {name: 'd'})",
                              _graphName,
                              CommitHash::head(),
                              change,
                              &sink,
                              &noParameters);
        ASSERT_TRUE(status.isOk()) << status.getError();

        const QueryState submitState(_graphName, &_env->getMem(), &_env->getCompilerContext(), &_queryConfig, nullptr, CommitHash::head(), change);
        const QueryStatus submitStatus = _env->getDB().query("CHANGE SUBMIT", submitState);
        ASSERT_TRUE(submitStatus.isOk()) << submitStatus.getError();
    }

    void expectRows(std::string_view query, const ParameterMap& parameters, const Rows& expected) {
        QueryStatus status;
        StringRowSink sink;
        runQuery(query, parameters, status, sink);

        ASSERT_TRUE(status.isOk()) << query << ": " << status.getError();
        EXPECT_EQ(sink.getRows(), expected) << query;
    }

    void expectRows(std::string_view query, const Rows& expected) {
        const ParameterMap noParameters;
        expectRows(query, noParameters, expected);
    }

    void expectError(std::string_view query, const ParameterMap& parameters, std::string_view reason) {
        QueryStatus status;
        StringRowSink sink;
        runQuery(query, parameters, status, sink);

        ASSERT_FALSE(status.isOk()) << "accepted: " << query;
        EXPECT_EQ(status.getStatus(), QueryStatus::Status::EXEC_ERROR) << query << ": " << status.getError();
        EXPECT_TRUE(contains(status.getError(), reason)) << query << ": " << status.getError();
    }

    void expectError(std::string_view query, std::string_view reason) {
        const ParameterMap noParameters;
        expectError(query, noParameters, reason);
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

TEST_F(VectorSearchExpressionTest, searchesForTheQueryVectorAMapParameterHolds) {
    createVectorIndex();

    ParameterMap parameters;
    setEmbedding(parameters.insert("m").setMap()["v"], {1.0f, 0.0f});

    expectRows("VECTOR SEARCH IN idx FOR 2 $m.v YIELD ids RETURN ids", parameters, {{"0"}, {"2"}});
}

TEST_F(VectorSearchExpressionTest, searchesForTheQueryVectorAWithProjects) {
    createVectorIndex();

    expectRows("WITH (1.0, 0.0) AS w VECTOR SEARCH IN idx FOR 2 w YIELD ids RETURN ids", {{"0"}, {"2"}});
}

TEST_F(VectorSearchExpressionTest, searchesTheIndexAParameterNames) {
    createVectorIndex();

    ParameterMap parameters;
    parameters.insert("idx").setString("idx");

    expectRows("VECTOR SEARCH IN $idx FOR 2 (1.0, 0.0) YIELD ids RETURN ids", parameters, {{"0"}, {"2"}});
    expectRows("VECTOR SEARCH IN 'idx' FOR 2 (1.0, 0.0) YIELD ids RETURN ids", parameters, {{"0"}, {"2"}});

    parameters.insert("m").setMap()["name"].setString("idx");
    expectRows("VECTOR SEARCH IN $m.name FOR 1 (0.0, 1.0) YIELD ids RETURN ids", parameters, {{"1"}});
}

TEST_F(VectorSearchExpressionTest, refusesAnIndexNameNamingNoIndex) {
    createVectorIndex();

    ParameterMap parameters;
    parameters.insert("idx").setString("nope");

    expectError("VECTOR SEARCH IN $idx FOR 2 (1.0, 0.0) YIELD ids RETURN ids", parameters, "Vector index 'nope' not found");
}

TEST_F(VectorSearchExpressionTest, searchesForANeighbourCountComputedFromParameters) {
    createVectorIndex();

    ParameterMap parameters;
    parameters.insert("k").setInt64(1);
    parameters.insert("s").setString("3");
    setEmbedding(parameters.insert("v"), {1.0f, 0.0f});

    expectRows("VECTOR SEARCH IN idx FOR $k + 1 $v YIELD ids RETURN ids", parameters, {{"0"}, {"2"}});
    expectRows("VECTOR SEARCH IN idx FOR toInteger($s) $v YIELD ids RETURN ids", parameters, {{"0"}, {"2"}, {"1"}});
}

TEST_F(VectorSearchExpressionTest, readsAComputedNeighbourCountWhenTheSearchStarts) {
    ParameterMap parameters;
    parameters.insert("k").setInt64(1);

    std::string program;
    dbProgramOf("VECTOR SEARCH IN idx FOR $k + 1 (1.0, 0.0) YIELD ids RETURN ids", parameters, program);

    EXPECT_TRUE(contains(program, "db.vector_search(\"idx\", %")) << program;
    EXPECT_FALSE(contains(program, "db.call_subquery")) << program;
}

TEST_F(VectorSearchExpressionTest, crossesTheMatchedRowsWithASearchForAComputedNeighbourCount) {
    createVectorIndex();
    createSearchedNodes();

    ParameterMap parameters;
    parameters.insert("s").setString("2");
    setEmbedding(parameters.insert("v"), {1.0f, 0.0f});

    expectRows("MATCH (n:X) VECTOR SEARCH IN idx FOR toInteger($s) $v YIELD ids, score "
               "RETURN n.name, ids ORDER BY n.name, score",
               parameters,
               {{"a", "0"}, {"a", "2"}, {"b", "0"}, {"b", "2"}, {"c", "0"}, {"c", "2"}});
}

TEST_F(VectorSearchExpressionTest, searchesForTheQueryVectorOfEachRow) {
    createVectorIndex();

    expectRows("UNWIND [(1.0, 0.0), (0.0, 1.0), (-1.0, 0.0)] AS v WITH v AS w "
               "VECTOR SEARCH IN idx FOR 2 w YIELD ids, score RETURN w, ids, score",
               {{"1, 0", "0", "0"},
                {"1, 0", "2", "1"},
                {"0, 1", "1", "0"},
                {"0, 1", "2", "1"},
                {"-1, 0", "3", "0"},
                {"-1, 0", "1", "2"}});
}

TEST_F(VectorSearchExpressionTest, searchesForTheNeighbourCountOfEachRow) {
    createVectorIndex();

    expectRows("UNWIND [1, 2, 3] AS k VECTOR SEARCH IN idx FOR k (1.0, 0.0) YIELD ids RETURN k, ids",
               {{"1", "0"}, {"2", "0"}, {"2", "2"}, {"3", "0"}, {"3", "2"}, {"3", "1"}});
}

TEST_F(VectorSearchExpressionTest, searchesForTheVectorEachMatchedNodeHolds) {
    createVectorIndex();
    createSearchedNodes();

    expectRows("MATCH (n:X) VECTOR SEARCH IN idx FOR 2 n.e YIELD ids, score "
               "RETURN n.name, ids, score ORDER BY n.name, score",
               {{"a", "0", "0"}, {"a", "2", "1"}, {"b", "1", "0"}, {"b", "2", "1"}, {"c", "3", "0"}, {"c", "1", "2"}});
}

TEST_F(VectorSearchExpressionTest, searchesOncePerRowInsideACallSubquery) {
    createVectorIndex();
    createSearchedNodes();

    expectRows("MATCH (n:X) CALL { WITH n VECTOR SEARCH IN idx FOR 1 n.e YIELD ids RETURN ids } "
               "RETURN n.name, ids ORDER BY n.name",
               {{"a", "0"}, {"b", "1"}, {"c", "3"}});
}

TEST_F(VectorSearchExpressionTest, runsAPerRowSearchInsideACallSubquery) {
    std::string program;
    const ParameterMap noParameters;
    dbProgramOf("UNWIND [(1.0, 0.0), (0.0, 1.0)] AS w VECTOR SEARCH IN idx FOR 2 w YIELD ids RETURN ids", noParameters, program);

    EXPECT_TRUE(contains(program, "db.call_subquery")) << program;
    EXPECT_TRUE(contains(program, "db.row_constant")) << program;
}

TEST_F(VectorSearchExpressionTest, searchesForNoRowWhenNoRowReachesTheSearch) {
    createVectorIndex();
    createSearchedNodes();

    expectRows("MATCH (n:X) WHERE n.name = 'none' VECTOR SEARCH IN idx FOR 2 n.e YIELD ids RETURN n.name, ids", {});
    expectRows("UNWIND [1, 2] AS k WITH k WHERE k > 5 VECTOR SEARCH IN idx FOR k (1.0, 0.0) YIELD ids RETURN k, ids", {});
}

TEST_F(VectorSearchExpressionTest, refusesARowVectorOfAnotherDimension) {
    createVectorIndex();

    expectError("UNWIND [(1.0, 0.0), (1.0, 0.0, 0.0)] AS w VECTOR SEARCH IN idx FOR 2 w YIELD ids RETURN ids",
                "holds vectors of dimension 2, but the query searches for one of dimension 3");
}

TEST_F(VectorSearchExpressionTest, refusesANullQueryVectorOrNeighbourCount) {
    createVectorIndex();
    createSearchedNodes();

    expectError("MATCH (n:Y) VECTOR SEARCH IN idx FOR 2 n.e YIELD ids RETURN ids",
                "VECTOR SEARCH query vector must be an Embedding, got Null");

    ParameterMap parameters;
    parameters.insert("m").setMap()["k"];
    expectError("VECTOR SEARCH IN idx FOR $m.k (1.0, 0.0) YIELD ids RETURN ids", parameters, "VECTOR SEARCH k value must be an integer, got Null");
}

TEST_F(VectorSearchExpressionTest, refusesARowNeighbourCountBelowOne) {
    createVectorIndex();

    expectError("UNWIND [2, 0] AS k VECTOR SEARCH IN idx FOR k (1.0, 0.0) YIELD ids RETURN ids",
                "VECTOR SEARCH k value must be greater than 0, got 0");
}

TEST_F(VectorSearchExpressionTest, searchesTheIndexEachRowNames) {
    createVectorIndex();

    expectRows("UNWIND ['idx', 'idx'] AS i VECTOR SEARCH IN (i) FOR 1 (0.0, 1.0) YIELD ids RETURN i, ids",
               {{"idx", "1"}, {"idx", "1"}});
    expectError("UNWIND ['idx', 'nope'] AS i VECTOR SEARCH IN (i) FOR 1 (0.0, 1.0) YIELD ids RETURN i, ids",
                "Vector index 'nope' not found");
}

TEST_F(VectorSearchExpressionTest, cutsAndCountsTheRowsOfPerRowSearches) {
    createVectorIndex();
    createSearchedNodes();

    expectRows("UNWIND [(1.0, 0.0), (0.0, 1.0), (-1.0, 0.0)] AS w VECTOR SEARCH IN idx FOR 2 w YIELD ids RETURN ids LIMIT 3",
               {{"0"}, {"2"}, {"1"}});
    expectRows("MATCH (n:X) VECTOR SEARCH IN idx FOR 2 n.e YIELD ids RETURN count(*)", {{"6"}});
    expectRows("MATCH (n:X) VECTOR SEARCH IN idx FOR 2 n.e YIELD ids RETURN n.name, count(ids) ORDER BY n.name",
               {{"a", "2"}, {"b", "2"}, {"c", "2"}});
}

TEST_F(VectorSearchExpressionTest, matchesFromTheNeighboursOfEachRow) {
    createVectorIndex();
    createSearchedNodes();

    expectRows("MATCH (n:X) VECTOR SEARCH IN idx FOR 1 n.e YIELD ids MATCH (ids)-[:INTERESTED_IN]->(m) "
               "RETURN n.name, ids.name, m.name ORDER BY n.name, m.name",
               {{"a", "Remy", "Computers"},
                {"a", "Remy", "Eighties"},
                {"a", "Remy", "Ghosts"},
                {"b", "Adam", "Bio"},
                {"b", "Adam", "Cooking"}});
}

TEST_F(VectorSearchExpressionTest, searchesForARowConstantOfABodyRunOncePerRow) {
    createVectorIndex();

    expectRows("UNWIND [(1.0, 0.0), (0.0, 1.0)] AS w CALL { WITH w VECTOR SEARCH IN idx FOR 2 w YIELD ids RETURN count(ids) AS c } "
               "RETURN w, c",
               {{"1, 0", "2"}, {"0, 1", "2"}});
    expectRows("UNWIND [(1.0, 0.0), (0.0, 1.0)] AS w CALL { WITH w MATCH (p:Person {name: 'Remy'}) "
               "VECTOR SEARCH IN idx FOR 1 w YIELD ids RETURN min(id(ids)) AS c } RETURN w, c",
               {{"1, 0", "0"}, {"0, 1", "1"}});
}

TEST_F(VectorSearchExpressionTest, searchesAroundTheNeighboursAnotherSearchReported) {
    createVectorIndex();
    createSearchedNodes();

    expectRows("MATCH (n:X {name: 'b'}) VECTOR SEARCH IN idx FOR 1 n.e YIELD ids AS first "
               "MATCH (m:X) WHERE m.name = 'c' VECTOR SEARCH IN idx FOR 2 m.e YIELD ids AS second "
               "RETURN first, second ORDER BY second",
               {{"1", "1"}, {"1", "3"}});
}
