#include <gtest/gtest.h>

#include <stddef.h>
#include <fstream>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "NLOutputSink.h"
#include "QueryInterpreterV3.h"
#include "QueryStatus.h"

#include "Graph.h"
#include "SimpleGraph.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "ID.h"
#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"

#include "StringRowSink.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

// The metric and the index kind a CREATE VECTOR INDEX names, searched through: EUCLID
// scores a squared distance and reports the smallest first, COSINE scores an inner product
// and reports the largest first, and the two index kinds report the same neighbours.
class VectorSearchIndexKindTest : public TuringTest {
public:
    void initialize() override {
        const fs::Path turingDir = fs::Path {_outDir} / "turing";
        _env = TuringTestEnv::create(turingDir);

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);

        _remy = SimpleGraph::findNodeID(graph, "Remy");
        _adam = SimpleGraph::findNodeID(graph, "Adam");

        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager());
    }

protected:
    void runQuery(std::string_view query, QueryStatus& status, NLOutputSink& sink) {
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              ChangeID::head(),
                              &_env->getMem(),
                              &sink);
    }

    void runQuery(std::string_view query, QueryStatus& status) {
        StringRowSink sink;
        runQuery(query, status, sink);
    }

    // Remy on the first axis and Adam on the second, so the two are one unit from the
    // origin and orthogonal to each other: the query vector alone decides which is nearer,
    // whichever metric scores them.
    void buildIndex(std::string_view createCommand, std::string_view indexName) {
        const fs::Path path = _env->getConfig().getDataDir() / "axes.csv";

        std::ofstream file(path.get());
        file << _remy.getValue() << ",1,0,0,0\n" << _adam.getValue() << ",0,1,0,0\n";
        file.close();

        QueryStatus createStatus;
        runQuery(createCommand, createStatus);
        ASSERT_TRUE(createStatus.isOk()) << createStatus.getError();

        QueryStatus loadStatus;
        runQuery(std::string("LOAD VECTOR FROM \"axes.csv\" IN ") + std::string(indexName), loadStatus);
        ASSERT_TRUE(loadStatus.isOk()) << loadStatus.getError();
    }

    void expectOrderedRows(std::string_view query, const std::vector<StringRowSink::Row>& expected) {
        QueryStatus status;
        StringRowSink sink;
        runQuery(query, status, sink);

        ASSERT_TRUE(status.isOk()) << status.getError();
        EXPECT_EQ(sink.getRows(), expected);
    }

    const std::string _graphName = "simpledb";
    NodeID _remy;
    NodeID _adam;
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

TEST_F(VectorSearchIndexKindTest, euclidScoresASquaredDistanceAndReportsTheSmallestFirst) {
    buildIndex("CREATE VECTOR INDEX byDistance WITH DIMENSION 4 METRIC EUCLID", "byDistance");

    expectOrderedRows("VECTOR SEARCH IN byDistance FOR 2 (1.0, 0.0, 0.0, 0.0) YIELD ids, score "
                      "RETURN ids.name, score",
                      {{"Remy", "0"}, {"Adam", "2"}});
}

// COSINE searches by inner product, so the score rises as the neighbour gets closer and
// the order runs the other way round from EUCLID's: the vector equal to the query scores
// 1 and the one orthogonal to it scores 0.
TEST_F(VectorSearchIndexKindTest, cosineScoresAnInnerProductAndReportsTheLargestFirst) {
    buildIndex("CREATE VECTOR INDEX bySimilarity WITH DIMENSION 4 METRIC COSINE", "bySimilarity");

    expectOrderedRows("VECTOR SEARCH IN bySimilarity FOR 2 (1.0, 0.0, 0.0, 0.0) YIELD ids, score "
                      "RETURN ids.name, score",
                      {{"Remy", "1"}, {"Adam", "0"}});
}

TEST_F(VectorSearchIndexKindTest, cosineFollowsTheAxisTheQueryVectorNames) {
    buildIndex("CREATE VECTOR INDEX bySimilarity WITH DIMENSION 4 METRIC COSINE", "bySimilarity");

    expectOrderedRows("VECTOR SEARCH IN bySimilarity FOR 2 (0.0, 1.0, 0.0, 0.0) YIELD ids, score "
                      "RETURN ids.name, score",
                      {{"Adam", "1"}, {"Remy", "0"}});
}

TEST_F(VectorSearchIndexKindTest, anIndexDeclaredFlatSearchesLikeTheDefault) {
    buildIndex("CREATE VECTOR INDEX declared WITH DIMENSION 4 METRIC EUCLID TYPE FLAT", "declared");

    expectOrderedRows("VECTOR SEARCH IN declared FOR 2 (1.0, 0.0, 0.0, 0.0) YIELD ids, score "
                      "RETURN ids.name, score",
                      {{"Remy", "0"}, {"Adam", "2"}});
}

// An HNSW index searches approximately, but a graph of two vectors is walked whole, so the
// neighbours it reports are the exact ones a flat index reports.
TEST_F(VectorSearchIndexKindTest, anIndexDeclaredHnswReportsTheSameNeighbours) {
    buildIndex("CREATE VECTOR INDEX approximate WITH DIMENSION 4 METRIC EUCLID TYPE HNSW", "approximate");

    expectOrderedRows("VECTOR SEARCH IN approximate FOR 2 (1.0, 0.0, 0.0, 0.0) YIELD ids, score "
                      "RETURN ids.name, score",
                      {{"Remy", "0"}, {"Adam", "2"}});
}

// An index nothing was loaded into holds no neighbour to report, which is an empty result
// rather than an error - the same set-membership semantics as asking for more neighbours
// than the index holds.
TEST_F(VectorSearchIndexKindTest, anIndexHoldingNoVectorReportsNoNeighbour) {
    QueryStatus createStatus;
    runQuery("CREATE VECTOR INDEX unloaded WITH DIMENSION 4 METRIC EUCLID", createStatus);
    ASSERT_TRUE(createStatus.isOk()) << createStatus.getError();

    QueryStatus status;
    StringRowSink sink;
    runQuery("VECTOR SEARCH IN unloaded FOR 3 (1.0, 0.0, 0.0, 0.0) YIELD ids RETURN ids", status, sink);

    ASSERT_TRUE(status.isOk()) << status.getError();
    EXPECT_TRUE(sink.getRows().empty());
}
