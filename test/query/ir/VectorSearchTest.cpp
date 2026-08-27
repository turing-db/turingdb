#include <gtest/gtest.h>

#include <stddef.h>
#include <stdint.h>
#include <fstream>
#include <memory>
#include <optional>
#include <span>
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
#include "columns/ColumnOptVector.h"
#include "metadata/PropertyType.h"
#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"

#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

namespace {

using IDColumn = ColumnOptVector<types::Int64::Primitive>;
using ScoreColumn = ColumnOptVector<types::Double::Primitive>;

// The two columns a VECTOR SEARCH reports, row by row: the ID the index holds each
// neighbour under and the distance it scored. A query yielding only one of them, or
// crossing them with a match, leaves the columns it did not ask for empty.
class VectorSearchSink : public NLOutputSink {
public:
    void setColumnNames(std::span<const std::string_view> names) override {
        _columnNames.assign(names.begin(), names.end());
    }

    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        _columnCount = chunks.size();
        _rowCount += rowCount;

        const IDColumn* const ids = dynamic_cast<const IDColumn*>(chunks.front());
        const ScoreColumn* const scores = chunks.size() > 1 ? dynamic_cast<const ScoreColumn*>(chunks[1])
                                                            : nullptr;

        for (size_t row = offset; row < offset + rowCount; row++) {
            if (ids) {
                _ids.push_back((*ids)[row].value());
            }

            if (scores) {
                _scores.push_back((*scores)[row].value());
            }
        }
    }

    const std::vector<std::string>& getColumnNames() const { return _columnNames; }
    const std::vector<int64_t>& getIDs() const { return _ids; }
    const std::vector<double>& getScores() const { return _scores; }
    size_t getColumnCount() const { return _columnCount; }
    size_t getRowCount() const { return _rowCount; }

private:
    std::vector<std::string> _columnNames;
    std::vector<int64_t> _ids;
    std::vector<double> _scores;
    size_t _columnCount {0};
    size_t _rowCount {0};
};

// Three vectors along the first axis, so the squared L2 distances from (1, 0, 0, 0)
// are 0, 1 and 9 - distinct, and exact in a float, so the order and the scores are
// the same on every platform.
constexpr std::string_view vectorFile = "1,1,0,0,0\n2,2,0,0,0\n3,4,0,0,0\n";

constexpr std::string_view createIndex = "CREATE VECTOR INDEX vectors WITH DIMENSION 4 METRIC EUCLID";
constexpr std::string_view loadVectors = "LOAD VECTOR FROM \"vectors.csv\" IN vectors";

// simpledb carries eight Person nodes, which is the left factor of the cross product a
// search crossed with a match builds.
constexpr size_t simpledbPersonCount = 8;

}

// VECTOR SEARCH through the MLIR engine: a source op of its own, whose two row-aligned
// columns the rest of the query reads like any other - crossed with a match, cut by a
// LIMIT, projected by a RETURN.
class VectorSearchTest : public TuringTest {
public:
    void initialize() override {
        const fs::Path turingDir = fs::Path {_outDir} / "turing";
        _env = TuringTestEnv::create(turingDir);

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);

        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager());
    }

protected:
    void runQuery(std::string_view query, QueryStatus& status, VectorSearchSink& sink) {
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              ChangeID::head(),
                              &_env->getMem(),
                              &sink);
    }

    void runQuery(std::string_view query, QueryStatus& status) {
        VectorSearchSink sink;
        runQuery(query, status, sink);
    }

    // Every search test needs an index holding the fixture vectors, so the two commands
    // that build one are run here rather than repeated in each case.
    void loadFixtureVectors() {
        const fs::Path path = _env->getConfig().getDataDir() / "vectors.csv";

        std::ofstream file(path.get());
        file << vectorFile;
        file.close();

        QueryStatus createStatus;
        runQuery(createIndex, createStatus);
        ASSERT_TRUE(createStatus.isOk()) << createStatus.getError();

        QueryStatus loadStatus;
        runQuery(loadVectors, loadStatus);
        ASSERT_TRUE(loadStatus.isOk()) << loadStatus.getError();
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

TEST_F(VectorSearchTest, searchReportsTheNearestNeighboursNearestFirst) {
    loadFixtureVectors();

    QueryStatus status;
    VectorSearchSink sink;
    runQuery("VECTOR SEARCH IN vectors FOR 2 (1.0, 0.0, 0.0, 0.0) YIELD ids, score RETURN ids, score",
             status,
             sink);

    ASSERT_TRUE(status.isOk()) << status.getError();
    EXPECT_EQ(sink.getColumnCount(), 2u);
    ASSERT_EQ(sink.getColumnNames().size(), 2u);
    EXPECT_EQ(sink.getColumnNames()[0], "ids");
    EXPECT_EQ(sink.getColumnNames()[1], "score");

    const std::vector<int64_t> expectedIDs {1, 2};
    const std::vector<double> expectedScores {0.0, 1.0};

    EXPECT_EQ(sink.getIDs(), expectedIDs);
    EXPECT_EQ(sink.getScores(), expectedScores);
}

// The score is the second column the op produces, so a query naming only the first must
// still bind the right one - and emit one column, not two.
TEST_F(VectorSearchTest, searchYieldingOnlyTheIDsEmitsThatColumnAlone) {
    loadFixtureVectors();

    QueryStatus status;
    VectorSearchSink sink;
    runQuery("VECTOR SEARCH IN vectors FOR 3 (1.0, 0.0, 0.0, 0.0) YIELD ids RETURN ids", status, sink);

    ASSERT_TRUE(status.isOk()) << status.getError();
    EXPECT_EQ(sink.getColumnCount(), 1u);

    const std::vector<int64_t> expectedIDs {1, 2, 3};
    EXPECT_EQ(sink.getIDs(), expectedIDs);
}

// The search reads none of the rows a MATCH produced, so the two are paired: every
// neighbour is reported against every matched node.
TEST_F(VectorSearchTest, searchCrossesItsNeighboursWithAMatch) {
    loadFixtureVectors();

    QueryStatus status;
    VectorSearchSink sink;
    runQuery("VECTOR SEARCH IN vectors FOR 2 (1.0, 0.0, 0.0, 0.0) YIELD ids "
             "MATCH (n:Person) RETURN ids, n",
             status,
             sink);

    ASSERT_TRUE(status.isOk()) << status.getError();
    EXPECT_EQ(sink.getColumnCount(), 2u);
    EXPECT_EQ(sink.getRowCount(), 2u * simpledbPersonCount);
}

// A search yields ordinary columns, so the projection sorts and cuts them the way it does
// the ones a match bound.
TEST_F(VectorSearchTest, searchNeighboursCanBeSortedAndCut) {
    loadFixtureVectors();

    QueryStatus status;
    VectorSearchSink sink;
    runQuery("VECTOR SEARCH IN vectors FOR 3 (1.0, 0.0, 0.0, 0.0) YIELD ids, score "
             "RETURN ids, score ORDER BY ids DESC LIMIT 1",
             status,
             sink);

    ASSERT_TRUE(status.isOk()) << status.getError();

    const std::vector<int64_t> expectedIDs {3};
    const std::vector<double> expectedScores {9.0};

    EXPECT_EQ(sink.getIDs(), expectedIDs);
    EXPECT_EQ(sink.getScores(), expectedScores);
}

// The predicate of a YIELD ... WHERE reads what the search yielded, so it cuts the
// neighbours the search reported rather than being ignored.
TEST_F(VectorSearchTest, searchFiltersItsNeighboursWithTheYieldPredicate) {
    loadFixtureVectors();

    QueryStatus status;
    VectorSearchSink sink;
    runQuery("VECTOR SEARCH IN vectors FOR 3 (1.0, 0.0, 0.0, 0.0) YIELD ids, score WHERE score > 0.5 "
             "RETURN ids, score",
             status,
             sink);

    ASSERT_TRUE(status.isOk()) << status.getError();

    const std::vector<int64_t> expectedIDs {2, 3};
    const std::vector<double> expectedScores {1.0, 9.0};

    EXPECT_EQ(sink.getIDs(), expectedIDs);
    EXPECT_EQ(sink.getScores(), expectedScores);
}

// Asking for more neighbours than the index holds is not an error: the search reports
// the ones it found, which is the set-membership semantics a const scan has too.
TEST_F(VectorSearchTest, searchStopsAtTheNeighboursTheIndexHolds) {
    loadFixtureVectors();

    QueryStatus status;
    VectorSearchSink sink;
    runQuery("VECTOR SEARCH IN vectors FOR 100 (1.0, 0.0, 0.0, 0.0) YIELD ids RETURN ids", status, sink);

    ASSERT_TRUE(status.isOk()) << status.getError();
    EXPECT_EQ(sink.getRowCount(), 3u);
}

TEST_F(VectorSearchTest, searchOfAnIndexThatDoesNotExistReportsIt) {
    QueryStatus status;
    runQuery("VECTOR SEARCH IN missing FOR 2 (1.0, 0.0, 0.0, 0.0) YIELD ids RETURN ids", status);

    EXPECT_EQ(status.getStatus(), QueryStatus::Status::EXEC_ERROR);
    EXPECT_NE(status.getError().find("Vector index 'missing' not found"), std::string::npos)
        << status.getError();
}

// The index reads the query vector as a flat span of exactly its dimension, so a shorter
// one has to be refused rather than read past its end.
TEST_F(VectorSearchTest, searchForAVectorOfTheWrongDimensionReportsIt) {
    loadFixtureVectors();

    QueryStatus status;
    runQuery("VECTOR SEARCH IN vectors FOR 2 (1.0, 0.0) YIELD ids RETURN ids", status);

    EXPECT_EQ(status.getStatus(), QueryStatus::Status::EXEC_ERROR);
    EXPECT_NE(status.getError().find("dimension"), std::string::npos) << status.getError();
}
