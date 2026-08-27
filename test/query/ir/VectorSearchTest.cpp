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
#include "ID.h"
#include "columns/ColumnIDs.h"
#include "columns/ColumnOptVector.h"
#include "metadata/PropertyType.h"
#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"

#include "StringRowSink.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

namespace {

using ScoreColumn = ColumnOptVector<types::Double::Primitive>;

// The two columns a VECTOR SEARCH reports, row by row: the node the index holds each
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

        const ColumnNodeIDs* const ids = dynamic_cast<const ColumnNodeIDs*>(chunks.front());
        const ScoreColumn* const scores = chunks.size() > 1 ? dynamic_cast<const ScoreColumn*>(chunks[1])
                                                            : nullptr;

        for (size_t row = offset; row < offset + rowCount; row++) {
            if (ids) {
                _ids.push_back((*ids)[row].getValue());
            }

            if (scores) {
                _scores.push_back((*scores)[row].value());
            }
        }
    }

    const std::vector<std::string>& getColumnNames() const { return _columnNames; }
    const std::vector<uint64_t>& getIDs() const { return _ids; }
    const std::vector<double>& getScores() const { return _scores; }
    size_t getColumnCount() const { return _columnCount; }
    size_t getRowCount() const { return _rowCount; }

private:
    std::vector<std::string> _columnNames;
    std::vector<uint64_t> _ids;
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

// The same three vectors under the IDs simpledb holds as ages, so a search reports IDs a
// MATCH can constrain a node property against: 32 is the age Remy and Adam carry, 20 is
// nobody's, and 99 is the farthest of the three from (1, 0, 0, 0).
constexpr std::string_view seedFile = "32,1,0,0,0\n20,2,0,0,0\n99,4,0,0,0\n";

constexpr std::string_view createSeedIndex = "CREATE VECTOR INDEX seeds WITH DIMENSION 4 METRIC EUCLID";
constexpr std::string_view loadSeeds = "LOAD VECTOR FROM \"seeds.csv\" IN seeds";

// simpledb carries eight Person nodes, which is the left factor of the cross product a
// search crossed with a match builds.
constexpr size_t simpledbPersonCount = 8;

constexpr std::string_view createNodeIndex = "CREATE VECTOR INDEX nodes WITH DIMENSION 4 METRIC EUCLID";
constexpr std::string_view loadNodes = "LOAD VECTOR FROM \"nodes.csv\" IN nodes";

// The three interests Remy is INTERESTED_IN, which is what a traversal seeded on Remy
// reports whichever way the query names him.
const std::vector<std::string> remyInterests {"Computers", "Eighties", "Ghosts"};

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

        // A node ID follows the label-set order of its data part rather than the order the
        // fixture writes its nodes, so the two the vector index is keyed on are looked up
        // rather than assumed.
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

    // The sibling index of loadFixtureVectors, holding the same vectors under IDs the
    // simpledb fixture carries as node properties.
    void loadSeedVectors() {
        const fs::Path path = _env->getConfig().getDataDir() / "seeds.csv";

        std::ofstream file(path.get());
        file << seedFile;
        file.close();

        QueryStatus createStatus;
        runQuery(createSeedIndex, createStatus);
        ASSERT_TRUE(createStatus.isOk()) << createStatus.getError();

        QueryStatus loadStatus;
        runQuery(loadSeeds, loadStatus);
        ASSERT_TRUE(loadStatus.isOk()) << loadStatus.getError();
    }

    // The index of nodes, keyed on the node IDs the graph gave Remy and Adam, so what the
    // search reports is a node a pattern can walk out of. Remy is the nearer of the two to
    // (1, 0, 0, 0).
    void loadNodeVectors() {
        const fs::Path path = _env->getConfig().getDataDir() / "nodes.csv";

        std::ofstream file(path.get());
        file << _remy.getValue() << ",1,0,0,0\n" << _adam.getValue() << ",2,0,0,0\n";
        file.close();

        QueryStatus createStatus;
        runQuery(createNodeIndex, createStatus);
        ASSERT_TRUE(createStatus.isOk()) << createStatus.getError();

        QueryStatus loadStatus;
        runQuery(loadNodes, loadStatus);
        ASSERT_TRUE(loadStatus.isOk()) << loadStatus.getError();
    }

    const std::string _graphName = "simpledb";
    NodeID _remy;
    NodeID _adam;
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

    const std::vector<uint64_t> expectedIDs {1, 2};
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

    const std::vector<uint64_t> expectedIDs {1, 2, 3};
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

    const std::vector<uint64_t> expectedIDs {3};
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

    const std::vector<uint64_t> expectedIDs {2, 3};
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

// The documented shape: what the search yielded seeds the traversal through a MATCH's
// WHERE, which is the only way an ID column can - it holds the IDs the index was loaded
// with, not node IDs, so it can never be a pattern variable. The constraint picks the
// nodes the pattern hops out of, and the yielded column rides through the hop beside them.
TEST_F(VectorSearchTest, searchSeedsATraversalThroughAWhereConstraint) {
    loadSeedVectors();

    QueryStatus status;
    StringRowSink sink;
    runQuery("VECTOR SEARCH IN seeds FOR 2 (1.0, 0.0, 0.0, 0.0) YIELD ids "
             "MATCH (n:Person)-[:INTERESTED_IN]->(m:Interest) WHERE n.age = ids "
             "RETURN n.name, m.name, ids",
             status,
             sink);

    ASSERT_TRUE(status.isOk()) << status.getError();

    // The two nearest IDs are 32 and 20; only 32 is an age simpledb holds, and Remy and
    // Adam are the two carrying it. A Person with no age at all fetches null, which the
    // equality drops.
    const std::vector<StringRowSink::Row> expected {{"Adam", "Bio", "32"},
                                                    {"Adam", "Cooking", "32"},
                                                    {"Remy", "Computers", "32"},
                                                    {"Remy", "Eighties", "32"},
                                                    {"Remy", "Ghosts", "32"}};

    std::vector<StringRowSink::Row> rows;
    sink.sortedRows(rows);

    EXPECT_EQ(rows, expected);
}

// The same shape with a query vector whose nearest neighbour is an ID no node carries: the
// constraint holds for no row, so the traversal reports none. A constraint that was dropped
// rather than applied would report every hop the pattern walks.
TEST_F(VectorSearchTest, searchSeedingATraversalMatchesNothingWhenNoNeighbourAgrees) {
    loadSeedVectors();

    QueryStatus status;
    StringRowSink sink;
    runQuery("VECTOR SEARCH IN seeds FOR 1 (4.0, 0.0, 0.0, 0.0) YIELD ids "
             "MATCH (n:Person)-[:INTERESTED_IN]->(m:Interest) WHERE n.age = ids "
             "RETURN n.name, m.name",
             status,
             sink);

    ASSERT_TRUE(status.isOk()) << status.getError();
    EXPECT_TRUE(sink.getRows().empty());
}

// A neighbour is the node the index holds it under, so the pattern names the yielded
// variable itself and the traversal expands that column - no scan of the graph, and no
// product to pair the two sides with.
TEST_F(VectorSearchTest, searchDrivesATraversalNamedOnTheYieldedVariable) {
    loadNodeVectors();

    QueryStatus status;
    StringRowSink sink;
    runQuery("VECTOR SEARCH IN nodes FOR 1 (1.0, 0.0, 0.0, 0.0) YIELD ids "
             "MATCH (ids)-[:INTERESTED_IN]->(m) RETURN ids, m.name",
             status,
             sink);

    ASSERT_TRUE(status.isOk()) << status.getError();

    const std::string remy = std::to_string(_remy.getValue());
    std::vector<StringRowSink::Row> expected;
    for (const std::string& interest : remyInterests) {
        expected.push_back({remy, interest});
    }

    std::vector<StringRowSink::Row> rows;
    sink.sortedRows(rows);

    EXPECT_EQ(rows, expected);
}

// The same traversal written the other way round: the pattern matches on its own and an
// equality ties one of its nodes to the neighbour. It reports the rows the driven form
// reports, since the two say the same thing about the same nodes.
TEST_F(VectorSearchTest, searchConstrainsAMatchedNodeByEqualityWithTheNeighbour) {
    loadNodeVectors();

    QueryStatus status;
    StringRowSink sink;
    runQuery("VECTOR SEARCH IN nodes FOR 1 (1.0, 0.0, 0.0, 0.0) YIELD ids "
             "MATCH (n)-[:INTERESTED_IN]->(m) WHERE n = ids RETURN n.name, m.name",
             status,
             sink);

    ASSERT_TRUE(status.isOk()) << status.getError();

    std::vector<StringRowSink::Row> expected;
    for (const std::string& interest : remyInterests) {
        expected.push_back({"Remy", interest});
    }

    std::vector<StringRowSink::Row> rows;
    sink.sortedRows(rows);

    EXPECT_EQ(rows, expected);
}

// The score rides through the hop beside the neighbour it belongs to, so a driven
// traversal can still project it: it is replicated once per edge the expansion walked.
TEST_F(VectorSearchTest, aDrivenTraversalCarriesTheScoreThroughTheHop) {
    loadNodeVectors();

    QueryStatus status;
    StringRowSink sink;
    runQuery("VECTOR SEARCH IN nodes FOR 2 (1.0, 0.0, 0.0, 0.0) YIELD ids, score "
             "MATCH (ids)-[:INTERESTED_IN]->(m) RETURN m.name, score",
             status,
             sink);

    ASSERT_TRUE(status.isOk()) << status.getError();

    // Remy scored 0 and is interested in three things, Adam scored 1 and in two.
    const std::vector<StringRowSink::Row> expected {{"Bio", "1"},
                                                    {"Computers", "0"},
                                                    {"Cooking", "1"},
                                                    {"Eighties", "0"},
                                                    {"Ghosts", "0"}};

    std::vector<StringRowSink::Row> rows;
    sink.sortedRows(rows);

    EXPECT_EQ(rows, expected);
}
