#include <gtest/gtest.h>

#include <stddef.h>
#include <algorithm>
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

namespace {

constexpr std::string_view createIndex = "CREATE VECTOR INDEX people WITH DIMENSION 4 METRIC EUCLID";
constexpr std::string_view loadPeople = "LOAD VECTOR FROM \"people.csv\" IN people";

// The three nodes the index is keyed on, nearest first, and the squared L2 distances the
// search scores them at against (1, 0, 0, 0): 0, 9 and 36. They are exact in a float and
// they sum to 45, so an average over the three is the whole number 15.
constexpr std::string_view searchThree = "VECTOR SEARCH IN people FOR 3 (1.0, 0.0, 0.0, 0.0) ";
constexpr std::string_view searchTwo = "VECTOR SEARCH IN people FOR 2 (1.0, 0.0, 0.0, 0.0) ";
constexpr std::string_view searchOne = "VECTOR SEARCH IN people FOR 1 (1.0, 0.0, 0.0, 0.0) ";

// Wider than the out-degree of every node the tests sample, so a sample is the whole
// neighbourhood and the rows do not depend on the seed.
constexpr std::string_view sampleWhole = "CALL gnn.neighbourhoodSample(ids, 8, 42) YIELD tgt ";

// simpledb carries eight Person nodes, which is one factor of the cross product a search
// crossed with an unconstrained match builds.
constexpr size_t simpledbPersonCount = 8;

}

// VECTOR SEARCH composed with the rest of the language rather than with a MATCH alone: its
// columns carried across a WITH, a CALL reading what it yielded and yielding into it, and
// an aggregate folding its rows.
class VectorSearchCompositionTest : public TuringTest {
public:
    void initialize() override {
        const fs::Path turingDir = fs::Path {_outDir} / "turing";
        _env = TuringTestEnv::create(turingDir);

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);

        // A node ID follows the label-set order of its data part rather than the order the
        // fixture writes its nodes, so the three the index is keyed on are looked up.
        _remy = SimpleGraph::findNodeID(graph, "Remy");
        _adam = SimpleGraph::findNodeID(graph, "Adam");
        _maxime = SimpleGraph::findNodeID(graph, "Maxime");

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

    void loadPeopleVectors() {
        const fs::Path path = _env->getConfig().getDataDir() / "people.csv";

        std::ofstream file(path.get());
        file << _remy.getValue() << ",1,0,0,0\n"
             << _adam.getValue() << ",4,0,0,0\n"
             << _maxime.getValue() << ",7,0,0,0\n";
        file.close();

        QueryStatus createStatus;
        runQuery(createIndex, createStatus);
        ASSERT_TRUE(createStatus.isOk()) << createStatus.getError();

        QueryStatus loadStatus;
        runQuery(loadPeople, loadStatus);
        ASSERT_TRUE(loadStatus.isOk()) << loadStatus.getError();
    }

    void expectOrderedRows(std::string_view query, const std::vector<StringRowSink::Row>& expected) {
        QueryStatus status;
        StringRowSink sink;
        runQuery(query, status, sink);

        ASSERT_TRUE(status.isOk()) << status.getError();
        EXPECT_EQ(sink.getRows(), expected);
    }

    void expectRows(std::string_view query, const std::vector<StringRowSink::Row>& expected) {
        QueryStatus status;
        StringRowSink sink;
        runQuery(query, status, sink);

        ASSERT_TRUE(status.isOk()) << status.getError();

        std::vector<StringRowSink::Row> sortedExpected = expected;
        std::ranges::sort(sortedExpected);

        std::vector<StringRowSink::Row> rows;
        sink.sortedRows(rows);

        EXPECT_EQ(rows, sortedExpected);
    }

    const std::string _graphName = "simpledb";
    NodeID _remy;
    NodeID _adam;
    NodeID _maxime;
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

TEST_F(VectorSearchCompositionTest, withCarriesBothSearchColumnsIntoTheNextPart) {
    loadPeopleVectors();

    expectRows(std::string(searchThree) + "YIELD ids, score "
               "WITH ids AS seed, score AS distance "
               "RETURN seed.name, distance",
               {{"Remy", "0"}, {"Adam", "9"}, {"Maxime", "36"}});
}

TEST_F(VectorSearchCompositionTest, withSitsBetweenTheSearchAndTheMatchItSeeds) {
    loadPeopleVectors();

    expectRows(std::string(searchTwo) + "YIELD ids AS seed "
               "WITH seed "
               "MATCH (seed)-[:INTERESTED_IN]->(m) "
               "RETURN seed.name, m.name",
               {{"Remy", "Ghosts"},
                {"Remy", "Computers"},
                {"Remy", "Eighties"},
                {"Adam", "Bio"},
                {"Adam", "Cooking"}});
}

// A search after a WITH reads no column the WITH carried, so it opens a dataflow of its
// own and what the WITH held becomes the other factor of a cross product.
TEST_F(VectorSearchCompositionTest, searchAfterAWithCrossesWhatTheWithCarried) {
    loadPeopleVectors();

    expectRows("MATCH (n {name: 'Luc'}) "
               "WITH n "
               + std::string(searchTwo) + "YIELD ids "
               "RETURN n.name, ids.name",
               {{"Luc", "Remy"}, {"Luc", "Adam"}});
}

TEST_F(VectorSearchCompositionTest, withWhereCutsTheSearchRowsBeforeTheProjection) {
    loadPeopleVectors();

    expectRows(std::string(searchThree) + "YIELD ids, score "
               "WITH ids, score WHERE score < 10 "
               "RETURN ids.name, score",
               {{"Remy", "0"}, {"Adam", "9"}});
}

TEST_F(VectorSearchCompositionTest, withSortsAndCutsTheSearchRows) {
    loadPeopleVectors();

    expectRows(std::string(searchThree) + "YIELD ids, score "
               "WITH ids, score ORDER BY score DESC LIMIT 1 "
               "RETURN ids.name, score",
               {{"Maxime", "36"}});
}

TEST_F(VectorSearchCompositionTest, withFoldsTheSearchRowsIntoOne) {
    loadPeopleVectors();

    expectRows(std::string(searchThree) + "YIELD ids, score "
               "WITH count(ids) AS neighbours, sum(score) AS total "
               "RETURN neighbours, total",
               {{"3", "45"}});
}

TEST_F(VectorSearchCompositionTest, withGroupsTheExpansionByTheNeighbourItCarried) {
    loadPeopleVectors();

    expectRows(std::string(searchThree) + "YIELD ids AS seed "
               "MATCH (seed)-[:INTERESTED_IN]->(m) "
               "WITH seed, count(m) AS interests "
               "RETURN seed.name, interests",
               {{"Remy", "3"}, {"Adam", "2"}, {"Maxime", "2"}});
}

// The direction the header comment on the search generation claims but no test covered: a
// CALL whose argument is the node column a search written before it yielded.
TEST_F(VectorSearchCompositionTest, callSamplesTheNodeTheSearchYielded) {
    loadPeopleVectors();

    expectRows(std::string(searchOne) + "YIELD ids "
               + std::string(sampleWhole) +
               "RETURN ids.name, tgt.name",
               {{"Remy", "Adam"},
                {"Remy", "Ghosts"},
                {"Remy", "Computers"},
                {"Remy", "Eighties"}});
}

TEST_F(VectorSearchCompositionTest, callCarriesTheScoreOfTheRowThatDroveIt) {
    loadPeopleVectors();

    expectRows(std::string(searchTwo) + "YIELD ids, score "
               + std::string(sampleWhole) +
               "RETURN score, tgt.name",
               {{"0", "Adam"},
                {"0", "Ghosts"},
                {"0", "Computers"},
                {"0", "Eighties"},
                {"9", "Remy"},
                {"9", "Bio"},
                {"9", "Cooking"}});
}

TEST_F(VectorSearchCompositionTest, searchAfterACallCrossesWhatTheCallYielded) {
    loadPeopleVectors();

    expectRows("MATCH (n {name: 'Adam'}) "
               "CALL gnn.neighbourhoodSample(n, 8, 42) YIELD tgt "
               + std::string(searchOne) + "YIELD ids "
               "RETURN tgt.name, ids.name",
               {{"Remy", "Remy"}, {"Bio", "Remy"}, {"Cooking", "Remy"}});
}

// A search between two calls: the first reads what the search yielded, the second what the
// first yielded, so the three statements are generated in the order the query writes them.
TEST_F(VectorSearchCompositionTest, searchDrivesTwoChainedCalls) {
    loadPeopleVectors();

    expectRows(std::string(searchOne) + "YIELD ids "
               + std::string(sampleWhole) +
               "CALL gnn.neighbourhoodSample(tgt, 8, 42) YIELD tgt AS hop2 "
               "RETURN tgt.name, hop2.name",
               {{"Adam", "Remy"},
                {"Adam", "Bio"},
                {"Adam", "Cooking"},
                {"Ghosts", "Remy"}});
}

TEST_F(VectorSearchCompositionTest, countsTheNeighboursTheSearchReported) {
    loadPeopleVectors();

    expectRows(std::string(searchThree) + "YIELD ids RETURN count(ids)", {{"3"}});
}

TEST_F(VectorSearchCompositionTest, aggregatesTheScoresTheSearchReported) {
    loadPeopleVectors();

    expectRows(std::string(searchThree) + "YIELD ids, score "
               "RETURN count(score), sum(score), avg(score), min(score), max(score)",
               {{"3", "45", "15", "0", "36"}});
}

TEST_F(VectorSearchCompositionTest, groupsTheExpansionByTheNeighbourTheSearchReported) {
    loadPeopleVectors();

    expectRows(std::string(searchThree) + "YIELD ids "
               "MATCH (ids)-[:INTERESTED_IN]->(m) "
               "RETURN ids.name, count(m)",
               {{"Remy", "3"}, {"Adam", "2"}, {"Maxime", "2"}});
}

TEST_F(VectorSearchCompositionTest, groupsTheExpansionByTheScoreTheSearchReported) {
    loadPeopleVectors();

    expectRows(std::string(searchThree) + "YIELD ids, score "
               "MATCH (ids)-[:INTERESTED_IN]->(m) "
               "RETURN score, count(m)",
               {{"0", "3"}, {"9", "2"}, {"36", "2"}});
}

TEST_F(VectorSearchCompositionTest, countsTheWholeExpansionWithNoGroupingKey) {
    loadPeopleVectors();

    expectRows(std::string(searchThree) + "YIELD ids "
               "MATCH (ids)-[:INTERESTED_IN]->(m) "
               "RETURN count(m)",
               {{"7"}});
}

// Bio is the interest both Adam and Maxime point at, so the seven expanded rows carry six
// distinct targets.
TEST_F(VectorSearchCompositionTest, countsTheDistinctTargetsOfTheExpansion) {
    loadPeopleVectors();

    expectRows(std::string(searchThree) + "YIELD ids "
               "MATCH (ids)-[:INTERESTED_IN]->(m) "
               "RETURN count(DISTINCT m)",
               {{"6"}});
}

TEST_F(VectorSearchCompositionTest, countsTheCrossProductOfASearchAndAMatch) {
    loadPeopleVectors();

    const std::string expected = std::to_string(2 * simpledbPersonCount);

    expectRows(std::string(searchTwo) + "YIELD ids "
               "MATCH (n:Person) "
               "RETURN count(n)",
               {{expected}});
}

TEST_F(VectorSearchCompositionTest, returnsTheDistinctNeighboursOfTheExpansion) {
    loadPeopleVectors();

    expectRows(std::string(searchThree) + "YIELD ids "
               "MATCH (ids)-[:INTERESTED_IN]->(m) "
               "RETURN DISTINCT m.name",
               {{"Ghosts"}, {"Computers"}, {"Eighties"}, {"Bio"}, {"Cooking"}, {"Padel"}});
}

TEST_F(VectorSearchCompositionTest, unwindsAListBesideTheNeighbour) {
    loadPeopleVectors();

    expectRows(std::string(searchTwo) + "YIELD ids "
               "UNWIND [1, 2] AS rank "
               "RETURN ids.name, rank",
               {{"Remy", "1"}, {"Remy", "2"}, {"Adam", "1"}, {"Adam", "2"}});
}

TEST_F(VectorSearchCompositionTest, skipsTheNearestNeighbours) {
    loadPeopleVectors();

    expectOrderedRows(std::string(searchThree) + "YIELD ids RETURN ids.name SKIP 1",
                      {{"Adam"}, {"Maxime"}});
}

TEST_F(VectorSearchCompositionTest, sortsTheNeighboursByAPropertyOfTheirOwn) {
    loadPeopleVectors();

    expectOrderedRows(std::string(searchThree) + "YIELD ids RETURN ids.name ORDER BY ids.name",
                      {{"Adam"}, {"Maxime"}, {"Remy"}});
}

TEST_F(VectorSearchCompositionTest, readsTheLabelsOfTheNeighbour) {
    loadPeopleVectors();

    expectRows(std::string(searchOne) + "YIELD ids RETURN labels(ids)",
               {{"Person, SoftwareEngineering, Founder"}});
}
