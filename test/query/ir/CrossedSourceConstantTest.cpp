#include <gtest/gtest.h>

#include <fstream>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "NLOutputSink.h"
#include "QueryInterpreterV3.h"
#include "QueryStatus.h"

#include "Graph.h"
#include "ID.h"
#include "SimpleGraph.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"

#include "IRTestRows.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

namespace {

constexpr std::string_view createNodeIndex = "CREATE VECTOR INDEX nodes WITH DIMENSION 4 METRIC EUCLID";
constexpr std::string_view loadNodes = "LOAD VECTOR FROM \"nodes.csv\" IN nodes";

}

// A source reading no column - a VECTOR SEARCH, or a CALL taking none of the matched
// variables - opens a dataflow of its own, so what the query matched so far becomes one
// factor of a cross product and the source the other. A constant beside that match holds
// no row of its own, so the product carries no column for it: its definition has to stay
// where the rest of the query can still read it.
class CrossedSourceConstantTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);

        // A node ID follows the label-set order of its data part rather than the order the
        // fixture writes its nodes, so the two the index is keyed on are looked up.
        _remy = SimpleGraph::findNodeID(graph, "Remy");
        _adam = SimpleGraph::findNodeID(graph, "Adam");

        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager());
    }

    QueryStatus runQuery(std::string_view query, NLOutputSink* sink) {
        QueryStatus status;
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              ChangeID::head(),
                              &_env->getMem(),
                              sink);

        return status;
    }

    // The index of nodes, keyed on the IDs the graph gave Remy and Adam, with Remy the
    // nearer of the two to (1, 0, 0, 0).
    void loadNodeVectors() {
        const fs::Path path = _env->getConfig().getDataDir() / "nodes.csv";

        std::ofstream file(path.get());
        file << _remy.getValue() << ",1,0,0,0\n" << _adam.getValue() << ",2,0,0,0\n";
        file.close();

        RowSink createSink;
        const QueryStatus createStatus = runQuery(createNodeIndex, &createSink);
        ASSERT_TRUE(createStatus.isOk()) << createStatus.getError();

        RowSink loadSink;
        const QueryStatus loadStatus = runQuery(loadNodes, &loadSink);
        ASSERT_TRUE(loadStatus.isOk()) << loadStatus.getError();
    }

    void expectRowsInOrder(std::string_view query, const Rows& expected) {
        RowSink sink;
        const QueryStatus status = runQuery(query, &sink);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        EXPECT_EQ(sink.rows(), expected) << "query: " << query;
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
    NodeID _remy;
    NodeID _adam;
};

// The control: with no constant in flight the whole block becomes the left factor and
// every column the query goes on to read is one of the product's results.
TEST_F(CrossedSourceConstantTest, crossesAMatchWithASearchThatCarriesNoConstant) {
    loadNodeVectors();

    expectRowsInOrder("MATCH (n:Person {name: 'Remy'}) "
                      "VECTOR SEARCH IN nodes FOR 2 (1.0, 0.0, 0.0, 0.0) YIELD ids "
                      "RETURN n.name, ids",
                      {{"Remy", std::to_string(_remy.getValue())},
                       {"Remy", std::to_string(_adam.getValue())}});
}

TEST_F(CrossedSourceConstantTest, readsAConstantBoundBeforeASearchAfterTheProduct) {
    loadNodeVectors();

    expectRowsInOrder("MATCH (n:Person {name: 'Remy'}) WITH n, 1 AS tag "
                      "VECTOR SEARCH IN nodes FOR 2 (1.0, 0.0, 0.0, 0.0) YIELD ids "
                      "RETURN n.name, tag, ids",
                      {{"Remy", "1", std::to_string(_remy.getValue())},
                       {"Remy", "1", std::to_string(_adam.getValue())}});
}

// The same constant read through an expression the projection builds over it, which is
// itself a constant and sits after the product.
TEST_F(CrossedSourceConstantTest, computesOverAConstantBoundBeforeASearch) {
    loadNodeVectors();

    expectRowsInOrder("MATCH (n:Person {name: 'Remy'}) WITH n, 2 AS factor "
                      "VECTOR SEARCH IN nodes FOR 2 (1.0, 0.0, 0.0, 0.0) YIELD ids "
                      "RETURN n.name, factor * 10",
                      {{"Remy", "20"}, {"Remy", "20"}});
}

// The same product, opened by a CALL that reads none of the matched variables instead of
// by a search.
TEST_F(CrossedSourceConstantTest, readsAConstantBoundBeforeACallAfterTheProduct) {
    RowSink labelsSink;
    const QueryStatus labelsStatus = runQuery("CALL db.labels() YIELD label RETURN label", &labelsSink);
    ASSERT_TRUE(labelsStatus.isOk()) << labelsStatus.getError();

    Rows expected;
    for (const Row& row : labelsSink.rows()) {
        expected.push_back({"Remy", "1", row.front()});
    }

    expectRowsInOrder("MATCH (n:Person {name: 'Remy'}) WITH n, 1 AS tag "
                      "CALL db.labels() YIELD label "
                      "RETURN n.name, tag, label",
                      expected);
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
