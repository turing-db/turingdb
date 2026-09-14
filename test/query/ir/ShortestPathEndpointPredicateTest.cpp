#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "QueryInterpreterV3.h"
#include "QueryStatus.h"

#include "Graph.h"
#include "SimpleGraph.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"

#include "StringRowSink.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

namespace {

using Rows = std::vector<StringRowSink::Row>;

}

// SHORTESTPATH under a WHERE whose predicate reads both endpoints. It is the one shape
// where the db.filter between the cross product and db.shortest_path carries a mask
// neither endpoint could compute on its own, so it is the shape that says whether the
// operator is fed the surviving rows or the scans.
//
// The duration-weighted edges of SimpleGraph reachable from the nodes below, the only
// ones the search can walk:
//
//   Remy(0) -e0-> Adam(1) 20
//   Remy(0) -e1-> Ghosts(6) 20
//   Adam(1) -e4-> Remy(0) 20
//   Ghosts(6) -e7-> Remy(0) 200
//
// Paths are read target-first as alternating node and edge IDs.
class ShortestPathEndpointPredicateTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager());

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);
    }

    void expectRows(std::string_view query, const Rows& expected) {
        StringRowSink sink;
        QueryStatus status;

        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              ChangeID::head(),
                              &_env->getMem(),
                              &sink);

        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        EXPECT_EQ(sink.getRows(), expected) << "query: " << query;
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

// Remy and Adam both reach Ghosts, Remy in one hop and Adam in two, so the source the
// predicate keeps is the one the answer names.
TEST_F(ShortestPathEndpointPredicateTest, keepsBothSourcesWithoutAPredicateOverTheEndpoints) {
    const Rows expected = {{"20", "6, 1, 0"}};
    expectRows("MATCH (n), (m) WHERE m.name = 'Ghosts' AND (n.name = 'Remy' OR n.name = 'Adam') "
               "SHORTESTPATH(n, m, duration, d, p) RETURN d, p",
               expected);
}

TEST_F(ShortestPathEndpointPredicateTest, narrowsTheSourceSetToRemy) {
    const Rows expected = {{"20", "6, 1, 0"}};
    expectRows("MATCH (n), (m) WHERE m.name = 'Ghosts' AND (n.name = 'Remy' OR n.name = 'Adam') "
               "AND n.name > m.name "
               "SHORTESTPATH(n, m, duration, d, p) RETURN d, p",
               expected);
}

TEST_F(ShortestPathEndpointPredicateTest, narrowsTheSourceSetToAdam) {
    const Rows expected = {{"40", "6, 1, 0, 4, 1"}};
    expectRows("MATCH (n), (m) WHERE m.name = 'Ghosts' AND (n.name = 'Remy' OR n.name = 'Adam') "
               "AND n.name < m.name "
               "SHORTESTPATH(n, m, duration, d, p) RETURN d, p",
               expected);
}

// The same over the target set: Ghosts reaches Remy at 200 and Adam one hop further.
TEST_F(ShortestPathEndpointPredicateTest, keepsBothTargetsWithoutAPredicateOverTheEndpoints) {
    const Rows expected = {{"200", "0, 7, 6"}};
    expectRows("MATCH (n), (m) WHERE n.name = 'Ghosts' AND (m.name = 'Remy' OR m.name = 'Adam') "
               "SHORTESTPATH(n, m, duration, d, p) RETURN d, p",
               expected);
}

TEST_F(ShortestPathEndpointPredicateTest, narrowsTheTargetSetToRemy) {
    const Rows expected = {{"200", "0, 7, 6"}};
    expectRows("MATCH (n), (m) WHERE n.name = 'Ghosts' AND (m.name = 'Remy' OR m.name = 'Adam') "
               "AND m.name > n.name "
               "SHORTESTPATH(n, m, duration, d, p) RETURN d, p",
               expected);
}

TEST_F(ShortestPathEndpointPredicateTest, narrowsTheTargetSetToAdam) {
    const Rows expected = {{"220", "1, 0, 0, 7, 6"}};
    expectRows("MATCH (n), (m) WHERE n.name = 'Ghosts' AND (m.name = 'Remy' OR m.name = 'Adam') "
               "AND m.name < n.name "
               "SHORTESTPATH(n, m, duration, d, p) RETURN d, p",
               expected);
}

// One node is named Remy, so the inequality kills the single row the match holds and both
// endpoint sets come out empty. This is not the unreachable case: there the sets hold
// nodes and the search exhausts the heap.
TEST_F(ShortestPathEndpointPredicateTest, emitsNoRowWhenThePredicateEmptiesTheMatch) {
    const Rows expected = {};
    expectRows("MATCH (n), (m) WHERE n.name = 'Remy' AND m.name = 'Remy' AND n <> m "
               "SHORTESTPATH(n, m, duration, d, p) RETURN d, p",
               expected);
}

TEST_F(ShortestPathEndpointPredicateTest, answersTheZeroLengthPathWithoutTheInequality) {
    const Rows expected = {{"0", "0"}};
    expectRows("MATCH (n), (m) WHERE n.name = 'Remy' AND m.name = 'Remy' "
               "SHORTESTPATH(n, m, duration, d, p) RETURN d, p",
               expected);
}

// The operator keeps the set of n values and the set of m values and forgets which row
// each came from, so an inequality over the whole graph answers what no predicate at all
// answers: every node is both a source and a target, and the zero-length path wins.
TEST_F(ShortestPathEndpointPredicateTest, answersZeroForTheInequalityOverTheWholeGraph) {
    const Rows expected = {{"0", "0"}};
    expectRows("MATCH (n), (m) WHERE n <> m SHORTESTPATH(n, m, duration, d, p) RETURN d, p",
               expected);
}

TEST_F(ShortestPathEndpointPredicateTest, answersZeroForTheWholeGraphWithoutAPredicate) {
    const Rows expected = {{"0", "0"}};
    expectRows("MATCH (n), (m) SHORTESTPATH(n, m, duration, d, p) RETURN d, p", expected);
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
