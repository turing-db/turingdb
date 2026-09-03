#include <gtest/gtest.h>

#include <stdint.h>

#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "NLOutputSink.h"
#include "QueryInterpreterV3.h"
#include "QueryStatus.h"

#include "Graph.h"
#include "JobSystem.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "datapart/EdgeRecord.h"
#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"
#include "writers/GraphWriter.h"

#include "StringRowSink.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

class CypherShortestPathTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager());

        SystemAccessor system = _env->getSystemManager().accessUnique();
        buildWeightedGraph(system.createGraph(_graphName));
        buildDoubleGraph(system.createGraph(_doubleGraphName));
    }

    std::vector<StringRowSink::Row> run(std::string_view graphName, std::string_view query) {
        StringRowSink sink;
        QueryStatus status;
        _interpreter->execute(status,
                              query,
                              graphName,
                              CommitHash::head(),
                              ChangeID::head(),
                              &_env->getMem(),
                              &sink);

        EXPECT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        std::vector<StringRowSink::Row> rows;
        sink.sortedRows(rows);
        return rows;
    }

    void expectRejected(std::string_view query, std::string_view reason) {
        StringRowSink sink;
        QueryStatus status;
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              ChangeID::head(),
                              &_env->getMem(),
                              &sink);

        EXPECT_EQ(status.getStatus(), QueryStatus::Status::ANALYZE_ERROR) << "query: " << query;

        const std::string error = status.getError();
        EXPECT_NE(error.find(reason), std::string::npos) << "query: " << query << "\nerror: " << error;
    }

    const std::string _graphName = "roads";
    const std::string _doubleGraphName = "roadsDouble";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;

private:
    // The same directed weighted graph the lowering test builds, but with a name property
    // on each node so a MATCH can address them: seven cities A(0)..G(6) with an Int64
    // `weight` on every road.
    //
    //   A -1-> B -2-> C -1-> D -1-> E -2-> F
    //   A -4-> C          C -3-> E
    //   B -7-> D          G -1-> A
    //
    // Roads are added grouped by source in node order, so the committed edge IDs are
    // A->B 0, A->C 1, B->C 2, B->D 3, C->D 4, C->E 5, D->E 6, E->F 7, G->A 8 - the paths
    // below are stored target-first as alternating node and edge IDs.
    void buildWeightedGraph(Graph* graph) {
        JobSystem jobSystem;
        jobSystem.init();

        GraphWriter writer(graph, &jobSystem);

        const auto city = [&](const char* name) -> NodeID {
            const NodeID node = writer.addNode({"City"});
            writer.addNodeProperty<types::String>(node, "name", name);
            return node;
        };

        const NodeID nodeA = city("A");
        const NodeID nodeB = city("B");
        const NodeID nodeC = city("C");
        const NodeID nodeD = city("D");
        const NodeID nodeE = city("E");
        const NodeID nodeF = city("F");
        const NodeID nodeG = city("G");

        const auto road = [&](NodeID source, NodeID target, int64_t weight) {
            const EdgeRecord edge = writer.addEdge("ROAD", source, target);
            writer.addEdgeProperty<types::Int64>(edge, "weight", std::move(weight));
        };

        road(nodeA, nodeB, 1);
        road(nodeA, nodeC, 4);
        road(nodeB, nodeC, 2);
        road(nodeB, nodeD, 7);
        road(nodeC, nodeD, 1);
        road(nodeC, nodeE, 3);
        road(nodeD, nodeE, 1);
        road(nodeE, nodeF, 2);
        road(nodeG, nodeA, 1);

        writer.submit();
        jobSystem.terminate();
    }

    // A -> B -> C plus a direct A -> C, weighted by a Double, so the value-type dispatch
    // runs for a non-integer weight through the whole Cypher pipeline. The cheapest
    // A -> C is A -> B -> C at 3.5, under the direct edge at 5.0.
    void buildDoubleGraph(Graph* graph) {
        JobSystem jobSystem;
        jobSystem.init();

        GraphWriter writer(graph, &jobSystem);

        const auto city = [&](const char* name) -> NodeID {
            const NodeID node = writer.addNode({"City"});
            writer.addNodeProperty<types::String>(node, "name", name);
            return node;
        };

        const NodeID nodeA = city("A");
        const NodeID nodeB = city("B");
        const NodeID nodeC = city("C");

        const auto road = [&](NodeID source, NodeID target, double weight) {
            const EdgeRecord edge = writer.addEdge("ROAD", source, target);
            writer.addEdgeProperty<types::Double>(edge, "weight", std::move(weight));
        };

        road(nodeA, nodeB, 1.5);
        road(nodeA, nodeC, 5.0);
        road(nodeB, nodeC, 2.0);

        writer.submit();
        jobSystem.terminate();
    }
};

TEST_F(CypherShortestPathTest, findsCheapestTwoHopPath) {
    const std::vector<StringRowSink::Row> expected {{"3", "2, 2, 1, 0, 0"}};
    EXPECT_EQ(run(_graphName,
                  "MATCH (a {name: 'A'}), (b {name: 'C'}) "
                  "SHORTESTPATH(a, b, weight, d, p) RETURN d, p"),
              expected);
}

TEST_F(CypherShortestPathTest, findsCheapestFourHopPath) {
    const std::vector<StringRowSink::Row> expected {{"5", "4, 6, 3, 4, 2, 2, 1, 0, 0"}};
    EXPECT_EQ(run(_graphName,
                  "MATCH (a {name: 'A'}), (b {name: 'E'}) "
                  "SHORTESTPATH(a, b, weight, d, p) RETURN d, p"),
              expected);
}

TEST_F(CypherShortestPathTest, multiSourceMatchPicksCheapest) {
    const std::vector<StringRowSink::Row> expected {{"3", "3, 4, 2, 2, 1"}};
    EXPECT_EQ(run(_graphName,
                  "MATCH (a), (b {name: 'D'}) WHERE a.name = 'A' OR a.name = 'B' "
                  "SHORTESTPATH(a, b, weight, d, p) RETURN d, p"),
              expected);
}

TEST_F(CypherShortestPathTest, multiTargetMatchStopsAtNearest) {
    const std::vector<StringRowSink::Row> expected {{"4", "3, 4, 2, 2, 1, 0, 0"}};
    EXPECT_EQ(run(_graphName,
                  "MATCH (a {name: 'A'}), (b) WHERE b.name = 'D' OR b.name = 'F' "
                  "SHORTESTPATH(a, b, weight, d, p) RETURN d, p"),
              expected);
}

TEST_F(CypherShortestPathTest, zeroLengthPathWhenSourceIsTarget) {
    const std::vector<StringRowSink::Row> expected {{"0", "0"}};
    EXPECT_EQ(run(_graphName,
                  "MATCH (a {name: 'A'}), (b {name: 'A'}) "
                  "SHORTESTPATH(a, b, weight, d, p) RETURN d, p"),
              expected);
}

TEST_F(CypherShortestPathTest, directedEdgesAreOneWay) {
    EXPECT_TRUE(run(_graphName,
                    "MATCH (a {name: 'B'}), (b {name: 'A'}) "
                    "SHORTESTPATH(a, b, weight, d, p) RETURN d, p")
                    .empty());
}

TEST_F(CypherShortestPathTest, emitsNoRowWhenTargetUnreachable) {
    EXPECT_TRUE(run(_graphName,
                    "MATCH (a {name: 'A'}), (b {name: 'G'}) "
                    "SHORTESTPATH(a, b, weight, d, p) RETURN d, p")
                    .empty());
}

TEST_F(CypherShortestPathTest, handlesDoubleWeights) {
    const std::vector<StringRowSink::Row> expected {{"3.5", "2, 2, 1, 0, 0"}};
    EXPECT_EQ(run(_doubleGraphName,
                  "MATCH (a {name: 'A'}), (b {name: 'C'}) "
                  "SHORTESTPATH(a, b, weight, d, p) RETURN d, p"),
              expected);
}

TEST_F(CypherShortestPathTest, rejectsReturningAMatchedNode) {
    expectRejected("MATCH (a), (b) SHORTESTPATH(a, b, weight, d, p) RETURN a, d, p",
                   "SHORTESTPATH");
}

TEST_F(CypherShortestPathTest, rejectsReturningAMatchedNodeProperty) {
    expectRejected("MATCH (a), (b) SHORTESTPATH(a, b, weight, d, p) RETURN d, p, a.name",
                   "SHORTESTPATH");
}

TEST_F(CypherShortestPathTest, rejectsUndeclaredEndpoint) {
    expectRejected("MATCH (a) SHORTESTPATH(a, zzz, weight, d, p) RETURN d, p",
                   "Variable 'zzz' not found");
}

TEST_F(CypherShortestPathTest, rejectsEdgeEndpoint) {
    expectRejected("MATCH ()-[e]->() SHORTESTPATH(e, e, weight, d, p) RETURN d, p",
                   "SHORTESTPATH endpoint 'e' must be a node, but is EdgePattern instead");
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
