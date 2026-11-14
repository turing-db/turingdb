#include <gtest/gtest.h>

#include <stddef.h>
#include <stdint.h>

#include <string>
#include <string_view>
#include <vector>

#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/OwningOpRef.h"

#include "Graph.h"
#include "ID.h"
#include "JobSystem.h"
#include "writers/GraphWriter.h"

#include "HashJoinQueryTest.h"
#include "ProductPlacement.h"

using namespace db;
using namespace turing::test;

namespace {

constexpr int64_t NODE_COUNT = 3000;
constexpr int64_t S_PERIOD = 84;
constexpr int64_t EDGES_PER_NODE = 20;

}

// TUR-430. On 3,000 nodes, MATCH (x:N {i: 41}) MATCH (a:N) WHERE a.s = x.s - 1
// MATCH (a)-[:E]-(b:N) walked the edges of every N node before the join kept the 36 whose
// s matched: the planner read x.i = 41 as keeping 300 of the nodes, where it keeps 1.
class PropertyEqualityEstimateTest : public HashJoinQueryTest {
protected:
    // SimpleGraph, then 3,000 N nodes {i, s: i % 84, t: i % 2 = 0}, each with an E edge to
    // the next 20
    void initialize() override {
        HashJoinQueryTest::initialize();

        JobSystem jobSystem;
        jobSystem.init();

        GraphWriter writer(_graph.get(), &jobSystem);

        std::vector<NodeID> nodes;
        for (int64_t index = 0; index < NODE_COUNT; index++) {
            const NodeID node = writer.addNode({"N"});
            writer.addNodeProperty<types::Int64>(node, "i", int64_t {index});
            writer.addNodeProperty<types::Int64>(node, "s", index % S_PERIOD);
            writer.addNodeProperty<types::Bool>(node, "t", index % 2 == 0);
            nodes.push_back(node);
        }

        for (int64_t source = 0; source < NODE_COUNT; source++) {
            for (int64_t target = source + 1; target <= source + EDGES_PER_NODE && target < NODE_COUNT; target++) {
                writer.addEdge("E", nodes[source], nodes[target]);
            }
        }

        ASSERT_TRUE(writer.submit());
    }

    void expectEdgesReadAfterTheJoin(std::string_view query, size_t expected) {
        mlir::OwningOpRef<mlir::ModuleOp> module;
        dbModule(query, module);

        std::string program;
        render(module.get(), program);
        EXPECT_EQ(countEdgeReadsBeforeTheJoin(module.get()), 0u) << "query: " << query << "\n" << program;

        expectCount(query, expected);
    }
};

// x has s 41, so a is the 36 nodes of s 40, i from 40 to 2980. Each has an edge from the
// 20 before it and to the 20 after it, but for 2980, which has 19 after it: 36 * 40 - 1.
TEST_F(PropertyEqualityEstimateTest, hopsAfterAJoinToANodeAPropertyNames) {
    expectEdgesReadAfterTheJoin("MATCH (x:N {i: 41}) MATCH (a:N) WHERE a.s = x.s - 1 "
                                "MATCH (a)-[:E]-(b:N) RETURN count(b)",
                                1439);
}

TEST_F(PropertyEqualityEstimateTest, hopsAfterAJoinToANodeAWhereClauseNames) {
    expectEdgesReadAfterTheJoin("MATCH (x:N) WHERE x.i = 41 MATCH (a:N) WHERE a.s = x.s - 1 "
                                "MATCH (a)-[:E]-(b:N) RETURN count(b)",
                                1439);
}

// 41 and 125 both have s 41, so each joins the 36 nodes above
TEST_F(PropertyEqualityEstimateTest, hopsAfterAJoinToTheNodesAListNames) {
    expectEdgesReadAfterTheJoin("MATCH (x:N) WHERE x.i IN [41, 125] MATCH (a:N) WHERE a.s = x.s - 1 "
                                "MATCH (a)-[:E]-(b:N) RETURN count(b)",
                                2 * 1439);
}

// x is the 36 nodes of s 5, i from 5 to 2945, so a is the 36 of i from 6 to 2946. Each has
// an edge from the 20 before it and to the 20 after it, but for 6, which has 6 before it:
// 36 * 40 - 14.
TEST_F(PropertyEqualityEstimateTest, hopsAfterAJoinToTheNodesARepeatedPropertyNames) {
    expectEdgesReadAfterTheJoin("MATCH (x:N {s: 5}) MATCH (a:N) WHERE a.i = x.i + 1 "
                                "MATCH (a)-[:E]-(b:N) RETURN count(b)",
                                1426);
}

// A boolean holds two values, so t = true keeps half the nodes: crossed in before the hop,
// the 1,500 x would join into 53,580 rows to walk from, against the 3,000 nodes walked once.
// s = i % 84 keeps the parity of i, so a is the 1,500 even nodes.
TEST_F(PropertyEqualityEstimateTest, hopsBeforeAJoinToTheNodesABooleanNames) {
    const std::string_view query = "MATCH (x:N {t: true}) MATCH (a:N) WHERE a.s = x.s "
                                   "MATCH (a)-[:E]-(b:N) RETURN count(DISTINCT a)";

    mlir::OwningOpRef<mlir::ModuleOp> module;
    dbModule(query, module);

    std::string program;
    render(module.get(), program);
    EXPECT_GT(countEdgeReadsBeforeTheJoin(module.get()), 0u) << "query: " << query << "\n" << program;

    expectCount(query, 1500);
}
