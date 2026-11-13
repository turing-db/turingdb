#include <gtest/gtest.h>

#include <stddef.h>

#include <string>
#include <string_view>

#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/OwningOpRef.h"

#include "DBOps.h"
#include "HashJoinQueryTest.h"
#include "ProductPlacement.h"

using namespace turing::test;

namespace {

bool buildsAMaximum(mlir::ModuleOp module) {
    bool builds = false;
    module.walk([&builds](mlir::db::HashJoin join) {
        join.getRightFactor().walk([&builds](mlir::db::Max) {
            builds = true;
        });
    });

    return builds;
}

}

// TUR-431. MATCH (q:N) WHERE q.i < 2 WITH collect(q.s + 40) AS l UNWIND l AS x MATCH (a:N)
// WHERE a.s = x MATCH (a)-[:E]-(b:N) walked the edges of every N node before the join kept
// the nodes whose s is in the list: the planner took the collect to make a row per node.
class UngroupedAggregateEstimateTest : public HashJoinQueryTest {
protected:
    void expectEdgesReadAfterTheJoin(std::string_view query, size_t expected) {
        mlir::OwningOpRef<mlir::ModuleOp> module;
        dbModule(query, module);

        std::string program;
        render(module.get(), program);
        EXPECT_EQ(countEdgeReadsBeforeTheJoin(module.get()), 0u) << "query: " << query << "\n" << program;

        expectCount(query, expected);
    }
};

// The list holds 32 twice, the age of Remy and Adam, who have 6 and 4 edges
TEST_F(UngroupedAggregateEstimateTest, hopsAfterAJoinToAnUnwoundCollectedList) {
    expectEdgesReadAfterTheJoin("MATCH (q:Person) WITH collect(q.age) AS l UNWIND l AS x "
                                "MATCH (a) WHERE a.age = x MATCH (a)--(b) RETURN count(b)",
                                20);
}

// The oldest people are Remy and Adam, aged 32, with 3 and 2 interests
TEST_F(UngroupedAggregateEstimateTest, hopsAfterAJoinToAMaximum) {
    expectEdgesReadAfterTheJoin("MATCH (q:Person) WITH max(q.age) AS m "
                                "MATCH (p:Person) WHERE p.age = m "
                                "MATCH (p)-[:INTERESTED_IN]->(i:Interest) RETURN count(i)",
                                5);
}

// Remy and Adam are the only people with an age, so the ages sum to twice 32
TEST_F(UngroupedAggregateEstimateTest, hopsAfterAJoinToASum) {
    expectEdgesReadAfterTheJoin("MATCH (q:Person) WITH sum(q.age) / 2 AS s "
                                "MATCH (p:Person) WHERE p.age = s "
                                "MATCH (p)-[:INTERESTED_IN]->(i:Interest) RETURN count(i)",
                                5);
}

// 4 times the 8 people is 32. The count is read from the node counts.
TEST_F(UngroupedAggregateEstimateTest, hopsAfterAJoinToACount) {
    expectEdgesReadAfterTheJoin("MATCH (q:Person) WITH 4 * count(q) AS n "
                                "MATCH (p:Person) WHERE p.age = n "
                                "MATCH (p)-[:INTERESTED_IN]->(i:Interest) RETURN count(i)",
                                5);
}

// hasPhD is true or false, and 16 times 2 is 32
TEST_F(UngroupedAggregateEstimateTest, hopsAfterAJoinToADistinctCount) {
    expectEdgesReadAfterTheJoin("MATCH (q:Person) WITH 16 * count(DISTINCT q.hasPhD) AS n "
                                "MATCH (p:Person) WHERE p.age = n "
                                "MATCH (p)-[:INTERESTED_IN]->(i:Interest) RETURN count(i)",
                                5);
}

// The maximum over the 18 nodes is one row, which the join builds rather than the 8 people
TEST_F(UngroupedAggregateEstimateTest, buildsTheSideOfAMaximum) {
    const std::string_view query = "MATCH (q) WITH max(q.age) AS m "
                                   "MATCH (p:Person) WHERE p.age = m RETURN count(p)";

    mlir::OwningOpRef<mlir::ModuleOp> module;
    dbModule(query, module);

    std::string program;
    render(module.get(), program);
    EXPECT_TRUE(buildsAMaximum(module.get())) << "query: " << query << "\n" << program;

    expectCount(query, 2);
}
