#include <gtest/gtest.h>

#include <stdint.h>

#include <algorithm>
#include <string>
#include <string_view>

#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/OwningOpRef.h"
#include "llvm/ADT/SmallVector.h"
#include "llvm/Support/raw_ostream.h"

#include "DBOps.h"

#include "HashJoinQueryTest.h"
#include "IRTestOps.h"
#include "StringRowSink.h"
#include "TuringTest.h"

using namespace db;
using namespace turing::test;

namespace {

constexpr std::string_view NODE_KEY = "!db.column<!storage.node_id>";
constexpr std::string_view EDGE_KEY = "!db.column<!storage.edge_id>";

void printKeyType(mlir::Region& factor, uint64_t key, std::string& type) {
    mlir::db::Yield yield = mlir::cast<mlir::db::Yield>(factor.front().getTerminator());

    type.clear();
    llvm::raw_string_ostream stream(type);
    yield.getColumns()[key].getType().print(stream);
}

}

// A join keyed on a node or an edge column itself, or on one against an integer, on the shared
// SimpleGraph. A pattern the planner can walk out from the key is looked up by ID instead, so
// each entity here is reached through a variable-length walk, which runs from its start only,
// or comes from an OPTIONAL MATCH.
class HashJoinEntityKeyTest : public HashJoinQueryTest {
protected:
    void joinKeyTypes(std::string_view query, std::string& probeKey, std::string& buildKey) {
        mlir::OwningOpRef<mlir::ModuleOp> module;
        dbModule(query, module);

        EXPECT_EQ(countOps<mlir::db::CrossProduct>(module.get()), 0u) << "query: " << query;

        const llvm::SmallVector<mlir::db::HashJoin> joins = collect<mlir::db::HashJoin>(module.get());
        ASSERT_EQ(joins.size(), 1u) << "query: " << query;

        mlir::db::HashJoin join = joins.front();
        printKeyType(join.getLeftFactor(), join.getLeftKey(), probeKey);
        printKeyType(join.getRightFactor(), join.getRightKey(), buildKey);
    }

    void expectJoinedRows(std::string_view query, const Rows& expected) {
        Rows sortedExpected = expected;
        std::sort(sortedExpected.begin(), sortedExpected.end());

        StringRowSink joined;
        runQuery(query, joined);
        Rows joinedRows;
        joined.sortedRows(joinedRows);
        EXPECT_EQ(joinedRows, sortedExpected) << "query: " << query;

        StringRowSink product;
        runQuery(query, product, false);
        Rows productRows;
        product.sortedRows(productRows);
        EXPECT_EQ(productRows, sortedExpected) << "query: " << query;
    }
};

// KNOWS_WELL runs Remy (0) -> Adam (1), Adam -> Remy and Ghosts (6) -> Remy, so a walk of one
// or two hops from each of 0, 1 and 6 reaches Remy once and Adam once.
TEST_F(HashJoinEntityKeyTest, joinsTwoWalksOnTheNodeBothReach) {
    const std::string_view query = "MATCH (a)-[:KNOWS_WELL*1..2]->(b) MATCH (c)-[:KNOWS_WELL*1..2]->(d) "
                                   "WHERE b = d RETURN a, b, c";

    std::string probeKey;
    std::string buildKey;
    joinKeyTypes(query, probeKey, buildKey);
    EXPECT_EQ(probeKey, NODE_KEY);
    EXPECT_EQ(buildKey, NODE_KEY);

    Rows expected;
    for (const std::string_view reached : {"0", "1"}) {
        for (const std::string_view start : {"0", "1", "6"}) {
            for (const std::string_view otherStart : {"0", "1", "6"}) {
                expected.push_back({std::string(start), std::string(reached), std::string(otherStart)});
            }
        }
    }

    expectJoinedRows(query, expected);
}

// Remy knows Adam and Adam knows Remy, each the end of a walk from 0, 1 and 6. The six other
// people know no one, and their null joins with no walk.
TEST_F(HashJoinEntityKeyTest, leavesANullNodeUnmatched) {
    const std::string_view query = "MATCH (p:Person) OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f) "
                                   "MATCH (x)-[:KNOWS_WELL*1..2]->(y) WHERE f = y RETURN p, x";

    std::string probeKey;
    std::string buildKey;
    joinKeyTypes(query, probeKey, buildKey);
    EXPECT_EQ(probeKey, NODE_KEY);
    EXPECT_EQ(buildKey, NODE_KEY);

    expectJoinedRows(query, {{"0", "0"}, {"0", "1"}, {"0", "6"}, {"1", "0"}, {"1", "1"}, {"1", "6"}});
}

// Remy's exotic interests are Ghosts (edge 1) and Eighties (edge 3), and a walk from 0, 1 and 6
// reaches Remy, who holds both. No one else has an exotic interest.
TEST_F(HashJoinEntityKeyTest, leavesANullEdgeUnmatched) {
    const std::string_view query = "MATCH (p:Person) OPTIONAL MATCH (p)-[e:INTERESTED_IN]->(:Exotic) "
                                   "MATCH (a)-[:KNOWS_WELL*1..2]->()-[f:INTERESTED_IN]->() "
                                   "WHERE e = f RETURN p, e, a";

    std::string probeKey;
    std::string buildKey;
    joinKeyTypes(query, probeKey, buildKey);
    EXPECT_EQ(probeKey, EDGE_KEY);
    EXPECT_EQ(buildKey, EDGE_KEY);

    expectJoinedRows(query,
                     {{"0", "1", "0"}, {"0", "1", "1"}, {"0", "1", "6"}, {"0", "3", "0"}, {"0", "3", "1"}, {"0", "3", "6"}});
}

// Remy (0) is 32 less the age of Remy and Adam (1), the one Bioinformatics person with an age.
// Adam and Ghosts (6) know Remy by KNOWS_WELL, Remy knows Adam and the others know no one.
TEST_F(HashJoinEntityKeyTest, joinsANodeToTheIntegerOfItsID) {
    const std::string_view buildsTheNode = "MATCH (p:Person) OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f) "
                                           "MATCH (m) WHERE f = m.age - 32 RETURN p, f, m";

    std::string probeKey;
    std::string buildKey;
    joinKeyTypes(buildsTheNode, probeKey, buildKey);
    EXPECT_EQ(buildKey, NODE_KEY);

    expectJoinedRows(buildsTheNode, {{"1", "0", "0"}, {"1", "0", "1"}});

    const std::string_view probesTheNode = "MATCH (p) OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f) "
                                           "MATCH (m:Bioinformatics) WHERE f = m.age - 32 RETURN p, f, m";

    joinKeyTypes(probesTheNode, probeKey, buildKey);
    EXPECT_EQ(probeKey, NODE_KEY);

    expectJoinedRows(probesTheNode, {{"1", "0", "1"}, {"6", "0", "1"}});
}

// Edge 0 is Remy's KNOWS_WELL, and 32 less the age of Remy (0) and Adam (1), the one
// Bioinformatics person with an age. Adam's is edge 4 and Ghosts' edge 7, which no age names.
TEST_F(HashJoinEntityKeyTest, joinsAnEdgeToTheIntegerOfItsID) {
    const std::string_view buildsTheEdge = "MATCH (p:Person) OPTIONAL MATCH (p)-[e:KNOWS_WELL]->() "
                                           "MATCH (m) WHERE e = m.age - 32 RETURN p, e, m";

    std::string probeKey;
    std::string buildKey;
    joinKeyTypes(buildsTheEdge, probeKey, buildKey);
    EXPECT_EQ(buildKey, EDGE_KEY);

    expectJoinedRows(buildsTheEdge, {{"0", "0", "0"}, {"0", "0", "1"}});

    const std::string_view probesTheEdge = "MATCH (p) OPTIONAL MATCH (p)-[e:KNOWS_WELL]->() "
                                           "MATCH (m:Bioinformatics) WHERE e = m.age - 32 RETURN p, e, m";

    joinKeyTypes(probesTheEdge, probeKey, buildKey);
    EXPECT_EQ(probeKey, EDGE_KEY);

    expectJoinedRows(probesTheEdge, {{"0", "0", "1"}});
}

// 132 is no node's ID, and -1 hashes as the null an OPTIONAL MATCH leaves, on either side of
// the join.
TEST_F(HashJoinEntityKeyTest, joinsNoNodeToAnIntegerNamingNone) {
    const std::string_view pastTheLastNode = "MATCH (p:Person) OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f) "
                                             "MATCH (m) WHERE f = m.age + 100 RETURN p, f, m";
    const std::string_view probingTheNulls = "MATCH (p:Person) OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f) "
                                             "MATCH (m) WHERE f = m.age - 33 RETURN p, f, m";
    const std::string_view builtUnderTheNulls = "MATCH (p) OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f) "
                                                "MATCH (m:Bioinformatics) WHERE f = m.age - 33 RETURN p, f, m";

    std::string probeKey;
    std::string buildKey;
    joinKeyTypes(pastTheLastNode, probeKey, buildKey);
    EXPECT_EQ(buildKey, NODE_KEY);
    expectJoinedRows(pastTheLastNode, {});

    joinKeyTypes(probingTheNulls, probeKey, buildKey);
    EXPECT_EQ(buildKey, NODE_KEY);
    expectJoinedRows(probingTheNulls, {});

    joinKeyTypes(builtUnderTheNulls, probeKey, buildKey);
    EXPECT_EQ(probeKey, NODE_KEY);
    expectJoinedRows(builtUnderTheNulls, {});
}

TEST_F(HashJoinEntityKeyTest, joinsNoEdgeToAnIntegerNamingNone) {
    const std::string_view pastTheLastEdge = "MATCH (p:Person) OPTIONAL MATCH (p)-[e:KNOWS_WELL]->() "
                                             "MATCH (m) WHERE e = m.age + 100 RETURN p, e, m";
    const std::string_view probingTheNulls = "MATCH (p:Person) OPTIONAL MATCH (p)-[e:KNOWS_WELL]->() "
                                             "MATCH (m) WHERE e = m.age - 33 RETURN p, e, m";
    const std::string_view builtUnderTheNulls = "MATCH (p) OPTIONAL MATCH (p)-[e:KNOWS_WELL]->() "
                                                "MATCH (m:Bioinformatics) WHERE e = m.age - 33 RETURN p, e, m";

    std::string probeKey;
    std::string buildKey;
    joinKeyTypes(pastTheLastEdge, probeKey, buildKey);
    EXPECT_EQ(buildKey, EDGE_KEY);
    expectJoinedRows(pastTheLastEdge, {});

    joinKeyTypes(probingTheNulls, probeKey, buildKey);
    EXPECT_EQ(buildKey, EDGE_KEY);
    expectJoinedRows(probingTheNulls, {});

    joinKeyTypes(builtUnderTheNulls, probeKey, buildKey);
    EXPECT_EQ(probeKey, EDGE_KEY);
    expectJoinedRows(builtUnderTheNulls, {});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
