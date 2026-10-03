#include <gtest/gtest.h>

#include <stddef.h>

#include <string>
#include <vector>

#include "HashJoinQueryTest.h"
#include "StringRowSink.h"
#include "TuringTest.h"

using namespace db;
using namespace turing::test;

// The fuse_hash_join pass and the db.hash_join it emits, end to end on the shared
// SimpleGraph: which shapes fuse, what the fused program looks like at both levels, and
// that the rows it produces are the rows the cross product and its filter produced.
//
// SimpleGraph's 18 nodes carry a distinct name each, so a join on the name is the
// diagonal; only Remy (0) and Adam (1) carry an age, so a join on the age is where the
// null keys have to stay unmatched.
class HashJoinTest : public HashJoinQueryTest {};

TEST_F(HashJoinTest, fusesAProductAndItsPropertyEqualityIntoAHashJoin) {
    std::string program;
    dbProgram("MATCH (n), (m) WHERE n.name = m.name RETURN n, m", program);

    EXPECT_TRUE(contains(program, "db.hash_join")) << program;
    EXPECT_TRUE(contains(program, "on 1, 1")) << program;
    EXPECT_FALSE(contains(program, "db.cross_product")) << program;
    EXPECT_FALSE(contains(program, "db.filter")) << program;
    EXPECT_FALSE(contains(program, "db.eq")) << program;
}

// The key is read inside each factor, not over the product's rows: the join has to index
// the build side as it walks it, so the property fetch moves in beside the scan.
TEST_F(HashJoinTest, sinksTheKeyIntoBothFactors) {
    std::string program;
    dbProgram("MATCH (n), (m) WHERE n.name = m.name RETURN n, m", program);

    const size_t firstFetch = program.find("db.get_node_properties");
    const size_t secondFetch = program.find("db.get_node_properties", firstFetch + 1);

    ASSERT_NE(secondFetch, std::string::npos) << program;
    EXPECT_EQ(program.find("db.get_node_properties", secondFetch + 1), std::string::npos) << program;
    EXPECT_LT(firstFetch, program.find("} factor {")) << program;
}

// A key that is the column itself needs no sinking, so the join names the column already
// yielded rather than adding one.
TEST_F(HashJoinTest, joinsOnTheColumnItselfWhenTheKeyIsTheVariable) {
    std::string program;
    dbProgram("MATCH (n), (m) WHERE n = m RETURN n", program);

    EXPECT_TRUE(contains(program, "db.hash_join")) << program;
    EXPECT_TRUE(contains(program, "on 0, 0")) << program;
}

TEST_F(HashJoinTest, lowersTheJoinToASiblingBuildLoopAndProbeLoop) {
    std::string program;
    nlProgram("MATCH (n), (m) WHERE n.name = m.name RETURN n, m", program);

    EXPECT_TRUE(contains(program, "nl.hash_join_buffer build_key 1 probe_key 1")) << program;
    EXPECT_TRUE(contains(program, "nl.hash_join_collect")) << program;
    EXPECT_TRUE(contains(program, "nl.hash_join_probe")) << program;
    EXPECT_FALSE(contains(program, "nl.cross_product")) << program;

    // Two loops one after the other rather than one inside the other: the build side is
    // read once, not once per chunk of the probe side.
    const size_t buildLoop = program.find("nl.for");
    const size_t probeLoop = program.find("nl.for", buildLoop + 1);
    ASSERT_NE(probeLoop, std::string::npos) << program;
    EXPECT_LT(program.find("nl.hash_join_collect"), probeLoop) << program;
    EXPECT_LT(probeLoop, program.find("nl.hash_join_probe")) << program;
}

// Every node carries its own name, so the join is the diagonal: one row per node, each
// pairing a node with itself.
TEST_F(HashJoinTest, joinsEveryNodeWithItselfOnItsName) {
    expectCount("MATCH (n), (m) WHERE n.name = m.name RETURN count(*)", 18);

    StringRowSink sink;
    runQuery("MATCH (n), (m) WHERE n.name = m.name RETURN n, m", sink);

    Rows expected;
    for (size_t node = 0; node < 18; node++) {
        expected.push_back({std::to_string(node), std::to_string(node)});
    }

    EXPECT_EQ(sink.getRows(), expected);
}

// `=` against a null gives null and the filter kept only the true rows, so a node with no
// age joins with nothing - not even with another node that has no age either. Remy (0) and
// Adam (1) are the only two aged nodes, and they share 32.
TEST_F(HashJoinTest, leavesARowWithANullKeyUnmatched) {
    const Rows expected {{"0", "0"}, {"0", "1"}, {"1", "0"}, {"1", "1"}};
    expectRows("MATCH (n), (m) WHERE n.age = m.age RETURN n, m", expected);
}

// Four French people and four who are not, so the join is the two blocks: 4x4 twice. The
// interests carry no isFrench at all and drop out.
TEST_F(HashJoinTest, joinsEveryPairSharingABooleanProperty) {
    expectCount("MATCH (n), (m) WHERE n.isFrench = m.isFrench RETURN count(*)", 32);
}

// The join key of each side is bound by a traversal rather than by a scan, so the factors
// are loop nests and not single scans.
TEST_F(HashJoinTest, joinsTwoTraversalsOnAPropertyOfTheirEnds) {
    std::string program;
    dbProgram("MATCH (a)-->(b), (c)-->(d) WHERE b.name = c.name RETURN a, d", program);
    EXPECT_TRUE(contains(program, "db.hash_join")) << program;

    // Each node's name is its own, so the join holds exactly where b and c are one node:
    // the pairs of edges meeting head to tail, which is what the two-hop pattern walks -
    // the same count, read off a plan with no join in it.
    expectCount("MATCH (a)-->(b), (c)-->(d) WHERE b.name = c.name RETURN count(*)", 12);
    expectCount("MATCH (a)-->(b)-->(d) RETURN count(*)", 12);
}

// Once the key is a column of its own, the column it was read from is dead unless the
// query reads it too: trim_unread_columns drops it from the yield and renumbers the key
// behind it, so neither b nor c is buffered on the build side or gathered on the probe.
TEST_F(HashJoinTest, dropsTheColumnAKeyWasReadFromWhenNothingElseReadsIt) {
    std::string program;
    dbProgram("MATCH (a)-->(b), (c)-->(d) WHERE b.name = c.name RETURN a, d", program);

    // Two columns per factor - the one the projection reads and the key it matches on -
    // where the fusion left three, b and c among them.
    EXPECT_TRUE(contains(program, "%0:4 = db.hash_join")) << program;
    EXPECT_TRUE(contains(program, "on 1, 1")) << program;
    EXPECT_TRUE(contains(program, "db.output(%0#0, %0#2)")) << program;
}

// An edge property keys a join the same way a node property does.
TEST_F(HashJoinTest, joinsTwoEdgesOnASharedEdgeProperty) {
    expectCount("MATCH (a)-[e]->(b), (c)-[f]->(d) WHERE e.duration = f.duration RETURN count(*)", 28);
}

// A predicate the join does not absorb still applies over its rows.
TEST_F(HashJoinTest, appliesAFurtherPredicateOverTheJoinedRows) {
    const Rows expected {{"0", "0"}};
    expectRows("MATCH (n), (m) WHERE n.age = m.age AND n.name = 'Remy' AND m.name = 'Remy' RETURN n, m",
               expected);
}

TEST_F(HashJoinTest, cutsTheJoinedRowsWithALimit) {
    const Rows expected {{"0", "0"}, {"1", "1"}, {"2", "2"}};
    expectRows("MATCH (n), (m) WHERE n.name = m.name RETURN n, m LIMIT 3", expected);
}

// Two columns whose types the db level cannot tell apart are left to the cross product:
// the join matches keys by the bytes they serialize to, and a node ID beside a number
// would answer a comparison the equality did not.
TEST_F(HashJoinTest, keepsTheProductWhenTheKeysNeedNotShareAType) {
    std::string program;
    dbProgram("MATCH (n), (m) WHERE n = m.age RETURN n, m", program);

    EXPECT_TRUE(contains(program, "db.cross_product")) << program;
    EXPECT_FALSE(contains(program, "db.hash_join")) << program;
}

// Two different property names could resolve to two different value types, so only a
// shared name fuses.
TEST_F(HashJoinTest, keepsTheProductWhenTheSidesReadDifferentProperties) {
    std::string program;
    dbProgram("MATCH (n), (m) WHERE n.name = m.dob RETURN n, m", program);

    EXPECT_TRUE(contains(program, "db.cross_product")) << program;
    EXPECT_FALSE(contains(program, "db.hash_join")) << program;
}

// A hash join answers equality alone; an ordering predicate stays a filter over the
// product.
TEST_F(HashJoinTest, keepsTheProductWhenThePredicateIsNotAnEquality) {
    std::string program;
    dbProgram("MATCH (n), (m) WHERE n.age > m.age RETURN n, m", program);

    EXPECT_TRUE(contains(program, "db.cross_product")) << program;
    EXPECT_FALSE(contains(program, "db.hash_join")) << program;
}

// Both sides of the equality read one factor, so it is a single-variable predicate and
// there is no join to make of it.
TEST_F(HashJoinTest, keepsTheProductWhenTheEqualityReadsOneFactor) {
    std::string program;
    dbProgram("MATCH (n), (m) WHERE n.name = n.dob RETURN n, m", program);

    EXPECT_FALSE(contains(program, "db.hash_join")) << program;
}

// A column a procedure yielded carries no type until lowering resolves it, so two of them
// could hold two kinds of value: nothing here proves the join would answer the equality
// the way the filter does, and the product stays.
TEST_F(HashJoinTest, keepsTheProductWhenNeitherKeyHasAResolvedType) {
    std::string program;
    dbProgram("CALL db.labels() YIELD label CALL db.edgeTypes() YIELD edgeType "
              "WHERE label = edgeType RETURN label, edgeType",
              program);

    EXPECT_TRUE(contains(program, "db.cross_product")) << program;
    EXPECT_FALSE(contains(program, "db.hash_join")) << program;
}

// The built side is buffered whole while the probed side streams, so the join builds the
// plain scan and probes the factor holding the cascade's product: buffering that one would
// hold every pair of b and c where the scan holds one row per node.
TEST_F(HashJoinTest, buildsTheFactorWithNoProductAndProbesTheOther) {
    std::string program;
    nlProgram("MATCH (a), (b), (c) WHERE a.name = c.name RETURN a, b, c", program);

    // The collect fills the build side, so what stands after it is on the probed side.
    const size_t collect = program.find("nl.hash_join_collect");
    const size_t product = program.find("nl.cross_product");
    ASSERT_NE(collect, std::string::npos) << program;
    ASSERT_NE(product, std::string::npos) << program;
    EXPECT_LT(collect, product) << program;
}

// Every node's name is its own, so the equality is the diagonal and b is crossed with it:
// 18 pairs of a and c, each against all 18 nodes.
TEST_F(HashJoinTest, joinsThreePatternsOnAPropertyOfTwoOfThem) {
    expectCount("MATCH (a), (b), (c) WHERE a.name = c.name RETURN count(*)", 324);
}

// A LIMIT over a join bounds the loop the probe drives, so a step pairs only the rows the
// cut can emit and the loop stops once the budget is spent.
TEST_F(HashJoinTest, boundsTheProbeWithALimit) {
    std::string program;
    nlProgram("MATCH (n), (m) WHERE n.name = m.name RETURN n, m LIMIT 3", program);

    const size_t probe = program.find("nl.hash_join_probe");
    ASSERT_NE(probe, std::string::npos) << program;

    const size_t probeLoop = program.find("nl.for", probe);
    ASSERT_NE(probeLoop, std::string::npos) << program;

    const std::string_view loopLine(program.data() + probeLoop, program.find('\n', probeLoop) - probeLoop);
    EXPECT_TRUE(contains(loopLine, "limit")) << program;
}

// The budget runs out inside a probe row's matches, not on a row boundary: the four French
// people all match Remy (0), so the fifth row is the first match of Adam (1).
TEST_F(HashJoinTest, cutsTheMatchesOfOneProbeRowWithALimit) {
    const Rows expected {{"0", "0"}, {"0", "1"}, {"0", "8"}, {"0", "9"}, {"1", "0"}};
    expectRows("MATCH (n), (m) WHERE n.isFrench = m.isFrench RETURN n, m LIMIT 5", expected);
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
