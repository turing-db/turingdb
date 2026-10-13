#include <gtest/gtest.h>

#include <stddef.h>

#include <string_view>

#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/OwningOpRef.h"

#include "DBOps.h"

#include "HashJoinQueryTest.h"

using namespace turing::test;

// A WHERE after a WITH runs on the rows the earlier WHERE kept. range() raises on a step of
// 0, which size(a.name) - 4 is for Remy and Adam, the two people with an age.
class PredicateOverKeptRowsTest : public HashJoinQueryTest {
};

// Maxime, Martina, Suhas, Cyrus and Doruk build a non-empty list, Luc an empty one
TEST_F(PredicateOverKeptRowsTest, runsAfterAFilterOfBothFactors) {
    expectCount("MATCH (a:Person), (b:Person) WHERE a.name = b.name AND b.age IS NULL "
                "WITH a, b WHERE size(range(1, 2, size(a.name) - 4)) > 0 RETURN count(*)",
                5);
}

TEST_F(PredicateOverKeptRowsTest, runsOnNoRowWhenTheOtherFactorIsEmpty) {
    expectCount("MATCH (a:Person), (b:Person {name: 'Nobody'}) "
                "WHERE size(range(1, 2, size(a.name) - 4)) > 0 RETURN count(*)",
                0);
}

// 10 / 2, 10 / 3 and 10 / 1 are positive, Luc's 10 / -1 is not
TEST_F(PredicateOverKeptRowsTest, dividesAfterAFilterOfBothFactors) {
    expectCount("MATCH (a:Person), (b:Person) WHERE a.name = b.name AND b.age IS NULL "
                "WITH a, b WHERE 10 / (size(a.name) - 4) > 0 RETURN count(*)",
                5);
}

TEST_F(PredicateOverKeptRowsTest, dividesOnNoRowAnEdgeHopDrops) {
    expectCount("MATCH (a:Person)-->(b {name: 'Nobody'}) WHERE 10 / (a.age - 32) > 0 RETURN count(*)", 0);
}

TEST_F(PredicateOverKeptRowsTest, dividesOnNoRowAnUnwindDrops) {
    expectCount("MATCH (a:Person) UNWIND a.missing AS x WITH a, x WHERE 10 / (a.age - 32) > 0 RETURN count(*)", 0);
}

// 3e17 microseconds times 32 overflows a duration; the six people without an age give null
TEST_F(PredicateOverKeptRowsTest, multipliesADurationAfterAFilterOfBothFactors) {
    expectCount("MATCH (a:Person), (b:Person) WHERE a.name = b.name AND b.age IS NULL "
                "WITH a, b WHERE duration(300000000000000000) * a.age IS NULL RETURN count(*)",
                6);
}

TEST_F(PredicateOverKeptRowsTest, splitsAfterAFilterOfBothFactors) {
    expectCount("MATCH (a:Person), (b:Person) WHERE a.name = b.name AND b.age IS NULL "
                "WITH a, b WHERE split([a.age, a.name][0], ',') IS NULL RETURN count(*)",
                6);
}

// [a.age, a.name][0] holds 32 for Remy and Adam, null for the six people the first WHERE keeps
TEST_F(PredicateOverKeptRowsTest, readsATextCellAfterAFilterOfBothFactors) {
    expectCount("MATCH (a:Person), (b:Person) WHERE a.name = b.name AND b.age IS NULL "
                "WITH a, b WHERE toUpper([a.age, a.name][0]) IS NULL RETURN count(*)",
                6);
}

TEST_F(PredicateOverKeptRowsTest, reversesACellAfterAFilterOfBothFactors) {
    expectCount("MATCH (a:Person), (b:Person) WHERE a.name = b.name AND b.age IS NULL "
                "WITH a, b WHERE reverse([a.age, a.name][0]) IS NULL RETURN count(*)",
                6);
}

TEST_F(PredicateOverKeptRowsTest, sizesACellAfterAFilterOfBothFactors) {
    expectCount("MATCH (a:Person), (b:Person) WHERE a.name = b.name AND b.age IS NULL "
                "WITH a, b WHERE size([a.age, a.name][0]) IS NULL RETURN count(*)",
                6);
}

TEST_F(PredicateOverKeptRowsTest, readsAListCellAfterAFilterOfBothFactors) {
    expectCount("MATCH (a:Person), (b:Person) WHERE a.name = b.name AND b.age IS NULL "
                "WITH a, b WHERE head([a.age, a.name][0]) IS NULL RETURN count(*)",
                6);
}

TEST_F(PredicateOverKeptRowsTest, readsAnEntityCellAfterAFilterOfBothFactors) {
    expectCount("MATCH (a:Person), (b:Person) WHERE a.name = b.name AND b.age IS NULL "
                "WITH a, b WHERE id([a.age, a][0]) IS NULL RETURN count(*)",
                6);
}

TEST_F(PredicateOverKeptRowsTest, readsTheKeysOfACellAfterAFilterOfBothFactors) {
    expectCount("MATCH (a:Person), (b:Person) WHERE a.name = b.name AND b.age IS NULL "
                "WITH a, b WHERE keys([a.age, a][0]) IS NULL RETURN count(*)",
                6);
}

TEST_F(PredicateOverKeptRowsTest, readsThePropertiesOfACellAfterAFilterOfBothFactors) {
    expectCount("MATCH (a:Person), (b:Person) WHERE a.name = b.name AND b.age IS NULL "
                "WITH a, b WHERE properties([a.age, a][0]) IS NULL RETURN count(*)",
                6);
}

TEST_F(PredicateOverKeptRowsTest, readsAnEdgeCellAfterAFilterOfBothFactors) {
    expectCount("MATCH (a:Person), (b:Person) WHERE a.name = b.name AND b.age IS NULL "
                "WITH a, b WHERE startNode([a.age, a][0]) IS NULL RETURN count(*)",
                6);
}

// [toString(a.age), a][0] holds "32" for Remy and Adam
TEST_F(PredicateOverKeptRowsTest, readsANumberCellAfterAFilterOfBothFactors) {
    expectCount("MATCH (a:Person), (b:Person) WHERE a.name = b.name AND b.age IS NULL "
                "WITH a, b WHERE abs([toString(a.age), a][0]) IS NULL RETURN count(*)",
                6);
}

TEST_F(PredicateOverKeptRowsTest, takesTheRootOfACellAfterAFilterOfBothFactors) {
    expectCount("MATCH (a:Person), (b:Person) WHERE a.name = b.name AND b.age IS NULL "
                "WITH a, b WHERE sqrt([toString(a.age), a][0]) IS NULL RETURN count(*)",
                6);
}

// No row of a string column raises, so the function still moves into the factor: Maxime
// and Martina
TEST_F(PredicateOverKeptRowsTest, movesAFunctionOverATypedColumnIntoItsFactor) {
    const std::string_view query = "MATCH (a:Person), (b:Person) WHERE a.name = b.name AND b.age IS NULL "
                                   "WITH a, b WHERE toUpper(a.name) STARTS WITH 'M' RETURN count(*)";

    expectCount(query, 2);

    mlir::OwningOpRef<mlir::ModuleOp> module;
    dbModule(query, module);

    size_t inAFactor = 0;
    module->walk([&inAFactor](mlir::db::ToUpper toUpper) {
        inAFactor += toUpper->getParentOfType<mlir::db::HashJoin>() ? 1 : 0;
    });

    EXPECT_EQ(inAFactor, 1u);
}
