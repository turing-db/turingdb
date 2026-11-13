#include <gtest/gtest.h>

#include <string_view>

#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/OwningOpRef.h"

#include "HashJoinQueryTest.h"
#include "ProductPlacement.h"

using namespace turing::test;

namespace {

// Remy and Adam are the two people aged 32
const Rows interestsOfThe32YearOlds {
    {"Adam", "Bio"},
    {"Adam", "Cooking"},
    {"Remy", "Computers"},
    {"Remy", "Eighties"},
    {"Remy", "Ghosts"},
};

}

// TUR-424. UNWIND [41] AS x MATCH (a:N) WHERE a.s = x - 1 MATCH (a)-[:E]-(b:N) walked the
// edges of every N node, then kept the rows of the 1 node in 84 whose s matched.
class HopsAfterJoinTest : public HashJoinQueryTest {
protected:
    void expectEdgesReadAfterTheJoin(std::string_view query, const Rows& expected) {
        mlir::OwningOpRef<mlir::ModuleOp> module;
        dbModule(query, module);

        EXPECT_EQ(countEdgeReadsBeforeTheJoin(module.get()), 0u) << "query: " << query;

        expectSortedRows(query, expected);
    }

    void expectSortedRows(std::string_view query, const Rows& expected) {
        StringRowSink sink;
        runQuery(query, sink);

        Rows rows;
        sink.sortedRows(rows);

        EXPECT_EQ(rows, expected) << "query: " << query;
    }
};

TEST_F(HopsAfterJoinTest, hopsAfterAJoinOnTheNodeAnEarlierMatchBinds) {
    expectEdgesReadAfterTheJoin("UNWIND [33] AS x MATCH (p:Person) WHERE p.age = x - 1 "
                                "MATCH (p)-[:INTERESTED_IN]->(i:Interest) RETURN p.name, i.name",
                                interestsOfThe32YearOlds);
}

TEST_F(HopsAfterJoinTest, hopsAfterAJoinWrittenAfterThePattern) {
    expectEdgesReadAfterTheJoin("UNWIND [33] AS x MATCH (p:Person)-[:INTERESTED_IN]->(i:Interest) "
                                "WHERE p.age = x - 1 RETURN p.name, i.name",
                                interestsOfThe32YearOlds);
}

// Remy and Adam know each other well both ways, and Ghosts, which knows Remy, is no Person
TEST_F(HopsAfterJoinTest, hopsBothWaysAfterAJoin) {
    expectEdgesReadAfterTheJoin("UNWIND [33] AS x MATCH (p:Person) WHERE p.age = x - 1 "
                                "MATCH (p)-[:KNOWS_WELL]-(q:Person) RETURN p.name, q.name",
                                {
                                    {"Adam", "Remy"},
                                    {"Adam", "Remy"},
                                    {"Remy", "Adam"},
                                    {"Remy", "Adam"},
                                });
}

TEST_F(HopsAfterJoinTest, hopsAfterAJoinOnAnUnwoundKey) {
    expectEdgesReadAfterTheJoin("UNWIND [32, 40] AS x MATCH (p:Person) WHERE p.age = x "
                                "MATCH (p)-[:INTERESTED_IN]->(i:Interest) RETURN p.name, i.name",
                                interestsOfThe32YearOlds);
}

TEST_F(HopsAfterJoinTest, hopsAfterARangeJoin) {
    expectEdgesReadAfterTheJoin("UNWIND [30] AS x MATCH (p:Person) WHERE p.age > x "
                                "MATCH (p)-[:INTERESTED_IN]->(i:Interest) RETURN p.name, i.name",
                                interestsOfThe32YearOlds);
}

// Maxime, Suhas, Cyrus and Doruk have no PhD
TEST_F(HopsAfterJoinTest, hopsAfterAJoinOfTwoScans) {
    expectEdgesReadAfterTheJoin("MATCH (q:Person {name: 'Maxime'}) MATCH (p:Person) WHERE p.hasPhD = q.hasPhD "
                                "MATCH (p)-[:INTERESTED_IN]->(i:Interest) RETURN p.name, i.name",
                                {
                                    {"Cyrus", "Gym"},
                                    {"Cyrus", "Travel"},
                                    {"Doruk", "Gym"},
                                    {"Maxime", "Bio"},
                                    {"Maxime", "Padel"},
                                    {"Suhas", "Gym"},
                                    {"Suhas", "JiuJitsu"},
                                });
}

TEST_F(HopsAfterJoinTest, walksFromTheJoinedEndOfAPattern) {
    expectEdgesReadAfterTheJoin("UNWIND [33] AS x MATCH (i:Interest)<-[:INTERESTED_IN]-(p:Person) "
                                "WHERE p.age = x - 1 RETURN p.name, i.name",
                                interestsOfThe32YearOlds);
}

TEST_F(HopsAfterJoinTest, hopsTwiceAfterAJoin) {
    expectEdgesReadAfterTheJoin("UNWIND [33] AS x MATCH (p:Person) WHERE p.age = x - 1 "
                                "MATCH (p)-[:KNOWS_WELL]->(q:Person)-[:INTERESTED_IN]->(i:Interest) "
                                "RETURN p.name, q.name, i.name",
                                {
                                    {"Adam", "Remy", "Computers"},
                                    {"Adam", "Remy", "Eighties"},
                                    {"Adam", "Remy", "Ghosts"},
                                    {"Remy", "Adam", "Bio"},
                                    {"Remy", "Adam", "Cooking"},
                                });
}

TEST_F(HopsAfterJoinTest, walksAVariableLengthPathAfterAJoin) {
    expectEdgesReadAfterTheJoin("UNWIND [33] AS x MATCH (p:Person) WHERE p.age = x - 1 "
                                "MATCH (p)-[:KNOWS_WELL*1..2]->(q:Person) RETURN p.name, q.name",
                                {
                                    {"Adam", "Adam"},
                                    {"Adam", "Remy"},
                                    {"Remy", "Adam"},
                                    {"Remy", "Remy"},
                                });
}

TEST_F(HopsAfterJoinTest, carriesTheUnwoundValuePastTheHops) {
    expectEdgesReadAfterTheJoin("UNWIND [33, 34] AS x MATCH (p:Person) WHERE p.age = x - 1 "
                                "MATCH (p)-[:INTERESTED_IN]->(i:Interest) RETURN x, p.name, i.name",
                                {
                                    {"33", "Adam", "Bio"},
                                    {"33", "Adam", "Cooking"},
                                    {"33", "Remy", "Computers"},
                                    {"33", "Remy", "Eighties"},
                                    {"33", "Remy", "Ghosts"},
                                });
}

TEST_F(HopsAfterJoinTest, filtersTheEndOfAHopOnAConstantAfterTheJoin) {
    expectEdgesReadAfterTheJoin("UNWIND [33] AS x MATCH (p:Person) WHERE p.age = x - 1 "
                                "MATCH (p)-[:INTERESTED_IN]->(i:Interest) WHERE i.name <> 'Bio' "
                                "RETURN p.name, i.name",
                                {
                                    {"Adam", "Cooking"},
                                    {"Remy", "Computers"},
                                    {"Remy", "Eighties"},
                                    {"Remy", "Ghosts"},
                                });
}

// Cooking, Computers and Eighties are the interests of Remy and Adam named in more than 6
// letters
TEST_F(HopsAfterJoinTest, joinsTheEndOfAHopAfterIt) {
    expectEdgesReadAfterTheJoin("UNWIND [33] AS x MATCH (p:Person) WHERE p.age = x - 1 "
                                "MATCH (p)-[:INTERESTED_IN]->(i:Interest) WHERE size(i.name) > x - 27 "
                                "RETURN p.name, i.name",
                                {
                                    {"Adam", "Cooking"},
                                    {"Remy", "Computers"},
                                    {"Remy", "Eighties"},
                                });
}

// Only Remy and Adam know a Person well, so the hop keeps 2 of the 8 people, while the 100
// unwound values would make the join run on 800 rows. Remy and Adam are older than 31 of
// them, are 32, and each knows the other well.
TEST_F(HopsAfterJoinTest, keepsARareHopBeforeTheJoinOfManyRows) {
    const std::string_view olderThan = "UNWIND range(1, 100) AS x MATCH (p:Person) WHERE p.age > x "
                                       "MATCH (p)-[:KNOWS_WELL]->(q:Person) RETURN count(*)";
    const std::string_view aged = "UNWIND range(1, 100) AS x MATCH (p:Person) WHERE p.age = x "
                                  "MATCH (p)-[:KNOWS_WELL]->(q:Person) RETURN count(*)";

    for (const std::string_view query : {olderThan, aged}) {
        mlir::OwningOpRef<mlir::ModuleOp> module;
        dbModule(query, module);

        EXPECT_GT(countEdgeReadsBeforeTheJoin(module.get()), 0u) << "query: " << query;
    }

    expectCount(olderThan, 62);
    expectCount(aged, 2);
}

// No Person knows an Interest well, so the division never runs: before the hop it would run
// on every Person, and raise
TEST_F(HopsAfterJoinTest, keepsAJoinThatMayRaiseAfterThePattern) {
    expectCount("UNWIND [0] AS x MATCH (p:Person) WHERE p.age = 64 / x "
                "MATCH (p)-[:KNOWS_WELL]->(i:Interest) RETURN count(*)",
                0);
}
