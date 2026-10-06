#include <gtest/gtest.h>

#include <algorithm>
#include <string>
#include <string_view>

#include "QueryInterpreterV3.h"
#include "versioning/CommitHash.h"

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// A diamond beside simpledb: A->B->D costs 10 + 10, A->C->D costs 30 + 30.
class ShortestPathUncommittedWeightTest : public WriteQueryTest {
protected:
    void initialize() override {
        WriteQueryTest::initialize();

        applyWrite("CREATE (a:Stop {name: 'A'})-[:LEG {w: 10.0}]->(b:Stop {name: 'B'})-[:LEG {w: 10.0}]->(d:Stop {name: 'D'}), "
                   "(a)-[:LEG {w: 30.0}]->(c:Stop {name: 'C'})-[:LEG {w: 30.0}]->(d)");
    }

    void expectRowsInChange(std::string_view query, const ChangeID& changeID, const Rows& expected) {
        RowSink sink;
        QueryStatus status;
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              changeID,
                              &sink);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        Rows actual;
        sink.sortedRows(actual);

        Rows sortedExpected = expected;
        std::sort(sortedExpected.begin(), sortedExpected.end());

        EXPECT_EQ(actual, sortedExpected) << "query: " << query;
    }

    void write(std::string_view query, const ChangeID& changeID) {
        const QueryStatus status = runWrite(query, changeID);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();
    }

    const std::string _shortestPath = "MATCH (a:Stop {name: 'A'}), (d:Stop {name: 'D'}) "
                                      "SHORTESTPATH(a, d, w, dist, path) RETURN dist";
};

TEST_F(ShortestPathUncommittedWeightTest, raisedWeightReroutesBeforeCommit) {
    ChangeID changeID;
    openChange(changeID);

    write("MATCH (:Stop {name: 'A'})-[e:LEG]->(:Stop {name: 'B'}) SET e.w = 100.0", changeID);

    expectRowsInChange("MATCH (:Stop {name: 'A'})-[e:LEG]->(:Stop {name: 'B'}) RETURN e.w", changeID, {{"100.000000"}});
    expectRowsInChange(_shortestPath, changeID, {{"60.000000"}});
}

TEST_F(ShortestPathUncommittedWeightTest, loweredWeightReroutesBeforeCommit) {
    ChangeID changeID;
    openChange(changeID);

    write("MATCH (:Stop {name: 'A'})-[e:LEG]->(:Stop {name: 'C'})-[f:LEG]->(:Stop {name: 'D'}) SET e.w = 1.0, f.w = 2.0", changeID);

    expectRowsInChange(_shortestPath, changeID, {{"3.000000"}});
}

TEST_F(ShortestPathUncommittedWeightTest, weightSetToNullSkipsTheEdgeBeforeCommit) {
    ChangeID changeID;
    openChange(changeID);

    write("MATCH (:Stop {name: 'A'})-[e:LEG]->(:Stop {name: 'B'}) SET e.w = null", changeID);

    expectRowsInChange(_shortestPath, changeID, {{"60.000000"}});
}

TEST_F(ShortestPathUncommittedWeightTest, headKeepsTheCommittedWeight) {
    ChangeID changeID;
    openChange(changeID);

    write("MATCH (:Stop {name: 'A'})-[e:LEG]->(:Stop {name: 'B'}) SET e.w = 100.0", changeID);

    expectRows(_shortestPath, {{"20.000000"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
