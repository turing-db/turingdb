#include <gtest/gtest.h>

#include <stddef.h>

#include <algorithm>
#include <memory>
#include <string>
#include <string_view>

#include "QueryInterpreterV3.h"
#include "QueryStatus.h"

#include "Graph.h"
#include "SimpleGraph.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"

#include "IRTestRows.h"
#include "TuringTestEnv.h"
#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

namespace {

struct ReadCounts {
    size_t _personInterests {0};
    size_t _friendInterests {0};
    size_t _sharedInterests {0};
    size_t _sameNamePairs {0};
    size_t _edges {0};
};

const std::string_view personInterests = "MATCH (a:Person)-[:INTERESTED_IN]->(i), (m) "
                                         "WHERE i.name = m.name "
                                         "RETURN count(*)";

const std::string_view friendInterests = "MATCH (a:Person)-[:KNOWS_WELL]->(b)-[:INTERESTED_IN]->(i), (m) "
                                         "WHERE i.name = m.name "
                                         "RETURN count(*)";

const std::string_view sharedInterests = "MATCH (a:Person)-[:INTERESTED_IN]->(i) "
                                         "MATCH (b:Person)-[:INTERESTED_IN]->(i) "
                                         "RETURN count(*)";

const std::string_view sharedInterestsReversed = "MATCH (b:Person)-[:INTERESTED_IN]->(i) "
                                                 "MATCH (a:Person)-[:INTERESTED_IN]->(i) "
                                                 "RETURN count(*)";

const std::string_view sameNamePairs = "MATCH (n), (m) "
                                       "WHERE n.name = m.name "
                                       "RETURN count(*)";

const std::string_view edgeCount = "MATCH ()-[e]->() RETURN count(e)";

const std::string_view alienSharedInterests = "MATCH (a:Alien)-[:INTERESTED_IN]->(i)<-[:INTERESTED_IN]-(p:Person) "
                                              "RETURN count(*)";

const std::string_view alienCount = "MATCH (n:Alien) RETURN count(n)";

}

// SimpleGraph names each of its 18 nodes and has 18 edges: 15 INTERESTED_IN from a Person
// and 3 KNOWS_WELL. The interests' squared Person in-degrees sum to 27, the shared-interest
// count, which is read with its two MATCH clauses in either order.
class EdgeStatisticsAfterWritesTest : public WriteQueryTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::createSyncedOnDisk(fs::Path {_outDir} / "turing");
        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager(), &_env->getMem(), &_env->getCompilerContext());

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);
    }

    void restartAndLoadGraph() {
        _interpreter.reset();
        _env.reset();

        _env = TuringTestEnv::createSyncedOnDisk(fs::Path {_outDir} / "turing");
        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager(), &_env->getMem(), &_env->getCompilerContext());

        const std::string load = "LOAD GRAPH " + _graphName;

        RowSink sink;
        QueryStatus status;
        _interpreter->execute(status,
                              load,
                              "default",
                              CommitHash::head(),
                              ChangeID::head(),
                              &sink);
        ASSERT_TRUE(status.isOk()) << "query: " << load << "\nerror: " << status.getError();
    }

    void expectWrite(const ChangeID& changeID, std::string_view query) {
        const QueryStatus status = runWrite(query, changeID);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();
    }

    void commit(const ChangeID& changeID) {
        expectWrite(changeID, "COMMIT");
    }

    void expectRowsIn(const ChangeID& changeID, std::string_view query, const Rows& expected) {
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

        std::string actualText;
        describeRows(actual, actualText);

        EXPECT_EQ(actual, sortedExpected) << "query: " << query << "\ngot:\n" << actualText;
    }

    void expectReads(const ChangeID& changeID, const ReadCounts& expected) {
        expectRowsIn(changeID, personInterests, {{std::to_string(expected._personInterests)}});
        expectRowsIn(changeID, friendInterests, {{std::to_string(expected._friendInterests)}});
        expectRowsIn(changeID, sharedInterests, {{std::to_string(expected._sharedInterests)}});
        expectRowsIn(changeID, sharedInterestsReversed, {{std::to_string(expected._sharedInterests)}});
        expectRowsIn(changeID, sameNamePairs, {{std::to_string(expected._sameNamePairs)}});
        expectRowsIn(changeID, edgeCount, {{std::to_string(expected._edges)}});
    }
};

TEST_F(EdgeStatisticsAfterWritesTest, countsTheEdgesOfTheUnwrittenGraph) {
    expectReads(ChangeID::head(),
                ReadCounts {._personInterests = 15,
                            ._friendInterests = 5,
                            ._sharedInterests = 27,
                            ._sameNamePairs = 18,
                            ._edges = 18});

    expectRowsIn(ChangeID::head(),
                 "MATCH (a:Person)-[:INTERESTED_IN]->(i), (b:Person)-[:INTERESTED_IN]->(i) "
                 "WHERE a.name < b.name "
                 "RETURN count(*)",
                 {{"6"}});
}

TEST_F(EdgeStatisticsAfterWritesTest, countsTheEdgesLeftByADeletedEdge) {
    ChangeID changeID;
    openChange(changeID);

    expectWrite(changeID,
                "MATCH (a:Person {name: 'Cyrus'})-[e:INTERESTED_IN]->(i:Interest {name: 'Gym'}) "
                "DELETE e");
    commit(changeID);

    const ReadCounts expected {._personInterests = 14,
                               ._friendInterests = 5,
                               ._sharedInterests = 22,
                               ._sameNamePairs = 18,
                               ._edges = 17};
    expectReads(changeID, expected);

    submit(changeID);
    expectReads(ChangeID::head(), expected);
}

TEST_F(EdgeStatisticsAfterWritesTest, countsTheEdgesLeftByDetachDeletedNodes) {
    ChangeID changeID;
    openChange(changeID);

    expectWrite(changeID, "MATCH (i:Interest {name: 'Bio'}) DETACH DELETE i");
    expectWrite(changeID, "MATCH (i:Interest {name: 'Gym'}) DETACH DELETE i");
    commit(changeID);

    const ReadCounts expected {._personInterests = 10,
                               ._friendInterests = 4,
                               ._sharedInterests = 14,
                               ._sameNamePairs = 16,
                               ._edges = 13};
    expectReads(changeID, expected);

    submit(changeID);
    expectReads(ChangeID::head(), expected);
}

TEST_F(EdgeStatisticsAfterWritesTest, countsEdgesCreatedAndDeletedInOneChange) {
    ChangeID changeID;
    openChange(changeID);

    expectWrite(changeID,
                "MATCH (a:Person {name: 'Remy'}), (b:Person {name: 'Luc'}) "
                "CREATE (x:Person {name: 'X1'})-[:INTERESTED_IN]->(g:Interest {name: 'Golf'}), "
                "(a)-[:INTERESTED_IN]->(g), "
                "(x)-[:INTERESTED_IN]->(c:Interest {name: 'Chess'}), "
                "(b)-[:INTERESTED_IN]->(c), "
                "(x)-[:KNOWS_WELL]->(a)");
    commit(changeID);

    expectReads(changeID,
                ReadCounts {._personInterests = 19,
                            ._friendInterests = 10,
                            ._sharedInterests = 35,
                            ._sameNamePairs = 21,
                            ._edges = 23});

    expectWrite(changeID,
                "MATCH (a:Person {name: 'Remy'})-[e:INTERESTED_IN]->(g:Interest {name: 'Golf'}) "
                "DELETE e");
    expectWrite(changeID,
                "MATCH (x:Person {name: 'X1'})-[e:KNOWS_WELL]->() "
                "DELETE e");
    commit(changeID);

    const ReadCounts expected {._personInterests = 18,
                               ._friendInterests = 5,
                               ._sharedInterests = 32,
                               ._sameNamePairs = 21,
                               ._edges = 21};
    expectReads(changeID, expected);

    submit(changeID);
    expectReads(ChangeID::head(), expected);
}

TEST_F(EdgeStatisticsAfterWritesTest, countsTwoChangesOfOneBaseSubmittedInTurnAndReloaded) {
    ChangeID robotChange;
    openChange(robotChange);

    ChangeID droidChange;
    openChange(droidChange);

    expectWrite(robotChange,
                "MATCH (g:Interest {name: 'Gym'}) "
                "CREATE (r:Robot {name: 'R1'})-[:INTERESTED_IN]->(g)");
    expectWrite(robotChange,
                "MATCH (a:Person {name: 'Remy'})-[e:KNOWS_WELL]->(b:Person {name: 'Adam'}) "
                "DELETE e");
    commit(robotChange);

    expectWrite(droidChange,
                "MATCH (g:Interest {name: 'Bio'}), (p:Person {name: 'Remy'}) "
                "CREATE (d:Droid {name: 'D1'})-[:INTERESTED_IN]->(g), "
                "(p)-[:INTERESTED_IN]->(g), "
                "(d)-[:KNOWS_WELL]->(p)");
    commit(droidChange);

    expectWrite(droidChange,
                "MATCH (d:Droid {name: 'D1'})-[e:KNOWS_WELL]->() "
                "DELETE e");
    commit(droidChange);

    expectReads(droidChange,
                ReadCounts {._personInterests = 16,
                            ._friendInterests = 6,
                            ._sharedInterests = 32,
                            ._sameNamePairs = 19,
                            ._edges = 20});

    submit(robotChange);
    submit(droidChange);

    const ReadCounts merged {._personInterests = 16,
                             ._friendInterests = 4,
                             ._sharedInterests = 32,
                             ._sameNamePairs = 20,
                             ._edges = 20};
    expectReads(ChangeID::head(), merged);
    expectRowsIn(ChangeID::head(), "MATCH (n) RETURN count(n)", {{"20"}});

    restartAndLoadGraph();
    expectReads(ChangeID::head(), merged);
}

TEST_F(EdgeStatisticsAfterWritesTest, countsNewLabelSetsThroughARebasedSubmitAndReloads) {
    ChangeID loopChange;
    openChange(loopChange);

    ChangeID uncommittedChange;
    openChange(uncommittedChange);

    expectWrite(loopChange,
                "MATCH (a:Person {name: 'Remy'}) "
                "CREATE (a)-[:KNOWS_WELL]->(a)");
    expectWrite(loopChange, "CREATE (p:Alien:Person {name: 'P1'})-[:KNOWS_WELL]->(q:Alien {name: 'Q1'})");
    commit(loopChange);

    expectWrite(loopChange,
                "MATCH (a:Person {name: 'Remy'})-[e:KNOWS_WELL]->(a) "
                "DELETE e");
    expectWrite(loopChange,
                "MATCH (p:Alien {name: 'P1'})-[e]->(q) "
                "DELETE e");
    commit(loopChange);

    expectReads(loopChange,
                ReadCounts {._personInterests = 15,
                            ._friendInterests = 5,
                            ._sharedInterests = 27,
                            ._sameNamePairs = 20,
                            ._edges = 18});
    expectRowsIn(loopChange, alienSharedInterests, {{"0"}});
    expectRowsIn(loopChange, alienCount, {{"2"}});

    expectWrite(uncommittedChange,
                "MATCH (g:Interest {name: 'Gym'}) "
                "CREATE (z:Alien {name: 'Z1'})-[:INTERESTED_IN]->(g)");

    submit(loopChange);
    submit(uncommittedChange);

    const ReadCounts merged {._personInterests = 15,
                             ._friendInterests = 5,
                             ._sharedInterests = 27,
                             ._sameNamePairs = 21,
                             ._edges = 19};
    expectReads(ChangeID::head(), merged);
    expectRowsIn(ChangeID::head(), alienSharedInterests, {{"3"}});
    expectRowsIn(ChangeID::head(), alienCount, {{"3"}});

    restartAndLoadGraph();
    expectReads(ChangeID::head(), merged);
    expectRowsIn(ChangeID::head(), alienSharedInterests, {{"3"}});
    expectRowsIn(ChangeID::head(), alienCount, {{"3"}});

    ChangeID gymChange;
    openChange(gymChange);

    expectWrite(gymChange, "MATCH (g:Interest {name: 'Gym'}) DETACH DELETE g");
    commit(gymChange);

    const ReadCounts withoutGym {._personInterests = 12,
                                 ._friendInterests = 5,
                                 ._sharedInterests = 18,
                                 ._sameNamePairs = 20,
                                 ._edges = 15};
    expectReads(gymChange, withoutGym);
    expectRowsIn(gymChange, alienSharedInterests, {{"0"}});
    expectRowsIn(gymChange, alienCount, {{"3"}});

    submit(gymChange);
    expectReads(ChangeID::head(), withoutGym);

    restartAndLoadGraph();
    expectReads(ChangeID::head(), withoutGym);
    expectRowsIn(ChangeID::head(), alienSharedInterests, {{"0"}});
    expectRowsIn(ChangeID::head(), alienCount, {{"3"}});
}

TEST_F(EdgeStatisticsAfterWritesTest, keepsTheCountsOfMainWhenAConflictingSubmitIsRejected) {
    ChangeID robotChange;
    openChange(robotChange);

    ChangeID gymChange;
    openChange(gymChange);

    expectWrite(robotChange,
                "MATCH (g:Interest {name: 'Gym'}) "
                "CREATE (r:Robot {name: 'R1'})-[:INTERESTED_IN]->(g)");
    commit(robotChange);

    expectWrite(gymChange, "MATCH (g:Interest {name: 'Gym'}) DETACH DELETE g");
    commit(gymChange);

    submit(robotChange);

    const QueryStatus rejected = runWrite("CHANGE SUBMIT", gymChange);
    ASSERT_FALSE(rejected.isOk()) << "expected the submit of the change deleting Gym to fail";
    EXPECT_NE(rejected.getError().find("Commits on main have created an edge"), std::string::npos)
        << "error: " << rejected.getError();

    expectReads(ChangeID::head(),
                ReadCounts {._personInterests = 15,
                            ._friendInterests = 5,
                            ._sharedInterests = 27,
                            ._sameNamePairs = 19,
                            ._edges = 19});

    ChangeID cleanupChange;
    openChange(cleanupChange);

    expectWrite(cleanupChange, "MATCH (r:Robot) DETACH DELETE r");
    commit(cleanupChange);

    expectReads(cleanupChange,
                ReadCounts {._personInterests = 15,
                            ._friendInterests = 5,
                            ._sharedInterests = 27,
                            ._sameNamePairs = 18,
                            ._edges = 18});
}
