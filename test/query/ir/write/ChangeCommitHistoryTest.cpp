#include <gtest/gtest.h>

#include <string>
#include <string_view>

#include "QueryStatus.h"

#include "versioning/ChangeID.h"

#include "IRTestRows.h"
#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// db.history reports, for each commit, the nodes, edges and dataparts that commit added.
// A commit that COMMIT closes inside a change reports them before CHANGE SUBMIT, with the
// values it reports after it. The open tip has added nothing, so it reports 0 / 0 / 0.
class ChangeCommitHistoryTest : public WriteQueryTest {
protected:
    static constexpr std::string_view HISTORY_QUERY = "CALL db.history() YIELD nodeCount, edgeCount, partCount "
                                                      "RETURN nodeCount, edgeCount, partCount";

    void write(std::string_view query, const ChangeID& changeID) {
        const QueryStatus status = runWrite(query, changeID);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();
    }

    void queryRows(std::string_view query, const ChangeID& changeID, Rows& rows) {
        RowSink sink;
        const QueryStatus status = runQuery(query, changeID, &sink);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        rows = sink.rows();
    }

    void historyInChange(const ChangeID& changeID, Rows& rows) {
        queryRows(HISTORY_QUERY, changeID, rows);
    }

    void historyAtHead(Rows& rows) {
        queryRows(HISTORY_QUERY, ChangeID::head(), rows);
    }

    // The history head-first: @param changeRows for the commits the change made, then the
    // history of the graph the change was opened on
    void expectHistory(const Rows& actual, const Rows& changeRows, const Rows& baseRows) {
        Rows expected = changeRows;
        expected.insert(expected.end(), baseRows.begin(), baseRows.end());

        std::string actualText;
        describeRows(actual, actualText);

        EXPECT_EQ(actual, expected) << "got:\n" << actualText;
    }
};

TEST_F(ChangeCommitHistoryTest, committedCreateReportsItsNodesEdgesAndPart) {
    Rows baseRows;
    historyAtHead(baseRows);

    ChangeID changeID;
    openChange(changeID);

    write("CREATE (a:Person {name: 'X'})-[:KNOWS]->(b:Person {name: 'Y'})", changeID);
    write("COMMIT", changeID);

    Rows rows;
    historyInChange(changeID, rows);

    expectHistory(rows, {{"0", "0", "0"}, {"2", "1", "1"}}, baseRows);
}

TEST_F(ChangeCommitHistoryTest, committedCreateBetweenExistingNodesCountsOnlyWhatItAdded) {
    Rows baseRows;
    historyAtHead(baseRows);

    ChangeID changeID;
    openChange(changeID);

    write("MATCH (r:Person {name: 'Remy'}), (a:Person {name: 'Adam'}) "
          "CREATE (r)-[:PARTY_TO]->(c:Crime {id: 'x'})-[:OCCURRED_AT]->(a)",
          changeID);
    write("COMMIT", changeID);

    Rows rows;
    historyInChange(changeID, rows);

    expectHistory(rows, {{"0", "0", "0"}, {"1", "2", "1"}}, baseRows);
}

TEST_F(ChangeCommitHistoryTest, everyCommitOfAChangeReportsItsOwnCounts) {
    Rows baseRows;
    historyAtHead(baseRows);

    ChangeID changeID;
    openChange(changeID);

    write("CREATE (a:Person {name: 'X'})-[:KNOWS]->(b:Person {name: 'Y'})", changeID);
    write("COMMIT", changeID);

    write("CREATE (:Person {name: 'Z'}), (:Person {name: 'W'}), (:Person {name: 'V'})", changeID);
    write("COMMIT", changeID);

    Rows rows;
    historyInChange(changeID, rows);

    expectHistory(rows, {{"0", "0", "0"}, {"3", "0", "1"}, {"2", "1", "1"}}, baseRows);
}

TEST_F(ChangeCommitHistoryTest, oneCommitOfSeveralQueriesReportsTheirSum) {
    Rows baseRows;
    historyAtHead(baseRows);

    ChangeID changeID;
    openChange(changeID);

    write("CREATE (a:Person {name: 'X'})-[:KNOWS]->(b:Person {name: 'Y'})", changeID);
    write("CREATE (:Person {name: 'Z'})", changeID);
    write("COMMIT", changeID);

    Rows rows;
    historyInChange(changeID, rows);

    expectHistory(rows, {{"0", "0", "0"}, {"3", "1", "1"}}, baseRows);
}

TEST_F(ChangeCommitHistoryTest, submitKeepsTheCountsTheChangeReported) {
    Rows baseRows;
    historyAtHead(baseRows);

    ChangeID changeID;
    openChange(changeID);

    write("CREATE (a:Person {name: 'X'})-[:KNOWS]->(b:Person {name: 'Y'})", changeID);
    write("COMMIT", changeID);

    write("CREATE (:Person {name: 'Z'})", changeID);
    write("COMMIT", changeID);

    Rows changeRows;
    historyInChange(changeID, changeRows);

    submit(changeID);

    Rows headRows;
    historyAtHead(headRows);

    const Rows expectedChangeRows {{"0", "0", "0"}, {"1", "0", "1"}, {"2", "1", "1"}};
    expectHistory(changeRows, expectedChangeRows, baseRows);
    expectHistory(headRows, expectedChangeRows, baseRows);
}

// Main moves while the change is open, so the submit rebases the change's commits. The
// rebase must not change what each commit added.
TEST_F(ChangeCommitHistoryTest, rebaseOnSubmitKeepsTheCounts) {
    Rows baseRows;
    historyAtHead(baseRows);

    ChangeID changeID;
    openChange(changeID);

    write("CREATE (a:Person {name: 'X'})-[:KNOWS]->(b:Person {name: 'Y'})", changeID);
    write("COMMIT", changeID);

    Rows changeRows;
    historyInChange(changeID, changeRows);
    expectHistory(changeRows, {{"0", "0", "0"}, {"2", "1", "1"}}, baseRows);

    applyWrite("CREATE (:City {name: 'Paris'})");

    Rows mainRows;
    historyAtHead(mainRows);

    submit(changeID);

    Rows headRows;
    historyAtHead(headRows);

    expectHistory(headRows, {{"0", "0", "0"}, {"2", "1", "1"}}, mainRows);
}

// A CREATE and a SET on an existing node flush into two dataparts of one commit
TEST_F(ChangeCommitHistoryTest, commitWithCreatesAndUpdatesCountsBothParts) {
    Rows baseRows;
    historyAtHead(baseRows);

    ChangeID changeID;
    openChange(changeID);

    write("CREATE (:Person {name: 'Z'})", changeID);
    write("MATCH (n:Person {name: 'Remy'}) SET n.age = 1", changeID);
    write("COMMIT", changeID);

    Rows changeRows;
    historyInChange(changeID, changeRows);

    expectHistory(changeRows, {{"0", "0", "0"}, {"1", "0", "2"}}, baseRows);

    submit(changeID);

    Rows headRows;
    historyAtHead(headRows);

    EXPECT_EQ(changeRows, headRows);
}

TEST_F(ChangeCommitHistoryTest, describeCommitReportsACommittedCommit) {
    ChangeID changeID;
    openChange(changeID);

    write("CREATE (a:Person {name: 'X'})-[:KNOWS]->(b:Person {name: 'Y'})", changeID);
    write("COMMIT", changeID);

    Rows hashRows;
    queryRows("CALL db.history() YIELD commit RETURN commit", changeID, hashRows);
    ASSERT_GE(hashRows.size(), 2);

    const std::string describeQuery = "CALL db.describeCommit('" + hashRows[1][0] + "') "
                                      "YIELD nodeCount, edgeCount, partCount "
                                      "RETURN nodeCount, edgeCount, partCount";
    Rows rows;
    queryRows(describeQuery, changeID, rows);

    EXPECT_EQ(rows, (Rows {{"2", "1", "1"}}));
}

// The open tip has added nothing, even with writes staged in it before a COMMIT
TEST_F(ChangeCommitHistoryTest, openTipReportsNothingBeforeCommit) {
    Rows baseRows;
    historyAtHead(baseRows);

    ChangeID changeID;
    openChange(changeID);

    write("CREATE (a:Person {name: 'X'})-[:KNOWS]->(b:Person {name: 'Y'})", changeID);

    Rows rows;
    historyInChange(changeID, rows);

    expectHistory(rows, {{"0", "0", "0"}}, baseRows);
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
