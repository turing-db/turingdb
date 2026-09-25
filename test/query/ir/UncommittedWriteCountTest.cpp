#include <gtest/gtest.h>

#include <algorithm>
#include <string>
#include <string_view>

#include "QueryInterpreterV3.h"
#include "QueryStatus.h"

#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// A query in a change reads the property updates the change's earlier queries staged, with
// no COMMIT between. Of simpledb's 8 Person nodes, Remy and Adam carry an age.
class UncommittedWriteCountTest : public WriteQueryTest {
protected:
    void stageWrite(std::string_view query, const ChangeID& changeID) {
        const QueryStatus status = runWrite(query, changeID);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();
    }

    void expectChangeRows(std::string_view query, const ChangeID& changeID, const Rows& expected) {
        RowSink sink;
        QueryStatus status;
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              changeID,
                              &_env->getMem(),
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
};

TEST_F(UncommittedWriteCountTest, filtersOutTheNodeAnUncommittedSetTookThePropertyOff) {
    ChangeID changeID;
    openChange(changeID);

    stageWrite("MATCH (p:Person {name: 'Remy'}) SET p.age = null", changeID);

    expectChangeRows("MATCH (p:Person) WHERE p.age IS NOT NULL RETURN count(p)", changeID, {{"1"}});
}

// Reading the count off the graph's committed counts is an optimisation, so it must agree
// with the scan above rather than leave out what the change staged
TEST_F(UncommittedWriteCountTest, countsTheHoldersAnUncommittedSetLeft) {
    ChangeID changeID;
    openChange(changeID);

    stageWrite("MATCH (p:Person {name: 'Remy'}) SET p.age = null", changeID);

    expectChangeRows("MATCH (p:Person) RETURN count(p.age)", changeID, {{"1"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
