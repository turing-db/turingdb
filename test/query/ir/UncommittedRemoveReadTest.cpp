#include <gtest/gtest.h>

#include <algorithm>
#include <string_view>

#include "QueryInterpreterV3.h"
#include "QueryStatus.h"
#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// These do not decide whether a change's next query sees a REMOVE it has not committed:
// they assert that every read of that query gives the same answer, which holds either way.
class UncommittedRemoveReadTest : public WriteQueryTest {
protected:
    void openChangeWithRemysAgeRemoved(ChangeID& changeID) {
        openChange(changeID);

        const QueryStatus status = runWrite("MATCH (p:Person {name: 'Remy'}) REMOVE p.age", changeID);
        ASSERT_TRUE(status.isOk()) << status.getError();
    }

    void readInChange(std::string_view query, const ChangeID& changeID, Rows& rows) {
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

        sink.sortedRows(rows);
    }

    void expectEveryRowHoldsTheAge(std::string_view query, const ChangeID& changeID) {
        Rows rows;
        readInChange(query, changeID, rows);

        EXPECT_NE(std::ranges::find(rows, Row {"Adam", "32"}), rows.end()) << "query: " << query;

        for (const Row& row : rows) {
            EXPECT_EQ(row[1], "32") << "query: " << query << "\nrow of " << row[0];
        }
    }
};

TEST_F(UncommittedRemoveReadTest, everyRowAWherePredicateKeepsHoldsTheValue) {
    ChangeID changeID;
    openChangeWithRemysAgeRemoved(changeID);

    expectEveryRowHoldsTheAge("MATCH (p:Person) WHERE p.age = 32 RETURN p.name, p.age", changeID);
}

TEST_F(UncommittedRemoveReadTest, everyRowAPatternPropertyKeepsHoldsTheValue) {
    ChangeID changeID;
    openChangeWithRemysAgeRemoved(changeID);

    expectEveryRowHoldsTheAge("MATCH (p:Person {age: 32}) RETURN p.name, p.age", changeID);
}

TEST_F(UncommittedRemoveReadTest, countsTheValuesACollectGathers) {
    ChangeID changeID;
    openChangeWithRemysAgeRemoved(changeID);

    Rows counted;
    readInChange("MATCH (p:Person) RETURN count(p.age)", changeID, counted);

    Rows collected;
    readInChange("MATCH (p:Person) RETURN size(collect(p.age))", changeID, collected);

    EXPECT_EQ(counted, collected);
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
