#include <gtest/gtest.h>

#include <stddef.h>
#include <stdint.h>

#include <algorithm>
#include <string>
#include <string_view>

#include "IRTestRows.h"
#include "QueryStatus.h"
#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// coalesce computes each argument only over the rows every argument before it left null, and
// writes each row the first value that is not null, in the order of the rows. A division by
// i % 3 raises on every third row, so an argument holding one shows which rows reached it.
class CoalesceRowsStillNullTest : public WriteQueryTest {
protected:
    // More rows than one chunk holds, so the null rows of each argument straddle the chunks
    static constexpr int64_t ROW_COUNT = 3 * 65536 + 7;

    // Names the first row that differs rather than printing every row
    void expectManyRowsInOrder(std::string_view query, const Rows& expected) {
        RowSink sink;
        const QueryStatus status = runQuery(query, &sink);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        const Rows& rows = sink.rows();
        ASSERT_EQ(rows.size(), expected.size());

        const auto mismatch = std::mismatch(rows.begin(), rows.end(), expected.begin());
        const bool matches = mismatch.first == rows.end();
        EXPECT_TRUE(matches) << "row " << (mismatch.first - rows.begin()) << " differs";
    }
};

TEST_F(CoalesceRowsStillNullTest, computesEachArgumentOverTheRowsStillNull) {
    expectRowsInOrder("UNWIND range(0, 8) AS i "
                      "RETURN i, coalesce(CASE WHEN i % 3 = 0 THEN i END, CASE WHEN i % 3 = 1 THEN -i END, 100 / (i % 3))",
                      {{"0", "0"}, {"1", "-1"}, {"2", "50"}, {"3", "3"}, {"4", "-4"}, {"5", "50"},
                       {"6", "6"}, {"7", "-7"}, {"8", "50"}});
}

TEST_F(CoalesceRowsStillNullTest, raisesWhereARowReachesTheArgumentThatRaises) {
    expectError("UNWIND range(0, 8) AS i RETURN coalesce(CASE WHEN i % 3 = 1 THEN i END, 100 / (i % 3))",
                "divide by zero");
    expectError("UNWIND range(1, 9) AS i RETURN coalesce(CASE WHEN i < 9 THEN i END, null, 100 / (i % 3))",
                "divide by zero");
}

TEST_F(CoalesceRowsStillNullTest, keepsTheRowOrderAcrossChunks) {
    const std::string query = "UNWIND range(0, " + std::to_string(ROW_COUNT - 1) + ") AS i "
                              "RETURN coalesce(CASE WHEN i % 3 = 0 THEN i END, CASE WHEN i % 3 = 1 THEN -i END, "
                              "i * 100 / (i % 3))";

    Rows expected;
    for (int64_t i = 0; i < ROW_COUNT; i++) {
        if (i % 3 == 0) {
            expected.push_back({std::to_string(i)});
        } else if (i % 3 == 1) {
            expected.push_back({std::to_string(-i)});
        } else {
            expected.push_back({std::to_string(i * 50)});
        }
    }

    expectManyRowsInOrder(query, expected);
}

TEST_F(CoalesceRowsStillNullTest, fillsWholeChunksFromOneArgument) {
    const std::string query = "UNWIND range(0, " + std::to_string(ROW_COUNT - 1) + ") AS i "
                              "RETURN coalesce(CASE WHEN i < 65536 OR i >= 131072 THEN i END, -i)";

    Rows expected;
    for (int64_t i = 0; i < ROW_COUNT; i++) {
        const bool takesTheFirst = i < 65536 || i >= 131072;
        expected.push_back({std::to_string(takesTheFirst ? i : -i)});
    }

    expectManyRowsInOrder(query, expected);
}

TEST_F(CoalesceRowsStillNullTest, raisesInACoalesceNothingReads) {
    expectError("UNWIND range(0, 5) AS i "
                "WITH i, coalesce(CASE WHEN i % 3 = 1 THEN i END, 100 / (i % 3)) AS unused RETURN count(*)",
                "divide by zero");
    expectRows("UNWIND range(0, 5) AS i "
               "WITH i, coalesce(CASE WHEN i % 3 = 0 THEN i END, 100 / (i % 3)) AS unused RETURN count(*)",
               {{"6"}});
}

TEST_F(CoalesceRowsStillNullTest, coalescesConstants) {
    expectRows("RETURN coalesce(null, 2, 1 / 0)", {{"2"}});
    expectRows("RETURN coalesce(null, null, 'a')", {{"a"}});
    expectRows("UNWIND [1, 2] AS t RETURN coalesce(null, 3)", {{"3"}, {"3"}});
    expectError("RETURN coalesce(1 / 0, 2)", "divide by zero");
    expectError("UNWIND [1, 2] AS t RETURN coalesce(null, 10 / 0)", "divide by zero");
}

TEST_F(CoalesceRowsStillNullTest, computesFiveThousandArguments) {
    constexpr size_t ARGUMENT_COUNT = 5000;

    std::string query = "UNWIND range(1, 1000) AS t RETURN t, coalesce(";
    for (size_t argumentIndex = 1; argumentIndex <= ARGUMENT_COUNT; argumentIndex++) {
        const std::string index = std::to_string(argumentIndex);
        query += "CASE WHEN t = " + index + " THEN -" + index + " END, ";
    }
    query += "1 / 0)";

    Rows expected;
    for (int64_t t = 1; t <= 1000; t++) {
        expected.push_back({std::to_string(t), std::to_string(-t)});
    }

    expectRowsInOrder(query, expected);
}
