#include <gtest/gtest.h>

#include <string>
#include <string_view>

#include "IRTestRows.h"
#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

class TemporalCellTypeErrorTest : public WriteQueryTest {
protected:
    void expectQueryError(std::string_view query, std::string_view expectedError) {
        RowSink sink;
        const QueryStatus status = runQuery(query, &sink);
        ASSERT_FALSE(status.isOk()) << "query: " << query << "\nexpected it to fail";

        const std::string& error = status.getError();

        EXPECT_EQ(error, expectedError) << "query: " << query;
        EXPECT_EQ(error.find("Unexpected exception"), std::string::npos) << "query: " << query;
    }
};

TEST_F(TemporalCellTypeErrorTest, rejectsADateTimeOfACellHoldingNoStringIntegerOrMap) {
    expectQueryError("UNWIND [1.5, 'a'] AS x RETURN datetime(x)",
                     "datetime() reads a string, an integer or a map, and this row holds a value that is not one");
    expectQueryError("UNWIND [true, 'a'] AS x RETURN datetime(x)",
                     "datetime() reads a string, an integer or a map, and this row holds a value that is not one");
}

TEST_F(TemporalCellTypeErrorTest, rejectsADurationOfACellHoldingNoIntegerOrMap) {
    expectQueryError("UNWIND [1.5, 1] AS x RETURN duration(x)",
                     "duration() reads an integer or a map, and this row holds a value that is not one");
    expectQueryError("UNWIND ['a', 1] AS x RETURN duration(x)",
                     "duration() reads an integer or a map, and this row holds a value that is not one");
}
