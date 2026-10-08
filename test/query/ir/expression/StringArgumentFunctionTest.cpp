#include <gtest/gtest.h>

#include <string>
#include <string_view>

#include "QueryStatus.h"

#include "IRTestRows.h"
#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

class StringArgumentFunctionTest : public WriteQueryTest {
protected:
    void initialize() override {
        WriteQueryTest::initialize();

        applyWrite("MATCH (p:Person {name: 'Remy'}) SET p.words = ['hello', 3, null]");
    }

    void expectError(std::string_view query, std::string_view expectedError) {
        RowSink sink;
        const QueryStatus status = runQuery(query, &sink);

        ASSERT_FALSE(status.isOk()) << "query: " << query << "\nexpected it to fail";
        EXPECT_NE(status.getError().find(expectedError), std::string::npos)
            << "query: " << query << "\nerror: " << status.getError();
    }
};

TEST_F(StringArgumentFunctionTest, substringOfALiteral) {
    expectRows("RETURN substring('hello', 1, 3), substring('hello', 1), substring('hello', 9), substring('hello', 2, 99)",
               {{"ell", "ello", "", "llo"}});
}

TEST_F(StringArgumentFunctionTest, substringCountsCharactersNotBytes) {
    expectRows("RETURN substring('été', 1, 1), substring('été', 1)", {{"t", "té"}});
}

TEST_F(StringArgumentFunctionTest, readsAProperty) {
    expectRows("MATCH (p:Person {name: 'Remy'}) RETURN substring(p.name, 1, 2), substring(p.name, 2)",
               {{"em", "my"}});
}

TEST_F(StringArgumentFunctionTest, readsAnArgumentPerRow) {
    expectRows("UNWIND [1, 2, 3] AS n RETURN substring('hello', 0, n), substring('hello', n)", {{"h", "ello"}, {"he", "llo"}, {"hel", "lo"}});
}

TEST_F(StringArgumentFunctionTest, substringOfNullIsNull) {
    expectRows("RETURN substring(null, 1), substring(null, 1, 2)", {{"null", "null"}});
}

TEST_F(StringArgumentFunctionTest, substringRejectsANullStartOrLength) {
    expectError("RETURN substring('hello', null)", "substring()");
    expectError("RETURN substring('hello', 0, null)", "substring()");
    expectError("MATCH (p:Person {name: 'Remy'}) RETURN substring(p.name, p.nosuch)", "substring()");
    expectError("MATCH (p:Person {name: 'Remy'}) RETURN substring(p.name, 0, p.words[2])", "substring()");
}

TEST_F(StringArgumentFunctionTest, substringRejectsAStartOrLengthPastTheLargestInteger) {
    expectError("RETURN substring('hello', 2147483648)", "substring()");
    expectError("RETURN substring('hello', 0, 2147483648)", "substring()");
    expectRows("RETURN substring('hello', 2147483647), substring('hello', 0, 2147483647)", {{"", "hello"}});
}

TEST_F(StringArgumentFunctionTest, absentPropertyIsNull) {
    expectRows("MATCH (p:Person {name: 'Remy'}) RETURN substring(p.nosuch, 1)", {{"null"}});
}

TEST_F(StringArgumentFunctionTest, readsATaggedCell) {
    expectRows("MATCH (p:Person {name: 'Remy'}) RETURN substring(p.words[0], 1, p.words[1]), substring(p.words[2], 1)",
               {{"ell", "null"}});
}

TEST_F(StringArgumentFunctionTest, rejectsATaggedCellOfTheWrongType) {
    expectError("MATCH (p:Person {name: 'Remy'}) RETURN substring(p.words[1], 2)", "substring()");
}

TEST_F(StringArgumentFunctionTest, rejectsANegativeLength) {
    expectError("RETURN substring('hello', 1, -1)", "substring()");
    expectError("RETURN substring('hello', -1)", "substring()");
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
