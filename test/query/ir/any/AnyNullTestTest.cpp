#include <gtest/gtest.h>

#include <string_view>

#include "CallV3Test.h"
#include "IRTestRows.h"

using namespace turing::test;

// An element read out of a mixed list is null where the list holds a null there, and where
// the list holds nothing there
class AnyNullTestTest : public CallV3Test {
protected:
    void expectRows(std::string_view query, const Rows& expected) {
        RowSink sink;
        runQuery(query, sink);

        Rows rows;
        sink.sortedRows(rows);

        EXPECT_EQ(rows, expected) << query;
    }
};

TEST_F(AnyNullTestTest, testsANullElementForNull) {
    expectRows("UNWIND [[null, 'b'], [1, 'b']] AS pair RETURN pair[1], pair[0] IS NULL, pair[0] IS NOT NULL",
               {{"b", "false", "true"}, {"b", "true", "false"}});
}

TEST_F(AnyNullTestTest, testsAMissingElementForNull) {
    expectRows("UNWIND [[1, 'b']] AS pair RETURN pair[5] IS NULL, pair[5] IS NOT NULL", {{"true", "false"}});
}

TEST_F(AnyNullTestTest, filtersOnANullElement) {
    expectRows("UNWIND [[null, 'a'], [1, 'b'], [null, 'c']] AS pair WITH pair WHERE pair[0] IS NULL RETURN pair[1]",
               {{"a"}, {"c"}});
    expectRows("UNWIND [[null, 'a'], [1, 'b'], [null, 'c']] AS pair WITH pair WHERE pair[0] IS NOT NULL RETURN pair[1]",
               {{"b"}});
}

TEST_F(AnyNullTestTest, testsAConstantNullElementForNull) {
    expectRows("RETURN [null, 'b'][0] IS NULL, [null, 'b'][1] IS NULL", {{"true", "false"}});
}

TEST_F(AnyNullTestTest, testsANullElementOfAnAliasForNull) {
    expectRows("UNWIND [[null, 'b'], [1, 'b']] AS pair WITH pair[0] AS x RETURN x IS NULL", {{"false"}, {"true"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
