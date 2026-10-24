#include <gtest/gtest.h>

#include <string_view>
#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

// A WHERE and a WHEN read a value of type any as the truth value its row holds, as NOT and
// AND do. Remy and Adam are 32; the other six people have no age.
class CaseAnyConditionTest : public CallV3Test {
protected:
    void expectRows(std::string_view query, const std::vector<StringRowSink::Row>& expected) {
        StringRowSink sink;
        runQuery(query, sink);

        std::vector<StringRowSink::Row> rows;
        sink.sortedRows(rows);

        EXPECT_EQ(rows, expected) << query;
    }
};

TEST_F(CaseAnyConditionTest, filtersOnAConstantCell) {
    expectRows("WITH [true, 'x'] AS xs WHERE xs[0] RETURN 1", {{"1"}});
    expectRows("MATCH (p:Person) WHERE [true, 1][0] RETURN count(p)", {{"8"}});
    expectRows("MATCH (p:Person) WHERE [true, 1][5] RETURN count(p)", {{"0"}});
}

TEST_F(CaseAnyConditionTest, filtersOnACellPerRow) {
    expectRows("MATCH (p:Person) WITH p, [p.age > 30, 'x'][0] AS flag WHERE flag RETURN p.name",
               {{"Adam"}, {"Remy"}});
}

TEST_F(CaseAnyConditionTest, selectsABranchOnACellPerRow) {
    expectRows("MATCH (p:Person) WHERE p.name IN ['Remy', 'Luc'] "
               "RETURN p.name, CASE WHEN [p.age > 30, 'x'][0] THEN 'old' ELSE 'other' END",
               {{"Luc", "other"}, {"Remy", "old"}});
}

TEST_F(CaseAnyConditionTest, selectsABranchOnAConstantCell) {
    expectRows("RETURN CASE WHEN [true, 1][0] THEN 'y' ELSE 'n' END", {{"y"}});
    expectRows("RETURN CASE WHEN [true, 1][5] THEN 'y' ELSE 'n' END", {{"n"}});
}

TEST_F(CaseAnyConditionTest, rejectsAConditionHoldingNoBoolean) {
    runQueryExpectingError("MATCH (p:Person) WHERE p.name IN ['Remy', 'Luc'] "
                           "RETURN CASE WHEN CASE WHEN p.age > 30 THEN true ELSE 'no' END THEN 1 ELSE 2 END",
                           "A condition reads a value that is not a boolean");
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
