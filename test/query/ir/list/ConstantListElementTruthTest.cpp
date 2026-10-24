#include <gtest/gtest.h>

#include <string_view>
#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

// An element read out of a mixed list at a constant position is one cell for every row,
// which may be absent where the position is past the end. simpledb has 8 people.
class ConstantListElementTruthTest : public CallV3Test {
protected:
    void expectRows(std::string_view query, const std::vector<StringRowSink::Row>& expected) {
        StringRowSink sink;
        runQuery(query, sink);

        EXPECT_EQ(sink.getRows(), expected) << query;
    }
};

TEST_F(ConstantListElementTruthTest, negatesAnElement) {
    expectRows("RETURN NOT [true, 1][0]", {{"false"}});
    expectRows("WITH [false, 1] AS xs RETURN NOT xs[0]", {{"true"}});
    expectRows("RETURN NOT head([true, 1])", {{"false"}});
}

TEST_F(ConstantListElementTruthTest, negatesAMissingElementToNull) {
    expectRows("RETURN NOT [true, 1][5]", {{"null"}});
}

TEST_F(ConstantListElementTruthTest, negatesAnElementOnEveryRow) {
    expectRows("MATCH (p:Person) WHERE NOT [false, 1][0] RETURN count(p)", {{"8"}});
}

TEST_F(ConstantListElementTruthTest, combinesAnElement) {
    expectRows("RETURN [true, 1][0] AND true, [false, 1][0] OR false", {{"true", "false"}});
    expectRows("RETURN [true, 1][5] AND true, [true, 1][5] OR true", {{"null", "true"}});
}

TEST_F(ConstantListElementTruthTest, testsEveryItemOnAnElement) {
    expectRows("RETURN any(x IN [1, 2] WHERE [true, 1][0]), all(x IN [1, 2] WHERE [false, 1][0])",
               {{"true", "false"}});
    expectRows("RETURN any(x IN [1, 2] WHERE [true, 1][5])", {{"null"}});
}

TEST_F(ConstantListElementTruthTest, keepsItemsOnAnElement) {
    expectRows("RETURN [x IN [1, 2] WHERE [true, 1][0]], [x IN [1, 2] WHERE [true, 1][5]]",
               {{"1, 2", ""}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
