#include <gtest/gtest.h>

#include <string_view>
#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

namespace {

using Rows = std::vector<StringRowSink::Row>;

}

// A null on either side of '+' makes the result null, a list on the other side included.
// Only Remy and Adam of simpledb's Person nodes have an age, 32 each.
class ListPlusNullTest : public CallV3Test {
protected:
    void expectRows(std::string_view query, const Rows& expected) {
        StringRowSink sink;
        runQuery(query, sink);

        Rows rows;
        sink.sortedRows(rows);

        EXPECT_EQ(rows, expected) << query;
    }
};

TEST_F(ListPlusNullTest, answersNullForTheNullLiteralBesideAList) {
    expectRows("RETURN [1] + null, null + [1]", {{"null", "null"}});
    expectRows("RETURN [1] + null + [2]", {{"null"}});
}

TEST_F(ListPlusNullTest, appendsTheNullAListHolds) {
    expectRows("RETURN [1] + [null], [null] + [1]", {{"1, null", "null, 1"}});
}

TEST_F(ListPlusNullTest, answersNullForAMissingPropertyBesideAList) {
    expectRows("MATCH (n:Person) RETURN n.name, [1] + n.age, n.age + [1]",
               {{"Adam", "1, 32", "32, 1"},
                {"Cyrus", "null", "null"},
                {"Doruk", "null", "null"},
                {"Luc", "null", "null"},
                {"Martina", "null", "null"},
                {"Maxime", "null", "null"},
                {"Remy", "1, 32", "32, 1"},
                {"Suhas", "null", "null"}});
}

TEST_F(ListPlusNullTest, answersNullForAnUnwoundNullBesideAList) {
    expectRows("UNWIND [1, null] AS x RETURN [0] + x, x + [0]",
               {{"0, 1", "1, 0"},
                {"null", "null"}});
}

TEST_F(ListPlusNullTest, answersNullForAnUnmatchedNodeBesideAList) {
    expectRows("OPTIONAL MATCH (n:Person {name: 'Nobody'}) RETURN [1] + n, n + [1]", {{"null", "null"}});
}

TEST_F(ListPlusNullTest, answersNullForANullCellBesideAList) {
    expectRows("WITH [[1], null, 'a'] AS xs RETURN [1] + xs[1], xs[1] + [1]", {{"null", "null"}});
}

TEST_F(ListPlusNullTest, answersNullForAMissingPropertyBesideAListCell) {
    expectRows("MATCH (n:Person) WHERE n.name IN ['Remy', 'Maxime'] WITH n, [[1], 'a'] AS xs RETURN n.name, xs[0] + n.age, n.age + xs[0]",
               {{"Maxime", "null", "null"},
                {"Remy", "[1, 32]", "[32, 1]"}});
}

TEST_F(ListPlusNullTest, answersNullForAMissingListPropertyBesideAList) {
    runWrite("MATCH (n:Person {name: 'Remy'}) SET n.scores = [7]");

    expectRows("MATCH (n:Person) WHERE n.name IN ['Remy', 'Adam'] RETURN n.name, [0] + n.scores, n.scores + [0]",
               {{"Adam", "null", "null"},
                {"Remy", "0, 7", "7, 0"}});
}

TEST_F(ListPlusNullTest, answersNullForAReduceAppendingANull) {
    expectRows("RETURN reduce(acc = [], x IN [1, null, 2] | acc + x)", {{"null"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
