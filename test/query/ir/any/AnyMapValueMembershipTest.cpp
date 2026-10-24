#include <gtest/gtest.h>

#include <string_view>

#include "CallV3Test.h"
#include "IRTestRows.h"

using namespace turing::test;

class AnyMapValueMembershipTest : public CallV3Test {
protected:
    void expectRows(std::string_view query, const Rows& expected) {
        RowSink sink;
        runQuery(query, sink);

        Rows rows;
        sink.sortedRows(rows);

        EXPECT_EQ(rows, expected) << query;
    }
};

TEST_F(AnyMapValueMembershipTest, looksUpAValueInAListHeldByAMap) {
    expectRows("UNWIND [{a: [1, 2], n: 'p'}, {a: [3], n: 'q'}, {n: 'r'}] AS m RETURN m.n, 1 IN m.a",
               {{"p", "true"}, {"q", "false"}, {"r", "null"}});
}

TEST_F(AnyMapValueMembershipTest, looksUpAValueInAListHeldByAConstantMap) {
    expectRows("WITH {a: [1, 2]} AS m RETURN 1 IN m.a, 3 IN m.a", {{"true", "false"}});
}

TEST_F(AnyMapValueMembershipTest, looksUpAMapValueInAListHeldByAMap) {
    expectRows("UNWIND [{a: [1, 2], k: 2, n: 'p'}, {a: [3], k: 2, n: 'q'}] AS m RETURN m.n, m.k IN m.a",
               {{"p", "true"}, {"q", "false"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
