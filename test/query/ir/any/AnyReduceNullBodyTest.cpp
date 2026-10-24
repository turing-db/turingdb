#include <gtest/gtest.h>

#include <string_view>

#include "CallV3Test.h"
#include "IRTestRows.h"

using namespace turing::test;

class AnyReduceNullBodyTest : public CallV3Test {
protected:
    void expectRows(std::string_view query, const Rows& expected) {
        RowSink sink;
        runQuery(query, sink);

        Rows rows;
        sink.sortedRows(rows);

        EXPECT_EQ(rows, expected) << query;
    }
};

TEST_F(AnyReduceNullBodyTest, reducesACellAccumulatorToNull) {
    expectRows("RETURN reduce(acc = [1, 'a'][0], x IN [1, 2] | null)", {{"null"}});
}

TEST_F(AnyReduceNullBodyTest, reducesACellAccumulatorToNullOnEveryRow) {
    expectRows("UNWIND [[1, 'a'], ['b', 2]] AS xs RETURN reduce(acc = xs[0], x IN [1] | null)",
               {{"null"}, {"null"}});
}

TEST_F(AnyReduceNullBodyTest, reducesATypedAccumulatorToNull) {
    expectRows("RETURN reduce(acc = 1, x IN [1, 2] | null), reduce(acc = 'a', x IN [1] | null)", {{"null", "null"}});
}

TEST_F(AnyReduceNullBodyTest, reducesToNullOnlyWhereTheBodyYieldsIt) {
    expectRows("RETURN reduce(acc = 0, x IN [1, 2, 3] | CASE WHEN x = 2 THEN null ELSE x END)", {{"3"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
