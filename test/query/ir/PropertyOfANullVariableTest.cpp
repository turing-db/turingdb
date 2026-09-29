#include <gtest/gtest.h>

#include <string_view>

#include "CallV3Test.h"
#include "IRTestRows.h"

using namespace turing::test;

// A variable bound to null has no entity to read from, so its property is null
class PropertyOfANullVariableTest : public CallV3Test {
protected:
    void expectRows(std::string_view query, const Rows& expected) {
        RowSink sink;
        runQuery(query, sink);

        Rows rows;
        sink.sortedRows(rows);

        EXPECT_EQ(rows, expected) << query;
    }
};

TEST_F(PropertyOfANullVariableTest, readsNull) {
    expectRows("WITH null AS x RETURN x.name, x.name IS NULL", {{"null", "true"}});
}

TEST_F(PropertyOfANullVariableTest, readsNullBesideARow) {
    expectRows("MATCH (n:Person {name: 'Remy'}) WITH n, null AS x RETURN n.name, x.name", {{"Remy", "null"}});
}
