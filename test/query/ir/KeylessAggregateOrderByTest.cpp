#include <gtest/gtest.h>

#include <string_view>
#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

namespace {

using Rows = std::vector<StringRowSink::Row>;

}

// An ORDER BY over a projection with no grouping key reads the aggregates of that one
// group: the ones the projection returns, and the ones it computes for the key alone
class KeylessAggregateOrderByTest : public CallV3Test {
protected:
    void expectRows(std::string_view query, const Rows& expected) {
        StringRowSink sink;
        runQuery(query, sink);

        EXPECT_EQ(sink.getRows(), expected) << query;
    }
};

TEST_F(KeylessAggregateOrderByTest, ordersByAnExpressionOverTheReturnedCollect) {
    expectRows("MATCH (n:Person) RETURN collect(n.name) ORDER BY size(collect(n.name))",
               {{"Remy, Adam, Maxime, Luc, Martina, Suhas, Cyrus, Doruk"}});
}

TEST_F(KeylessAggregateOrderByTest, ordersByACollectTheProjectionDoesNotReturn) {
    expectRows("MATCH (n:Person) RETURN count(n) ORDER BY size(collect(n.name))", {{"8"}});
}

TEST_F(KeylessAggregateOrderByTest, ordersByACollectBesideAnotherAggregate) {
    expectRows("MATCH (n:Person) RETURN collect(n.name), count(n) ORDER BY size(collect(n.name)) DESC",
               {{"Remy, Adam, Maxime, Luc, Martina, Suhas, Cyrus, Doruk", "8"}});
}

TEST_F(KeylessAggregateOrderByTest, ordersByTwoCollectsOfOneKey) {
    expectRows("MATCH (n:Person) RETURN size(collect(n.name)) ORDER BY size(collect(n.name)) + size(collect(n.age))",
               {{"8"}});
}

TEST_F(KeylessAggregateOrderByTest, ordersAWithByACollect) {
    expectRows("MATCH (n:Person) WITH collect(n.name) AS names ORDER BY size(collect(n.name)) RETURN size(names)",
               {{"8"}});
}

TEST_F(KeylessAggregateOrderByTest, cutsTheOneGroupOrderedByACollect) {
    expectRows("MATCH (n:Person) RETURN count(n) ORDER BY size(collect(n.name)) SKIP 1", {});
}
