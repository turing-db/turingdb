#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

namespace {

using Rows = std::vector<StringRowSink::Row>;

}

// simpledb's KNOWS_WELL edges are Remy (0) -> Adam (1) and Adam -> Remy, so a path from Remy
// sorts before the one from Adam.
class PathSortKeyTest : public CallV3Test {
protected:
    void expectRows(const std::string& query, const Rows& expected) {
        StringRowSink sink;
        runQuery(query, sink);

        EXPECT_EQ(sink.getRows(), expected) << query;
    }
};

TEST_F(PathSortKeyTest, ordersMatchedPaths) {
    expectRows("MATCH p = (a:Person)-[:KNOWS_WELL]->(b:Person) RETURN a.name ORDER BY p",
               {{"Remy"}, {"Adam"}});
    expectRows("MATCH p = (a:Person)-[:KNOWS_WELL]->(b:Person) RETURN a.name ORDER BY p DESC",
               {{"Adam"}, {"Remy"}});
}

TEST_F(PathSortKeyTest, ordersAShorterPathBeforeTheLongerOneItStarts) {
    expectRows("MATCH p = (a:Person {name: 'Remy'})-[:KNOWS_WELL*1..2]->(b) RETURN length(p) ORDER BY p",
               {{"1"}, {"2"}});
}
