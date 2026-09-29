#include <gtest/gtest.h>

#include <string_view>

#include "CallV3Test.h"
#include "IRTestRows.h"

using namespace turing::test;

// An element of nodes(p) or relationships(p) read by its index, null past the end of the
// path or where an OPTIONAL MATCH bound no path
class PathElementIndexTest : public CallV3Test {
protected:
    void expectRows(std::string_view query, const Rows& expected) {
        RowSink sink;
        runQuery(query, sink);

        Rows rows;
        sink.sortedRows(rows);

        EXPECT_EQ(rows, expected) << query;
    }
};

TEST_F(PathElementIndexTest, readsTheElementsOfAFixedHop) {
    expectRows("MATCH p = (n:Person)-[e]->(m:Person) RETURN nodes(p)[0], nodes(p)[1], relationships(p)[0], nodes(p)[2]",
               {{"0", "1", "0", "null"}, {"1", "0", "4", "null"}});
}

TEST_F(PathElementIndexTest, readsTheElementsOfAWalk) {
    expectRows("MATCH p = (n:Person)-[e]->+(m:Person) WHERE length(p) = 2 RETURN nodes(p)[1], relationships(p)[1]",
               {{"0", "0"}, {"1", "4"}, {"6", "7"}});
}

TEST_F(PathElementIndexTest, readsTheNodesCarriedThroughAWith) {
    expectRows("MATCH p = (n:Person)-[e]->(m:Person) WITH nodes(p) AS ns RETURN ns[1]", {{"0"}, {"1"}});
}

TEST_F(PathElementIndexTest, readsNullWhereAnOptionalMatchMissed) {
    expectRows("MATCH (n:Person) OPTIONAL MATCH p = (n)-[e]->(m:Person) RETURN n.name, nodes(p)[0]", {
        {"Adam", "1"}, {"Cyrus", "null"}, {"Doruk", "null"}, {"Luc", "null"},
        {"Martina", "null"}, {"Maxime", "null"}, {"Remy", "0"}, {"Suhas", "null"},
    });
}
