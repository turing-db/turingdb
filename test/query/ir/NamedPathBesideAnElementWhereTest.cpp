#include <gtest/gtest.h>

#include <algorithm>
#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

namespace {

using Rows = std::vector<StringRowSink::Row>;

Rows sorted(Rows rows) {
    std::sort(rows.begin(), rows.end());
    return rows;
}

}

// A MATCH naming its path and cutting its rows with a WHERE that runs over the elements of
// a list: both KNOWS_WELL edges between Remy and Adam last 20, so both rows survive
class NamedPathBesideAnElementWhereTest : public CallV3Test {
protected:
    void expectRows(std::string_view query, const Rows& expected) {
        StringRowSink sink;
        runQuery(query, sink);

        Rows rows;
        sink.sortedRows(rows);

        EXPECT_EQ(rows, sorted(expected)) << query;
    }
};

TEST_F(NamedPathBesideAnElementWhereTest, readsTheLengthAfterAListPredicate) {
    expectRows("MATCH p = (n:Person)-[e]->(m:Person) WHERE any(r IN [e] WHERE r.duration > 10) RETURN n.name, length(p)",
               {{"Remy", "1"}, {"Adam", "1"}});
}

TEST_F(NamedPathBesideAnElementWhereTest, readsTheNodesAfterAListPredicate) {
    expectRows("MATCH p = (n:Person)-[e]->(m:Person) WHERE all(r IN [e] WHERE r.duration = 20) RETURN nodes(p)",
               {{"0, 1"}, {"1, 0"}});
}

TEST_F(NamedPathBesideAnElementWhereTest, returnsThePathAfterAListPredicate) {
    expectRows("MATCH p = (n:Person)-[e]->(m:Person) WHERE none(r IN [e] WHERE r.duration > 100) RETURN p",
               {{"(0), [0], (1)"}, {"(1), [4], (0)"}});
}

TEST_F(NamedPathBesideAnElementWhereTest, readsTheLengthAfterAListComprehension) {
    expectRows("MATCH p = (n:Person)-[e]->(m:Person) WHERE size([r IN [e] WHERE r.duration = 20]) = 1 RETURN n.name, length(p)",
               {{"Remy", "1"}, {"Adam", "1"}});
}
