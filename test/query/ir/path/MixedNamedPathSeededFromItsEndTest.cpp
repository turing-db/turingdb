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

// A path of a fixed hop then a walk, whose walk is seeded from the end the first MATCH
// bound. Every walk into Adam (1) ends on Remy -> Adam (0); from Adam the fixed hop is
// Adam -> Remy (4), and the walks from Remy are [0] and [1, 7, 0] through Ghosts (6)
class MixedNamedPathSeededFromItsEndTest : public CallV3Test {
protected:
    void expectRows(std::string_view query, const Rows& expected) {
        StringRowSink sink;
        runQuery(query, sink);

        Rows rows;
        sink.sortedRows(rows);

        EXPECT_EQ(rows, sorted(expected)) << query;
    }
};

TEST_F(MixedNamedPathSeededFromItsEndTest, readsTheNodesInWrittenOrder) {
    expectRows("MATCH (m:Person {name: 'Adam'}) MATCH p = (n:Person)-[x]->(k)-[e]->+(m) WHERE n.name = 'Adam' RETURN nodes(p)",
               {{"1, 0, 1"}, {"1, 0, 6, 0, 1"}});
}

TEST_F(MixedNamedPathSeededFromItsEndTest, readsTheRelationshipsInWrittenOrder) {
    expectRows("MATCH (m:Person {name: 'Adam'}) MATCH p = (n:Person)-[x]->(k)-[e]->+(m) WHERE n.name = 'Adam' RETURN relationships(p)",
               {{"4, 0"}, {"4, 1, 7, 0"}});
}

TEST_F(MixedNamedPathSeededFromItsEndTest, returnsThePathInWrittenOrder) {
    expectRows("MATCH (m:Person {name: 'Adam'}) MATCH p = (n:Person)-[x]->(k)-[e]->+(m) WHERE n.name = 'Adam' RETURN p",
               {{"(1), [4], (0), [0], (1)"}, {"(1), [4], (0), [1], (6), [7], (0), [0], (1)"}});
}

TEST_F(MixedNamedPathSeededFromItsEndTest, readsAWalkOfNoHopsAsTheNodeItStayedOn) {
    expectRows("MATCH (m:Person {name: 'Adam'}) MATCH p = (n:Person)-[x]->(k)-[e]->*(m) WHERE n.name = 'Remy' AND length(p) = 1 RETURN nodes(p), relationships(p)",
               {{"0, 1", "0"}});
}
