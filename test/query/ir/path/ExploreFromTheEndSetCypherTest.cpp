#include <gtest/gtest.h>

#include <string_view>
#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

namespace {

using Rows = std::vector<StringRowSink::Row>;

}

// A walk from a hub to a set of sparse ends runs from the ends back to the hub: 30 leaves
// point at the hub and 3 parents at each leaf, and the drug targets two parents, so the walk
// from the hub sees all 90 parents where the walk from the drug's targets sees 2. The rows
// are the ones the walk from the hub emits, whichever end it ran from.
class ExploreFromTheEndSetCypherTest : public CallV3Test {
protected:
    void initialize() override {
        CallV3Test::initialize();

        runWrite("CREATE (:Hub {name: 'hub'}), (:Drug {name: 'drug'})");
        runWrite("MATCH (h:Hub) UNWIND range(0, 29) AS i CREATE (:Leaf {i: i})-[:R]->(h)");
        runWrite("MATCH (l:Leaf) UNWIND range(0, 2) AS j CREATE (:Parent {i: l.i, j: j})-[:R]->(l)");
        runWrite("MATCH (d:Drug), (p:Parent) WHERE p.j = 0 AND p.i < 2 CREATE (d)-[:T]->(p)");
    }

    Rows run(std::string_view query) {
        StringRowSink sink;
        runQuery(query, sink);

        Rows rows;
        sink.sortedRows(rows);
        return rows;
    }
};

TEST_F(ExploreFromTheEndSetCypherTest, emitsTheRowsOfTheWalkFromTheHub) {
    const std::string_view hubFirst = "MATCH (h:Hub {name: 'hub'})<-[:R]-{2,2}(t:Parent)<-[:T]-(d:Drug {name: 'drug'}) RETURN t.i, t.j";
    const std::string_view drugFirst = "MATCH (d:Drug {name: 'drug'})-[:T]->(t:Parent)-[:R]->{2,2}(h:Hub {name: 'hub'}) RETURN t.i, t.j";

    EXPECT_EQ(run(hubFirst), (Rows {{"0", "0"}, {"1", "0"}}));
    EXPECT_EQ(run(drugFirst), run(hubFirst));

    EXPECT_EQ(run("MATCH (h:Hub {name: 'hub'})<-[:R]-{1,3}(t)<-[:T]-(d:Drug {name: 'drug'}) RETURN count(*)"), (Rows {{"2"}}));
}

TEST_F(ExploreFromTheEndSetCypherTest, keepsEveryRowOfARepeatedSeed) {
    const std::string_view query = "UNWIND [1, 2, 2] AS k "
                                   "MATCH (h:Hub {name: 'hub'})<-[:R]-{2,2}(t:Parent)<-[:T]-(d:Drug {name: 'drug'}) "
                                   "RETURN k, t.i";

    EXPECT_EQ(run(query), (Rows {{"1", "0"}, {"1", "1"}, {"2", "0"}, {"2", "0"}, {"2", "1"}, {"2", "1"}}));
}

TEST_F(ExploreFromTheEndSetCypherTest, emitsEachEndOnceWhenOnlyDistinctEndsAreRead) {
    const std::string_view query = "MATCH (h:Hub {name: 'hub'})<-[:R]-{1,3}(t)<-[:T]-(d:Drug {name: 'drug'}) RETURN count(DISTINCT t)";

    EXPECT_EQ(run(query), (Rows {{"2"}}));
}

TEST_F(ExploreFromTheEndSetCypherTest, keepsTheWalkFromTheSeedsWhenThePathIsRead) {
    const std::string_view query = "MATCH p = (h:Hub {name: 'hub'})<-[:R]-{2,2}(t:Parent)<-[:T]-(d:Drug {name: 'drug'}) RETURN length(p), t.i";

    EXPECT_EQ(run(query), (Rows {{"3", "0"}, {"3", "1"}}));
}
