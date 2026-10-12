#include <gtest/gtest.h>

#include <string>
#include <string_view>
#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

namespace {

using Rows = std::vector<StringRowSink::Row>;

}

// A walk to an end set that follows a CREATE in the same query sees what the CREATE wrote:
// the edges and nodes it adds lead to ends as the committed ones do
class ExploreEndSetAfterWritesCypherTest : public CallV3Test {
protected:
    Rows runWriting(std::string_view query) {
        StringRowSink sink;
        runWrite(query, sink);

        Rows rows;
        sink.sortedRows(rows);
        return rows;
    }

    std::string explainAfterTheFusion(std::string_view query) {
        StringRowSink sink;
        runQuery(std::string("EXPLAIN (around fuse_explore_end_set) ") + std::string(query), sink);

        for (const StringRowSink::Row& row : sink.getRows()) {
            if (row.front() == "after fuse_explore_end_set") {
                return row.back();
            }
        }

        return {};
    }
};

TEST_F(ExploreEndSetAfterWritesCypherTest, followsACreatedEdgeBackFromThePinnedNode) {
    const std::string_view query = "MATCH (r {name:'Remy'}), (c {name:'Cooking'}) "
                                   "CREATE (r)-[:INTERESTED_IN]->(c) "
                                   "WITH r "
                                   "MATCH (a {name:'Remy'})-[:KNOWS_WELL*1..3]->(b)-[:INTERESTED_IN]->(i {name:'Cooking'}) "
                                   "RETURN a, b, i";

    EXPECT_EQ(runWriting(query), (Rows {{"0", "0", "5"}, {"0", "1", "5"}}));
}

TEST_F(ExploreEndSetAfterWritesCypherTest, endsOnACreatedNodePinnedByItsID) {
    const std::string_view query = "CREATE (x:Person {name:'New'}) "
                                   "WITH x "
                                   "MATCH (r {name:'Remy'}) "
                                   "CREATE (r)-[:KNOWS_WELL]->(x) "
                                   "WITH x "
                                   "MATCH (a {name:'Remy'})-[:KNOWS_WELL*1..1]->(b) WHERE id(b) = 18 "
                                   "RETURN a, b, id(x)";

    EXPECT_EQ(runWriting(query), (Rows {{"0", "18", "18"}}));
}

TEST_F(ExploreEndSetAfterWritesCypherTest, endsOnTheSetWhenTheWriteFollowsTheWalk) {
    const std::string_view query = "MATCH (a {name:'Remy'})-[:KNOWS_WELL*1..3]->(b {name:'Adam'}) "
                                   "CREATE (b)-[:KNOWS_WELL]->(a) "
                                   "RETURN a, b";

    const std::string fused = explainAfterTheFusion(query);
    EXPECT_NE(fused.find("end_nodes"), std::string::npos) << fused;

    EXPECT_EQ(runWriting(query), (Rows {{"0", "1"}}));
}
