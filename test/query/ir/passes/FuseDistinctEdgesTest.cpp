#include <gtest/gtest.h>

#include <string>
#include <string_view>
#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace db;
using namespace turing::test;

using Rows = std::vector<StringRowSink::Row>;

// The edge-distinctness check a hop or a walk is followed by folds into the op itself as
// distinct_from, so the writer leaves the excluded edges out and no filter runs after it;
// the rows are the same either way.
class FuseDistinctEdgesTest : public CallV3Test {
protected:
    void dump(std::string_view query, std::string& program) {
        StringRowSink sink;
        runQuery(std::string("EXPLAIN (db) ") + std::string(query), sink);

        for (const StringRowSink::Row& row : sink.getRows()) {
            if (row.front() == "db") {
                program = row.back();
            }
        }
    }

    void expectFolded(std::string_view query, std::string_view hopOp, std::string_view count) {
        std::string program;
        dump(query, program);

        EXPECT_TRUE(contains(program, std::string(hopOp) + "(")) << program;
        EXPECT_TRUE(contains(program, "distinct_from [")) << program;
        EXPECT_FALSE(contains(program, "db.check_edge_distinct")) << program;

        StringRowSink sink;
        runQuery(query, sink);
        EXPECT_EQ(sink.getRows(), (Rows {{std::string(count)}})) << query;
    }

    static bool contains(std::string_view text, std::string_view part) {
        return text.find(part) != std::string_view::npos;
    }
};

TEST_F(FuseDistinctEdgesTest, foldsIntoAnUndirectedHop) {
    expectFolded("MATCH (a)-[e1]->(b)-[e2]-(c) RETURN count(*)", "db.get_edges", "26");
}

TEST_F(FuseDistinctEdgesTest, foldsIntoAnInHop) {
    expectFolded("MATCH (a)-[e1]->(b)<-[e2]-(c) RETURN count(*)", "db.get_in_edges", "14");
}

TEST_F(FuseDistinctEdgesTest, foldsIntoATypedHop) {
    expectFolded("MATCH (a)-[:KNOWS_WELL]->(b)<-[:KNOWS_WELL]-(c) RETURN count(*)", "db.get_in_edges_by_type", "2");
}

TEST_F(FuseDistinctEdgesTest, foldsIntoALabelledHop) {
    expectFolded("MATCH (a)-->(b)<--(c:Person) RETURN count(*)", "db.get_in_edges_by_label", "13");
}

TEST_F(FuseDistinctEdgesTest, foldsIntoATypedAndLabelledHop) {
    expectFolded("MATCH (a)-[e1:KNOWS_WELL]->(b)<-[e2:KNOWS_WELL]-(c:Founder) RETURN count(*)", "db.get_in_edges_by_type_and_label", "1");
}

TEST_F(FuseDistinctEdgesTest, foldsIntoAWalkAfterAHop) {
    expectFolded("MATCH (a)-[e1]->(b)-[e*1..2]->(c) RETURN count(*)", "db.explore_paths", "24");
}

TEST_F(FuseDistinctEdgesTest, foldsIntoAHopAfterAWalk) {
    expectFolded("MATCH (a)-[e*1..2]->(b)-[f]->(c) RETURN count(*)", "db.get_out_edges", "24");
}

TEST_F(FuseDistinctEdgesTest, foldsAThreeHopChainTwice) {
    std::string program;
    dump("MATCH (a)-[e1]-(b)-[e2]-(c)-[e3]-(d) RETURN count(*)", program);

    EXPECT_FALSE(contains(program, "db.check_edge_distinct")) << program;
    EXPECT_TRUE(contains(program, "distinct_from [0]") || contains(program, "distinct_from [1]")) << program;
    EXPECT_TRUE(contains(program, "distinct_from [0, 1]") || contains(program, "distinct_from [1, 2]") || contains(program, "distinct_from [0, 2]")) << program;

    StringRowSink sink;
    runQuery("MATCH (a)-[e1]-(b)-[e2]-(c)-[e3]-(d) RETURN count(*)", sink);
    EXPECT_EQ(sink.getRows(), (Rows {{"106"}}));
}

// Two patterns meeting in a cross product bind their edges before any hop can leave the
// other's out, so the check stays a filter over the product
TEST_F(FuseDistinctEdgesTest, keepsTheCheckOverACrossProduct) {
    std::string program;
    dump("MATCH (a)-[e1]->(b), (c)-[e2]->(d) RETURN count(*)", program);

    EXPECT_TRUE(contains(program, "db.check_edge_distinct")) << program;
    EXPECT_FALSE(contains(program, "distinct_from")) << program;

    StringRowSink sink;
    runQuery("MATCH (a)-[e1]->(b), (c)-[e2]->(d) RETURN count(*)", sink);
    EXPECT_EQ(sink.getRows(), (Rows {{"306"}}));
}
