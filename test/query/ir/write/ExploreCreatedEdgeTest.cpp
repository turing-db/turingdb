#include <gtest/gtest.h>

#include <algorithm>
#include <ranges>
#include <string>
#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace db;
using namespace turing::test;

namespace {

using Rows = std::vector<StringRowSink::Row>;

Rows sorted(Rows rows) {
    std::sort(rows.begin(), rows.end());
    return rows;
}

}

// The variable-length sibling of MatchCreatedEdgeTest: a walk below the cut reads the nodes
// and edges the CREATE above it wrote out of the write buffer, as a single hop does.
class ExploreCreatedEdgeTest : public CallV3Test {
};

TEST_F(ExploreCreatedEdgeTest, walksOneCreatedEdge) {
    StringRowSink sink;
    runWrite("CREATE (a:Person {name: 'Ana'})-[:KNOWS_WELL]->(b:Person {name: 'Bo'}) "
             "WITH a MATCH (a)-[e]->+(m) RETURN m.name",
             sink);

    const Rows expected {{"Bo"}};
    EXPECT_EQ(sink.getRows(), expected);
}

// Two created edges in a row: the walk has to leave a pending node by a pending edge, which
// is the step a single hop never takes.
TEST_F(ExploreCreatedEdgeTest, walksTwoCreatedEdgesInARow) {
    StringRowSink sink;
    runWrite("CREATE (a:Person {name: 'Ana'})-[:KNOWS_WELL]->(b:Person {name: 'Bo'})"
             "-[:KNOWS_WELL]->(c:Person {name: 'Cy'}) "
             "WITH a MATCH (a)-[e]->+(m) RETURN m.name",
             sink);

    Rows rows;
    sink.sortedRows(rows);
    EXPECT_EQ(rows, sorted({{"Bo"}, {"Cy"}}));
}

// The hop bound counts the pending edges the same way it counts committed ones.
TEST_F(ExploreCreatedEdgeTest, boundsTheHopsOverCreatedEdges) {
    StringRowSink sink;
    runWrite("CREATE (a:Person {name: 'Ana'})-[:KNOWS_WELL]->(b:Person {name: 'Bo'})"
             "-[:KNOWS_WELL]->(c:Person {name: 'Cy'}) "
             "WITH a MATCH (a)-[e]->{1,1}(m) RETURN m.name",
             sink);

    const Rows expected {{"Bo"}};
    EXPECT_EQ(sink.getRows(), expected);
}

// A walk that starts on a committed node and leaves it by an edge this change wrote.
TEST_F(ExploreCreatedEdgeTest, walksOffACommittedNode) {
    StringRowSink sink;
    runWrite("MATCH (p:Person {name: 'Remy'}) "
             "CREATE (p)-[:MENTORS]->(b:Person {name: 'Bo'})-[:MENTORS]->(c:Person {name: 'Cy'}) "
             "WITH p MATCH (p)-[e:MENTORS]->+(m) RETURN m.name",
             sink);

    Rows rows;
    sink.sortedRows(rows);
    EXPECT_EQ(rows, sorted({{"Bo"}, {"Cy"}}));
}

// A pending edge lands back on a committed node, so the walk crosses from the buffer into
// the graph and keeps going on the commit's own edges.
TEST_F(ExploreCreatedEdgeTest, walksFromACreatedEdgeOntoCommittedOnes) {
    StringRowSink sink;
    runWrite("MATCH (r:Person {name: 'Remy'}) "
             "CREATE (a:Person {name: 'Ana'})-[:KNOWS_WELL]->(r) "
             "WITH a MATCH (a)-[e:KNOWS_WELL]->+(m:Person) RETURN m.name",
             sink);

    Rows rows;
    sink.sortedRows(rows);
    EXPECT_EQ(rows, sorted({{"Remy"}, {"Adam"}, {"Remy"}}));
}

// The end label of a pending node is read off the write buffer, since the graph holds no
// label set for it until the commit.
TEST_F(ExploreCreatedEdgeTest, filtersTheEndLabelOfACreatedNode) {
    StringRowSink sink;
    runWrite("CREATE (a:Person {name: 'Ana'})-[:INTERESTED_IN]->(b:Interest {name: 'Chess'}) "
             "WITH a MATCH (a)-[e]->+(m:Interest) RETURN m.name",
             sink);

    const Rows expected {{"Chess"}};
    EXPECT_EQ(sink.getRows(), expected);
}

// An end label no pending node carries turns the created edge away.
TEST_F(ExploreCreatedEdgeTest, filtersOutACreatedNodeWithoutTheEndLabel) {
    StringRowSink sink;
    runWrite("CREATE (a:Person {name: 'Ana'})-[:KNOWS_WELL]->(b:Person {name: 'Bo'}) "
             "WITH a MATCH (a)-[e]->+(m:Interest) RETURN m.name",
             sink);

    EXPECT_TRUE(sink.getRows().empty());
}

// The edge type filter reads the type off the pending edge.
TEST_F(ExploreCreatedEdgeTest, walksNoCreatedEdgeOfAnotherType) {
    StringRowSink sink;
    runWrite("CREATE (a:Person {name: 'Ana'})-[:KNOWS_WELL]->(b:Person {name: 'Bo'}) "
             "WITH a MATCH (a)-[e:INTERESTED_IN]->+(m) RETURN m.name",
             sink);

    EXPECT_TRUE(sink.getRows().empty());
}

TEST_F(ExploreCreatedEdgeTest, walksACreatedEdgeBackwards) {
    StringRowSink sink;
    runWrite("CREATE (a:Person {name: 'Ana'})-[:KNOWS_WELL]->(b:Person {name: 'Bo'}) "
             "WITH b MATCH (b)<-[e]-+(m) RETURN m.name",
             sink);

    const Rows expected {{"Ana"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(ExploreCreatedEdgeTest, walksACreatedEdgeInEitherDirection) {
    StringRowSink sink;
    runWrite("CREATE (a:Person {name: 'Ana'})-[:KNOWS_WELL]->(b:Person {name: 'Bo'})"
             "-[:KNOWS_WELL]->(c:Person {name: 'Cy'}) "
             "WITH b MATCH (b)-[e]-+(m) RETURN m.name",
             sink);

    Rows rows;
    sink.sortedRows(rows);
    EXPECT_EQ(rows, sorted({{"Ana"}, {"Cy"}}));
}

// A zero-hop walk binds the seed itself, pending or not, and the created edge adds the rest.
TEST_F(ExploreCreatedEdgeTest, bindsACreatedSeedAtZeroHops) {
    StringRowSink sink;
    runWrite("CREATE (a:Person {name: 'Ana'})-[:KNOWS_WELL]->(b:Person {name: 'Bo'}) "
             "WITH a MATCH (a)-[e]->*(m) RETURN m.name",
             sink);

    Rows rows;
    sink.sortedRows(rows);
    EXPECT_EQ(rows, sorted({{"Ana"}, {"Bo"}}));
}

// The distinct mode is a breadth-first search of its own, so it reads the buffer through the
// same adjacency the depth-first walk does.
TEST_F(ExploreCreatedEdgeTest, searchesDistinctEndsOverCreatedEdges) {
    StringRowSink sink;
    runWrite("CREATE (a:Person {name: 'Ana'})-[:KNOWS_WELL]->(b:Person {name: 'Bo'})"
             "-[:KNOWS_WELL]->(c:Person {name: 'Cy'}) "
             "WITH a MATCH (a)-[e]->+(m) RETURN DISTINCT m.name",
             sink);

    Rows rows;
    sink.sortedRows(rows);
    EXPECT_EQ(rows, sorted({{"Bo"}, {"Cy"}}));
}

// A created node with no created edge reaches nothing, and at a minimum of zero it is still
// its own end: the distinct search has to hold a seed the graph has no ID for yet.
TEST_F(ExploreCreatedEdgeTest, searchesDistinctEndsFromACreatedSeedWithoutEdges) {
    StringRowSink sink;
    runWrite("CREATE (n:Person {name: 'Zed'}) WITH n MATCH (n)-[e]->*(m) RETURN DISTINCT m.name", sink);

    const Rows expected {{"Zed"}};
    EXPECT_EQ(sink.getRows(), expected);
}

// A walk driven by a hop runs once per chunk the hop emits, and every run reads the one
// index the program shares.
TEST_F(ExploreCreatedEdgeTest, walksNestedInAHopOverCreatedEdges) {
    StringRowSink sink;
    runWrite("CREATE (a:Person {name: 'Ana'})-[:MENTORS]->(b:Person {name: 'Bo'})"
             "-[:MENTORS]->(c:Person {name: 'Cy'}) "
             "WITH a MATCH (a)-[:MENTORS]->(x) MATCH (x)-[e:MENTORS]->+(m) RETURN m.name",
             sink);

    const Rows expected {{"Cy"}};
    EXPECT_EQ(sink.getRows(), expected);
}

// The walk runs once per row of the relation driving it, and the edges it reads are indexed
// once for the program: a run that indexed them again would walk each of them twice.
TEST_F(ExploreCreatedEdgeTest, walksACreatedEdgeOnceInEveryRunOfTheWalk) {
    StringRowSink sink;
    runWrite("CREATE (a:Person {name: 'Ana'})-[:MENTORS]->(b:Person {name: 'Bo'}) "
             "WITH a MATCH (n:Person) MATCH (a)-[e:MENTORS]->+(m) RETURN n.name, m.name",
             sink);

    Rows rows;
    sink.sortedRows(rows);

    // The eight people of the fixture and the two the CREATE wrote, each reaching Bo once
    ASSERT_EQ(rows.size(), 10u);

    const auto reachesBoOnce = [](const StringRowSink::Row& row) {
        return row[1] == "Bo";
    };
    EXPECT_TRUE(std::ranges::all_of(rows, reachesBoOnce));
    EXPECT_EQ(std::ranges::adjacent_find(rows), rows.end());
}

// size(e) counts the pending edges of a walk the way it counts committed ones.
TEST_F(ExploreCreatedEdgeTest, countsTheHopsOverCreatedEdges) {
    StringRowSink sink;
    runWrite("CREATE (a:Person {name: 'Ana'})-[:KNOWS_WELL]->(b:Person {name: 'Bo'})"
             "-[:KNOWS_WELL]->(c:Person {name: 'Cy'}) "
             "WITH a MATCH (a)-[e]->+(m) RETURN m.name, size(e)",
             sink);

    Rows rows;
    sink.sortedRows(rows);
    EXPECT_EQ(rows, sorted({{"Bo", "1"}, {"Cy", "2"}}));
}

// The named path of a walk over pending edges holds them in order, as it does committed
// ones. The IDs the change will commit as depend on the fixture's size, so the rows are
// read by the hops they hold rather than by those IDs.
TEST_F(ExploreCreatedEdgeTest, namesAPathOverCreatedEdges) {
    StringRowSink sink;
    runWrite("CREATE (a:Person {name: 'Ana'})-[:KNOWS_WELL]->(b:Person {name: 'Bo'})"
             "-[:KNOWS_WELL]->(c:Person {name: 'Cy'}) "
             "WITH a MATCH p = (a)-[e]->+(m) RETURN m.name, p",
             sink);

    Rows rows;
    sink.sortedRows(rows);
    ASSERT_EQ(rows.size(), 2u);

    const auto hopsOf = [](const std::string& path) {
        return std::ranges::count(path, '[');
    };
    EXPECT_EQ(rows[0][0], "Bo");
    EXPECT_EQ(hopsOf(rows[0][1]), 1);
    EXPECT_EQ(rows[1][0], "Cy");
    EXPECT_EQ(hopsOf(rows[1][1]), 2);
}
