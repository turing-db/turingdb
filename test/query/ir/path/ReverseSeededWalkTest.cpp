#include <gtest/gtest.h>

#include <algorithm>
#include <set>
#include <string>
#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

// A pattern whose far end is already bound is explored from that end, because seeding the
// walk there is cheaper. Which end it starts from is the engine's choice; the edges it
// reports are the pattern's, so they read from the pattern's start whichever way it walked.
// The exploration is told which way it ran, so its lists and its path read back the way
// the pattern spells them whichever end seeded the walk.
class ReverseSeededWalkTest : public CallV3Test {
protected:
    void tripleSet(const std::string& query, std::set<StringRowSink::Row>& triples) {
        StringRowSink sink;
        runQuery(query, sink);

        triples.clear();
        for (const StringRowSink::Row& row : sink.getRows()) {
            triples.insert(row);
        }
    }
};

TEST_F(ReverseSeededWalkTest, bindingTheEndFirstDoesNotReverseTheEdges) {
    std::set<StringRowSink::Row> unseeded;
    tripleSet("MATCH (c:Person)-[e:KNOWS_WELL*1..2]->(b:Person) RETURN c.name, e, b.name", unseeded);
    ASSERT_FALSE(unseeded.empty());

    std::set<StringRowSink::Row> seeded;
    tripleSet("MATCH (a:Person)-[r:KNOWS_WELL]->(b) "
              "MATCH (c:Person)-[e:KNOWS_WELL*1..2]->(b:Person) RETURN c.name, e, b.name",
              seeded);
    ASSERT_FALSE(seeded.empty());

    std::vector<StringRowSink::Row> unexpected;
    std::set_difference(seeded.begin(), seeded.end(),
                        unseeded.begin(), unseeded.end(),
                        std::back_inserter(unexpected));

    EXPECT_TRUE(unexpected.empty()) << "rows the constrained query invented: " << unexpected.size()
                                    << " (first: " << (unexpected.empty() ? "" : unexpected.front().at(1)) << ")";
}

TEST_F(ReverseSeededWalkTest, bindingTheEndFirstDoesNotReverseTheNamedPath) {
    std::set<StringRowSink::Row> unseeded;
    tripleSet("MATCH p = (c:Person)-[e:KNOWS_WELL*1..2]->(b:Person) RETURN c.name, p, b.name", unseeded);
    ASSERT_FALSE(unseeded.empty());

    std::set<StringRowSink::Row> seeded;
    tripleSet("MATCH (a:Person)-[r:KNOWS_WELL]->(b) "
              "MATCH p = (c:Person)-[e:KNOWS_WELL*1..2]->(b:Person) RETURN c.name, p, b.name",
              seeded);
    ASSERT_FALSE(seeded.empty());

    std::vector<StringRowSink::Row> unexpected;
    std::set_difference(seeded.begin(), seeded.end(),
                        unseeded.begin(), unseeded.end(),
                        std::back_inserter(unexpected));

    EXPECT_TRUE(unexpected.empty()) << "rows the constrained query invented: " << unexpected.size();
}

TEST_F(ReverseSeededWalkTest, bindingTheEndFirstDoesNotSwapTheGroupLists) {
    std::set<StringRowSink::Row> unseeded;
    tripleSet("MATCH (c:Person)((x)-[e:KNOWS_WELL]->(y)){1,2}(b:Person) RETURN c.name, x, y", unseeded);
    ASSERT_FALSE(unseeded.empty());

    std::set<StringRowSink::Row> seeded;
    tripleSet("MATCH (a:Person)-[r:KNOWS_WELL]->(b) "
              "MATCH (c:Person)((x)-[e:KNOWS_WELL]->(y)){1,2}(b:Person) RETURN c.name, x, y",
              seeded);
    ASSERT_FALSE(seeded.empty());

    std::vector<StringRowSink::Row> unexpected;
    std::set_difference(seeded.begin(), seeded.end(),
                        unseeded.begin(), unseeded.end(),
                        std::back_inserter(unexpected));

    EXPECT_TRUE(unexpected.empty()) << "rows the constrained query invented: " << unexpected.size();
}

TEST_F(ReverseSeededWalkTest, theFirstGroupNodeIsTheNodeThePatternOpensOn) {
    StringRowSink sink;
    runQuery("MATCH (a:Person)-[r:KNOWS_WELL]->(b) "
             "MATCH (c:Person)((x)-[e:KNOWS_WELL]->(y)){1}(b:Person) RETURN c, x, y, b",
             sink);

    ASSERT_FALSE(sink.getRows().empty());

    // One repetition, so each list holds one node: the source list is the node the pattern
    // opens on and the end list the node it closes on
    for (const StringRowSink::Row& row : sink.getRows()) {
        ASSERT_EQ(row.size(), 4u);
        EXPECT_EQ(row[1], row[0]) << "the source list must open on c";
        EXPECT_EQ(row[2], row[3]) << "the end list must close on b";
    }
}
