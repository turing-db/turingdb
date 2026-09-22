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
// Both cases below fail today: the walk records its hops in the order it took them, and
// nothing turns them back around. Disabled until db.expand_path and db.make_path can be
// told the walk ran against the pattern - see REVIEW.md C7.
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

TEST_F(ReverseSeededWalkTest, DISABLED_bindingTheEndFirstDoesNotReverseTheEdges) {
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

TEST_F(ReverseSeededWalkTest, DISABLED_bindingTheEndFirstDoesNotReverseTheNamedPath) {
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
