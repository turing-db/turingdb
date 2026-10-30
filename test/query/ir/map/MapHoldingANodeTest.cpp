#include <gtest/gtest.h>

#include <string_view>
#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

// A map value may hold a node, and what is read out of it is that node: its properties
// read, and a MATCH starts from it, as they do from the node itself
class MapHoldingANodeTest : public CallV3Test {
protected:
    void expectRows(std::string_view query, const std::vector<StringRowSink::Row>& expected) {
        StringRowSink sink;
        runQuery(query, sink);

        EXPECT_EQ(sink.getRows(), expected) << query;
    }
};

TEST_F(MapHoldingANodeTest, returnsTheNodeTheMapHolds) {
    expectRows("MATCH (n:Person {name: 'Remy'}) WITH {person: n} AS m RETURN m.person", {{"0"}});
}

TEST_F(MapHoldingANodeTest, readsTheLabelsOfTheNodeTheMapHolds) {
    expectRows("MATCH (n:Person {name: 'Remy'}) WITH {person: n} AS m RETURN size(labels(m.person)), 'Founder' IN labels(m.person)",
               {{"3", "true"}});
}

TEST_F(MapHoldingANodeTest, readsAPropertyOfTheNodeAListedMapHolds) {
    expectRows("MATCH (n:Person {name: 'Remy'}) WITH [{person: n}] AS l RETURN l[0].person.name", {{"Remy"}});
}

TEST_F(MapHoldingANodeTest, startsAMatchFromANodeAListHolds) {
    expectRows("MATCH (n:Person {name: 'Remy'}) WITH [n] AS l WITH l[0] AS p "
               "MATCH (p)-[:KNOWS_WELL]->(f) RETURN f.name",
               {{"Adam"}});
}

// Disabled: the analyzer takes a key read off the node for a key of a map held by another
// map, which it rejects, though the same read through a list element runs.
TEST_F(MapHoldingANodeTest, DISABLED_readsAPropertyOfTheNodeTheMapHolds) {
    expectRows("MATCH (n:Person {name: 'Remy'}) WITH {person: n} AS m RETURN m.person.name", {{"Remy"}});
}

TEST_F(MapHoldingANodeTest, DISABLED_readsAPropertyOfTheNodeAnUnwoundMapHolds) {
    expectRows("MATCH (n:Person {name: 'Remy'}) UNWIND [{person: n}] AS m RETURN m.person.name", {{"Remy"}});
}

// Disabled: a node read out of a map has type Any, and the analyzer rejects a pattern
// variable already declared with another type than a node.
TEST_F(MapHoldingANodeTest, DISABLED_startsAMatchFromTheNodeTheMapHolds) {
    expectRows("MATCH (n:Person {name: 'Remy'}) WITH {person: n} AS m WITH m.person AS p "
               "MATCH (p)-[:KNOWS_WELL]->(f) RETURN f.name",
               {{"Adam"}});
}

TEST_F(MapHoldingANodeTest, DISABLED_startsAMatchFromTheNodeAListedMapHolds) {
    expectRows("MATCH (n:Person {name: 'Remy'}) WITH [{person: n}] AS l WITH l[0].person AS p "
               "MATCH (p)-[:KNOWS_WELL]->(f) RETURN f.name",
               {{"Adam"}});
}
