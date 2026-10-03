#include <gtest/gtest.h>

#include <string>
#include <string_view>
#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace db;
using namespace turing::test;

using Rows = std::vector<StringRowSink::Row>;

// Relationship isomorphism over the edges simpledb lacks - a parallel edge, a self-loop -
// and over the graph a write changed: an edge deleted, edges a later commit added, and edges
// the reading query itself created and has not committed. Expected counts are an
// enumeration of simpledb with the same writes under openCypher's rule.
class EdgeUniquenessWritesTest : public CallV3Test {
protected:
    void expectRows(std::string_view query, const Rows& expected) {
        StringRowSink sink;
        runQuery(query, sink);

        EXPECT_EQ(sink.getRows(), expected) << query;
    }

    void expectWriteRows(std::string_view query, const Rows& expected) {
        StringRowSink sink;
        runWrite(query, sink);

        EXPECT_EQ(sink.getRows(), expected) << query;
    }

    void addParallelKnowsWell() {
        runWrite("MATCH (r {name: 'Remy'}), (a {name: 'Adam'}) CREATE (r)-[:KNOWS_WELL]->(a)");
    }
};

TEST_F(EdgeUniquenessWritesTest, pairsTwoParallelEdgesBothWays) {
    addParallelKnowsWell();

    expectRows("MATCH (a)-[e1]->(b)<-[e2]-(a) RETURN count(*)", {{"2"}});
    expectRows("MATCH (a)-[e1:KNOWS_WELL]->(b)<-[e2:KNOWS_WELL]-(a) RETURN count(*)", {{"2"}});
    expectRows("MATCH (a)-[e1]->(b), (a)-[e2]->(b) RETURN count(*)", {{"2"}});
}

TEST_F(EdgeUniquenessWritesTest, walksOnThroughAParallelEdge) {
    addParallelKnowsWell();

    expectRows("MATCH (a)-[e1]->(b)-[e2]-(c) RETURN count(*)", {{"33"}});
    expectRows("MATCH (a)-[e1]-(b)-[e2]-(c) RETURN count(*)", {{"84"}});
    expectRows("MATCH (a)-[e1]-(b)-[e2]-(c)-[e3]-(d) RETURN count(*)", {{"192"}});
}

TEST_F(EdgeUniquenessWritesTest, joinsAPatternWithTheWalkThatContainsItOverAParallelEdge) {
    addParallelKnowsWell();

    expectRows("MATCH (b)-->(c), (a)-->(b)-->(c) RETURN a.name, b.name, c.name ORDER BY a.name",
               {{"Adam", "Remy", "Adam"},
                {"Adam", "Remy", "Adam"},
                {"Ghosts", "Remy", "Adam"},
                {"Ghosts", "Remy", "Adam"}});
}

TEST_F(EdgeUniquenessWritesTest, joinsFourPatternsThroughAParallelEdge) {
    addParallelKnowsWell();

    expectRows("MATCH (a)-->(b),(c)-->(d)-->(e),(a)-->(f)-->(g),(c)-->(g) RETURN count(*)", {{"4"}});
    expectRows("MATCH (a)-->(b),(c)-->(d)-->(e),(a)-->(f)-->(g),(c)-->(g) "
               "RETURN a.name, b.name, c.name, d.name, e.name, f.name, g.name ORDER BY b.name",
               {{"Adam", "Bio", "Remy", "Ghosts", "Remy", "Remy", "Adam"},
                {"Adam", "Bio", "Remy", "Ghosts", "Remy", "Remy", "Adam"},
                {"Adam", "Cooking", "Remy", "Ghosts", "Remy", "Remy", "Adam"},
                {"Adam", "Cooking", "Remy", "Ghosts", "Remy", "Remy", "Adam"}});
}

TEST_F(EdgeUniquenessWritesTest, bindsThreeParallelEdgesOncePerOrder) {
    addParallelKnowsWell();
    addParallelKnowsWell();

    expectRows("MATCH (a)-[:KNOWS_WELL]->(b), (a)-[e1]->(b), (a)-[e2]->(b) RETURN count(*)", {{"6"}});
    expectRows("MATCH (a)-[e1]->(b), (a)-[e2]->(b), (a)-[e3]->(b) RETURN count(*)", {{"6"}});
}

TEST_F(EdgeUniquenessWritesTest, walksASelfLoopOnce) {
    runWrite("MATCH (r {name: 'Remy'}) CREATE (r)-[:KNOWS_WELL]->(r)");

    expectRows("MATCH (a)-[e1]->(b)-[e2]->(c) RETURN count(*)", {{"18"}});
    expectRows("MATCH (a)-[e1]->(b)<-[e2]-(c) RETURN count(*)", {{"18"}});
    expectRows("MATCH (a)-[e1]->(b)-[e2]->(c)-[e3]->(d) RETURN count(*)", {{"26"}});
    expectRows("MATCH (a)-[e1]->(a)-[e2]->(b) RETURN count(*)", {{"4"}});
    expectRows("MATCH (a)-[e1]->(b)-[e*1..2]->(c) RETURN count(*)", {{"44"}});
}

TEST_F(EdgeUniquenessWritesTest, skipsADeletedEdge) {
    runWrite("MATCH (r {name: 'Remy'})-[e:KNOWS_WELL]->(a {name: 'Adam'}) DELETE e");

    expectRows("MATCH (a)-[e1]->(b)-[e2]-(c) RETURN count(*)", {{"21"}});
    expectRows("MATCH (a)-[e1]-(b)-[e2]-(c) RETURN count(*)", {{"48"}});
    expectRows("MATCH (a)-[e1]-(b)-[e2]-(c)-[e3]-(d) RETURN count(*)", {{"56"}});
}

TEST_F(EdgeUniquenessWritesTest, excludesEdgesOfAnotherCommit) {
    runWrite("MATCH (r {name: 'Remy'}), (m {name: 'Maxime'}), (c {name: 'Computers'}) "
             "CREATE (r)-[:INTERESTED_IN]->(m), (c)-[:KNOWS_WELL]->(r)");

    expectRows("MATCH (a)-[e1]->(b)<-[e2]-(c) RETURN count(*)", {{"18"}});
    expectRows("MATCH (a)-[e1]-(b)-[e2]-(c) RETURN count(*)", {{"98"}});
    expectRows("MATCH (a)-[e1]-(b)-[e2]-(c)-[e3]-(d) RETURN count(*)", {{"212"}});
    expectRows("MATCH (a)-[e1]->(b)-[e2]->(c)-[e3]->(d) RETURN count(*)", {{"35"}});
    expectRows("MATCH (a)-[e1]->(b)-[e*1..2]->(c) RETURN count(*)", {{"58"}});
}

TEST_F(EdgeUniquenessWritesTest, excludesAnEdgeTheQueryCreated) {
    expectWriteRows("CREATE (a:Person {name: 'Ana'})-[:KNOWS_WELL]->(b:Person {name: 'Bo'}) "
                    "WITH a MATCH (a)-[e1]->(m)<-[e2]-(a) RETURN count(*)",
                    {{"0"}});
    expectWriteRows("CREATE (a:Person {name: 'Ana'})-[:KNOWS_WELL]->(b:Person {name: 'Bo'}), (b)-[:KNOWS_WELL]->(a) "
                    "WITH a MATCH (a)-[e1]-(m)-[e2]-(x) RETURN count(*)",
                    {{"2"}});
}

TEST_F(EdgeUniquenessWritesTest, pairsACreatedEdgeWithTheCommittedOneParallelToIt) {
    expectWriteRows("MATCH (r {name: 'Remy'}), (a {name: 'Adam'}) CREATE (r)-[:KNOWS_WELL]->(a) "
                    "WITH r MATCH (r)-[e1]->(b)<-[e2]-(r) RETURN count(*)",
                    {{"2"}});
}

TEST_F(EdgeUniquenessWritesTest, excludesCreatedAndCommittedEdgesFromEachOther) {
    expectWriteRows("MATCH (r {name: 'Remy'}) CREATE (r)-[:KNOWS_WELL]->(:Person {name: 'Bo'}) "
                    "WITH r MATCH (a)-[e1]-(b)-[e2]-(c) RETURN count(*)",
                    {{"76"}});
}

TEST_F(EdgeUniquenessWritesTest, excludesACreatedEdgeFromThePathAfterIt) {
    expectWriteRows("MATCH (r {name: 'Remy'}) CREATE (r)-[:KNOWS_WELL]->(:Person {name: 'Bo'}) "
                    "WITH r MATCH (a)-[e1]->(b)-[e*1..2]-(c) RETURN count(*)",
                    {{"70"}});
}
