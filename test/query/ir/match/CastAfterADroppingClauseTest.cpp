#include <gtest/gtest.h>

#include <string_view>
#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

// Only Remy KNOWS_WELL Adam, so the clauses before the pattern reading p keep the one row
// whose p holds a node: the rows holding 'none' never reach it.
class CastAfterADroppingClauseTest : public CallV3Test {
protected:
    void expectRows(std::string_view query, const std::vector<StringRowSink::Row>& expected) {
        StringRowSink sink;
        runQuery(query, sink);

        std::vector<StringRowSink::Row> rows;
        sink.sortedRows(rows);

        EXPECT_EQ(rows, expected) << query;
    }

    void expectWrittenRows(std::string_view write,
                           std::string_view read,
                           const std::vector<StringRowSink::Row>& expected) {
        runWrite(write);
        expectRows(read, expected);
    }
};

TEST_F(CastAfterADroppingClauseTest, matchesFromTheRowsAnEarlierMatchKept) {
    expectRows("MATCH (n:Person) WITH n, CASE WHEN n.name = 'Remy' THEN n ELSE 'none' END AS p "
               "MATCH (n)-[:KNOWS_WELL]->(:Person {name: 'Adam'}) MATCH (p)-[:KNOWS_WELL]->(f) RETURN f.name",
               {{"Adam"}});
}

TEST_F(CastAfterADroppingClauseTest, matchesFromTheRowsTheWhereOfAnEarlierMatchKept) {
    expectRows("MATCH (n:Person {name: 'Remy'}) WITH n, [n, 'none'] AS l UNWIND l AS x "
               "MATCH (n)-[:KNOWS_WELL]->(y) WHERE x = n MATCH (x)-[:KNOWS_WELL]->(z) RETURN z.name",
               {{"Adam"}});
}

TEST_F(CastAfterADroppingClauseTest, createsFromTheRowsAnEarlierMatchKept) {
    expectWrittenRows("MATCH (n:Person) WITH n, CASE WHEN n.name = 'Remy' THEN n ELSE 'none' END AS p "
                      "MATCH (n)-[:KNOWS_WELL]->(:Person {name: 'Adam'}) CREATE (p)-[:LIKES]->(:Thing {name: 'Kite'})",
                      "MATCH (p)-[:LIKES]->(t:Thing) RETURN p.name, t.name",
                      {{"Remy", "Kite"}});
}

TEST_F(CastAfterADroppingClauseTest, mergesFromTheRowsAnEarlierMatchKept) {
    expectWrittenRows("MATCH (n:Person) WITH n, CASE WHEN n.name = 'Remy' THEN n ELSE 'none' END AS p "
                      "MATCH (n)-[:KNOWS_WELL]->(:Person {name: 'Adam'}) MERGE (p)-[:LIKES]->(:Thing {name: 'Kite'})",
                      "MATCH (p)-[:LIKES]->(t:Thing) RETURN p.name, t.name",
                      {{"Remy", "Kite"}});
}

TEST_F(CastAfterADroppingClauseTest, searchesTheShortestPathFromTheRowsAnEarlierMatchKept) {
    expectRows("MATCH (n:Person) WITH n, CASE WHEN n.name = 'Remy' THEN n ELSE 'none' END AS a "
               "MATCH (n)-[:KNOWS_WELL]->(b:Person {name: 'Adam'}) SHORTESTPATH(a, b, duration, d, path) RETURN d",
               {{"20"}});
}

TEST_F(CastAfterADroppingClauseTest, matchesFromTheRowsAnUnwindKept) {
    expectRows("MATCH (n:Person) WITH n, CASE WHEN n.name = 'Remy' THEN n ELSE 'none' END AS p "
               "UNWIND CASE WHEN n.name = 'Remy' THEN [1] ELSE [] END AS i MATCH (p)-[:KNOWS_WELL]->(f) RETURN f.name",
               {{"Adam"}});
}

TEST_F(CastAfterADroppingClauseTest, createsFromTheRowsAnUnwindKept) {
    expectWrittenRows("MATCH (n:Person) WITH n, CASE WHEN n.name = 'Remy' THEN n ELSE 'none' END AS p "
                      "UNWIND CASE WHEN n.name = 'Remy' THEN [1] ELSE [] END AS i CREATE (p)-[:LIKES]->(:Thing {name: 'Kite'})",
                      "MATCH (p)-[:LIKES]->(t:Thing) RETURN p.name, t.name",
                      {{"Remy", "Kite"}});
}

TEST_F(CastAfterADroppingClauseTest, matchesFromTheRowsACallSubqueryKept) {
    expectRows("MATCH (n:Person) WITH n, CASE WHEN n.name = 'Remy' THEN n ELSE 'none' END AS p "
               "CALL { WITH n MATCH (n)-[:KNOWS_WELL]->(x:Person {name: 'Adam'}) RETURN x } "
               "MATCH (p)-[:KNOWS_WELL]->(f) RETURN f.name",
               {{"Adam"}});
}
