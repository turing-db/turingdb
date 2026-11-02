#include <gtest/gtest.h>

#include <stddef.h>

#include <string>
#include <string_view>

#include "QueryStatus.h"

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// A hash join answers as the product it replaces, whichever side the cost model builds.
// 3000 A nodes have k = i % 500 and 300 B nodes k = i % 50, each value 6 times; 20 A and 10 B
// have no k. T joins A with k < 100 to the B of k = a.k % 50, and A with k < 10 to the B
// without k; U, V and W are 36-edge types out of the A of k 1, 2 and 3.
class HashJoinBuildSideTest : public WriteQueryTest {
protected:
    void buildJoinGraph() {
        applyWrite("UNWIND range(1, 3000) AS i CREATE (:A {k: i % 500, name: 'a' + toString(i)})");
        applyWrite("UNWIND range(1, 20) AS i CREATE (:A {name: 'nok' + toString(i)})");
        applyWrite("UNWIND range(1, 300) AS i CREATE (:B {k: i % 50})");
        applyWrite("UNWIND range(1, 10) AS i CREATE (:B {name: 'bnok'})");
        applyWrite("UNWIND range(1, 1000) AS i CREATE (:C {f: (i % 50) * 1.0})");
        applyWrite("UNWIND range(1, 500) AS i CREATE (:D {f: 0.0 * -1.0})");
        applyWrite("UNWIND range(1, 500) AS i CREATE (:F {bi: 9007199254740993})");
        applyWrite("UNWIND range(1, 500) AS i CREATE (:G {bd: 9007199254740992.0})");

        applyWrite("MATCH (a:A), (b:B) WHERE a.k % 50 = b.k AND a.k < 100 CREATE (a)-[:T]->(b)");
        applyWrite("MATCH (a:A), (b:B) WHERE a.k < 10 AND b.name = 'bnok' CREATE (a)-[:T]->(b)");
        applyWrite("MATCH (a:A), (b:B) WHERE a.k = 1 AND b.k = 1 CREATE (a)-[:U]->(b)");
        applyWrite("MATCH (a:A), (b:B) WHERE a.k = 2 AND b.k = 2 CREATE (a)-[:V {w: a.k}]->(b)");
        applyWrite("MATCH (a:A), (b:B) WHERE a.k = 3 AND (b.k = 3 OR b.name = 'bnok') CREATE (a)-[:W]->(b)");
    }

    void explainProgram(std::string_view query, std::string& program) {
        RowSink sink;
        const QueryStatus status = runQuery(std::string("EXPLAIN (db) ") + std::string(query), &sink);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        program.clear();
        for (const Row& row : sink.rows()) {
            if (row.front() == "db") {
                program = row.back();
            }
        }
    }

    size_t countJoins(std::string_view query) {
        std::string program;
        explainProgram(query, program);

        size_t joins = 0;
        for (size_t position = program.find("db.hash_join"); position != std::string::npos;
             position = program.find("db.hash_join", position + 1)) {
            joins++;
        }

        return joins;
    }

    void expectJoin(std::string_view query) {
        EXPECT_EQ(countJoins(query), 1u) << "query: " << query;
    }

    void builtFactor(std::string_view query, std::string& factor) {
        std::string program;
        explainProgram(query, program);

        const size_t join = program.find("db.hash_join");
        ASSERT_NE(join, std::string::npos) << "query: " << query << "\n" << program;

        const size_t begin = program.find("} factor {", join);
        const size_t end = program.find("} on", begin);
        ASSERT_NE(begin, std::string::npos) << program;
        ASSERT_NE(end, std::string::npos) << program;

        factor = program.substr(begin, end - begin);
    }

    void expectBuildsTheWalk(std::string_view query) {
        std::string factor;
        builtFactor(query, factor);

        EXPECT_NE(factor.find("edges"), std::string::npos) << "query: " << query << "\nbuilt:\n" << factor;
    }

    void expectBuildsTheScanOf(std::string_view query, std::string_view label) {
        std::string factor;
        builtFactor(query, factor);

        const std::string scan = "db.scan_nodes_by_label([\"" + std::string(label) + "\"])";
        EXPECT_NE(factor.find(scan), std::string::npos) << "query: " << query << "\nbuilt:\n" << factor;
        EXPECT_EQ(factor.find("edges"), std::string::npos) << "query: " << query << "\nbuilt:\n" << factor;
    }

    void deleteEdgesOutsideKeyZero() {
        applyWrite("MATCH (a:A)-[e:T]->(b:B) WHERE b.k IS NULL OR b.k >= 1 DELETE e");
    }
};

TEST_F(HashJoinBuildSideTest, joinsTwoScansOfOneLabel) {
    buildJoinGraph();

    const std::string_view joined = "MATCH (n:A), (m:A) WHERE n.k = m.k RETURN count(*), sum(n.k), sum(m.k)";
    expectJoin(joined);
    expectRows(joined, {{"18000", "4491000", "4491000"}});
    expectRows("MATCH (n:A), (m:A) WHERE n.k IN [m.k] RETURN count(*), sum(n.k), sum(m.k)",
               {{"18000", "4491000", "4491000"}});

    const std::string_view byName = "MATCH (n:A), (m:A) WHERE n.name = m.name RETURN count(*)";
    expectJoin(byName);
    expectRows(byName, {{"3020"}});

    const std::string_view byExpression = "MATCH (n:A), (m:A) WHERE n.k + 0 = m.k RETURN count(*)";
    expectJoin(byExpression);
    expectRows(byExpression, {{"18000"}});
    expectRows("MATCH (n:A), (m:A) WHERE n.k + 0 = m.k RETURN count(*), sum(n.k)", {{"18000", "4491000"}});

    const std::string_view distinctCounts = "MATCH (n:A), (m:A) "
                                            "WHERE n.k = m.k "
                                            "RETURN count(DISTINCT n.k), count(DISTINCT m)";
    expectJoin(distinctCounts);
    expectRows(distinctCounts, {{"500", "3000"}});

    const std::string_view distinctKeys = "MATCH (n:A), (m:A) "
                                          "WHERE n.k = m.k "
                                          "WITH DISTINCT n.k AS k "
                                          "RETURN count(*), sum(k)";
    expectJoin(distinctKeys);
    expectRows(distinctKeys, {{"500", "124750"}});
}

TEST_F(HashJoinBuildSideTest, joinsAWalkToAScan) {
    buildJoinGraph();

    const std::string_view joined = "MATCH (a:A)-[:T]->(b:B), (m:A) "
                                    "WHERE b.k = m.k "
                                    "RETURN count(*), sum(m.k), sum(a.k)";
    expectBuildsTheScanOf(joined, "A");
    expectRows(joined, {{"21600", "529200", "1069200"}});
    expectRows("MATCH (m:A), (a:A)-[:T]->(b:B) "
               "WHERE b.k = m.k "
               "RETURN count(*), sum(m.k), sum(a.k)",
               {{"21600", "529200", "1069200"}});
    expectRows("MATCH (a:A)-[:T]->(b:B), (m:A) "
               "WHERE b.k IN [m.k] "
               "RETURN count(*), sum(m.k), sum(a.k)",
               {{"21600", "529200", "1069200"}});

    const std::string_view bySource = "MATCH (a:A)-[:T]->(b:B), (m:B) WHERE a.k = m.k RETURN count(*), sum(a.k)";
    expectBuildsTheScanOf(bySource, "B");
    expectRows(bySource, {{"14400", "280800"}});
    expectRows("MATCH (m:B), (a:A)-[:T]->(b:B) WHERE a.k = m.k RETURN count(*), sum(a.k)", {{"14400", "280800"}});
    expectRows("MATCH (a:A)-[:T]->(b:B), (m:B) WHERE a.k IN [m.k] RETURN count(*), sum(a.k)", {{"14400", "280800"}});

    const std::string_view thenWalks = "MATCH (n:A), (m:B) "
                                       "WHERE n.k = m.k "
                                       "MATCH (m)<-[:T]-(x:A) "
                                       "RETURN count(*), sum(x.k)";
    expectJoin(thenWalks);
    expectRows(thenWalks, {{"21600", "1069200"}});
    expectRows("MATCH (n:A), (m:B) "
               "WHERE n.k IN [m.k] "
               "MATCH (m)<-[:T]-(x:A) "
               "RETURN count(*), sum(x.k)",
               {{"21600", "1069200"}});
}

TEST_F(HashJoinBuildSideTest, joinsTwoWalksOnTheirSources) {
    buildJoinGraph();

    const std::string_view joined = "MATCH (a:A)-[:T]->(b:B), (c:A)-[:T]->(d:B) "
                                    "WHERE a.k = c.k "
                                    "RETURN count(*), sum(c.k)";
    expectJoin(joined);
    expectRows(joined, {{"208800", "6771600"}});

    const std::string_view targets = "MATCH (a:A)-[:T]->(b:B), (c:A)-[:T]->(d:B) "
                                     "WHERE a.k = c.k "
                                     "RETURN count(*), sum(d.k), sum(b.k), count(d.k), count(b.k)";
    expectJoin(targets);
    expectRows(targets, {{"208800", "3272400", "3272400", "151200", "151200"}});
    expectRows("MATCH (a:A)-[:T]->(b:B), (c:A)-[:T]->(d:B) "
               "WHERE a.k IN [c.k] "
               "RETURN count(*), sum(c.k), sum(d.k), sum(b.k), count(d.k), count(b.k)",
               {{"208800", "6771600", "3272400", "3272400", "151200", "151200"}});
}

TEST_F(HashJoinBuildSideTest, joinsTwoWalksOnTheirTargets) {
    buildJoinGraph();

    const std::string_view joined = "MATCH (a:A)-[:T]->(b:B), (c:A)-[:T]->(d:B) "
                                    "WHERE b.k = d.k "
                                    "RETURN count(*), sum(d.k)";
    expectJoin(joined);
    expectRows(joined, {{"259200", "6350400"}});
    expectRows("MATCH (a:A)-[:T]->(b:B), (c:A)-[:T]->(d:B) "
               "WHERE b.k IN [d.k] "
               "RETURN count(*), sum(d.k)",
               {{"259200", "6350400"}});
}

TEST_F(HashJoinBuildSideTest, comparesIntegerAndFloatKeysByValue) {
    buildJoinGraph();

    const std::string_view joined = "MATCH (n:A), (c:C) WHERE n.k = c.f RETURN count(*), sum(n.k), sum(c.f)";
    expectBuildsTheScanOf(joined, "C");
    expectRows(joined, {{"6000", "147000", "147000.000000"}});

    const std::string_view swapped = "MATCH (c:C), (n:A) WHERE n.k = c.f RETURN count(*), sum(n.k), sum(c.f)";
    expectBuildsTheScanOf(swapped, "C");
    expectRows(swapped, {{"6000", "147000", "147000.000000"}});
    expectRows("MATCH (n:A), (c:C) WHERE n.k IN [c.f] RETURN count(*), sum(n.k), sum(c.f)",
               {{"6000", "147000", "147000.000000"}});

    const std::string_view negativeZero = "MATCH (n:A), (d:D) WHERE n.k = d.f RETURN count(*)";
    expectJoin(negativeZero);
    expectRows(negativeZero, {{"3000"}});
    expectRows("MATCH (n:A), (d:D) WHERE n.k IN [d.f] RETURN count(*)", {{"3000"}});

    const std::string_view pastDoublePrecision = "MATCH (f:F), (g:G) WHERE f.bi = g.bd RETURN count(*)";
    expectJoin(pastDoublePrecision);
    expectRows(pastDoublePrecision, {{"0"}});
    expectRows("MATCH (g:G), (f:F) WHERE f.bi = g.bd RETURN count(*)", {{"0"}});
    expectRows("MATCH (f:F), (g:G) WHERE f.bi IN [g.bd] RETURN count(*)", {{"0"}});
    expectRows("MATCH (f:F), (g:G) WHERE f.bi >= g.bd AND f.bi <= g.bd RETURN count(*)", {{"0"}});
    expectRows("MATCH (f:F), (g:G) WHERE f.bi = g.bd RETURN f LIMIT 1", {});
    expectRows("MATCH (f:F) "
               "WITH f LIMIT 1 "
               "MATCH (g:G) "
               "WITH f, g LIMIT 1 "
               "RETURN f.bi = g.bd, f.bi IN [g.bd]",
               {{"false", "false"}});
}

TEST_F(HashJoinBuildSideTest, joinsTheSidesTheLabelsNarrow) {
    buildJoinGraph();

    const std::string_view joined = "MATCH (n:A), (m:B) WHERE n.k = m.k RETURN count(*), sum(n.k)";
    expectBuildsTheScanOf(joined, "B");
    expectRows(joined, {{"1800", "44100"}});
    expectRows("MATCH (n:A), (m:B) WHERE n.k IN [m.k] RETURN count(*), sum(n.k)", {{"1800", "44100"}});

    Rows everyKey;
    for (size_t key = 0; key < 50; key++) {
        everyKey.push_back({std::to_string(key), "36"});
    }

    const std::string_view grouped = "MATCH (n:A), (m:B) WHERE n.k = m.k RETURN m.k, count(*) ORDER BY m.k";
    expectJoin(grouped);
    expectRowsInOrder(grouped, everyKey);
    expectRowsInOrder("MATCH (n:A), (m:B) WHERE n.k = m.k RETURN m.k, count(*) ORDER BY m.k LIMIT 3",
                      {{"0", "36"}, {"1", "36"}, {"2", "36"}});
}

TEST_F(HashJoinBuildSideTest, padsTheRowsAnOptionalJoinMisses) {
    buildJoinGraph();

    const std::string_view joined = "MATCH (m:B) "
                                    "OPTIONAL MATCH (n:A) WHERE n.k = m.k "
                                    "RETURN count(*), count(n), sum(n.k)";
    expectJoin(joined);
    expectRows(joined, {{"1810", "1800", "44100"}});
    expectRows("MATCH (m:B) "
               "OPTIONAL MATCH (n:A) WHERE n.k IN [m.k] "
               "RETURN count(*), count(n), sum(n.k)",
               {{"1810", "1800", "44100"}});

    expectRows("MATCH (a:A)-[:W]->(b:B) "
               "OPTIONAL MATCH (m:A) WHERE m.k = b.k "
               "RETURN count(*), count(m)",
               {{"276", "216"}});
    expectRows("MATCH (a:A)-[:W]->(b:B) "
               "OPTIONAL MATCH (m:A) WHERE m.k IN [b.k] "
               "RETURN count(*), count(m)",
               {{"276", "216"}});
}

TEST_F(HashJoinBuildSideTest, buildsTheWalkOfARareType) {
    buildJoinGraph();

    const std::string_view joined = "MATCH (a:A)-[:U]->(b:B), (m:A) "
                                    "WHERE b.k = m.k "
                                    "RETURN count(*), sum(a.k), sum(m.k), count(DISTINCT a), count(DISTINCT m)";
    expectBuildsTheWalk(joined);
    expectRows(joined, {{"216", "216", "216", "6", "6"}});

    const std::string_view swapped = "MATCH (m:A), (a:A)-[:U]->(b:B) "
                                     "WHERE b.k = m.k "
                                     "RETURN count(*), sum(a.k), sum(m.k), count(DISTINCT a), count(DISTINCT m)";
    expectBuildsTheWalk(swapped);
    expectRows(swapped, {{"216", "216", "216", "6", "6"}});
    expectRows("MATCH (a:A)-[:U]->(b:B), (m:A) "
               "WHERE b.k IN [m.k] "
               "RETURN count(*), sum(a.k), sum(m.k), count(DISTINCT a), count(DISTINCT m)",
               {{"216", "216", "216", "6", "6"}});

    const std::string_view incoming = "MATCH (b:B)<-[:U]-(a:A), (m:A) "
                                      "WHERE a.k = m.k "
                                      "RETURN count(*), sum(b.k), count(DISTINCT m)";
    expectBuildsTheWalk(incoming);
    expectRows(incoming, {{"216", "216", "6"}});
    expectRows("MATCH (b:B)<-[:U]-(a:A), (m:A) "
               "WHERE a.k IN [m.k] "
               "RETURN count(*), sum(b.k), count(DISTINCT m)",
               {{"216", "216", "6"}});

    const std::string_view undirected = "MATCH (a:A)-[:U]-(b:B), (m:A) WHERE b.k = m.k RETURN count(*), sum(a.k)";
    expectBuildsTheWalk(undirected);
    expectRows(undirected, {{"216", "216"}});
    expectRows("MATCH (a:A)-[:U]-(b:B), (m:A) WHERE b.k IN [m.k] RETURN count(*), sum(a.k)", {{"216", "216"}});

    const std::string_view twoWalks = "MATCH (a:A)-[:U]->(b:B), (c:A)-[:T]->(d:B) "
                                      "WHERE b.k = d.k "
                                      "RETURN count(*), sum(c.k), count(DISTINCT a)";
    expectJoin(twoWalks);
    expectRows(twoWalks, {{"2592", "67392", "6"}});
    expectRows("MATCH (a:A)-[:U]->(b:B), (c:A)-[:T]->(d:B) "
               "WHERE b.k IN [d.k] "
               "RETURN count(*), sum(c.k), count(DISTINCT a)",
               {{"2592", "67392", "6"}});

    const std::string_view twoHops = "MATCH (a:A)-[:U]->(b:B)<-[:T]-(x:A), (m:A) "
                                     "WHERE x.k = m.k "
                                     "RETURN count(*), sum(m.k)";
    expectBuildsTheWalk(twoHops);
    expectRows(twoHops, {{"2592", "67392"}});
    expectRows("MATCH (a:A)-[:U]->(b:B)<-[:T]-(x:A), (m:A) "
               "WHERE x.k IN [m.k] "
               "RETURN count(*), sum(m.k)",
               {{"2592", "67392"}});

    const std::string_view edgeKey = "MATCH (a:A)-[e:V]->(b:B), (m:A) WHERE e.w = m.k RETURN count(*), sum(m.k)";
    expectBuildsTheWalk(edgeKey);
    expectRows(edgeKey, {{"216", "432"}});
    expectRows("MATCH (a:A)-[e:V]->(b:B), (m:A) WHERE e.w IN [m.k] RETURN count(*), sum(m.k)", {{"216", "432"}});

    const std::string_view nullKeys = "MATCH (a:A)-[:W]->(b:B), (m:A) "
                                      "WHERE b.k = m.k "
                                      "RETURN count(*), sum(m.k), count(DISTINCT b)";
    expectBuildsTheWalk(nullKeys);
    expectRows(nullKeys, {{"216", "648", "6"}});
    expectRows("MATCH (a:A)-[:W]->(b:B), (m:A) "
               "WHERE b.k IN [m.k] "
               "RETURN count(*), sum(m.k), count(DISTINCT b)",
               {{"216", "648", "6"}});
}

TEST_F(HashJoinBuildSideTest, buildsTheWalkTheDeletedEdgesNarrow) {
    buildJoinGraph();

    const std::string_view joined = "MATCH (a:A)-[:T]->(b:B), (m:A) "
                                    "WHERE b.k = m.k "
                                    "RETURN count(*), sum(a.k), sum(m.k)";
    expectBuildsTheScanOf(joined, "A");

    deleteEdgesOutsideKeyZero();

    expectRows("MATCH (a:A)-[e:T]->(b:B) RETURN count(e), sum(a.k)", {{"72", "1800"}});

    expectBuildsTheWalk(joined);
    expectRows(joined, {{"432", "10800", "0"}});
    expectRows("MATCH (m:A), (a:A)-[:T]->(b:B) "
               "WHERE b.k = m.k "
               "RETURN count(*), sum(a.k), sum(m.k)",
               {{"432", "10800", "0"}});
    expectRows("MATCH (a:A)-[:T]->(b:B), (m:A) "
               "WHERE b.k IN [m.k] "
               "RETURN count(*), sum(a.k), sum(m.k)",
               {{"432", "10800", "0"}});

    const std::string_view byExpression = "MATCH (a:A)-[:T]->(b:B), (c:A)-[:U]->(d:B) "
                                          "WHERE a.k + 1 = c.k "
                                          "RETURN count(*), sum(c.k)";
    expectJoin(byExpression);
    expectRows(byExpression, {{"1296", "1296"}});
    expectRows("MATCH (a:A)-[:T]->(b:B), (c:A)-[:U]->(d:B) "
               "WHERE a.k + 1 IN [c.k] "
               "RETURN count(*), sum(c.k)",
               {{"1296", "1296"}});
}

TEST_F(HashJoinBuildSideTest, joinsWhatTheDeletedNodesLeave) {
    buildJoinGraph();
    deleteEdgesOutsideKeyZero();
    applyWrite("MATCH (n:A) WHERE n.k >= 10 DETACH DELETE n");

    expectRows("MATCH (n:A) RETURN count(n)", {{"80"}});

    expectRows("MATCH (n:A), (c:C) WHERE n.k = c.f RETURN count(*), sum(n.k)", {{"1200", "5400"}});
    expectRows("MATCH (c:C), (n:A) WHERE n.k = c.f RETURN count(*), sum(n.k)", {{"1200", "5400"}});
    expectRows("MATCH (n:A), (c:C) WHERE n.k IN [c.f] RETURN count(*), sum(n.k)", {{"1200", "5400"}});

    expectRows("MATCH (a:A)-[:T]->(b:B), (m:A) WHERE b.k = m.k RETURN count(*), sum(a.k), sum(m.k)",
               {{"216", "0", "0"}});
    expectRows("MATCH (a:A)-[:T]->(b:B), (m:A) WHERE b.k IN [m.k] RETURN count(*), sum(a.k), sum(m.k)",
               {{"216", "0", "0"}});

    const std::string_view toFloats = "MATCH (a:A)-[:T]->(b:B), (m:C) "
                                      "WHERE b.k = m.f "
                                      "RETURN count(*), sum(a.k), sum(m.f)";
    expectBuildsTheWalk(toFloats);
    expectRows(toFloats, {{"720", "0", "0.000000"}});
    expectRows("MATCH (a:A)-[:T]->(b:B), (m:C) "
               "WHERE b.k IN [m.f] "
               "RETURN count(*), sum(a.k), sum(m.f)",
               {{"720", "0", "0.000000"}});
}

TEST_F(HashJoinBuildSideTest, chainsTwoJoins) {
    buildJoinGraph();

    const std::string_view joined = "MATCH (n:A), (m:A), (c:C) "
                                    "WHERE n.k = m.k AND m.k = c.f "
                                    "RETURN count(*), sum(c.f)";
    EXPECT_EQ(countJoins(joined), 2u);
    expectRows(joined, {{"36000", "882000.000000"}});
    expectRows("MATCH (n:A), (m:A), (c:C) "
               "WHERE n.k IN [m.k] AND m.k IN [c.f] "
               "RETURN count(*), sum(c.f)",
               {{"36000", "882000.000000"}});
}
