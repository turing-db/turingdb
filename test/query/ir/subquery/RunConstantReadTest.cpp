#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "QueryInterpreterV3.h"
#include "QueryStatus.h"

#include "Graph.h"
#include "SimpleGraph.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"

#include "StringRowSink.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

namespace {

const std::vector<StringRowSink::Row> everyPersonName = {{"Remy"},
                                                         {"Adam"},
                                                         {"Maxime"},
                                                         {"Luc"},
                                                         {"Martina"},
                                                         {"Suhas"},
                                                         {"Cyrus"},
                                                         {"Doruk"}};

const std::vector<StringRowSink::Row> everyPersonOnce = {{"Remy", "1"},
                                                         {"Adam", "1"},
                                                         {"Maxime", "1"},
                                                         {"Luc", "1"},
                                                         {"Martina", "1"},
                                                         {"Suhas", "1"},
                                                         {"Cyrus", "1"},
                                                         {"Doruk", "1"}};

const std::vector<StringRowSink::Row> everyKnowsWellPathOnce = {{"1"}, {"1"}, {"1"}};

const std::vector<StringRowSink::Row> everyKnowsWellPathNodes = {{"Remy, Adam"},
                                                                 {"Adam, Remy"},
                                                                 {"Ghosts, Remy"}};

}

// A body that aggregates, sorts or dedups runs once per input row and reads each import as a
// constant column. Every query here reads a node, an edge, a path or a list import that way.
class RunConstantReadTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager(), &_env->getMem(), &_env->getCompilerContext());

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);
    }

    void runQuery(std::string_view query, StringRowSink& sink, QueryStatus& status) {
        _interpreter->execute(status, query, _graphName, CommitHash::head(), ChangeID::head(), &sink);
    }

    void expectRows(std::string_view query, const std::vector<StringRowSink::Row>& expected) {
        StringRowSink sink;
        QueryStatus status;
        runQuery(query, sink, status);
        ASSERT_TRUE(status.isOk()) << query << ": " << status.getError();

        std::vector<StringRowSink::Row> actual;
        sink.sortedRows(actual);

        std::vector<StringRowSink::Row> sortedExpected = expected;
        std::sort(sortedExpected.begin(), sortedExpected.end());

        EXPECT_EQ(actual, sortedExpected) << "query: " << query;
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

TEST_F(RunConstantReadTest, readsTheLabelsAndPropertiesOfAnImportedNode) {
    expectRows("MATCH (p:Person) "
               "CALL (p) { WITH count(*) AS c RETURN labels(p) AS k } "
               "RETURN p.name, size(k), k = labels(p)",
               {{"Remy", "3", "true"},
                {"Adam", "3", "true"},
                {"Maxime", "2", "true"},
                {"Luc", "2", "true"},
                {"Martina", "2", "true"},
                {"Suhas", "2", "true"},
                {"Cyrus", "2", "true"},
                {"Doruk", "2", "true"}});

    expectRows("MATCH (p:Person) "
               "CALL (p) { WITH count(*) AS c RETURN keys(p) AS k, id(p) AS i, keys(p)[0] AS f } "
               "RETURN p.name, k = keys(p), i = id(p), f = keys(p)[0]",
               {{"Remy", "true", "true", "true"},
                {"Adam", "true", "true", "true"},
                {"Maxime", "true", "true", "true"},
                {"Luc", "true", "true", "true"},
                {"Martina", "true", "true", "true"},
                {"Suhas", "true", "true", "true"},
                {"Cyrus", "true", "true", "true"},
                {"Doruk", "true", "true", "true"}});

    expectRows("MATCH (p:Person) "
               "CALL (p) { WITH count(*) AS c RETURN size(keys(p)) AS k } "
               "RETURN p.name, k",
               {{"Remy", "5"},
                {"Adam", "5"},
                {"Maxime", "4"},
                {"Luc", "4"},
                {"Martina", "3"},
                {"Suhas", "3"},
                {"Cyrus", "3"},
                {"Doruk", "3"}});

    expectRows("MATCH (p:Person) "
               "CALL (p) { WITH count(*) AS c RETURN properties(p) AS k } "
               "RETURN k.name, k.age",
               {{"Remy", "32"},
                {"Adam", "32"},
                {"Maxime", "null"},
                {"Luc", "null"},
                {"Martina", "null"},
                {"Suhas", "null"},
                {"Cyrus", "null"},
                {"Doruk", "null"}});

    expectRows("MATCH (p:Person) CALL (p) { WITH count(*) AS c RETURN properties(p).name AS k } RETURN k",
               everyPersonName);

    expectRows("MATCH (p:Person) CALL (p) { WITH count(*) AS c RETURN p AS k } RETURN labels(k) = labels(p)",
               {{"true"}, {"true"}, {"true"}, {"true"}, {"true"}, {"true"}, {"true"}, {"true"}});
}

TEST_F(RunConstantReadTest, comparesAnImportedNode) {
    const std::vector<StringRowSink::Row> remyOnly = {{"Remy", "true"},
                                                      {"Adam", "false"},
                                                      {"Maxime", "false"},
                                                      {"Luc", "false"},
                                                      {"Martina", "false"},
                                                      {"Suhas", "false"},
                                                      {"Cyrus", "false"},
                                                      {"Doruk", "false"}};

    const std::vector<StringRowSink::Row> allButRemy = {{"Remy", "false"},
                                                        {"Adam", "true"},
                                                        {"Maxime", "true"},
                                                        {"Luc", "true"},
                                                        {"Martina", "true"},
                                                        {"Suhas", "true"},
                                                        {"Cyrus", "true"},
                                                        {"Doruk", "true"}};

    expectRows("MATCH (p:Person) "
               "MATCH (q:Person) WHERE q.name = 'Remy' "
               "CALL (p, q) { WITH count(*) AS c RETURN p = q AS k } "
               "RETURN p.name, k",
               remyOnly);

    expectRows("MATCH (p:Person) "
               "MATCH (q:Person) WHERE q.name = 'Remy' "
               "CALL (p, q) { WITH count(*) AS c RETURN p <> q AS k } "
               "RETURN p.name, k",
               allButRemy);

    expectRows("MATCH (p:Person) "
               "MATCH (q:Person) WHERE q.name = 'Remy' "
               "CALL (p, q) { WITH count(*) AS c RETURN p IN [q] AS k } "
               "RETURN p.name, k",
               remyOnly);

    expectRows("MATCH (p:Person) "
               "MATCH (q:Person) WHERE q.name = 'Remy' "
               "CALL (p, q) { WITH count(*) AS c RETURN coalesce(p, q) AS k } "
               "RETURN k.name",
               everyPersonName);

    expectRows("MATCH (p:Person) CALL (p) { WITH count(*) AS c RETURN CASE WHEN c > 0 THEN p END AS k } RETURN k.name",
               everyPersonName);

    expectRows("MATCH (p:Person) CALL (p) { WITH count(*) AS c RETURN p AS k } RETURN k.name",
               everyPersonName);
}

TEST_F(RunConstantReadTest, readsAnImportedEdge) {
    expectRows("MATCH (a)-[e:KNOWS_WELL]->(b) CALL (e) { WITH count(*) AS c RETURN type(e) AS k } RETURN k",
               {{"KNOWS_WELL"}, {"KNOWS_WELL"}, {"KNOWS_WELL"}});

    expectRows("MATCH (a)-[e:KNOWS_WELL]->(b) CALL (e) { RETURN type(e) AS k, count(*) AS c } RETURN k, c",
               {{"KNOWS_WELL", "1"}, {"KNOWS_WELL", "1"}, {"KNOWS_WELL", "1"}});

    expectRows("MATCH (a)-[e:KNOWS_WELL]->(b) CALL (e) { WITH count(*) AS c RETURN startNode(e).name AS k } RETURN k",
               {{"Remy"}, {"Adam"}, {"Ghosts"}});

    expectRows("MATCH (a)-[e:KNOWS_WELL]->(b) CALL (e) { WITH count(*) AS c RETURN startNode(e) AS k } RETURN k.name",
               {{"Remy"}, {"Adam"}, {"Ghosts"}});

    expectRows("MATCH (a)-[e:KNOWS_WELL]->(b) CALL (e) { WITH count(*) AS c RETURN endNode(e) AS k } RETURN k.name",
               {{"Adam"}, {"Remy"}, {"Remy"}});

    expectRows("MATCH (a)-[e:KNOWS_WELL]->(b) "
               "CALL (e) { WITH count(*) AS c RETURN id(e) AS k, properties(e) AS m } "
               "RETURN e.name, k = id(e), m.duration",
               {{"Remy -> Adam", "true", "20"},
                {"Adam -> Remy", "true", "20"},
                {"Ghosts -> Remy", "true", "200"}});

    expectRows("MATCH (a)-[e:KNOWS_WELL]->(b) CALL (e) { WITH count(*) AS c RETURN keys(e) AS k } RETURN e.name, size(k)",
               {{"Remy -> Adam", "2"}, {"Adam -> Remy", "2"}, {"Ghosts -> Remy", "3"}});

    expectRows("MATCH (a)-[e:KNOWS_WELL]->(b) CALL (e) { WITH count(*) AS c RETURN e AS k } RETURN k.name",
               {{"Remy -> Adam"}, {"Adam -> Remy"}, {"Ghosts -> Remy"}});

    expectRows("MATCH (a)-[e:KNOWS_WELL]->(b) CALL (e) { WITH count(*) AS c RETURN e AS k } RETURN type(k)",
               {{"KNOWS_WELL"}, {"KNOWS_WELL"}, {"KNOWS_WELL"}});

    expectRows("MATCH (a)-[e:KNOWS_WELL]->(b) CALL (e) { WITH count(*) AS c RETURN e AS k } RETURN startNode(k).name",
               {{"Remy"}, {"Adam"}, {"Ghosts"}});
}

TEST_F(RunConstantReadTest, readsAnImportedPathInsideTheBody) {
    expectRows("MATCH pt = (a)-[:KNOWS_WELL]->(b) CALL (pt) { WITH count(*) AS c RETURN length(pt) AS k } RETURN k",
               everyKnowsWellPathOnce);

    expectRows("MATCH pt = (a)-[:KNOWS_WELL]->(b) CALL (pt) { RETURN length(pt) AS k, count(*) AS c } RETURN k, c",
               {{"1", "1"}, {"1", "1"}, {"1", "1"}});

    expectRows("MATCH pt = (a)-[:KNOWS_WELL]->(b) "
               "CALL (pt) { WITH count(*) AS c RETURN nodes(pt) AS k, relationships(pt) AS r } "
               "RETURN [n IN k | n.name], [x IN r | x.name]",
               {{"Remy, Adam", "Remy -> Adam"},
                {"Adam, Remy", "Adam -> Remy"},
                {"Ghosts, Remy", "Ghosts -> Remy"}});

    expectRows("MATCH pt = (a)-[:KNOWS_WELL]->(b) CALL (pt) { WITH count(*) AS c RETURN size(nodes(pt)) AS k } RETURN k",
               {{"2"}, {"2"}, {"2"}});

    expectRows("MATCH pt = (a)-[:KNOWS_WELL]->(b) "
               "CALL (pt) { WITH count(*) AS c WITH pt, c RETURN length(pt) AS k } "
               "RETURN k",
               everyKnowsWellPathOnce);

    expectRows("MATCH pt = (a)-[:KNOWS_WELL]->(b) "
               "CALL (pt) { WITH count(*) AS c WITH pt, c RETURN nodes(pt) AS k } "
               "RETURN [n IN k | n.name]",
               everyKnowsWellPathNodes);

    expectRows("MATCH pt = (a)-[:KNOWS_WELL]->(b) "
               "CALL (pt) { WITH pt, count(*) AS c RETURN nodes(pt) AS k } "
               "RETURN [n IN k | n.name]",
               everyKnowsWellPathNodes);

    expectRows("MATCH pt = (a)-[:KNOWS_WELL]->(b) "
               "CALL (pt) { WITH pt, count(*) AS c RETURN length(pt) AS k } "
               "RETURN k",
               everyKnowsWellPathOnce);
}

TEST_F(RunConstantReadTest, readsAPathReturnedPastASortADedupOrAUnion) {
    expectRows("MATCH pt = (a)-[:KNOWS_WELL]->(b) CALL (pt) { RETURN pt AS k } RETURN length(k)",
               everyKnowsWellPathOnce);

    expectRows("MATCH pt = (a)-[:KNOWS_WELL]->(b) CALL (pt) { RETURN pt AS k, count(*) AS c } RETURN length(k)",
               everyKnowsWellPathOnce);

    expectRows("MATCH pt = (a)-[:KNOWS_WELL]->(b) "
               "CALL (pt) { WITH pt ORDER BY length(pt) LIMIT 1 RETURN pt AS k } "
               "RETURN length(k)",
               everyKnowsWellPathOnce);

    expectRows("MATCH pt = (a)-[:KNOWS_WELL]->(b) CALL (pt) { WITH DISTINCT pt RETURN pt AS k } RETURN length(k)",
               everyKnowsWellPathOnce);

    expectRows("MATCH pt = (a)-[:KNOWS_WELL]->(b) "
               "CALL (pt) { RETURN pt AS k UNION RETURN pt AS k } "
               "RETURN length(k)",
               everyKnowsWellPathOnce);
}

TEST_F(RunConstantReadTest, readsAnImportedList) {
    expectRows("MATCH (p:Person) WITH [p] AS ps CALL (ps) { WITH count(*) AS c RETURN size(ps[0..1]) AS k } RETURN k",
               {{"1"}, {"1"}, {"1"}, {"1"}, {"1"}, {"1"}, {"1"}, {"1"}});

    expectRows("MATCH (p:Person) WITH [p] AS ps CALL (ps) { WITH count(*) AS c RETURN size(ps) AS k } RETURN k",
               {{"1"}, {"1"}, {"1"}, {"1"}, {"1"}, {"1"}, {"1"}, {"1"}});

    expectRows("MATCH (p:Person) WITH [p] AS ps CALL (ps) { WITH count(*) AS c RETURN tail(ps) AS k } RETURN k",
               {{""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}});

    expectRows("MATCH (p:Person) WITH [p] AS ps "
               "CALL (ps) { WITH count(*) AS c RETURN ps + ps AS k, reverse(ps) AS r, ps[0..1] AS s } "
               "RETURN [x IN k | x.name], r[0].name, s[0].name",
               {{"Remy, Remy", "Remy", "Remy"},
                {"Adam, Adam", "Adam", "Adam"},
                {"Maxime, Maxime", "Maxime", "Maxime"},
                {"Luc, Luc", "Luc", "Luc"},
                {"Martina, Martina", "Martina", "Martina"},
                {"Suhas, Suhas", "Suhas", "Suhas"},
                {"Cyrus, Cyrus", "Cyrus", "Cyrus"},
                {"Doruk, Doruk", "Doruk", "Doruk"}});

    expectRows("MATCH (p:Person) WITH [p] AS ps CALL (ps) { WITH count(*) AS c RETURN ps AS k } RETURN k[0].name",
               everyPersonName);

    expectRows("MATCH (p:Person) CALL (p) { WITH count(*) AS c RETURN [p] AS l } RETURN l[0].name, size(l)",
               everyPersonOnce);

    expectRows("MATCH (p:Person) WITH [p.name, p.age] AS ps "
               "CALL (ps) { WITH count(*) AS c RETURN ps[0] AS k, reverse(ps) AS r, ps + ps AS pp } "
               "RETURN k, r, pp",
               {{"Remy", "32, Remy", "Remy, 32, Remy, 32"},
                {"Adam", "32, Adam", "Adam, 32, Adam, 32"},
                {"Maxime", "null, Maxime", "Maxime, null, Maxime, null"},
                {"Luc", "null, Luc", "Luc, null, Luc, null"},
                {"Martina", "null, Martina", "Martina, null, Martina, null"},
                {"Suhas", "null, Suhas", "Suhas, null, Suhas, null"},
                {"Cyrus", "null, Cyrus", "Cyrus, null, Cyrus, null"},
                {"Doruk", "null, Doruk", "Doruk, null, Doruk, null"}});
}

TEST_F(RunConstantReadTest, appliesStringFunctionsToAnImportedProperty) {
    expectRows("MATCH (p:Person) "
               "CALL (p) { "
               "  WITH count(*) AS c "
               "  RETURN toUpper(p.name) AS k, substring(p.name, 0, 2) AS s, split(p.name, 'a') AS sp, "
               "         p.name STARTS WITH 'R' AS sw "
               "} "
               "RETURN k, s, sp, sw",
               {{"REMY", "Re", "Remy", "true"},
                {"ADAM", "Ad", "Ad, m", "false"},
                {"MAXIME", "Ma", "M, xime", "false"},
                {"LUC", "Lu", "Luc", "false"},
                {"MARTINA", "Ma", "M, rtin, ", "false"},
                {"SUHAS", "Su", "Suh, s", "false"},
                {"CYRUS", "Cy", "Cyrus", "false"},
                {"DORUK", "Do", "Doruk", "false"}});
}

TEST_F(RunConstantReadTest, matchesAPatternFromAnImportedNode) {
    expectRows("MATCH (p:Person) CALL (p) { WITH count(*) AS c RETURN exists((p)-->()) AS k } RETURN p.name, k",
               {{"Remy", "true"},
                {"Adam", "true"},
                {"Maxime", "true"},
                {"Luc", "true"},
                {"Martina", "true"},
                {"Suhas", "true"},
                {"Cyrus", "true"},
                {"Doruk", "true"}});

    expectRows("MATCH (p:Person) CALL (p) { WITH count(*) AS c RETURN exists((p)-[:KNOWS_WELL]->()) AS k } RETURN p.name, k",
               {{"Remy", "true"},
                {"Adam", "true"},
                {"Maxime", "false"},
                {"Luc", "false"},
                {"Martina", "false"},
                {"Suhas", "false"},
                {"Cyrus", "false"},
                {"Doruk", "false"}});

    expectRows("MATCH (p:Person) CALL (p) { WITH count(*) AS c RETURN COUNT { (p)-->() } AS k } RETURN p.name, k",
               {{"Remy", "4"},
                {"Adam", "3"},
                {"Maxime", "2"},
                {"Luc", "2"},
                {"Martina", "1"},
                {"Suhas", "2"},
                {"Cyrus", "2"},
                {"Doruk", "1"}});

    expectRows("MATCH (p:Person) "
               "CALL (p) { WITH count(*) AS c RETURN [(p)-[:INTERESTED_IN]->(i) | i.name] AS k } "
               "RETURN p.name, k = [(p)-[:INTERESTED_IN]->(i) | i.name], size(k)",
               {{"Remy", "true", "3"},
                {"Adam", "true", "2"},
                {"Maxime", "true", "2"},
                {"Luc", "true", "2"},
                {"Martina", "true", "1"},
                {"Suhas", "true", "2"},
                {"Cyrus", "true", "2"},
                {"Doruk", "true", "1"}});

    expectRows("MATCH (p:Person) "
               "RETURN p.name, COUNT { WITH collect(p.name) AS k, count(*) AS c RETURN k } AS n",
               everyPersonOnce);
}

TEST_F(RunConstantReadTest, sortsDedupsAndAggregatesTheImportInsideTheBody) {
    expectRows("MATCH (p:Person) "
               "CALL (p) { MATCH (p)-[:INTERESTED_IN]->(i) RETURN i.name AS k ORDER BY i.name LIMIT 1 } "
               "RETURN p.name, k",
               {{"Remy", "Computers"},
                {"Adam", "Bio"},
                {"Maxime", "Bio"},
                {"Luc", "Animals"},
                {"Martina", "Cooking"},
                {"Suhas", "Gym"},
                {"Cyrus", "Gym"},
                {"Doruk", "Gym"}});

    expectRows("MATCH (p:Person) CALL (p) { WITH DISTINCT p RETURN p.name AS k } RETURN k",
               everyPersonName);

    expectRows("MATCH (p:Person) CALL (p) { RETURN collect(p) AS k } RETURN p.name, size(k)",
               everyPersonOnce);

    expectRows("MATCH (p:Person) CALL (p) { RETURN collect(p.name) AS k } RETURN k",
               {{"Remy"},
                {"Adam"},
                {"Maxime"},
                {"Luc"},
                {"Martina"},
                {"Suhas"},
                {"Cyrus"},
                {"Doruk"}});

    expectRows("MATCH (p:Person) CALL (p) { RETURN count(DISTINCT p) AS k } RETURN p.name, k",
               everyPersonOnce);

    expectRows("MATCH (p:Person) CALL (p) { RETURN count(p) AS k } RETURN p.name, k",
               everyPersonOnce);

    expectRows("MATCH (p:Person) CALL (p) { RETURN count(DISTINCT p.name) AS k } RETURN p.name, k",
               everyPersonOnce);

    expectRows("MATCH (p:Person) CALL (p) { RETURN min(p.name) AS k } RETURN k",
               everyPersonName);

    expectRows("MATCH (p:Person) CALL (p) { RETURN max(p.age) AS k } RETURN p.name, k",
               {{"Remy", "32"},
                {"Adam", "32"},
                {"Maxime", "null"},
                {"Luc", "null"},
                {"Martina", "null"},
                {"Suhas", "null"},
                {"Cyrus", "null"},
                {"Doruk", "null"}});

    expectRows("MATCH (p:Person) CALL (p) { RETURN count(p.name) AS k, min(p.name) AS m } RETURN m, k",
               everyPersonOnce);

    expectRows("MATCH (p:Person) CALL (p) { RETURN count(DISTINCT p) AS d, min(p.name) AS m } RETURN m, d",
               everyPersonOnce);

    expectRows("MATCH (p:Person) CALL (p) { RETURN count(DISTINCT p.name) AS d, min(p.name) AS m } RETURN m, d",
               everyPersonOnce);

    expectRows("MATCH (p:Person) CALL (p) { RETURN count(DISTINCT p) AS d, count(p) AS c } RETURN d, c",
               {{"1", "1"}, {"1", "1"}, {"1", "1"}, {"1", "1"}, {"1", "1"}, {"1", "1"}, {"1", "1"}, {"1", "1"}});

    expectRows("MATCH (p:Person) CALL (p) { RETURN count(DISTINCT p) AS d, count(*) AS c } RETURN d, c",
               {{"1", "1"}, {"1", "1"}, {"1", "1"}, {"1", "1"}, {"1", "1"}, {"1", "1"}, {"1", "1"}, {"1", "1"}});

    expectRows("MATCH (p:Person) CALL (p) { RETURN collect(p) AS k, collect(p) AS l } RETURN size(k), size(l)",
               {{"1", "1"}, {"1", "1"}, {"1", "1"}, {"1", "1"}, {"1", "1"}, {"1", "1"}, {"1", "1"}, {"1", "1"}});

    expectRows("MATCH (p:Person) "
               "CALL (p) { MATCH (p)-->(x) RETURN collect(x) AS k, count(*) AS c } "
               "RETURN p.name, size(k), c",
               {{"Remy", "4", "4"},
                {"Adam", "3", "3"},
                {"Maxime", "2", "2"},
                {"Luc", "2", "2"},
                {"Martina", "1", "1"},
                {"Suhas", "2", "2"},
                {"Cyrus", "2", "2"},
                {"Doruk", "1", "1"}});
}
