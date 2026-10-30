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

const std::vector<StringRowSink::Row> everyPersonOnce = {{"Remy", "1"},
                                                         {"Adam", "1"},
                                                         {"Maxime", "1"},
                                                         {"Luc", "1"},
                                                         {"Martina", "1"},
                                                         {"Suhas", "1"},
                                                         {"Cyrus", "1"},
                                                         {"Doruk", "1"}};

const std::vector<StringRowSink::Row> interestCounts = {{"Remy", "3"},
                                                        {"Adam", "2"},
                                                        {"Maxime", "2"},
                                                        {"Luc", "2"},
                                                        {"Martina", "1"},
                                                        {"Suhas", "2"},
                                                        {"Cyrus", "2"},
                                                        {"Doruk", "1"}};

const std::vector<StringRowSink::Row> everyPersonName = {{"Remy"},
                                                         {"Adam"},
                                                         {"Maxime"},
                                                         {"Luc"},
                                                         {"Martina"},
                                                         {"Suhas"},
                                                         {"Cyrus"},
                                                         {"Doruk"}};

}

// A body run once per input row reads its import past a keyless WITH or a NEXT, beside the
// other clauses. In SimpleGraph only Remy and Adam know someone well, each the other.
class PerRunImportCompositionTest : public TuringTest {
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

    void expectRowsInOrder(std::string_view query, const std::vector<StringRowSink::Row>& expected) {
        StringRowSink sink;
        QueryStatus status;
        runQuery(query, sink, status);
        ASSERT_TRUE(status.isOk()) << query << ": " << status.getError();

        EXPECT_EQ(sink.getRows(), expected) << "query: " << query;
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

TEST_F(PerRunImportCompositionTest, ordersAndFiltersOnTheImportPastAKeylessWith) {
    expectRows("MATCH (p:Person) "
               "CALL (p) { MATCH (p)-[:INTERESTED_IN]->(i) WITH count(i) AS n RETURN p.name AS name, n ORDER BY p.name } "
               "RETURN name, n ORDER BY name",
               interestCounts);

    expectRows("MATCH (p:Person) "
               "CALL (p) { MATCH (p)-[:INTERESTED_IN]->(i) WITH count(i) AS n WHERE p.name STARTS WITH 'M' RETURN p.name AS name, n } "
               "RETURN name, n ORDER BY name",
               {{"Martina", "1"}, {"Maxime", "2"}});
}

TEST_F(PerRunImportCompositionTest, ordersAndSkipsTheRowsAMatchFromTheImportFindsPastAKeylessWith) {
    expectRows("MATCH (p:Person) "
               "CALL (p) { WITH count(*) AS c MATCH (p)-[:INTERESTED_IN]->(i) RETURN i.name AS interest ORDER BY i.name LIMIT 1 } "
               "RETURN p.name, interest ORDER BY p.name",
               {{"Remy", "Computers"},
                {"Adam", "Bio"},
                {"Maxime", "Bio"},
                {"Luc", "Animals"},
                {"Martina", "Cooking"},
                {"Suhas", "Gym"},
                {"Cyrus", "Gym"},
                {"Doruk", "Gym"}});

    expectRows("MATCH (p:Person) "
               "CALL (p) { WITH count(*) AS c MATCH (p)-[:INTERESTED_IN]->(i) WITH i, c ORDER BY i.name SKIP 1 RETURN collect(i.name) AS rest, c } "
               "RETURN p.name, rest, c ORDER BY p.name",
               {{"Remy", "Eighties, Ghosts", "1"},
                {"Adam", "Cooking", "1"},
                {"Maxime", "Padel", "1"},
                {"Luc", "Computers", "1"},
                {"Suhas", "JiuJitsu", "1"},
                {"Cyrus", "Travel", "1"}});
}

TEST_F(PerRunImportCompositionTest, comparesTheImportPastAKeylessWith) {
    expectRows("MATCH (p:Person) "
               "CALL (p) { WITH count(*) AS c MATCH (x:Person) WHERE x <> p RETURN count(x) AS others, c } "
               "RETURN p.name, others, c ORDER BY p.name",
               {{"Remy", "7", "1"},
                {"Adam", "7", "1"},
                {"Maxime", "7", "1"},
                {"Luc", "7", "1"},
                {"Martina", "7", "1"},
                {"Suhas", "7", "1"},
                {"Cyrus", "7", "1"},
                {"Doruk", "7", "1"}});

    expectRows("MATCH (p:Person) "
               "CALL (p) { WITH count(*) AS c MATCH (x:Person)-[:INTERESTED_IN]->(i) WHERE x = p RETURN DISTINCT p.name AS name, c } "
               "RETURN name, c ORDER BY name",
               everyPersonOnce);
}

TEST_F(PerRunImportCompositionTest, matchesOptionallyFromTheImportPastAKeylessWith) {
    expectRows("MATCH (p:Person) "
               "CALL (p) { WITH count(*) AS c OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f) RETURN p.name AS name, c, f.name AS friend } "
               "RETURN name, c, friend ORDER BY name",
               {{"Remy", "1", "Adam"},
                {"Adam", "1", "Remy"},
                {"Maxime", "1", "null"},
                {"Luc", "1", "null"},
                {"Martina", "1", "null"},
                {"Suhas", "1", "null"},
                {"Cyrus", "1", "null"},
                {"Doruk", "1", "null"}});
}

TEST_F(PerRunImportCompositionTest, readsANullImportPastAKeylessWith) {
    expectRows("MATCH (p:Person) "
               "OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f) "
               "CALL (f) { WITH count(*) AS c RETURN f.name AS fn, f IS NULL AS isnull, c } "
               "RETURN p.name, fn, isnull, c ORDER BY p.name",
               {{"Remy", "Adam", "false", "1"},
                {"Adam", "Remy", "false", "1"},
                {"Maxime", "null", "true", "1"},
                {"Luc", "null", "true", "1"},
                {"Martina", "null", "true", "1"},
                {"Suhas", "null", "true", "1"},
                {"Cyrus", "null", "true", "1"},
                {"Doruk", "null", "true", "1"}});

    expectRows("MATCH (p:Person) "
               "OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f) "
               "CALL (f) { WITH count(*) AS c OPTIONAL MATCH (f)-[:INTERESTED_IN]->(i) RETURN count(i) AS k, c } "
               "RETURN p.name, k, c ORDER BY p.name",
               {{"Remy", "2", "1"},
                {"Adam", "3", "1"},
                {"Maxime", "0", "1"},
                {"Luc", "0", "1"},
                {"Martina", "0", "1"},
                {"Suhas", "0", "1"},
                {"Cyrus", "0", "1"},
                {"Doruk", "0", "1"}});
}

TEST_F(PerRunImportCompositionTest, unwindsAListOfTheImportPastAKeylessWith) {
    expectRows("MATCH (p:Person) "
               "CALL (p) { WITH count(*) AS c UNWIND [p, p] AS q RETURN q.name AS name, c } "
               "RETURN name, c ORDER BY name",
               {{"Remy", "1"},
                {"Remy", "1"},
                {"Adam", "1"},
                {"Adam", "1"},
                {"Maxime", "1"},
                {"Maxime", "1"},
                {"Luc", "1"},
                {"Luc", "1"},
                {"Martina", "1"},
                {"Martina", "1"},
                {"Suhas", "1"},
                {"Suhas", "1"},
                {"Cyrus", "1"},
                {"Cyrus", "1"},
                {"Doruk", "1"},
                {"Doruk", "1"}});
}

TEST_F(PerRunImportCompositionTest, nestsACallImportingTheImportPastAKeylessWith) {
    expectRows("MATCH (p:Person) "
               "CALL (p) { WITH count(*) AS c CALL (p) { MATCH (p)-[:INTERESTED_IN]->(i) RETURN count(i) AS k } RETURN p.name AS name, c, k } "
               "RETURN name, c, k ORDER BY name",
               {{"Remy", "1", "3"},
                {"Adam", "1", "2"},
                {"Maxime", "1", "2"},
                {"Luc", "1", "2"},
                {"Martina", "1", "1"},
                {"Suhas", "1", "2"},
                {"Cyrus", "1", "2"},
                {"Doruk", "1", "1"}});
}

TEST_F(PerRunImportCompositionTest, readsTheOuterVariablePastAKeylessWithInASubqueryExpression) {
    expectRows("MATCH (p:Person) "
               "RETURN p.name, COUNT { WITH count(*) AS c MATCH (p)-[:INTERESTED_IN]->(i) RETURN i } AS k ORDER BY p.name",
               interestCounts);

    expectRows("MATCH (p:Person) "
               "RETURN p.name, COLLECT { WITH count(*) AS c MATCH (p)-[:INTERESTED_IN]->(i) RETURN i.name ORDER BY i.name } AS k ORDER BY p.name",
               {{"Remy", "Computers, Eighties, Ghosts"},
                {"Adam", "Bio, Cooking"},
                {"Maxime", "Bio, Padel"},
                {"Luc", "Animals, Computers"},
                {"Martina", "Cooking"},
                {"Suhas", "Gym, JiuJitsu"},
                {"Cyrus", "Gym, Travel"},
                {"Doruk", "Gym"}});

    expectRows("MATCH (p:Person) "
               "RETURN p.name, EXISTS { WITH count(*) AS c MATCH (p)-[:KNOWS_WELL]->(f) RETURN f } AS k ORDER BY p.name",
               {{"Remy", "true"},
                {"Adam", "true"},
                {"Maxime", "false"},
                {"Luc", "false"},
                {"Martina", "false"},
                {"Suhas", "false"},
                {"Cyrus", "false"},
                {"Doruk", "false"}});
}

TEST_F(PerRunImportCompositionTest, passesTheImportPastAKeylessWithToASubqueryExpression) {
    expectRows("MATCH (p:Person) "
               "CALL (p) { WITH count(*) AS c RETURN COUNT { MATCH (p)-[:INTERESTED_IN]->(i) } AS k, c } "
               "RETURN p.name, k, c ORDER BY p.name",
               {{"Remy", "3", "1"},
                {"Adam", "2", "1"},
                {"Maxime", "2", "1"},
                {"Luc", "2", "1"},
                {"Martina", "1", "1"},
                {"Suhas", "2", "1"},
                {"Cyrus", "2", "1"},
                {"Doruk", "1", "1"}});

    expectRows("MATCH (p:Person) "
               "CALL (p) { WITH count(*) AS c RETURN EXISTS { MATCH (p)-[:KNOWS_WELL]->(f) } AS k, c } "
               "RETURN p.name, k, c ORDER BY p.name",
               {{"Remy", "true", "1"},
                {"Adam", "true", "1"},
                {"Maxime", "false", "1"},
                {"Luc", "false", "1"},
                {"Martina", "false", "1"},
                {"Suhas", "false", "1"},
                {"Cyrus", "false", "1"},
                {"Doruk", "false", "1"}});

    expectRows("MATCH (p:Person) "
               "CALL (p) { WITH count(*) AS c RETURN COLLECT { MATCH (p)-[:INTERESTED_IN]->(i) RETURN i.name ORDER BY i.name } AS k, c } "
               "RETURN p.name, k, c ORDER BY p.name",
               {{"Remy", "Computers, Eighties, Ghosts", "1"},
                {"Adam", "Bio, Cooking", "1"},
                {"Maxime", "Bio, Padel", "1"},
                {"Luc", "Animals, Computers", "1"},
                {"Martina", "Cooking", "1"},
                {"Suhas", "Gym, JiuJitsu", "1"},
                {"Cyrus", "Gym, Travel", "1"},
                {"Doruk", "Gym", "1"}});
}

TEST_F(PerRunImportCompositionTest, evaluatesExpressionsOnTheImportPastAKeylessWith) {
    expectRows("MATCH (p:Person) "
               "CALL (p) { WITH count(*) AS c RETURN CASE WHEN p.age IS NULL THEN 'none' ELSE 'some' END AS a, c } "
               "RETURN p.name, a, c ORDER BY p.name",
               {{"Remy", "some", "1"},
                {"Adam", "some", "1"},
                {"Maxime", "none", "1"},
                {"Luc", "none", "1"},
                {"Martina", "none", "1"},
                {"Suhas", "none", "1"},
                {"Cyrus", "none", "1"},
                {"Doruk", "none", "1"}});

    expectRows("MATCH (p:Person) "
               "CALL (p) { WITH count(*) AS c RETURN reduce(s = 0, x IN [p.age, 1] | s + coalesce(x, 0)) AS r, c } "
               "RETURN p.name, r, c ORDER BY p.name",
               {{"Remy", "33", "1"},
                {"Adam", "33", "1"},
                {"Maxime", "1", "1"},
                {"Luc", "1", "1"},
                {"Martina", "1", "1"},
                {"Suhas", "1", "1"},
                {"Cyrus", "1", "1"},
                {"Doruk", "1", "1"}});

    expectRows("MATCH (p:Person) "
               "CALL (p) { WITH count(*) AS c RETURN all(x IN [p.name] WHERE x STARTS WITH 'M') AS r, c } "
               "RETURN p.name, r, c ORDER BY p.name",
               {{"Remy", "false", "1"},
                {"Adam", "false", "1"},
                {"Maxime", "true", "1"},
                {"Luc", "false", "1"},
                {"Martina", "true", "1"},
                {"Suhas", "false", "1"},
                {"Cyrus", "false", "1"},
                {"Doruk", "false", "1"}});
}

TEST_F(PerRunImportCompositionTest, readsAnImportedEdgePastAKeylessWith) {
    expectRows("MATCH (a)-[e:KNOWS_WELL]->(b) "
               "CALL (e) { WITH count(*) AS c RETURN e.duration AS d, c } "
               "RETURN d, c ORDER BY d",
               {{"20", "1"}, {"20", "1"}, {"200", "1"}});

    expectRows("MATCH (a)-[e:KNOWS_WELL]->(b) "
               "CALL (e) { WITH count(*) AS c RETURN e.name AS n, type(e) AS t, c } "
               "RETURN n, t, c ORDER BY n",
               {{"Adam -> Remy", "KNOWS_WELL", "1"},
                {"Ghosts -> Remy", "KNOWS_WELL", "1"},
                {"Remy -> Adam", "KNOWS_WELL", "1"}});
}

TEST_F(PerRunImportCompositionTest, readsAnImportedPathPastAKeylessWith) {
    expectRows("MATCH p = (a)-[:KNOWS_WELL]->(b) "
               "CALL (p) { WITH count(*) AS c RETURN length(p) AS l, c } "
               "RETURN l, c",
               {{"1", "1"}, {"1", "1"}, {"1", "1"}});

    expectRows("MATCH p = (a)-[:KNOWS_WELL]->(b) "
               "CALL (p) { WITH count(*) AS c RETURN nodes(p) AS ns, c } "
               "RETURN size(ns), c",
               {{"2", "1"}, {"2", "1"}, {"2", "1"}});
}

TEST_F(PerRunImportCompositionTest, indexesTheNodesOfAPathOutsideAPerRunBody) {
    expectRows("MATCH p = (a)-[:KNOWS_WELL]->(b) "
               "RETURN nodes(p)[0].name AS s ORDER BY s",
               {{"Adam"}, {"Ghosts"}, {"Remy"}});

    expectRows("MATCH p = (a)-[:KNOWS_WELL]->(b) "
               "CALL (p) { RETURN nodes(p)[0].name AS s } "
               "RETURN s ORDER BY s",
               {{"Adam"}, {"Ghosts"}, {"Remy"}});
}

TEST_F(PerRunImportCompositionTest, indexesAListOfNodesOutsideAPerRunBody) {
    expectRows("MATCH (p:Person) "
               "RETURN [p][0].name AS s ORDER BY s",
               everyPersonName);

    expectRows("MATCH (p:Person) "
               "CALL (p) { RETURN [p][0].name AS s } "
               "RETURN s ORDER BY s",
               everyPersonName);

    expectRows("MATCH (p:Person) "
               "WITH [p] AS ps "
               "RETURN ps[0].name AS s, count(*) AS c ORDER BY s",
               everyPersonOnce);

    expectRows("MATCH (p:Person) "
               "WITH [p] AS ps "
               "CALL (ps) { RETURN ps[0].name AS s } "
               "RETURN s ORDER BY s",
               everyPersonName);

    expectRows("MATCH (p:Person) "
               "WITH p, [p] AS ps "
               "CALL (ps) { RETURN ps[0].name AS s } "
               "RETURN s ORDER BY s",
               everyPersonName);

    expectRows("MATCH (p:Person) "
               "WITH [p] AS ps "
               "RETURN COUNT { MATCH (x:Person) WHERE x = ps[0] } AS k",
               {{"1"}, {"1"}, {"1"}, {"1"}, {"1"}, {"1"}, {"1"}, {"1"}});
}

TEST_F(PerRunImportCompositionTest, readsAnImportedListInAPerRunBody) {
    expectRows("MATCH (p:Person) "
               "WITH [p.name] AS ns "
               "CALL (ns) { RETURN ns[0] AS s, count(*) AS c } "
               "RETURN s, c ORDER BY s",
               everyPersonOnce);

    expectRows("MATCH (p:Person) "
               "WITH [p] AS ps "
               "CALL (ps) { RETURN size(ps) AS s, count(*) AS c } "
               "RETURN s, c",
               {{"1", "1"}, {"1", "1"}, {"1", "1"}, {"1", "1"}, {"1", "1"}, {"1", "1"}, {"1", "1"}, {"1", "1"}});

    expectRows("MATCH (p:Person) "
               "WITH [p] AS ps "
               "CALL (ps) { UNWIND ps AS q RETURN q.name AS s, count(*) AS c } "
               "RETURN s, c ORDER BY s",
               everyPersonOnce);

    expectRows("MATCH (p:Person) "
               "WITH [p] AS ps "
               "CALL (ps) { RETURN [q IN ps | q.name] AS s, count(*) AS c } "
               "RETURN s, c ORDER BY s[0]",
               everyPersonOnce);

    expectRows("MATCH (p:Person) "
               "WITH p, [p] AS ps "
               "CALL (p, ps) { RETURN p IN ps AS s, count(*) AS c } "
               "RETURN s, c",
               {{"true", "1"},
                {"true", "1"},
                {"true", "1"},
                {"true", "1"},
                {"true", "1"},
                {"true", "1"},
                {"true", "1"},
                {"true", "1"}});

    expectRows("MATCH (p:Person) "
               "WITH [p] AS ps "
               "CALL (ps) { RETURN size(ps[0..1]) AS s, count(*) AS c } "
               "RETURN s, c",
               {{"1", "1"}, {"1", "1"}, {"1", "1"}, {"1", "1"}, {"1", "1"}, {"1", "1"}, {"1", "1"}, {"1", "1"}});
}

TEST_F(PerRunImportCompositionTest, readsTheImportPastChainedBarriers) {
    expectRows("MATCH (p:Person) "
               "CALL (p) { WITH count(*) AS c WITH count(*) AS d RETURN p.name AS name, d } "
               "RETURN name, d ORDER BY name",
               everyPersonOnce);

    expectRows("MATCH (p:Person) "
               "CALL (p) { MATCH (p)-[:INTERESTED_IN]->(i) WITH count(i) AS c WITH p.name AS k, c RETURN k, c } "
               "RETURN k, c ORDER BY k",
               interestCounts);

    expectRows("MATCH (p:Person) "
               "CALL (p) { MATCH (p)-[:INTERESTED_IN]->(i) WITH count(i) AS c WITH p.name AS k, sum(c) AS s RETURN k, s } "
               "RETURN k, s ORDER BY k",
               interestCounts);
}

TEST_F(PerRunImportCompositionTest, matchesFromTheImportPastANextSegment) {
    expectRows("MATCH (p:Person) "
               "CALL (p) { MATCH (p)-[:INTERESTED_IN]->(i) RETURN count(i) AS c NEXT MATCH (p)-[:KNOWS_WELL]->(f) RETURN p.name AS name, c, f.name AS friend } "
               "RETURN name, c, friend ORDER BY name",
               {{"Adam", "2", "Remy"}, {"Remy", "3", "Adam"}});
}

TEST_F(PerRunImportCompositionTest, returnsNoRowsForNoInputRow) {
    expectRows("MATCH (p:Person) WHERE p.name = 'Nobody' "
               "CALL (p) { WITH count(*) AS c RETURN p.name AS name, c } "
               "RETURN name, c",
               {});
}

TEST_F(PerRunImportCompositionTest, runsTheBodyForEachRepeatedInputRow) {
    expectRows("MATCH (p:Person)-[:INTERESTED_IN]->(i) "
               "CALL (p) { WITH count(*) AS c RETURN p.name AS n, c } "
               "RETURN n, sum(c) AS s ORDER BY n",
               interestCounts);
}

TEST_F(PerRunImportCompositionTest, ordersTheUnionOfTwoKeylessBodies) {
    expectRowsInOrder("MATCH (p:Person) "
                      "CALL (p) { WITH count(*) AS c RETURN p.name AS name, c UNION ALL WITH count(*) AS c RETURN p.name + '!' AS name, c } "
                      "RETURN name, c ORDER BY name LIMIT 4",
                      {{"Adam", "1"}, {"Adam!", "1"}, {"Cyrus", "1"}, {"Cyrus!", "1"}});
}

TEST_F(PerRunImportCompositionTest, runsTheBodyOverManyInputRows) {
    expectRows("UNWIND range(1, 70000) AS i "
               "CALL (i) { WITH count(*) AS c RETURN i + c AS j } "
               "RETURN sum(j)",
               {{"2450105000"}});
}
