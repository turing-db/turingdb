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

// A body run once per input row, behind a keyless WITH, walks from and to the nodes it
// imports. KNOWS_WELL leaves Remy for Adam, Adam for Remy and Ghosts for Remy.
class RunConstantWalkTest : public TuringTest {
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

TEST_F(RunConstantWalkTest, countsTheOthersSharingAnInterest) {
    expectRows("MATCH (p:Person) "
               "CALL (p) { "
               "  WITH count(*) AS c "
               "  MATCH (p)-[:INTERESTED_IN]->(i)<-[:INTERESTED_IN]-(q) WHERE q <> p "
               "  RETURN count(q) AS n "
               "} "
               "RETURN p.name, n",
               {{"Remy", "1"},
                {"Adam", "2"},
                {"Maxime", "1"},
                {"Luc", "1"},
                {"Martina", "1"},
                {"Suhas", "2"},
                {"Cyrus", "2"},
                {"Doruk", "2"}});
}

TEST_F(RunConstantWalkTest, padsAnOptionalMatchFromTheImport) {
    expectRows("MATCH (p:Person) "
               "CALL (p) { WITH count(*) AS c OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f) RETURN f.name AS f } "
               "RETURN p.name, f",
               {{"Remy", "Adam"},
                {"Adam", "Remy"},
                {"Maxime", "null"},
                {"Luc", "null"},
                {"Martina", "null"},
                {"Suhas", "null"},
                {"Cyrus", "null"},
                {"Doruk", "null"}});
}

TEST_F(RunConstantWalkTest, collectsAVariableLengthWalkFromTheImport) {
    expectRows("MATCH (p:Person) "
               "CALL (p) { "
               "  WITH count(*) AS c "
               "  MATCH (p)-[:KNOWS_WELL*1..2]->(f) "
               "  WITH f ORDER BY f.name "
               "  RETURN collect(f.name) AS fs "
               "} "
               "RETURN p.name, fs",
               {{"Remy", "Adam, Remy"},
                {"Adam", "Adam, Remy"},
                {"Maxime", ""},
                {"Luc", ""},
                {"Martina", ""},
                {"Suhas", ""},
                {"Cyrus", ""},
                {"Doruk", ""}});
}

TEST_F(RunConstantWalkTest, collectsAVariableLengthWalkToTheImport) {
    expectRows("MATCH (b:Person) "
               "CALL (b) { "
               "  WITH count(*) AS c "
               "  MATCH (x)-[:KNOWS_WELL*1..2]->(b) "
               "  WITH x ORDER BY x.name "
               "  RETURN collect(x.name) AS xs "
               "} "
               "RETURN b.name, xs",
               {{"Remy", "Adam, Ghosts, Remy"},
                {"Adam", "Adam, Ghosts, Remy"},
                {"Maxime", ""},
                {"Luc", ""},
                {"Martina", ""},
                {"Suhas", ""},
                {"Cyrus", ""},
                {"Doruk", ""}});
}

TEST_F(RunConstantWalkTest, walksBetweenTwoImports) {
    const std::vector<StringRowSink::Row> onePathEach = {{"Remy", "Remy", "1"},
                                                         {"Remy", "Adam", "1"},
                                                         {"Adam", "Remy", "1"},
                                                         {"Adam", "Adam", "1"}};

    expectRows("MATCH (a:Person), (b:Person) "
               "WHERE a.name IN ['Remy', 'Adam'] AND b.name IN ['Remy', 'Adam'] "
               "CALL (a, b) { WITH count(*) AS c MATCH (a)-[:KNOWS_WELL*1..2]->(b) RETURN count(*) AS n } "
               "RETURN a.name, b.name, n",
               onePathEach);

    expectRows("MATCH (a:Person), (b:Person) "
               "WHERE a.name IN ['Remy', 'Adam'] AND b.name IN ['Remy', 'Adam'] "
               "CALL (a, b) { MATCH (a)-[:KNOWS_WELL*1..2]->(b) RETURN count(*) AS n } "
               "RETURN a.name, b.name, n",
               onePathEach);
}

TEST_F(RunConstantWalkTest, comprehendsAPatternFromTheImport) {
    expectRows("MATCH (p:Person {name: 'Remy'}) "
               "CALL (p) { WITH count(*) AS c RETURN [(p)-[:KNOWS_WELL]->(f) | f.name] AS k } "
               "RETURN k",
               {{"Adam"}});
}

TEST_F(RunConstantWalkTest, countsASortedAndCutWalkFromTheImport) {
    expectRows("MATCH (p:Person {name: 'Remy'}) "
               "CALL (p) { "
               "  WITH count(*) AS c "
               "  RETURN COUNT { MATCH (p)-[:INTERESTED_IN]->(i) RETURN i ORDER BY i.name LIMIT 2 } AS k "
               "} "
               "RETURN k",
               {{"2"}});
}

TEST_F(RunConstantWalkTest, testsAPatternFromTheImportInAComprehension) {
    expectRows("MATCH (p:Person {name: 'Remy'}) "
               "CALL (p) { WITH count(*) AS c RETURN [x IN [1] | exists((p)-[:KNOWS_WELL]->())] AS k } "
               "RETURN k",
               {{"true"}});
}

TEST_F(RunConstantWalkTest, readsTheImportInACaseAndAnUnwind) {
    expectRows("MATCH (p:Person {name: 'Remy'}) "
               "CALL (p) { WITH count(*) AS c RETURN CASE WHEN p.age > 30 THEN 'old' ELSE 'young' END AS k } "
               "RETURN k",
               {{"old"}});

    expectRows("MATCH (p:Person {name: 'Remy'}) "
               "CALL (p) { WITH count(*) AS c UNWIND [1, 2] AS x RETURN x + p.age AS k } "
               "RETURN k",
               {{"33"}, {"34"}});
}

TEST_F(RunConstantWalkTest, readsAnIntegerImportInAComprehension) {
    expectRows("UNWIND [5, 0] AS a CALL (a) { WITH count(*) AS c RETURN [x IN [1] | x + a] AS k } RETURN a, k",
               {{"5", "6"}, {"0", "1"}});
}
