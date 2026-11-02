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

// A body run once per input row aggregates its import over that one row, with a collect
// beside any other aggregate. Remy and Adam are 32; no other person has an age.
class PerRunImportAggregatesTest : public TuringTest {
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

TEST_F(PerRunImportAggregatesTest, collectsAndCountsTheImport) {
    expectRows("MATCH (p:Person) CALL (p) { RETURN collect(p.name) AS k, count(*) AS c } RETURN k, c",
               {{"Remy", "1"},
                {"Adam", "1"},
                {"Maxime", "1"},
                {"Luc", "1"},
                {"Martina", "1"},
                {"Suhas", "1"},
                {"Cyrus", "1"},
                {"Doruk", "1"}});
}

TEST_F(PerRunImportAggregatesTest, collectsTheImportBesideItsMinimum) {
    expectRows("MATCH (p:Person) CALL (p) { RETURN collect(p.name) AS k, min(p.name) AS m } RETURN k, m",
               {{"Remy", "Remy"},
                {"Adam", "Adam"},
                {"Maxime", "Maxime"},
                {"Luc", "Luc"},
                {"Martina", "Martina"},
                {"Suhas", "Suhas"},
                {"Cyrus", "Cyrus"},
                {"Doruk", "Doruk"}});
}

TEST_F(PerRunImportAggregatesTest, collectsAndCountsAnImportedEdge) {
    expectRows("MATCH (a)-[e:KNOWS_WELL]->(b) CALL (e) { RETURN collect(e.duration) AS k, count(*) AS c } RETURN k, c",
               {{"20", "1"}, {"20", "1"}, {"200", "1"}});
}

TEST_F(PerRunImportAggregatesTest, collectsAndCountsWhatAFilterLeaves) {
    expectRows("MATCH (p:Person) "
               "CALL (p) { WITH p WHERE p.age > 1 RETURN collect(p.name) AS k, count(*) AS c } "
               "RETURN p.name, k, c",
               {{"Remy", "Remy", "1"},
                {"Adam", "Adam", "1"},
                {"Maxime", "", "0"},
                {"Luc", "", "0"},
                {"Martina", "", "0"},
                {"Suhas", "", "0"},
                {"Cyrus", "", "0"},
                {"Doruk", "", "0"}});
}

TEST_F(PerRunImportAggregatesTest, collectsTheImportBesideEveryReduction) {
    expectRows("MATCH (p:Person) CALL (p) { "
               "RETURN collect(p.name) AS k, avg(p.age) AS a, stDev(p.age) AS d, sum(DISTINCT p.age) AS s, count(DISTINCT p.age) AS c "
               "} RETURN p.name, k, a, d, s, c",
               {{"Remy", "Remy", "32", "0", "32", "1"},
                {"Adam", "Adam", "32", "0", "32", "1"},
                {"Maxime", "Maxime", "null", "0", "0", "0"},
                {"Luc", "Luc", "null", "0", "0", "0"},
                {"Martina", "Martina", "null", "0", "0", "0"},
                {"Suhas", "Suhas", "null", "0", "0", "0"},
                {"Cyrus", "Cyrus", "null", "0", "0", "0"},
                {"Doruk", "Doruk", "null", "0", "0", "0"}});
}

TEST_F(PerRunImportAggregatesTest, takesAPercentileOfTheImportBesideACount) {
    expectRows("MATCH (p:Person) CALL (p) { RETURN percentileDisc(p.age, 0.5) AS q, count(*) AS c } RETURN p.name, q, c",
               {{"Remy", "32", "1"},
                {"Adam", "32", "1"},
                {"Maxime", "null", "1"},
                {"Luc", "null", "1"},
                {"Martina", "null", "1"},
                {"Suhas", "null", "1"},
                {"Cyrus", "null", "1"},
                {"Doruk", "null", "1"}});
}
