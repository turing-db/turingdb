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

// A list comprehension, a fold or a list predicate in a body run once per input row reads a
// property of an imported node as it reads any variable in scope. Remy and Adam are 32; no
// other person has an age.
class PerRunImportListExpressionTest : public TuringTest {
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

TEST_F(PerRunImportListExpressionTest, readsAPropertyOfTheImportInAComprehension) {
    expectRows("MATCH (p:Person) CALL (p) { WITH count(*) AS c RETURN [x IN [1] | p.name] AS k } RETURN k",
               {{"Remy"},
                {"Adam"},
                {"Maxime"},
                {"Luc"},
                {"Martina"},
                {"Suhas"},
                {"Cyrus"},
                {"Doruk"}});
}

TEST_F(PerRunImportListExpressionTest, foldsAPropertyOfTheImportInAReduce) {
    expectRows("MATCH (p:Person) CALL (p) { WITH count(*) AS c RETURN reduce(s = '', x IN [1, 2] | s + p.name) AS k } RETURN k",
               {{"RemyRemy"},
                {"AdamAdam"},
                {"MaximeMaxime"},
                {"LucLuc"},
                {"MartinaMartina"},
                {"SuhasSuhas"},
                {"CyrusCyrus"},
                {"DorukDoruk"}});
}

TEST_F(PerRunImportListExpressionTest, decidesAWhenOnAListPredicateOverTheImport) {
    expectRows("MATCH (p:Person) CALL (p) { WHEN any(x IN [1] WHERE x < p.age) THEN RETURN 1 AS k } RETURN p.name, k",
               {{"Remy", "1"}, {"Adam", "1"}});
}

TEST_F(PerRunImportListExpressionTest, countsUnderAWhenOnAnAllReduceOverTheRow) {
    expectRows("MATCH (p:Person) "
               "RETURN p.name, COUNT { WHEN allReduce(s = 0, x IN [1, 2] | s + x, s < p.age) THEN RETURN 1 AS k } AS c",
               {{"Remy", "1"},
                {"Adam", "1"},
                {"Maxime", "0"},
                {"Luc", "0"},
                {"Martina", "0"},
                {"Suhas", "0"},
                {"Cyrus", "0"},
                {"Doruk", "0"}});
}

TEST_F(PerRunImportListExpressionTest, filtersAComprehensionOnTheImport) {
    expectRows("MATCH (p:Person) CALL (p) { WITH count(*) AS c RETURN [x IN [1, 2] WHERE x < p.age | p.name] AS k } RETURN p.name, k",
               {{"Remy", "Remy, Remy"},
                {"Adam", "Adam, Adam"},
                {"Maxime", ""},
                {"Luc", ""},
                {"Martina", ""},
                {"Suhas", ""},
                {"Cyrus", ""},
                {"Doruk", ""}});
}

TEST_F(PerRunImportListExpressionTest, nestsAComprehensionOverTheImport) {
    expectRows("MATCH (p:Person) CALL (p) { WITH count(*) AS c RETURN [x IN [1] | [y IN [2] | p.name]] AS k } RETURN k",
               {{"[Remy]"},
                {"[Adam]"},
                {"[Maxime]"},
                {"[Luc]"},
                {"[Martina]"},
                {"[Suhas]"},
                {"[Cyrus]"},
                {"[Doruk]"}});
}

TEST_F(PerRunImportListExpressionTest, branchesOnTheImportInAComprehension) {
    expectRows("MATCH (p:Person) CALL (p) { WITH count(*) AS c "
               "RETURN [x IN [1] | CASE WHEN p.age > 1 THEN p.name ELSE 'none' END] AS k } RETURN k",
               {{"Remy"},
                {"Adam"},
                {"none"},
                {"none"},
                {"none"},
                {"none"},
                {"none"},
                {"none"}});
}

TEST_F(PerRunImportListExpressionTest, addsAnElementToAPropertyOfTheImport) {
    expectRows("MATCH (p:Person) CALL (p) { WITH count(*) AS c RETURN [x IN [1] | x + p.age] AS k } RETURN p.name, k",
               {{"Remy", "33"},
                {"Adam", "33"},
                {"Maxime", "null"},
                {"Luc", "null"},
                {"Martina", "null"},
                {"Suhas", "null"},
                {"Cyrus", "null"},
                {"Doruk", "null"}});
}

TEST_F(PerRunImportListExpressionTest, decidesASingleOverTheImport) {
    expectRows("MATCH (p:Person) CALL (p) { WITH count(*) AS c RETURN single(x IN [1, 40] WHERE x > p.age) AS k } RETURN p.name, k",
               {{"Remy", "true"},
                {"Adam", "true"},
                {"Maxime", "null"},
                {"Luc", "null"},
                {"Martina", "null"},
                {"Suhas", "null"},
                {"Cyrus", "null"},
                {"Doruk", "null"}});
}

TEST_F(PerRunImportListExpressionTest, unwindsAComprehensionOverTheImport) {
    expectRows("MATCH (p:Person) CALL (p) { WITH count(*) AS c UNWIND [x IN [1, 2] | p.name] AS y RETURN y } RETURN y",
               {{"Remy"}, {"Remy"},
                {"Adam"}, {"Adam"},
                {"Maxime"}, {"Maxime"},
                {"Luc"}, {"Luc"},
                {"Martina"}, {"Martina"},
                {"Suhas"}, {"Suhas"},
                {"Cyrus"}, {"Cyrus"},
                {"Doruk"}, {"Doruk"}});
}

TEST_F(PerRunImportListExpressionTest, readsAPropertyOfAnImportedEdgeInAComprehension) {
    expectRows("MATCH ()-[e:KNOWS_WELL]->() CALL (e) { WITH count(*) AS c RETURN [x IN [1] | e.duration] AS k } RETURN k",
               {{"20"}, {"20"}, {"200"}});
}
