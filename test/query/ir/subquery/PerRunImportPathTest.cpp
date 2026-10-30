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

// A path a body run once per input row returns is the imported path, read after the CALL as
// any path is. KNOWS_WELL leaves Remy for Adam, Adam for Remy and Ghosts for Remy.
class PerRunImportPathTest : public TuringTest {
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

TEST_F(PerRunImportPathTest, readsTheLengthOfTheReturnedPath) {
    expectRows("MATCH pt = (a)-[:KNOWS_WELL]->(b) CALL (pt) { WITH count(*) AS c RETURN pt AS k } RETURN length(k)",
               {{"1"}, {"1"}, {"1"}});
}

TEST_F(PerRunImportPathTest, readsTheNodesOfTheReturnedPath) {
    expectRows("MATCH pt = (a)-[:KNOWS_WELL]->(b) CALL (pt) { WITH count(*) AS c RETURN pt AS k } "
               "RETURN [n IN nodes(k) | n.name]",
               {{"Remy, Adam"}, {"Adam, Remy"}, {"Ghosts, Remy"}});
}

TEST_F(PerRunImportPathTest, readsTheRelationshipsOfTheReturnedPathPastAWith) {
    expectRows("MATCH pt = (a)-[:KNOWS_WELL]->(b) CALL (pt) { WITH count(*) AS c RETURN pt AS k } "
               "WITH k RETURN size(relationships(k))",
               {{"1"}, {"1"}, {"1"}});
}

TEST_F(PerRunImportPathTest, readsTheLengthOfAPathTheBodyFilters) {
    expectRows("MATCH pt = (a)-[:KNOWS_WELL]->(b) CALL (pt) { WITH count(*) AS c WHERE c > 0 RETURN pt AS k } "
               "RETURN length(k)",
               {{"1"}, {"1"}, {"1"}});
}

TEST_F(PerRunImportPathTest, readsTheLengthOfAPathEveryUnionBranchReturns) {
    expectRows("MATCH pt = (a)-[:KNOWS_WELL]->(b) "
               "CALL (pt) { WITH count(*) AS c RETURN pt AS k UNION WITH count(*) AS c RETURN pt AS k } "
               "RETURN length(k)",
               {{"1"}, {"1"}, {"1"}});
}

TEST_F(PerRunImportPathTest, readsTheNodesOfAPathTheBodyPublishesPastItsAggregate) {
    expectRows("MATCH pt = (a)-[:KNOWS_WELL]->(b) "
               "CALL (pt) { WITH count(*) AS c WITH pt RETURN [n IN nodes(pt) | n.name] AS names } "
               "RETURN names",
               {{"Remy, Adam"}, {"Adam, Remy"}, {"Ghosts, Remy"}});
}
