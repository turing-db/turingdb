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

// A body run once per input row reads the relationships of a variable-length pattern it
// imports as a body run over every row does. KNOWS_WELL leads Remy to Adam in 1 hop of
// duration 20 and back to Remy in 2.
class PerRunImportWalkTest : public TuringTest {
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

TEST_F(PerRunImportWalkTest, sizesTheWalkPastAKeylessWith) {
    expectRows("MATCH (:Person {name: 'Remy'})-[e:KNOWS_WELL*1..2]->(b) CALL (e) { WITH count(*) AS c RETURN size(e) AS n } RETURN b.name, n",
               {{"Adam", "1"}, {"Remy", "2"}});
}

TEST_F(PerRunImportWalkTest, sizesTheWalkBesideAnAggregate) {
    expectRows("MATCH (:Person {name: 'Remy'})-[e:KNOWS_WELL*1..2]->(b) CALL (e) { RETURN size(e) AS n, count(*) AS c } RETURN b.name, n, c",
               {{"Adam", "1", "1"}, {"Remy", "2", "1"}});
}

TEST_F(PerRunImportWalkTest, comprehendsTheWalk) {
    expectRows("MATCH (:Person {name: 'Remy'})-[e:KNOWS_WELL*1..2]->(b) CALL (e) { WITH count(*) AS c RETURN [r IN e | r.duration] AS ds } RETURN b.name, ds",
               {{"Adam", "20"}, {"Remy", "20, 20"}});
}

TEST_F(PerRunImportWalkTest, foldsTheWalk) {
    expectRows("MATCH (:Person {name: 'Remy'})-[e:KNOWS_WELL*1..2]->(b) CALL (e) { WITH count(*) AS c RETURN reduce(s = 0, r IN e | s + r.duration) AS d } RETURN b.name, d",
               {{"Adam", "20"}, {"Remy", "40"}});
}

TEST_F(PerRunImportWalkTest, returnsTheWalk) {
    expectRows("MATCH (:Person {name: 'Remy'})-[e:KNOWS_WELL*1..2]->(b) CALL (e) { WITH count(*) AS c RETURN e AS k } RETURN b.name, size(k)",
               {{"Adam", "1"}, {"Remy", "2"}});
}

TEST_F(PerRunImportWalkTest, sizesTheWalkInAUnionBranch) {
    expectRows("MATCH (:Person {name: 'Remy'})-[e:KNOWS_WELL*1..2]->(b) CALL (e) { RETURN size(e) AS n UNION RETURN 0 AS n } RETURN b.name, n",
               {{"Adam", "1"}, {"Adam", "0"}, {"Remy", "2"}, {"Remy", "0"}});
}

TEST_F(PerRunImportWalkTest, decidesAWhenOnTheWalk) {
    expectRows("MATCH (:Person {name: 'Remy'})-[e:KNOWS_WELL*1..2]->(b) CALL (e) { WHEN size(e) > 1 THEN RETURN 'long' AS k ELSE RETURN 'short' AS k } RETURN b.name, k",
               {{"Adam", "short"}, {"Remy", "long"}});
}

TEST_F(PerRunImportWalkTest, unwindsTheWalkInACountSubquery) {
    expectRows("MATCH (:Person {name: 'Remy'})-[e:KNOWS_WELL*1..2]->(b) RETURN b.name, COUNT { UNWIND e AS r RETURN r LIMIT 5 }",
               {{"Adam", "1"}, {"Remy", "2"}});
}

TEST_F(PerRunImportWalkTest, sizesAGroupRelationshipVariable) {
    expectRows("MATCH (:Person {name: 'Remy'}) ((x)-[r:KNOWS_WELL]->(y)){1,2} (b) "
               "CALL (r) { WITH count(*) AS c RETURN size(r) AS n } RETURN b.name, n",
               {{"Adam", "1"}, {"Remy", "2"}});
}
