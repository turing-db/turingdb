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

// A variable-length named path is read inside a subquery that imports it, and past a CALL,
// as it is read where its MATCH binds it. KNOWS_WELL leads Remy to Adam in 1 hop and back
// to Remy in 2.
class NamedWalkAcrossSubqueryTest : public TuringTest {
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

TEST_F(NamedWalkAcrossSubqueryTest, returnsTheImportedWalk) {
    expectRows("MATCH p = (:Person {name: 'Remy'})-[:KNOWS_WELL*1..2]->(b) CALL (p) { RETURN p AS k } RETURN b.name, length(k)",
               {{"Adam", "1"}, {"Remy", "2"}});
}

TEST_F(NamedWalkAcrossSubqueryTest, readsTheImportedWalkInTheBody) {
    expectRows("MATCH p = (:Person {name: 'Remy'})-[:KNOWS_WELL*1..2]->(b) "
               "CALL (p) { RETURN [n IN nodes(p) | n.name] AS ns } "
               "RETURN b.name, ns",
               {{"Adam", "Remy, Adam"}, {"Remy", "Remy, Adam, Remy"}});
}

TEST_F(NamedWalkAcrossSubqueryTest, readsTheImportedWalkInABodyRunPerRow) {
    expectRows("MATCH p = (:Person {name: 'Remy'})-[:KNOWS_WELL*1..2]->(b) "
               "CALL (p) { WITH count(*) AS c RETURN length(p) AS n } "
               "RETURN b.name, n",
               {{"Adam", "1"}, {"Remy", "2"}});
}

TEST_F(NamedWalkAcrossSubqueryTest, readsTheWalkPastACallImportingIt) {
    expectRows("MATCH p = (:Person {name: 'Remy'})-[:KNOWS_WELL*1..2]->(b) "
               "CALL (p) { WITH count(*) AS c RETURN c } "
               "RETURN b.name, length(p), c",
               {{"Adam", "1", "1"}, {"Remy", "2", "1"}});
}

TEST_F(NamedWalkAcrossSubqueryTest, readsTheWalkPastACallNotImportingIt) {
    expectRows("MATCH p = (:Person {name: 'Remy'})-[:KNOWS_WELL*1..2]->(b) "
               "CALL (b) { RETURN b.name AS n } "
               "RETURN n, length(p)",
               {{"Adam", "1"}, {"Remy", "2"}});
}

TEST_F(NamedWalkAcrossSubqueryTest, countsTheRelationshipsOfTheWalkInACountSubquery) {
    expectRows("MATCH p = (:Person {name: 'Remy'})-[:KNOWS_WELL*1..2]->(b) "
               "RETURN b.name, COUNT { UNWIND relationships(p) AS r RETURN r }",
               {{"Adam", "1"}, {"Remy", "2"}});
}

TEST_F(NamedWalkAcrossSubqueryTest, testsTheWalkInAnExistsSubquery) {
    expectRows("MATCH p = (:Person {name: 'Remy'})-[:KNOWS_WELL*1..2]->(b) "
               "RETURN b.name, EXISTS { WITH p WHERE length(p) > 1 RETURN 1 }",
               {{"Adam", "false"}, {"Remy", "true"}});
}
