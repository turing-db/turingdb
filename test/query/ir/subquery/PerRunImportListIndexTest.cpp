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

}

// A body run once per input row takes a node or an edge out of a list it imports, or builds
// from an import, as a body run over every row does.
class PerRunImportListIndexTest : public TuringTest {
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

TEST_F(PerRunImportListIndexTest, indexesTheImportOutOfAListLiteral) {
    expectRows("MATCH (p:Person) CALL (p) { RETURN [p][0].name AS s, count(*) AS c } RETURN s, c",
               everyPersonOnce);
}

TEST_F(PerRunImportListIndexTest, indexesAnImportedListOfNodes) {
    expectRows("MATCH (p:Person) WITH [p] AS ps CALL (ps) { RETURN ps[0].name AS s, count(*) AS c } RETURN s, c",
               everyPersonOnce);
}

TEST_F(PerRunImportListIndexTest, takesTheHeadOfAnImportedListOfNodes) {
    expectRows("MATCH (p:Person) WITH [p] AS ps CALL (ps) { RETURN head(ps).name AS s, count(*) AS c } RETURN s, c",
               everyPersonOnce);
}

TEST_F(PerRunImportListIndexTest, takesTheLastOfAnImportedListOfNodes) {
    expectRows("MATCH (p:Person) WITH [p] AS ps CALL (ps) { RETURN last(ps).name AS s, count(*) AS c } RETURN s, c",
               everyPersonOnce);
}

TEST_F(PerRunImportListIndexTest, indexesAnImportedListByAPositionBoundInTheBody) {
    expectRows("MATCH (p:Person) WITH [p] AS ps CALL (ps) { UNWIND [0, -1] AS i RETURN ps[i].name AS s, count(*) AS c } RETURN s, c",
               {{"Remy", "2"},
                {"Adam", "2"},
                {"Maxime", "2"},
                {"Luc", "2"},
                {"Martina", "2"},
                {"Suhas", "2"},
                {"Cyrus", "2"},
                {"Doruk", "2"}});
}

TEST_F(PerRunImportListIndexTest, readsNullPastTheEndOfAnImportedList) {
    expectRows("MATCH (p:Person) WITH [p] AS ps CALL (ps) { RETURN ps[5] AS n, count(*) AS c } RETURN n, c",
               {{"null", "1"},
                {"null", "1"},
                {"null", "1"},
                {"null", "1"},
                {"null", "1"},
                {"null", "1"},
                {"null", "1"},
                {"null", "1"}});
}

TEST_F(PerRunImportListIndexTest, readsTheNullAnImportedListHolds) {
    expectRows("MATCH (p:Person) OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f) WITH [f] AS fs CALL (fs) { RETURN fs[0] IS NULL AS n, count(*) AS c } RETURN n, c",
               {{"false", "1"},
                {"false", "1"},
                {"true", "1"},
                {"true", "1"},
                {"true", "1"},
                {"true", "1"},
                {"true", "1"},
                {"true", "1"}});
}

TEST_F(PerRunImportListIndexTest, indexesAnImportedListOfEdges) {
    expectRows("MATCH (a)-[e:KNOWS_WELL]->(b) WITH [e] AS es CALL (es) { RETURN es[0].duration AS d, count(*) AS c } RETURN d, c",
               {{"20", "1"}, {"20", "1"}, {"200", "1"}});
}

TEST_F(PerRunImportListIndexTest, indexesTheNodesOfAnImportedPath) {
    expectRows("MATCH pt = (a)-[:KNOWS_WELL]->(b) CALL (pt) { WITH count(*) AS c RETURN nodes(pt)[0].name AS s, c } RETURN s, c",
               {{"Remy", "1"}, {"Adam", "1"}, {"Ghosts", "1"}});
}

TEST_F(PerRunImportListIndexTest, indexesTheRelationshipsOfAnImportedPath) {
    expectRows("MATCH pt = (a)-[:KNOWS_WELL]->(b) CALL (pt) { WITH count(*) AS c RETURN relationships(pt)[0].duration AS d, c } RETURN d, c",
               {{"20", "1"}, {"20", "1"}, {"200", "1"}});
}
