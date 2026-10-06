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

// A label or edge-type predicate over a node or edge an OPTIONAL MATCH left null is null,
// not false, so NOT over it is null too and a WHERE drops the row.
class LabelPredicateOnNullTest : public TuringTest {
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

TEST_F(LabelPredicateOnNullTest, keepsTheMatchedNeighbourCarryingTheLabel) {
    expectRows("MATCH (a:Interest) OPTIONAL MATCH (a)-[:KNOWS_WELL]->(m) WITH a, m WHERE m:Person RETURN a.name",
               {{"Ghosts"}});
}

TEST_F(LabelPredicateOnNullTest, dropsAnUnmatchedNodeUnderNotLabel) {
    expectRows("OPTIONAL MATCH (m:Person {name: 'Nobody'}) WITH m WHERE NOT m:Person RETURN count(*)",
               {{"0"}});
}

TEST_F(LabelPredicateOnNullTest, findsTheLabelOfAnUnmatchedNodeNull) {
    expectRows("OPTIONAL MATCH (m:Person {name: 'Nobody'}) WITH m WHERE (m:Person) IS NULL RETURN count(*)",
               {{"1"}});
}

TEST_F(LabelPredicateOnNullTest, dropsEveryUnmatchedNeighbourUnderNotLabel) {
    expectRows("MATCH (a:Interest) OPTIONAL MATCH (a)-[:KNOWS_WELL]->(m) WITH a, m WHERE NOT m:Person RETURN count(*)",
               {{"0"}});
}

TEST_F(LabelPredicateOnNullTest, takesTheElseBranchForAnUnmatchedNeighbour) {
    expectRows("MATCH (a:Interest) OPTIONAL MATCH (a)-[:KNOWS_WELL]->(m) "
               "RETURN a.name, CASE WHEN m:Person THEN 'yes' WHEN NOT m:Person THEN 'no' ELSE 'unknown' END",
               {{"Computers", "unknown"},
                {"Eighties", "unknown"},
                {"Bio", "unknown"},
                {"Cooking", "unknown"},
                {"Ghosts", "yes"},
                {"Padel", "unknown"},
                {"Animals", "unknown"},
                {"Gym", "unknown"},
                {"Travel", "unknown"},
                {"JiuJitsu", "unknown"}});
}

TEST_F(LabelPredicateOnNullTest, dropsEveryUnmatchedEdgeUnderNotType) {
    expectRows("MATCH (a:Interest) OPTIONAL MATCH (a)-[r:KNOWS_WELL]->(m) WITH a, r WHERE NOT r:KNOWS_WELL RETURN count(*)",
               {{"0"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
