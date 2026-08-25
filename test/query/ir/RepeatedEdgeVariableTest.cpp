#include <gtest/gtest.h>

#include <memory>
#include <span>
#include <string>
#include <string_view>

#include "NLOutputSink.h"
#include "QueryInterpreterV3.h"
#include "QueryStatus.h"

#include "Graph.h"
#include "SimpleGraph.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"

#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

namespace {

class RowCountingSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const>, size_t, size_t rowCount) override {
        _rowCount += rowCount;
    }

    size_t getRowCount() const { return _rowCount; }

private:
    size_t _rowCount {0};
};

}

// The query test suite's fail-reads-match-2 case on the v3 engine. An edge variable named
// twice in one pattern asks the same edge to be two hops of that pattern, which no edge
// can be, so the query is turned away - while the same variable shared by two patterns is
// a join on one edge, and runs.
class RepeatedEdgeVariableTest : public TuringTest {
public:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);

        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager());
    }

protected:
    void expectRepeatedEdgeRejection(std::string_view query) {
        RowCountingSink sink;
        QueryStatus status;
        runQuery(query, status, sink);

        EXPECT_EQ(status.getStatus(), QueryStatus::Status::PLAN_ERROR) << "query: " << query;

        const std::string error = status.getError();
        const std::string_view reason = "Re-using the same edge variable in a single pattern is not supported";
        EXPECT_NE(error.find(reason), std::string::npos) << "query: " << query << "\nerror: " << error;
    }

    void expectRowCount(std::string_view query, size_t expected) {
        RowCountingSink sink;
        QueryStatus status;
        runQuery(query, status, sink);

        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        EXPECT_EQ(sink.getRowCount(), expected) << "query: " << query;
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;

private:
    void runQuery(std::string_view query, QueryStatus& status, NLOutputSink& sink) {
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              ChangeID::head(),
                              &_env->getMem(),
                              &sink);
    }
};

// fail-reads-match-2: the second hop repeats both the edge variable and the node the first
// hop landed on
TEST_F(RepeatedEdgeVariableTest, rejectsARepeatedEdgeVariableClosingOnItsTarget) {
    expectRepeatedEdgeRejection("MATCH (n:Person)-[e]->(m:Person)-[e]->(m)\nRETURN n;");
}

// The same repetition with a fresh node at each end, where the pattern reads as a two-hop
// walk and only the edge variable is repeated
TEST_F(RepeatedEdgeVariableTest, rejectsARepeatedEdgeVariableBetweenDistinctNodes) {
    expectRepeatedEdgeRejection("MATCH (n:Person)-[e]->(m:Person)-[e]->(o:Person)\nRETURN n;");
}

// The valid neighbour of those two: one edge variable across two patterns joins them on
// that edge, one row per edge leaving a Person
TEST_F(RepeatedEdgeVariableTest, joinsAnEdgeVariableSharedByTwoPatterns) {
    expectRowCount("MATCH (n:Person)-[e]->(m)\nMATCH (a)-[e]->(b)\nRETURN n, a;", 17);
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
