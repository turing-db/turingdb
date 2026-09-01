#include <gtest/gtest.h>

#include <stddef.h>
#include <memory>
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

#include "StringRowSink.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

// The VECTOR SEARCH statements the analyzer turns away, written the way a query writes
// them. The index they name is never built: each is refused before anything is searched.
class VectorSearchRejectionTest : public TuringTest {
public:
    void initialize() override {
        const fs::Path turingDir = fs::Path {_outDir} / "turing";
        _env = TuringTestEnv::create(turingDir);

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);

        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager());
    }

protected:
    void expectRejected(std::string_view query, std::string_view message) {
        QueryStatus status;
        StringRowSink sink;
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              ChangeID::head(),
                              &_env->getMem(),
                              &sink);

        EXPECT_EQ(status.getStatus(), QueryStatus::Status::ANALYZE_ERROR) << status.getError();
        EXPECT_NE(status.getError().find(message), std::string::npos) << status.getError();
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

TEST_F(VectorSearchRejectionTest, rejectsASearchForNoNeighbour) {
    expectRejected("VECTOR SEARCH IN nodes FOR 0 (1.0, 0.0, 0.0, 0.0) YIELD ids RETURN ids",
                   "VECTOR SEARCH k value must be greater than 0");
}

TEST_F(VectorSearchRejectionTest, rejectsAYieldOfSomethingTheSearchDoesNotReport) {
    expectRejected("VECTOR SEARCH IN nodes FOR 2 (1.0, 0.0, 0.0, 0.0) YIELD bogus RETURN bogus",
                   "VECTOR SEARCH only supports YIELD ids and score, got 'bogus'");
}

TEST_F(VectorSearchRejectionTest, rejectsAYieldNamingTheSameValueTwice) {
    expectRejected("VECTOR SEARCH IN nodes FOR 2 (1.0, 0.0, 0.0, 0.0) YIELD ids, ids RETURN ids",
                   "Variable 'ids' already declared");
}

TEST_F(VectorSearchRejectionTest, rejectsAYieldCollidingWithAMatchedVariable) {
    expectRejected("MATCH (ids:Person) "
                   "VECTOR SEARCH IN nodes FOR 2 (1.0, 0.0, 0.0, 0.0) YIELD ids RETURN ids",
                   "Variable 'ids' already declared");
}
