#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <string_view>

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

class VariableLengthEdgeTypeTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager());

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);
    }

    void runQuery(std::string_view query, StringRowSink& sink, QueryStatus& status) {
        _interpreter->execute(status, query, _graphName, CommitHash::head(), ChangeID::head(), &_env->getMem(), &sink);
    }

    void expectError(std::string_view query, std::string_view reason) {
        StringRowSink sink;
        QueryStatus status;
        runQuery(query, sink, status);
        ASSERT_FALSE(status.isOk()) << "accepted: " << query;

        const std::string error = status.getError();
        EXPECT_NE(error.find(reason), std::string::npos) << query << ": " << error;
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

TEST_F(VariableLengthEdgeTypeTest, rejectsTypeOfAVariableLengthEdgeList) {
    expectError("MATCH ()-[e*]->() RETURN type(e)", "Invalid arguments for function 'type'");
}

TEST_F(VariableLengthEdgeTypeTest, rejectsTypeOfAVariableLengthEdgeListInWhere) {
    expectError("MATCH ()-[e*]->() WHERE type(e) = 'KNOWS_WELL' RETURN count(*)", "Invalid arguments for function 'type'");
}

TEST_F(VariableLengthEdgeTypeTest, rejectsTypeOfANode) {
    expectError("MATCH (n) RETURN type(n)", "Invalid arguments for function 'type'");
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
