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

constexpr size_t simpleGraphNodeCount = 18;
constexpr size_t simpleGraphEdgeCount = 18;

}

// A MATCH whose pattern binds no variable, followed by the clause that introduces the one
// the query returns: the anonymous edges still drive what follows, one row per edge.
class AnonymousMatchTest : public TuringTest {
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

TEST_F(AnonymousMatchTest, unwindsAfterAnAnonymousEdgeMatch) {
    std::vector<StringRowSink::Row> expected;
    for (size_t edge = 0; edge < simpleGraphEdgeCount; edge++) {
        expected.push_back({"1"});
        expected.push_back({"2"});
    }

    expectRows("MATCH ()-->() UNWIND [1,2] AS v0 RETURN v0", expected);
}

TEST_F(AnonymousMatchTest, matchesNodesAfterAnAnonymousEdgeMatch) {
    std::vector<StringRowSink::Row> expected;
    for (size_t edge = 0; edge < simpleGraphEdgeCount; edge++) {
        for (size_t nodeID = 0; nodeID < simpleGraphNodeCount; nodeID++) {
            expected.push_back({std::to_string(nodeID)});
        }
    }

    expectRows("MATCH ()-->() MATCH (v0) RETURN v0", expected);
}

TEST_F(AnonymousMatchTest, doesNotResolveAnAnonymousNodeByName) {
    expectError("MATCH ()-->() RETURN v0", "Variable 'v0' not found");
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
