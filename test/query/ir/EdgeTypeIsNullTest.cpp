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

class EdgeTypeIsNullTest : public TuringTest {
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

TEST_F(EdgeTypeIsNullTest, keepsEveryEdgeWhoseTypeIsNotNull) {
    expectRows("MATCH (a)-[e]->(b) WHERE type(e) IS NOT NULL RETURN count(e)", {{"18"}});
}

TEST_F(EdgeTypeIsNullTest, keepsNoEdgeWhoseTypeIsNull) {
    expectRows("MATCH (a)-[e]->(b) WHERE type(e) IS NULL RETURN count(e)", {{"0"}});
}

TEST_F(EdgeTypeIsNullTest, findsTheTypeOfAnUnmatchedOptionalEdgeNull) {
    expectRows("MATCH (a:Person) OPTIONAL MATCH (a)-[e:KNOWS_WELL]->(b) WITH a, e WHERE type(e) IS NULL RETURN count(a)",
               {{"6"}});
}

TEST_F(EdgeTypeIsNullTest, findsAnEdgePropertyNull) {
    expectRows("MATCH (a)-[e]->(b) WHERE e.duration IS NULL RETURN count(e)", {{"10"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
