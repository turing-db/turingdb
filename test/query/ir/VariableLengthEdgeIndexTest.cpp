#include <gtest/gtest.h>

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

class VariableLengthEdgeIndexTest : public TuringTest {
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

    void expectError(std::string_view query, std::string_view reason) {
        StringRowSink sink;
        QueryStatus status;
        runQuery(query, sink, status);
        ASSERT_FALSE(status.isOk()) << "accepted: " << query;

        const std::string error = status.getError();
        EXPECT_NE(error.find(reason), std::string::npos) << query << ": " << error;
    }

    void expectRows(std::string_view query, const std::vector<StringRowSink::Row>& expected) {
        StringRowSink sink;
        QueryStatus status;
        runQuery(query, sink, status);
        ASSERT_TRUE(status.isOk()) << query << ": " << status.getError();

        EXPECT_EQ(sink.getRows(), expected) << "query: " << query;
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

TEST_F(VariableLengthEdgeIndexTest, typesTheFirstEdgeOfEachPath) {
    expectRows("MATCH ()-[e*]->() RETURN type(e[0]) AS t, count(*) ORDER BY t",
               {{"INTERESTED_IN", "24"}, {"KNOWS_WELL", "34"}});
}

TEST_F(VariableLengthEdgeIndexTest, typesTheLastEdgeOfEachPath) {
    expectRows("MATCH ()-[e*]->() RETURN type(e[-1]) AS t, count(*) ORDER BY t",
               {{"INTERESTED_IN", "45"}, {"KNOWS_WELL", "13"}});
}

TEST_F(VariableLengthEdgeIndexTest, readsNullPastTheEndOfAPath) {
    expectRows("MATCH ()-[e*1..1]->() RETURN count(*), count(e[1])", {{"18", "0"}});
}

TEST_F(VariableLengthEdgeIndexTest, indexesAPathPastAWith) {
    expectRows("MATCH ()-[e*]->() WITH e RETURN type(e[0]) AS t, count(*) ORDER BY t",
               {{"INTERESTED_IN", "24"}, {"KNOWS_WELL", "34"}});
}

TEST_F(VariableLengthEdgeIndexTest, indexesAGroupedEdge) {
    expectRows("MATCH (x)((a)-[r]->(b)){1,2}(y) RETURN type(r[0]) AS t, count(*) ORDER BY t",
               {{"INTERESTED_IN", "16"}, {"KNOWS_WELL", "14"}});
}

TEST_F(VariableLengthEdgeIndexTest, indexesAGroupedNode) {
    expectRows("MATCH (x)((a)-[r]->(b)){1,2}(y) WHERE a[0] = x AND b[-1] = y RETURN count(*)", {{"30"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
