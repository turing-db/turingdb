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

TEST_F(VariableLengthEdgeTypeTest, rejectsTypeOfAVariableLengthEdgeList) {
    expectError("MATCH ()-[e*]->() RETURN type(e)", "Invalid arguments for function 'type'");
}

TEST_F(VariableLengthEdgeTypeTest, rejectsTypeOfAVariableLengthEdgeListInWhere) {
    expectError("MATCH ()-[e*]->() WHERE type(e) = 'KNOWS_WELL' RETURN count(*)", "Invalid arguments for function 'type'");
}

TEST_F(VariableLengthEdgeTypeTest, rejectsTypeOfANode) {
    expectError("MATCH (n) RETURN type(n)", "Invalid arguments for function 'type'");
}

TEST_F(VariableLengthEdgeTypeTest, rejectsStartNodeOfAVariableLengthEdgeList) {
    expectError("MATCH ()-[e*]->() RETURN startNode(e)", "Invalid arguments for function 'startNode'");
}

TEST_F(VariableLengthEdgeTypeTest, rejectsEndNodeOfAVariableLengthEdgeList) {
    expectError("MATCH ()-[e*]->() RETURN endNode(e)", "Invalid arguments for function 'endNode'");
}

TEST_F(VariableLengthEdgeTypeTest, rejectsIdOfAVariableLengthEdgeList) {
    expectError("MATCH ()-[e*]->() RETURN id(e)", "Invalid arguments for function 'id'");
}

TEST_F(VariableLengthEdgeTypeTest, rejectsTypeOfAVariableLengthEdgeListPastAWith) {
    expectError("MATCH ()-[e*]->() WITH e RETURN type(e)", "Invalid arguments for function 'type'");
}

TEST_F(VariableLengthEdgeTypeTest, rejectsStartNodeOfARenamedVariableLengthEdgeList) {
    expectError("MATCH ()-[e*]->() WITH e AS f RETURN startNode(f)", "Invalid arguments for function 'startNode'");
}

TEST_F(VariableLengthEdgeTypeTest, rejectsTypeOfAVariableLengthEdgeListReturnedByACallSubquery) {
    expectError("CALL { MATCH ()-[e*]->() RETURN e } RETURN type(e)", "Invalid arguments for function 'type'");
}

TEST_F(VariableLengthEdgeTypeTest, rejectsLabelsOfAGroupedNode) {
    expectError("MATCH (x)((a)-[r]->(b)){1,2}(y) RETURN labels(a)", "Invalid arguments for function 'labels'");
}

TEST_F(VariableLengthEdgeTypeTest, rejectsIdOfAGroupedNode) {
    expectError("MATCH (x)((a)-[r]->(b)){1,2}(y) RETURN id(a)", "Invalid arguments for function 'id'");
}

TEST_F(VariableLengthEdgeTypeTest, rejectsTypeOfAGroupedEdge) {
    expectError("MATCH (x)((a)-[r]->(b)){1,2}(y) RETURN type(r)", "Invalid arguments for function 'type'");
}

TEST_F(VariableLengthEdgeTypeTest, rejectsStartNodeOfAGroupedEdge) {
    expectError("MATCH (x)((a)-[r]->(b)){1,2}(y) RETURN startNode(r)", "Invalid arguments for function 'startNode'");
}

TEST_F(VariableLengthEdgeTypeTest, countsOneHopPathsAsEdges) {
    expectRows("MATCH ()-[e]->() RETURN count(e)", {{"18"}});
    expectRows("MATCH ()-[e*1..1]->() RETURN count(e)", {{"18"}});
}

TEST_F(VariableLengthEdgeTypeTest, readsTheSizeOfAVariableLengthEdgeList) {
    expectRows("MATCH ()-[e*1..1]->() RETURN DISTINCT size(e)", {{"1"}});
}

TEST_F(VariableLengthEdgeTypeTest, countsGroupedNodesAndEdges) {
    expectRows("MATCH (x)((a)-[r]->(b)){1,1}(y) RETURN count(a), count(r)", {{"18", "18"}});
}

TEST_F(VariableLengthEdgeTypeTest, typesAnEdgeOfAFixedLengthPattern) {
    expectRows("MATCH ()-[e:KNOWS_WELL]->() RETURN DISTINCT type(e)", {{"KNOWS_WELL"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
