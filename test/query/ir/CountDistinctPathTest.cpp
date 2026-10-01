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

class CountDistinctPathTest : public TuringTest {
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

TEST_F(CountDistinctPathTest, countsDistinctEdgeLists) {
    expectRows("MATCH ()-[e*]->() RETURN count(DISTINCT e)", {{"58"}});
}

TEST_F(CountDistinctPathTest, dedupsRepeatedEdgeLists) {
    expectRows("MATCH ()-[e*1..1]->() UNWIND [1, 2, 3] AS i RETURN count(e), count(DISTINCT e)", {{"54", "18"}});
}

TEST_F(CountDistinctPathTest, countsDistinctGroupedNodes) {
    expectRows("MATCH (x)((a)-[r]->(b)){1,2}(y) RETURN count(DISTINCT a), count(DISTINCT b), count(DISTINCT r)",
               {{"13", "20", "30"}});
}

TEST_F(CountDistinctPathTest, collectsDistinctEdgeLists) {
    expectRows("MATCH ()-[e*1..1]->() UNWIND [1, 2] AS i RETURN size(collect(DISTINCT e))", {{"18"}});
}

TEST_F(CountDistinctPathTest, keysDistinctRowsOnAnEdgeList) {
    expectRows("MATCH ()-[e*1..1]->() UNWIND [1, 2] AS i WITH DISTINCT e RETURN count(*)", {{"18"}});
}

TEST_F(CountDistinctPathTest, groupsOnAnEdgeList) {
    expectRows("MATCH ()-[e*1..1]->() UNWIND [1, 2] AS i WITH e, count(*) AS c RETURN count(*), sum(c)", {{"18", "36"}});
}

TEST_F(CountDistinctPathTest, countsDistinctGroupedNodesPerKey) {
    expectRows("MATCH (x)((a)-[r]->(b)){1,2}(y) RETURN size(r) AS s, count(DISTINCT a) ORDER BY s",
               {{"1", "9"}, {"2", "4"}});
}

TEST_F(CountDistinctPathTest, readsAnEdgeListItGroupedOn) {
    expectRows("MATCH ()-[e*1..2]->() UNWIND [1, 2] AS i WITH e, count(*) AS c RETURN size(e) AS s, sum(c) ORDER BY s",
               {{"1", "36"}, {"2", "24"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
