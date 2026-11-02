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

// elementId() names each node and each edge: one value per entity, distinct between entities
// of one kind, and the same every time the entity is read. simpledb has 18 nodes and 18 edges.
class ElementIdFunctionTest : public TuringTest {
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

TEST_F(ElementIdFunctionTest, namesANode) {
    expectRows("MATCH (p:Person {name: 'Remy'}) RETURN elementId(p) IS NOT NULL", {{"true"}});
}

TEST_F(ElementIdFunctionTest, distinguishesEveryNode) {
    expectRows("MATCH (n) RETURN count(DISTINCT elementId(n))", {{"18"}});
}

TEST_F(ElementIdFunctionTest, distinguishesEveryEdge) {
    expectRows("MATCH ()-[e]->() RETURN count(DISTINCT elementId(e))", {{"18"}});
}

TEST_F(ElementIdFunctionTest, findsTheNodeItNames) {
    expectRows("MATCH (p:Person {name: 'Remy'}) WITH elementId(p) AS i MATCH (n) WHERE elementId(n) = i RETURN n.name",
               {{"Remy"}});
}
