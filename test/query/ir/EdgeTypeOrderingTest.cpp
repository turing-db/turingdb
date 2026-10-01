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

// type() answers a string that orders lexicographically against any other string exactly
// as a stored property does.
// simpledb holds 3 KNOWS_WELL edges and 15 INTERESTED_IN ones, so "J" separates the two.
class EdgeTypeOrderingTest : public TuringTest {
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

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

TEST_F(EdgeTypeOrderingTest, ordersAnEdgeTypeAgainstALiteral) {
    expectRows("MATCH ()-[e]->() WHERE type(e) > 'J' RETURN count(*)", {{"3"}});
}

TEST_F(EdgeTypeOrderingTest, ordersALiteralAgainstAnEdgeType) {
    expectRows("MATCH ()-[e]->() WHERE 'J' < type(e) RETURN count(*)", {{"3"}});
}

TEST_F(EdgeTypeOrderingTest, ordersAnEdgeTypeInclusively) {
    expectRows("MATCH ()-[e]->() WHERE type(e) >= 'KNOWS_WELL' RETURN count(*)", {{"3"}});
}

TEST_F(EdgeTypeOrderingTest, ordersTwoEdgeTypesAgainstEachOther) {
    expectRows("MATCH (a)-[e1]->(b)-[e2]->(c) WHERE type(e1) > type(e2) RETURN count(*)", {{"8"}});
}

TEST_F(EdgeTypeOrderingTest, ordersAnEdgeTypeAgainstAStringProperty) {
    expectRows("MATCH (a)-[e]->(b) WHERE type(e) > e.name RETURN count(*)", {{"7"}});
}

TEST_F(EdgeTypeOrderingTest, comparesAnEdgeTypeForEquality) {
    expectRows("MATCH ()-[e]->() WHERE type(e) = 'KNOWS_WELL' RETURN count(*)", {{"3"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
