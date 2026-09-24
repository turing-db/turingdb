#include <gtest/gtest.h>

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

#include "IRTestRows.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

class TypeIsNullTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager());

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);
    }

    void expectCounts(std::string_view query, const Counts& expected) {
        CountSink sink;
        QueryStatus status;
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              ChangeID::head(),
                              &_env->getMem(),
                              &sink);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        Counts actual;
        sink.sortedCounts(actual);

        EXPECT_EQ(actual, expected) << "query: " << query;
    }

    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
    std::string _graphName {"simpledb"};
};

TEST_F(TypeIsNullTest, TypeOfAMatchedEdgeIsNotNull) {
    expectCounts("MATCH (a)-[e]->(b) WHERE type(e) IS NOT NULL RETURN count(e)", {18});
    expectCounts("MATCH (a)-[e]->(b) WHERE type(e) IS NULL RETURN count(e)", {0});
}

TEST_F(TypeIsNullTest, TypeOfAnOptionalEdgeIsNullWhereNoEdgeMatched) {
    expectCounts("MATCH (a:Person) OPTIONAL MATCH (a)-[e:KNOWS_WELL]->(b) WITH a, e WHERE type(e) IS NULL RETURN count(a)", {6});
    expectCounts("MATCH (a:Person) OPTIONAL MATCH (a)-[e:KNOWS_WELL]->(b) WITH a, e WHERE type(e) IS NOT NULL RETURN count(a)", {2});
}

TEST_F(TypeIsNullTest, AbsentPropertyOfAnEdgeIsNull) {
    expectCounts("MATCH (a)-[e]->(b) WHERE e.duration IS NULL RETURN count(e)", {10});
}
