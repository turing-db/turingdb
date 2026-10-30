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

// A map projection builds a map from a node or an edge: `.key` selects a property, null when
// the entity has none, `.*` selects every property, and `key: expr` adds a computed entry.
class MapProjectionTest : public TuringTest {
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

TEST_F(MapProjectionTest, selectsPropertiesOfANode) {
    expectRows("MATCH (p:Person {name: 'Remy'}) WITH p {.name, .age} AS m RETURN m.name, m.age",
               {{"Remy", "32"}});
}

TEST_F(MapProjectionTest, selectsAPropertyTheNodeLacksAsNull) {
    expectRows("MATCH (p:Person {name: 'Maxime'}) WITH p {.name, .age} AS m RETURN m.name, m.age",
               {{"Maxime", "null"}});
}

TEST_F(MapProjectionTest, addsAComputedEntry) {
    expectRows("MATCH (p:Person {name: 'Remy'}) WITH p {.name, rank: p.age + 1} AS m RETURN m.name, m.rank",
               {{"Remy", "33"}});
}

TEST_F(MapProjectionTest, selectsEveryProperty) {
    expectRows("MATCH (p:Person {name: 'Remy'}) WITH p {.*} AS m RETURN m.name, m.dob",
               {{"Remy", "18/01"}});
}

TEST_F(MapProjectionTest, selectsPropertiesOfAnEdge) {
    expectRows("MATCH (:Person {name: 'Remy'})-[e:KNOWS_WELL]->() WITH e {.duration} AS m RETURN m.duration",
               {{"20"}});
}
