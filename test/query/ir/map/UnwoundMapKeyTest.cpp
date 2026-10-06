#include <gtest/gtest.h>

#include <algorithm>
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

#include "IRTestRows.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

class UnwoundMapKeyTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager(), &_env->getMem(), &_env->getCompilerContext());

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);
    }

    void runQuery(std::string_view query, RowSink& sink, QueryStatus& status) {
        _interpreter->execute(status, query, _graphName, CommitHash::head(), ChangeID::head(), &sink);
    }

    void expectRows(std::string_view query, const Rows& expected) {
        RowSink sink;
        QueryStatus status;
        runQuery(query, sink, status);
        ASSERT_TRUE(status.isOk()) << query << ": " << status.getError();

        Rows actual;
        sink.sortedRows(actual);

        Rows sortedExpected = expected;
        std::sort(sortedExpected.begin(), sortedExpected.end());

        EXPECT_EQ(actual, sortedExpected) << "query: " << query;
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

TEST_F(UnwoundMapKeyTest, readsAKeyOfAMapBoundByWith) {
    expectRows("WITH {a: 1} AS m RETURN m.a",
               {{"1"}});
}

TEST_F(UnwoundMapKeyTest, readsAKeyOfEachUnwoundMap) {
    expectRows("UNWIND [{a: 1}, {a: 2}] AS m RETURN m.a",
               {{"1"}, {"2"}});
}

TEST_F(UnwoundMapKeyTest, readsAKeyOfEachMapUnwoundFromAWithList) {
    expectRows("WITH [{a: 1}, {a: 2}] AS rows UNWIND rows AS m RETURN m.a",
               {{"1"}, {"2"}});
}

TEST_F(UnwoundMapKeyTest, matchesOnAKeyOfAnUnwoundMap) {
    expectRows("UNWIND [{name: 'Remy'}, {name: 'Adam'}] AS row MATCH (n:Person {name: row.name}) RETURN n.name",
               {{"Remy"}, {"Adam"}});
}

TEST_F(UnwoundMapKeyTest, filtersOnAKeyOfAnUnwoundMap) {
    expectRows("UNWIND [{name: 'Remy'}, {name: 'Adam'}] AS row MATCH (n:Person) WHERE n.name = row.name RETURN n.name",
               {{"Remy"}, {"Adam"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
