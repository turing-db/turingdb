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

class MapCaseBranchTest : public TuringTest {
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

TEST_F(MapCaseBranchTest, selectsAMapInASearchedCase) {
    expectRows("UNWIND [1, 2] AS x WITH x, CASE WHEN x = 1 THEN {a: 1} ELSE {a: 2} END AS m "
               "WHERE m = {a: 2} RETURN x",
               {{"2"}});
}

TEST_F(MapCaseBranchTest, selectsAMapBuiltPerRow) {
    expectRows("MATCH (n:Person) WITH CASE WHEN n.age > 30 THEN {name: n.name} ELSE {name: 'other'} END AS m "
               "WHERE m = {name: 'Remy'} RETURN count(*)",
               {{"1"}});
}

TEST_F(MapCaseBranchTest, coalescesToAMap) {
    expectRows("RETURN coalesce(null, {a: 1}) = {a: 1}", {{"true"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
