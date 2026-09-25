#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "QueryInterpreterV3.h"
#include "QueryStatus.h"

#include "Graph.h"
#include "QueryConfig.h"
#include "SimpleGraph.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "TuringDB.h"
#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"

#include "StringRowSink.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

class MapOrderingTest : public TuringTest {
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

    void openChange(ChangeID& changeID) {
        SystemAccessor system = _env->getSystemManager().accessUnique();
        const auto res = system.newChange(_graphName);
        ASSERT_TRUE(res);

        changeID = res.value()->id();
    }

    void submit(const ChangeID& changeID) {
        const QueryState submitState(_graphName,
                                     &_env->getMem(),
                                     &_queryConfig,
                                     nullptr,
                                     CommitHash::head(),
                                     changeID);
        const QueryStatus status = _env->getDB().query("CHANGE SUBMIT", submitState);
        ASSERT_TRUE(status.isOk()) << "CHANGE SUBMIT failed";
    }

    void write(std::string_view query) {
        ChangeID changeID;
        openChange(changeID);

        StringRowSink sink;
        QueryStatus status;
        _interpreter->execute(status, query, _graphName, CommitHash::head(), changeID, &_env->getMem(), &sink);
        ASSERT_TRUE(status.isOk()) << query << ": " << status.getError();

        submit(changeID);
    }

    void expectOrderedRows(std::string_view query, const std::vector<StringRowSink::Row>& expected) {
        StringRowSink sink;
        QueryStatus status;
        runQuery(query, sink, status);
        ASSERT_TRUE(status.isOk()) << query << ": " << status.getError();

        EXPECT_EQ(sink.getRows(), expected) << "query: " << query;
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
    QueryConfig _queryConfig;
};

TEST_F(MapOrderingTest, ordersByAMapBuiltPerRow) {
    expectOrderedRows("UNWIND [2, 1, 3] AS x WITH x, {a: x} AS m RETURN x ORDER BY m", {{"1"}, {"2"}, {"3"}});
}

TEST_F(MapOrderingTest, ordersByAMapBuiltPerRowDescending) {
    expectOrderedRows("UNWIND [2, 1, 3] AS x WITH x, {a: x} AS m RETURN x ORDER BY m DESC", {{"3"}, {"2"}, {"1"}});
}

TEST_F(MapOrderingTest, ordersByAMapExpression) {
    expectOrderedRows("UNWIND [2, 1, 3] AS x RETURN x ORDER BY {a: x}", {{"1"}, {"2"}, {"3"}});
}

TEST_F(MapOrderingTest, ordersByAStoredMap) {
    write("MATCH (n:Person {name: 'Remy'}) SET n.attrs = {a: 2}");
    write("MATCH (n:Person {name: 'Adam'}) SET n.attrs = {a: 1}");
    expectOrderedRows("MATCH (n:Person) WHERE n.attrs IS NOT NULL RETURN n.name ORDER BY n.attrs", {{"Adam"}, {"Remy"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
