#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <string>
#include <string_view>

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

#include "IRTestRows.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

class KeywordNameTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager(), &_env->getMem(), &_env->getCompilerContext());

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);
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
                                     &_env->getCompilerContext(),
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

        RowSink sink;
        QueryStatus status;
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              changeID,
                              &sink);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        submit(changeID);
    }

    void expectRows(std::string_view query, const Rows& expected) {
        expectRowsIn(ChangeID::head(), query, expected);
    }

    void expectRowsIn(const ChangeID& changeID, std::string_view query, const Rows& expected) {
        RowSink sink;
        QueryStatus status;
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              changeID,
                              &sink);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        Rows actual;
        sink.sortedRows(actual);

        Rows sortedExpected = expected;
        std::sort(sortedExpected.begin(), sortedExpected.end());

        std::string actualText;
        describeRows(actual, actualText);

        EXPECT_EQ(actual, sortedExpected) << "query: " << query << "\ngot:\n" << actualText;
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
    QueryConfig _queryConfig;
};

TEST_F(KeywordNameTest, comparesMapsKeyedFrom) {
    expectRows("UNWIND [1] AS x WITH x WHERE {from: 1} = {from: 1} RETURN count(x)", {{"1"}});
}

TEST_F(KeywordNameTest, buildsAMapKeyedFromPerRow) {
    expectRows("MATCH (a:Person {name: 'Remy'})-[:KNOWS_WELL]->(b:Person) "
               "WITH {from: a.name, to: b.name} AS m WHERE m = {from: 'Remy', to: 'Adam'} RETURN count(*)",
               {{"1"}});
}

TEST_F(KeywordNameTest, readsKeysNamedAfterKeywords) {
    expectRows("WITH {graph: 1, index: 2, search: 3, load: 4, vector: 5, reduce: 6} AS m "
               "RETURN m.graph, m.index, m.search, m.load, m.vector, m.reduce",
               {{"1", "2", "3", "4", "5", "6"}});
}

TEST_F(KeywordNameTest, writesAndReadsPropertiesNamedAfterKeywords) {
    write("CREATE (:Tagged {from: 'a', graph: 'b', index: 3})");
    expectRows("MATCH (n:Tagged {from: 'a'}) RETURN n.from, n.graph, n.index", {{"a", "b", "3"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
