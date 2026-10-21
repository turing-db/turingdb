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

class MapNullTestTest : public TuringTest {
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

TEST_F(MapNullTestTest, findsNoNullMapBuiltPerRow) {
    expectRows("MATCH (n:Person) WITH {a: n.age} AS m WHERE m IS NULL RETURN count(*)", {{"0"}});
}

TEST_F(MapNullTestTest, findsEveryMapBuiltPerRowNotNull) {
    expectRows("MATCH (n:Person) WITH {a: n.age} AS m WHERE m IS NOT NULL RETURN count(*)", {{"8"}});
}

TEST_F(MapNullTestTest, findsAMapLiteralNotNull) {
    expectRows("UNWIND [1] AS x WITH x WHERE {a: 1} IS NULL RETURN count(x)", {{"0"}});
    expectRows("UNWIND [1] AS x WITH x WHERE {a: 1} IS NOT NULL RETURN count(x)", {{"1"}});
}

TEST_F(MapNullTestTest, returnsTheNullTestOfAMap) {
    expectRows("WITH {a: 1} AS m RETURN m IS NULL, m IS NOT NULL", {{"false", "true"}});
}

TEST_F(MapNullTestTest, findsTheNodesWithoutAStoredMap) {
    write("MATCH (n:Person {name: 'Remy'}) SET n.attrs = {a: 1}");
    expectRows("MATCH (n:Person) WHERE n.attrs IS NULL RETURN count(*)", {{"7"}});
    expectRows("MATCH (n:Person) WHERE n.attrs IS NOT NULL RETURN n.name", {{"Remy"}});
}

TEST_F(MapNullTestTest, comparesAMapBuiltPerRowEqualToAnInteger) {
    expectRows("MATCH (n:Person) WITH {a: n.age} AS m WHERE m = 32 RETURN count(*)", {{"0"}});
}

TEST_F(MapNullTestTest, comparesAMapBuiltPerRowUnequalToAnInteger) {
    expectRows("MATCH (n:Person) WITH {a: n.age} AS m WHERE m <> 32 RETURN count(*)", {{"8"}});
}

TEST_F(MapNullTestTest, comparesAMapLiteralToAString) {
    expectRows("WITH {a: 1} AS m RETURN m = 'a', m <> 'a'", {{"false", "true"}});
}

TEST_F(MapNullTestTest, readsAMapAsNullWhereAnOptionalCallYieldsNothing) {
    expectRows("MATCH (n:Person {name: 'Remy'}) "
               "OPTIONAL CALL { WITH n MATCH (n)-[:NO_SUCH_TYPE]->(m) RETURN {a: 1} AS attrs } "
               "RETURN attrs IS NULL",
               {{"true"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
