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

// Comparing a map value. The entry holds its own type, so it is equal only to a value of
// that type holding the same thing, and a row whose key is absent compares to null -
// which a WHERE drops, as it drops the null of any three-valued predicate.
class MapKeyComparisonTest : public TuringTest {
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
        RowSink sink;
        QueryStatus status;
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              ChangeID::head(),
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

    void expectError(std::string_view query, std::string_view expectedError) {
        RowSink sink;
        QueryStatus status;
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              ChangeID::head(),
                              &sink);

        ASSERT_FALSE(status.isOk()) << "query: " << query << "\nexpected it to fail";
        EXPECT_NE(status.getError().find(expectedError), std::string::npos)
            << "query: " << query << "\nerror: " << status.getError();
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
    QueryConfig _queryConfig;
};

TEST_F(MapKeyComparisonTest, comparesAKeyAgainstANumber) {
    write("CREATE (n:Tagged {name: 'a', attrs: {x: 1}})");
    write("CREATE (n:Tagged {name: 'b', attrs: {y: 2}})");
    write("CREATE (n:Tagged {name: 'c', attrs: {x: 3}})");

    expectRows("MATCH (n:Tagged) RETURN n.name, n.attrs.x = 1",
               {{"a", "true"}, {"b", "null"}, {"c", "false"}});
}

// A null is not true, so it survives neither the predicate nor its negation.
TEST_F(MapKeyComparisonTest, dropsTheAbsentKeyFromAFilterAndFromItsNegation) {
    write("CREATE (n:Tagged {name: 'a', attrs: {x: 1}})");
    write("CREATE (n:Tagged {name: 'b', attrs: {y: 2}})");
    write("CREATE (n:Tagged {name: 'c', attrs: {x: 3}})");

    expectRows("MATCH (n:Tagged) WHERE n.attrs.x = 1 RETURN n.name", {{"a"}});
    expectRows("MATCH (n:Tagged) WHERE NOT (n.attrs.x = 1) RETURN n.name", {{"c"}});
    expectRows("MATCH (n:Tagged) WHERE n.attrs.x = 1 OR n.attrs.x IS NULL RETURN n.name",
               {{"a"}, {"b"}});
}

TEST_F(MapKeyComparisonTest, comparesAKeyAgainstAStringAndABool) {
    write("CREATE (n:Tagged {name: 'a', attrs: {s: 'y', b: true}})");

    expectRows("MATCH (n:Tagged) RETURN n.attrs.s = 'y', n.attrs.s = 'z', n.attrs.b = true",
               {{"true", "false", "true"}});
}

// An entry is equal only to a value of the type it holds, so a number is not its text.
TEST_F(MapKeyComparisonTest, comparesUnequalAcrossTypes) {
    write("CREATE (n:Tagged {name: 'a', attrs: {x: 1}})");

    expectRows("MATCH (n:Tagged) RETURN n.attrs.x = '1'", {{"false"}});
}

TEST_F(MapKeyComparisonTest, comparesTwoKeysOfOneMap) {
    expectRows("WITH {a: 1, b: 1, c: 2} AS m RETURN m.a = m.b, m.a = m.c", {{"true", "false"}});
}

TEST_F(MapKeyComparisonTest, comparesAKeyWithNotEqual) {
    write("CREATE (n:Tagged {name: 'a', attrs: {x: 1}})");
    write("CREATE (n:Tagged {name: 'b', attrs: {x: 2}})");

    expectRows("MATCH (n:Tagged) WHERE n.attrs.x <> 1 RETURN n.name", {{"b"}});
}


int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
