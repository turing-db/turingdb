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

// Writing the value a conversion of a literal answers. toInteger('5') is one cell standing
// for every row that may hold no value - a nullable constant - which is a column shape the
// write path met nowhere else: a literal is a plain constant and a converted property is a
// nullable vector. Each of these queries raised "Unsupported unary operation on column of
// type ColumnConst<std::optional<...>>" before the extractor read that shape.
class ConstConversionPropertyTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager());

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
                              &_env->getMem(),
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
                              &_env->getMem(),
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

TEST_F(ConstConversionPropertyTest, createsAnIntegerFromALiteral) {
    write("CREATE (n:Item {name: 'a', count: toInteger('5')})");
    expectRows("MATCH (n:Item) RETURN n.count", {{"5"}});
}

TEST_F(ConstConversionPropertyTest, createsADoubleFromALiteral) {
    write("CREATE (n:Item {name: 'a', ratio: toFloat('2.5')})");
    expectRows("MATCH (n:Item) RETURN n.ratio", {{"2.500000"}});
}

TEST_F(ConstConversionPropertyTest, createsABooleanFromALiteral) {
    write("CREATE (n:Item {name: 'a', ready: toBoolean('true')})");
    expectRows("MATCH (n:Item) RETURN n.ready", {{"true"}});
}

TEST_F(ConstConversionPropertyTest, setsAnIntegerFromALiteral) {
    write("MATCH (n:Person {name: 'Remy'}) SET n.count = toInteger('7')");
    expectRows("MATCH (n:Person {name: 'Remy'}) RETURN n.count", {{"7"}});
}

// A conversion that reads no value writes the property as absent rather than as a wrong one
TEST_F(ConstConversionPropertyTest, createsNullWhereTheLiteralConvertsToNothing) {
    write("CREATE (n:Item {name: 'a', count: toInteger('not a number')})");
    expectRows("MATCH (n:Item) RETURN n.count", {{"null"}});
}

TEST_F(ConstConversionPropertyTest, writesTheSameValueOnEveryRowItMatched) {
    write("MATCH (n:Person) SET n.rank = toInteger('3')");
    expectRows("MATCH (n:Person {name: 'Remy'}) RETURN n.rank", {{"3"}});
    expectRows("MATCH (n:Person {name: 'Adam'}) RETURN n.rank", {{"3"}});
}
