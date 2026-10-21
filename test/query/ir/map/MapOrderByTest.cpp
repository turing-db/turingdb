#include <gtest/gtest.h>

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

class MapOrderByTest : public TuringTest {
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

    void expectOrderedRows(std::string_view query, const Rows& expected) {
        expectOrderedRowsIn(ChangeID::head(), query, expected);
    }

    void expectOrderedRowsIn(const ChangeID& changeID, std::string_view query, const Rows& expected) {
        RowSink sink;
        QueryStatus status;
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              changeID,
                              &sink);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        const Rows& actual = sink.rows();

        std::string actualText;
        describeRows(actual, actualText);

        EXPECT_EQ(actual, expected) << "query: " << query << "\ngot:\n" << actualText;
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
    QueryConfig _queryConfig;
};

TEST_F(MapOrderByTest, ordersByAMapBuiltPerRow) {
    expectOrderedRows("UNWIND [2, 1, 3] AS x WITH x, {a: x} AS m RETURN x ORDER BY m",
                      {{"1"}, {"2"}, {"3"}});
}

TEST_F(MapOrderByTest, ordersByAMapBuiltPerRowDescending) {
    expectOrderedRows("UNWIND [2, 1, 3] AS x WITH x, {a: x} AS m RETURN x ORDER BY m DESC",
                      {{"3"}, {"2"}, {"1"}});
}

TEST_F(MapOrderByTest, ordersByAMapExpression) {
    expectOrderedRows("UNWIND [2, 1, 3] AS x RETURN x ORDER BY {a: x}",
                      {{"1"}, {"2"}, {"3"}});
}

TEST_F(MapOrderByTest, ordersByTheKeysBeforeTheValues) {
    expectOrderedRows("UNWIND [1, 2, 3] AS x "
                      "WITH x, CASE x WHEN 1 THEN 'b' WHEN 2 THEN 'a' ELSE 'c' END AS k "
                      "WITH x, {name: k} AS m RETURN x ORDER BY m",
                      {{"2"}, {"1"}, {"3"}});
}

TEST_F(MapOrderByTest, ordersByAStoredMap) {
    write("MATCH (n:Person {name: 'Remy'}) SET n.attrs = {a: 2}");
    write("MATCH (n:Person {name: 'Adam'}) SET n.attrs = {a: 1}");
    expectOrderedRows("MATCH (n:Person) WHERE n.attrs IS NOT NULL RETURN n.name ORDER BY n.attrs",
                      {{"Adam"}, {"Remy"}});
}

TEST_F(MapOrderByTest, ordersAnAbsentStoredMapLast) {
    write("MATCH (n:Person {name: 'Remy'}) SET n.attrs = {a: 2}");
    write("MATCH (n:Person {name: 'Adam'}) SET n.attrs = {a: 1}");
    expectOrderedRows("MATCH (n:Person) WHERE n.name IN ['Remy', 'Adam', 'Luc'] RETURN n.name ORDER BY n.attrs",
                      {{"Adam"}, {"Remy"}, {"Luc"}});
}

TEST_F(MapOrderByTest, ordersByAnUnwoundMap) {
    expectOrderedRows("UNWIND [{a: 2}, {a: 1}] AS m RETURN m.a ORDER BY m",
                      {{"1"}, {"2"}});
}

TEST_F(MapOrderByTest, ordersAMapWithFewerEntriesFirst) {
    expectOrderedRows("UNWIND [{a: 0, b: 'foo'}, {a: 1}] AS m RETURN m.a ORDER BY m",
                      {{"1"}, {"0"}});
}

TEST_F(MapOrderByTest, ordersMapsOfOneSizeByTheirKeysBeforeTheirValues) {
    expectOrderedRows("UNWIND [{a: '', c: null}, {b: 100, a: 'foo'}] AS m RETURN m.a ORDER BY m",
                      {{"foo"}, {""}});
    expectOrderedRows("UNWIND [{a: 1, c: 0}, {a: 2, b: 1}] AS m RETURN m.a ORDER BY m",
                      {{"2"}, {"1"}});
}

TEST_F(MapOrderByTest, ordersMapsOfOneKeySetByTheirValues) {
    expectOrderedRows("UNWIND [{a: 'foo', b: null}, {b: 100, a: 'foo'}] AS m RETURN m.b ORDER BY m",
                      {{"100"}, {"null"}});
}

TEST_F(MapOrderByTest, ordersStoredMapsBySizeFirst) {
    write("MATCH (n:Person {name: 'Remy'}) SET n.attrs = {a: 0, b: 'foo'}");
    write("MATCH (n:Person {name: 'Adam'}) SET n.attrs = {z: 1}");
    expectOrderedRows("MATCH (n:Person) WHERE n.attrs IS NOT NULL RETURN n.name ORDER BY n.attrs",
                      {{"Adam"}, {"Remy"}});
}

TEST_F(MapOrderByTest, takesTheMinimumMapBySizeFirst) {
    expectOrderedRows("UNWIND [{a: 0, b: 'foo'}, {z: 1}] AS m WITH min(m) AS least RETURN least.z",
                      {{"1"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
