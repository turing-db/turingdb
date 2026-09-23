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

class MapPropertyEqualityTest : public TuringTest {
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

    void tagRemyAndAdam() {
        write("MATCH (n {name: 'Remy'}) SET n.attrs = {a: 1, b: 'x'}");
        write("MATCH (n {name: 'Adam'}) SET n.attrs = {a: 3}");
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
    QueryConfig _queryConfig;
};

TEST_F(MapPropertyEqualityTest, matchesAStoredMapByPattern) {
    tagRemyAndAdam();
    expectRows("MATCH (n {attrs: {a: 1, b: 'x'}}) RETURN n.name", {{"Remy"}});
}

TEST_F(MapPropertyEqualityTest, matchesAStoredMapInWhere) {
    tagRemyAndAdam();
    expectRows("MATCH (n) WHERE n.attrs = {a: 1, b: 'x'} RETURN n.name", {{"Remy"}});
}

TEST_F(MapPropertyEqualityTest, matchesWhateverOrderTheKeysAreWrittenIn) {
    tagRemyAndAdam();
    expectRows("MATCH (n) WHERE n.attrs = {b: 'x', a: 1} RETURN n.name", {{"Remy"}});
}

TEST_F(MapPropertyEqualityTest, doesNotMatchAMapMissingAKey) {
    tagRemyAndAdam();
    expectRows("MATCH (n) WHERE n.attrs = {a: 1} RETURN n.name", {});
}

TEST_F(MapPropertyEqualityTest, doesNotMatchAMapWithAnExtraKey) {
    tagRemyAndAdam();
    expectRows("MATCH (n) WHERE n.attrs = {a: 1, b: 'x', c: 2} RETURN n.name", {});
}

TEST_F(MapPropertyEqualityTest, matchesAnIntegerAgainstAnEqualDouble) {
    tagRemyAndAdam();
    expectRows("MATCH (n) WHERE n.attrs = {a: 1.0, b: 'x'} RETURN n.name", {{"Remy"}});
}

TEST_F(MapPropertyEqualityTest, doesNotMatchAValueOfAnotherType) {
    tagRemyAndAdam();
    expectRows("MATCH (n) WHERE n.attrs = {a: '1', b: 'x'} RETURN n.name", {});
}

TEST_F(MapPropertyEqualityTest, matchesANestedStoredMap) {
    write("MATCH (n {name: 'Remy'}) SET n.attrs = {outer: {inner: 1}}");
    expectRows("MATCH (n) WHERE n.attrs = {outer: {inner: 1}} RETURN n.name", {{"Remy"}});
}

TEST_F(MapPropertyEqualityTest, matchesAStoredMapHoldingAList) {
    write("MATCH (n {name: 'Remy'}) SET n.attrs = {xs: [1, 2]}");
    expectRows("MATCH (n) WHERE n.attrs = {xs: [1, 2]} RETURN n.name", {{"Remy"}});
    expectRows("MATCH (n) WHERE n.attrs = {xs: [2, 1]} RETURN n.name", {});
}

TEST_F(MapPropertyEqualityTest, matchesAnEmptyStoredMap) {
    write("MATCH (n {name: 'Remy'}) SET n.attrs = {}");
    write("MATCH (n {name: 'Adam'}) SET n.attrs = {a: 3}");
    expectRows("MATCH (n) WHERE n.attrs = {} RETURN n.name", {{"Remy"}});
}

TEST_F(MapPropertyEqualityTest, negatesTheComparison) {
    tagRemyAndAdam();
    expectRows("MATCH (n) WHERE n.attrs <> {a: 1, b: 'x'} RETURN n.name", {{"Adam"}});
}

TEST_F(MapPropertyEqualityTest, matchesNeitherSideWhereTheNodeHasNoMap) {
    tagRemyAndAdam();
    expectRows("MATCH (n) WHERE n.attrs = {a: 9} RETURN n.name", {});
    expectRows("MATCH (n) WHERE n.attrs <> {a: 9} RETURN n.name", {{"Adam"}, {"Remy"}});
}

TEST_F(MapPropertyEqualityTest, findsTheNodesHoldingNoMap) {
    tagRemyAndAdam();
    expectRows("MATCH (n {name: 'Maxime'}) WHERE n.attrs IS NULL RETURN n.name", {{"Maxime"}});
}

TEST_F(MapPropertyEqualityTest, findsTheNodesHoldingAMap) {
    tagRemyAndAdam();
    expectRows("MATCH (n) WHERE n.attrs IS NOT NULL RETURN n.name", {{"Adam"}, {"Remy"}});
}

TEST_F(MapPropertyEqualityTest, matchesAStoredMapOnAnEdge) {
    write("MATCH (a {name: 'Remy'})-[e:KNOWS_WELL]->(b) SET e.attrs = {w: 1}");
    expectRows("MATCH (a)-[e:KNOWS_WELL]->(b) WHERE e.attrs = {w: 1} RETURN a.name",
               {{"Remy"}});
}

TEST_F(MapPropertyEqualityTest, comparesTwoStoredMaps) {
    write("MATCH (n {name: 'Remy'}) SET n.attrs = {a: 1}");
    write("MATCH (n {name: 'Adam'}) SET n.attrs = {a: 1}");
    write("MATCH (n {name: 'Maxime'}) SET n.attrs = {a: 2}");
    expectRows("MATCH (a {name: 'Remy'}), (b) WHERE a.attrs = b.attrs RETURN b.name",
               {{"Adam"}, {"Remy"}});
}

TEST_F(MapPropertyEqualityTest, matchesAStoredMapAgainstAnUnwoundCell) {
    tagRemyAndAdam();
    expectRows("MATCH (n) UNWIND [{a: 1, b: 'x'}] AS candidate WITH n, candidate "
               "WHERE n.attrs = candidate RETURN n.name",
               {{"Remy"}});
}

TEST_F(MapPropertyEqualityTest, returnsTheDistinctStoredMaps) {
    tagRemyAndAdam();
    write("MATCH (n {name: 'Maxime'}) SET n.attrs = {a: 1, b: 'x'}");
    expectRows("MATCH (n) WHERE n.attrs IS NOT NULL RETURN DISTINCT n.attrs",
               {{"{a: 1, b: x}"}, {"{a: 3}"}});
}

TEST_F(MapPropertyEqualityTest, countsTheDistinctStoredMaps) {
    tagRemyAndAdam();
    write("MATCH (n {name: 'Maxime'}) SET n.attrs = {a: 1, b: 'x'}");
    expectRows("MATCH (n) RETURN count(DISTINCT n.attrs)", {{"2"}});
}

TEST_F(MapPropertyEqualityTest, groupsByAStoredMap) {
    tagRemyAndAdam();
    write("MATCH (n {name: 'Maxime'}) SET n.attrs = {a: 1, b: 'x'}");
    expectRows("MATCH (n) WHERE n.attrs IS NOT NULL RETURN n.attrs, count(n)",
               {{"{a: 1, b: x}", "2"}, {"{a: 3}", "1"}});
}
