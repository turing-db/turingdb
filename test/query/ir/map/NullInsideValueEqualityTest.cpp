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

class NullInsideValueEqualityTest : public TuringTest {
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

TEST_F(NullInsideValueEqualityTest, matchesNoRowWhereTwoMapsHoldNull) {
    expectRows("UNWIND [1] AS x WITH x WHERE {a: null} = {a: null} RETURN count(x)", {{"0"}});
}

TEST_F(NullInsideValueEqualityTest, dropsThePairsWhoseMapsHoldANull) {
    expectRows("MATCH (a:Person), (b:Person) WHERE {age: a.age} = {age: b.age} RETURN count(*)", {{"4"}});
}

TEST_F(NullInsideValueEqualityTest, matchesNoMapAgainstAMapHoldingNull) {
    expectRows("MATCH (n:Person) WITH {age: n.age} AS m WHERE m = {age: null} RETURN count(*)", {{"0"}});
}

TEST_F(NullInsideValueEqualityTest, keepsTheNegatedComparisonNull) {
    expectRows("MATCH (n:Person) WITH {age: n.age} AS m WHERE m <> {age: 32} RETURN count(*)", {{"0"}});
}

TEST_F(NullInsideValueEqualityTest, returnsNullForMapsHoldingNull) {
    expectRows("RETURN {a: null} = {a: null}, {a: null} <> {a: null}", {{"null", "null"}});
    expectRows("RETURN {a: 1, b: null} = {a: 1, b: 2}", {{"null"}});
}

TEST_F(NullInsideValueEqualityTest, returnsFalseWhereAnotherEntryDiffers) {
    expectRows("RETURN {a: 1, b: null} = {a: 2, b: null}, {a: 1, b: null} <> {a: 2, b: null}",
               {{"false", "true"}});
}

TEST_F(NullInsideValueEqualityTest, returnsFalseForMapsWithOtherKeys) {
    expectRows("RETURN {a: null} = {b: null}, {a: null} = {a: null, b: 1}", {{"false", "false"}});
}

TEST_F(NullInsideValueEqualityTest, returnsNullForANullNestedInAList) {
    expectRows("RETURN [[null]] = [[null]], [[1, null]] = [[1, 2]]", {{"null", "null"}});
    expectRows("RETURN [[1, null]] = [[2, null]]", {{"false"}});
}

TEST_F(NullInsideValueEqualityTest, returnsNullForAMapNestedInAList) {
    expectRows("RETURN [{a: null}] = [{a: null}]", {{"null"}});
}

TEST_F(NullInsideValueEqualityTest, returnsNullForAValueNestedInAMap) {
    expectRows("RETURN {a: [null]} = {a: [null]}, {a: {b: null}} = {a: {b: null}}", {{"null", "null"}});
}

TEST_F(NullInsideValueEqualityTest, keepsEqualMapsEqual) {
    expectRows("RETURN {a: 1, b: [1, {c: 'x'}]} = {b: [1, {c: 'x'}], a: 1}", {{"true"}});
}

TEST_F(NullInsideValueEqualityTest, comparesAStoredMapHoldingNull) {
    write("MATCH (n:Person {name: 'Remy'}) SET n.attrs = {a: 1, b: [null]}");
    expectRows("MATCH (n:Person) WHERE n.attrs = {a: 1, b: [null]} RETURN count(*)", {{"0"}});
    expectRows("MATCH (n:Person) WHERE n.attrs = {a: 2, b: [null]} RETURN count(*)", {{"0"}});
    expectRows("MATCH (n:Person {name: 'Remy'}) RETURN n.attrs = {a: 1, b: [null]}", {{"null"}});
}

TEST_F(NullInsideValueEqualityTest, comparesAnUnwoundMapHoldingNull) {
    expectRows("UNWIND [{a: null}, {a: 1}] AS m WITH m WHERE m = {a: null} RETURN count(*)", {{"0"}});
    expectRows("UNWIND [{a: null}, {a: 1}] AS m WITH m WHERE m = {a: 1} RETURN count(*)", {{"1"}});
}

TEST_F(NullInsideValueEqualityTest, findsAMapHoldingNullInAListAsNull) {
    expectRows("RETURN {a: null} IN [{a: null}], {a: null} IN [{a: 1}], {a: null} IN [{b: 1}], {a: 1} IN [{a: null}, {a: 1}]",
               {{"null", "null", "false", "true"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
