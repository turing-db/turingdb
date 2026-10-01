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

namespace {

constexpr size_t simpleGraphNodeCount = 18;

}

class ListInListTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager(), &_env->getMem(), &_env->getCompilerContext());

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);
    }

    void runQuery(std::string_view query, StringRowSink& sink, QueryStatus& status) {
        _interpreter->execute(status, query, _graphName, CommitHash::head(), ChangeID::head(), &sink);
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

TEST_F(ListInListTest, findsAListInAListOfLists) {
    expectRows("RETURN [1] IN [[1]]", {{"true"}});
    expectRows("RETURN [1] IN [[2], [1]]", {{"true"}});
    expectRows("RETURN [] IN [[]]", {{"true"}});
}

TEST_F(ListInListTest, answersFalseForAListMissingFromAListOfLists) {
    expectRows("RETURN [1] IN [[2]]", {{"false"}});
    expectRows("RETURN [1, 2] IN [[3, null]]", {{"false"}});
}

TEST_F(ListInListTest, answersFalseForAListInAListOfScalars) {
    expectRows("RETURN [1] IN [1, 2]", {{"false"}});
}

TEST_F(ListInListTest, answersFalseForAListInAnEmptyList) {
    expectRows("RETURN [1] IN []", {{"false"}});
}

TEST_F(ListInListTest, answersNullForAListInANullElement) {
    expectRows("RETURN [1] IN [null][0]", {{"null"}});
    expectRows("RETURN [1] IN [[2], null]", {{"null"}});
}

TEST_F(ListInListTest, answersNullForAListComparedToAListHoldingNull) {
    expectRows("RETURN [1] IN [[null]]", {{"null"}});
    expectRows("RETURN [null] IN [[null]]", {{"null"}});
}

TEST_F(ListInListTest, findsAListPastANullElement) {
    expectRows("RETURN [1] IN [null, [1]]", {{"true"}});
}

TEST_F(ListInListTest, findsAListInAListElement) {
    expectRows("RETURN [1] IN [[[1]]][0]", {{"true"}});
}

TEST_F(ListInListTest, findsAListElementInAListOfLists) {
    expectRows("RETURN [[1]][0] IN [[1]]", {{"true"}});
    expectRows("RETURN [[1]][0] IN [[2]]", {{"false"}});
}

TEST_F(ListInListTest, answersNullForAListElementComparedToAListHoldingNull) {
    expectRows("RETURN [[1]][0] IN [[null]]", {{"null"}});
}

TEST_F(ListInListTest, answersNullForAListInNull) {
    expectRows("RETURN [1] IN null", {{"null"}});
}

TEST_F(ListInListTest, findsAMapInAListOfMaps) {
    expectRows("RETURN {a: 1} IN [{a: 1}]", {{"true"}});
    expectRows("RETURN {a: 1} IN [{a: 2}]", {{"false"}});
    expectRows("RETURN {a: 1} IN [null]", {{"null"}});
}

TEST_F(ListInListTest, findsAListOfPropertiesInAListOfLists) {
    std::vector<StringRowSink::Row> expected(simpleGraphNodeCount - 2, {"false"});
    expected.push_back({"true"});
    expected.push_back({"true"});

    expectRows("MATCH (n) RETURN [n.name] IN [['Remy'], ['Adam']]", expected);
}

TEST_F(ListInListTest, filtersOnAListOfPropertiesInAListOfLists) {
    expectRows("MATCH (n) WHERE [n.name] IN [['Remy']] RETURN n.name", {{"Remy"}});
}

TEST_F(ListInListTest, answersNullForAListOfMissingPropertiesInAListOfLists) {
    const std::vector<StringRowSink::Row> expected(simpleGraphNodeCount, {"null"});

    expectRows("MATCH (n) RETURN [n.y] IN [[1]]", expected);
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
