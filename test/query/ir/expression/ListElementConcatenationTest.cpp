#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <string>
#include <string_view>

#include "QueryInterpreterV3.h"
#include "QueryStatus.h"

#include "Graph.h"
#include "SimpleGraph.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"

#include "IRTestRows.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

class ListElementConcatenationTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager(), &_env->getMem(), &_env->getCompilerContext());

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);
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

    void expectError(std::string_view query, std::string_view message) {
        RowSink sink;
        QueryStatus status;
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              ChangeID::head(),
                              &sink);

        ASSERT_FALSE(status.isOk()) << "query: " << query;
        EXPECT_NE(status.getError().find(message), std::string::npos)
            << "query: " << query << "\nerror: " << status.getError();
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

TEST_F(ListElementConcatenationTest, concatenatesTwoNestedLists) {
    expectRows("WITH [[1, 2], [3]] AS l RETURN l[0] || l[1] AS joined", {{"[1, 2, 3]"}});
}

TEST_F(ListElementConcatenationTest, addsTwoNestedLists) {
    expectRows("WITH [[1, 2], [3]] AS l RETURN l[0] + l[1] AS joined", {{"[1, 2, 3]"}});
}

TEST_F(ListElementConcatenationTest, concatenatesAListOntoANestedList) {
    expectRows("WITH [[1, 2], [3]] AS l RETURN l[0] || [4] AS joined", {{"[1, 2, 4]"}});
}

TEST_F(ListElementConcatenationTest, concatenatesANestedListOntoAList) {
    expectRows("WITH [[1, 2], [3]] AS l RETURN [4] || l[0] AS joined", {{"[4, 1, 2]"}});
}

TEST_F(ListElementConcatenationTest, addsAListToANestedList) {
    expectRows("WITH [[1, 2], [3]] AS l RETURN l[0] + [4] AS joined", {{"[1, 2, 4]"}});
}

TEST_F(ListElementConcatenationTest, addsANestedListToAList) {
    expectRows("WITH [[1, 2], [3]] AS l RETURN [4] + l[0] AS joined", {{"[4, 1, 2]"}});
}

TEST_F(ListElementConcatenationTest, appendsANumberToANestedList) {
    expectRows("WITH [[1, 2], [3]] AS l RETURN l[0] + 4 AS joined", {{"[1, 2, 4]"}});
}

TEST_F(ListElementConcatenationTest, prependsANumberToANestedList) {
    expectRows("WITH [[1, 2], [3]] AS l RETURN 4 + l[0] AS joined", {{"[4, 1, 2]"}});
}

TEST_F(ListElementConcatenationTest, appendsAStringToANestedList) {
    expectRows("WITH [[1, 2], 'a'] AS l RETURN l[0] + l[1] AS joined", {{"[1, 2, a]"}});
}

TEST_F(ListElementConcatenationTest, prependsAStringToANestedList) {
    expectRows("WITH [[1, 2], 'a'] AS l RETURN l[1] + l[0] AS joined", {{"[a, 1, 2]"}});
}

TEST_F(ListElementConcatenationTest, concatenatesWhatEachRowHolds) {
    expectRows("UNWIND [[1, 2], 'a'] AS x RETURN x || x AS joined", {{"[1, 2, 1, 2]"}, {"aa"}});
}

TEST_F(ListElementConcatenationTest, addsAStringToWhatEachRowHolds) {
    expectRows("UNWIND [[1], 'a', 2, true] AS x RETURN x + 'b' AS joined",
               {{"[1, b]"}, {"ab"}, {"2b"}, {"trueb"}});
}

TEST_F(ListElementConcatenationTest, addsAListToWhatEachRowHolds) {
    expectRows("UNWIND [[1], 'a'] AS x RETURN x + [3] AS a, [0] + x AS b", {{"[1, 3]", "[0, 1]"}, {"[a, 3]", "[0, a]"}});
}

TEST_F(ListElementConcatenationTest, addsABooleanToWhatEachRowHolds) {
    expectRows("UNWIND ['a', [1]] AS x RETURN x + true AS joined", {{"atrue"}, {"[1, true]"}});
}

TEST_F(ListElementConcatenationTest, answersNullForANullElement) {
    expectRows("WITH [[1, 2], 'a', null] AS l RETURN l[0] || l[2] AS a, l[2] || 'x' AS b, l[2] || [3] AS c",
               {{"null", "null", "null"}});
}

TEST_F(ListElementConcatenationTest, answersNullForANullNestedList) {
    expectRows("WITH [[1, 2], null] AS l RETURN l[0] || l[1] AS a, l[1] || [3] AS b", {{"null", "null"}});
}

TEST_F(ListElementConcatenationTest, appendsANullElementToANestedList) {
    expectRows("WITH [[1, 2], 'a', null] AS l RETURN l[0] + l[2] AS a, l[1] + l[2] AS b", {{"[1, 2, null]", "null"}});
}

TEST_F(ListElementConcatenationTest, convertsNoNumberAnElementHolds) {
    expectError("WITH [1] AS l RETURN l[0] || '!' AS said", "||");
}

TEST_F(ListElementConcatenationTest, convertsNoNumberARowHolds) {
    expectError("UNWIND ['a', 1] AS x RETURN x || '!' AS said", "||");
}

TEST_F(ListElementConcatenationTest, concatenatesNoStringOntoANestedList) {
    expectError("WITH [[1, 2], 'a'] AS l RETURN l[0] || l[1] AS joined", "||");
}

TEST_F(ListElementConcatenationTest, addsABooleanToAString) {
    expectRows("RETURN true + 'x' AS a, 'x' + false AS b", {{"truex", "xfalse"}});
}

TEST_F(ListElementConcatenationTest, addsABooleanPropertyToAString) {
    expectRows("MATCH (n:Person {name: 'Remy'}) RETURN n.name + ' ' + n.isFrench AS said", {{"Remy true"}});
}

TEST_F(ListElementConcatenationTest, concatenatesNoBooleanOntoAString) {
    expectError("RETURN true || 'x' AS said", "||");
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
