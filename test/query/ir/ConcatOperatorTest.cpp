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

// `||` concatenates two strings or two lists, which is all Cypher gives it: it is not a
// spelling of OR, and it computes nothing over numbers.
class ConcatOperatorTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager());

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

    void expectError(std::string_view query, std::string_view message) {
        RowSink sink;
        QueryStatus status;
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              ChangeID::head(),
                              &_env->getMem(),
                              &sink);

        ASSERT_FALSE(status.isOk()) << "query: " << query;
        EXPECT_NE(status.getError().find(message), std::string::npos)
            << "query: " << query << "\nerror: " << status.getError();
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

TEST_F(ConcatOperatorTest, concatenatesTwoStrings) {
    expectRows("RETURN 'a' || 'b' AS s", {{"ab"}});
}

TEST_F(ConcatOperatorTest, concatenatesTwoLists) {
    expectRows("RETURN [1,2] || [3,4] AS l", {{"[1, 2, 3, 4]"}});
}

TEST_F(ConcatOperatorTest, chainsThreeConcatenations) {
    expectRows("RETURN 'a' || ': ' || 'b' AS s", {{"a: b"}});
}

TEST_F(ConcatOperatorTest, concatenatesNestedLists) {
    expectRows("RETURN [[1,2]] || [[3]] AS l", {{"[[1, 2], [3]]"}});
}

TEST_F(ConcatOperatorTest, concatenatesTwoStringProperties) {
    expectRows("MATCH (n:Person {name: 'Remy'}) RETURN n.name || n.dob AS s", {{"Remy18/01"}});
}

TEST_F(ConcatOperatorTest, concatenatesAListProperty) {
    expectRows("MATCH (n:Person {name: 'Remy'}) RETURN [x IN [1] | x] || [2] AS l", {{"[1, 2]"}});
}

// The list it keeps holds the null, as the concatenated list does: only a null operand -
// no list at all - makes the whole concatenation null
TEST_F(ConcatOperatorTest, keepsTheNullElementsItConcatenates) {
    expectRows("RETURN [1, 2] || [3, null] AS l", {{"[1, 2, 3, null]"}});
}

TEST_F(ConcatOperatorTest, concatenatesWithNull) {
    expectRows("RETURN 'a' || null AS s, [1] || null AS l", {{"null", "null"}});
}

TEST_F(ConcatOperatorTest, filtersTheConcatenationOfTwoLists) {
    expectRows("RETURN [x IN ([1, null, 3] || [null, 5, null]) WHERE x IS NOT NULL] AS l",
               {{"[1, 3, 5]"}});
}

TEST_F(ConcatOperatorTest, bindsTighterThanAComparison) {
    expectRows("RETURN 'a' || 'b' = 'ab' AS same", {{"true"}});
}

TEST_F(ConcatOperatorTest, isNoBooleanOperator) {
    expectError("RETURN true || false AS b", "Operands are not valid and compatible types for '||'");
}

TEST_F(ConcatOperatorTest, computesNothingOverNumbers) {
    expectError("RETURN 1 || 2 AS n", "Operands are not valid and compatible types for '||'");
}

TEST_F(ConcatOperatorTest, concatenatesNoStringOntoAList) {
    expectError("RETURN [1] || 'a' AS l", "Operands are not valid and compatible types for '||'");
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
