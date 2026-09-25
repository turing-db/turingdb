#include <gtest/gtest.h>

#include <stddef.h>

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

// A bare pattern stands for whether it matches, so two of them compare and combine as
// booleans do: false orders below true, and AND / OR / XOR read them as truth values.
// Arithmetic over them is not Cypher, and the analyzer rejects it.
//
// The nine ordered pairs of Remy, Adam and Computers carry all four combinations of the
// two patterns the tests compare. Remy-->Adam and Adam-->Remy both exist, Remy-->Computers
// exists in that direction alone, and no edge runs between any other pair.
class ExistenceComparisonTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager());

        SystemAccessor system = _env->getSystemManager().accessUnique();
        SimpleGraph::createSimpleGraph(system.createGraph(_graphName));
    }

    void runQuery(QueryStatus& status, std::string_view query, RowSink* sink) {
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              ChangeID::head(),
                              &_env->getMem(),
                              sink);
    }

    void expectRows(std::string_view query, const Rows& expected) {
        RowSink sink;
        QueryStatus status;
        runQuery(status, query, &sink);

        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        Rows actual;
        sink.sortedRows(actual);

        Rows sortedExpected = expected;
        std::sort(sortedExpected.begin(), sortedExpected.end());

        std::string actualText;
        describeRows(actual, actualText);

        EXPECT_EQ(actual, sortedExpected) << "query: " << query << "\ngot:\n" << actualText;
    }

    // Reads the expression over the nine pairs, against one truth value per pair in the
    // order _pairs lists them
    void expectPairs(const std::string& expression, const Row& values) {
        ASSERT_EQ(values.size(), _pairs.size()) << "expression: " << expression;

        Rows expected;
        for (size_t pair = 0; pair < _pairs.size(); pair++) {
            Row& row = expected.emplace_back(_pairs[pair]);
            row.push_back(values[pair]);
        }

        expectRows(_threeNodes + "RETURN a.name, b.name, " + expression + " AS r", expected);
    }

    void expectError(std::string_view query, std::string_view message) {
        RowSink sink;
        QueryStatus status;
        runQuery(status, query, &sink);

        ASSERT_FALSE(status.isOk()) << "query: " << query << " was expected to fail";
        EXPECT_NE(status.getError().find(message), std::string::npos)
            << "query: " << query << "\nerror: " << status.getError();
    }

    void expectExpressionError(const std::string& expression, std::string_view message) {
        expectError(_threeNodes + "RETURN " + expression + " AS r", message);
    }

    const std::string _graphName = "simpledb";

    const std::string _threeNodes = "MATCH (a), (b) "
                                    "WHERE a.name IN ['Remy', 'Adam', 'Computers'] "
                                    "AND b.name IN ['Remy', 'Adam', 'Computers'] ";

    const Rows _pairs {{"Adam", "Adam"},
                       {"Adam", "Computers"},
                       {"Adam", "Remy"},
                       {"Computers", "Adam"},
                       {"Computers", "Computers"},
                       {"Computers", "Remy"},
                       {"Remy", "Adam"},
                       {"Remy", "Computers"},
                       {"Remy", "Remy"}};

    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

// The two patterns every table below combines, read on their own
TEST_F(ExistenceComparisonTest, forwardPattern) {
    expectPairs("(a)-->(b)", {"false", "false", "true", "false", "false", "false", "true", "true", "false"});
}

TEST_F(ExistenceComparisonTest, backwardPattern) {
    expectPairs("(b)-->(a)", {"false", "false", "true", "false", "false", "true", "true", "false", "false"});
}

TEST_F(ExistenceComparisonTest, lessThan) {
    expectPairs("(a)-->(b) < (b)-->(a)",
                {"false", "false", "false", "false", "false", "true", "false", "false", "false"});
}

TEST_F(ExistenceComparisonTest, greaterThan) {
    expectPairs("(a)-->(b) > (b)-->(a)",
                {"false", "false", "false", "false", "false", "false", "false", "true", "false"});
}

TEST_F(ExistenceComparisonTest, lessThanOrEqual) {
    expectPairs("(a)-->(b) <= (b)-->(a)",
                {"true", "true", "true", "true", "true", "true", "true", "false", "true"});
}

TEST_F(ExistenceComparisonTest, greaterThanOrEqual) {
    expectPairs("(a)-->(b) >= (b)-->(a)",
                {"true", "true", "true", "true", "true", "false", "true", "true", "true"});
}

TEST_F(ExistenceComparisonTest, equal) {
    expectPairs("(a)-->(b) = (b)-->(a)",
                {"true", "true", "true", "true", "true", "false", "true", "false", "true"});
}

TEST_F(ExistenceComparisonTest, notEqual) {
    expectPairs("(a)-->(b) <> (b)-->(a)",
                {"false", "false", "false", "false", "false", "true", "false", "true", "false"});
}

TEST_F(ExistenceComparisonTest, conjunction) {
    expectPairs("(a)-->(b) AND (b)-->(a)",
                {"false", "false", "true", "false", "false", "false", "true", "false", "false"});
}

TEST_F(ExistenceComparisonTest, disjunction) {
    expectPairs("(a)-->(b) OR (b)-->(a)",
                {"false", "false", "true", "false", "false", "true", "true", "true", "false"});
}

TEST_F(ExistenceComparisonTest, exclusiveDisjunction) {
    expectPairs("(a)-->(b) XOR (b)-->(a)",
                {"false", "false", "false", "false", "false", "true", "false", "true", "false"});
}

TEST_F(ExistenceComparisonTest, negatedExclusiveDisjunction) {
    expectPairs("NOT ((a)-->(b) XOR (b)-->(a))",
                {"true", "true", "true", "true", "true", "false", "true", "false", "true"});
}

TEST_F(ExistenceComparisonTest, conjunctionNeverOrdersAboveDisjunction) {
    expectPairs("((a)-->(b) AND (b)-->(a)) <= ((a)-->(b) OR (b)-->(a))",
                {"true", "true", "true", "true", "true", "true", "true", "true", "true"});
}

TEST_F(ExistenceComparisonTest, againstABooleanLiteral) {
    expectPairs("(a)-->(b) = true",
                {"false", "false", "true", "false", "false", "false", "true", "true", "false"});
}

TEST_F(ExistenceComparisonTest, belowABooleanLiteralOnTheLeft) {
    expectPairs("true >= (a)-->(b)",
                {"true", "true", "true", "true", "true", "true", "true", "true", "true"});
}

// KNOWS_WELL runs between Remy and Adam only, so the typed pattern holds nowhere the
// untyped one does not
TEST_F(ExistenceComparisonTest, typedPatternOrdersBelowTheUntypedOne) {
    expectPairs("(a)-[:KNOWS_WELL]->(b) < (a)-->(b)",
                {"false", "false", "false", "false", "false", "false", "false", "true", "false"});
}

TEST_F(ExistenceComparisonTest, againstABooleanProperty) {
    expectRows("MATCH (a:Person) RETURN a.name, (a)-->() = a.hasPhD AS r",
               {{"Adam", "true"},
                {"Cyrus", "false"},
                {"Doruk", "false"},
                {"Luc", "true"},
                {"Martina", "true"},
                {"Maxime", "false"},
                {"Remy", "true"},
                {"Suhas", "false"}});
}

TEST_F(ExistenceComparisonTest, belowABooleanProperty) {
    expectRows("MATCH (a:Person) RETURN a.name, (a)-[:KNOWS_WELL]->() < a.hasPhD AS r",
               {{"Adam", "false"},
                {"Cyrus", "false"},
                {"Doruk", "false"},
                {"Luc", "true"},
                {"Martina", "true"},
                {"Maxime", "false"},
                {"Remy", "false"},
                {"Suhas", "false"}});
}

TEST_F(ExistenceComparisonTest, filtersOnTheComparison) {
    expectRows(_threeNodes + "AND (a)-->(b) > (b)-->(a) RETURN a.name, b.name",
               {{"Remy", "Computers"}});
}

TEST_F(ExistenceComparisonTest, filtersOnTheExclusiveDisjunction) {
    expectRows(_threeNodes + "AND ((a)-->(b) XOR (b)-->(a)) RETURN a.name, b.name",
               {{"Computers", "Remy"}, {"Remy", "Computers"}});
}

// The 18 * 18 rows a two-pattern MATCH crosses. (a)-->(b) matched, so it is true on every
// row and orders at or above (n)-->(b) on all of them.
TEST_F(ExistenceComparisonTest, groupsOnTheComparisonOfTwoMatchedPatterns) {
    expectRows("MATCH (n)-->(m), (a)-->(b) RETURN (a)-->(b) >= (n)-->(b) AS c, count(*)",
               {{"true", "324"}});
}

TEST_F(ExistenceComparisonTest, groupsOnTheExclusiveDisjunctionOfTwoMatchedPatterns) {
    expectRows("MATCH (n)-->(m), (a)-->(b) RETURN (a)-->(b) XOR (n)-->(b) AS c, count(*)",
               {{"false", "73"}, {"true", "251"}});
}

TEST_F(ExistenceComparisonTest, additionOfTwoPatternsIsRejected) {
    expectExpressionError("(a)-->(b) + (b)-->(a)",
                          "Operands are not valid and compatible types for '+': 'Bool' and 'Bool'");
}

TEST_F(ExistenceComparisonTest, additionOfAPatternAndANumberIsRejected) {
    expectExpressionError("(a)-->(b) + 1",
                          "Operands are not valid and compatible types for '+': 'Bool' and 'Integer'");
}

TEST_F(ExistenceComparisonTest, subtractionIsRejected) {
    expectExpressionError("(a)-->(b) - (b)-->(a)",
                          "Operands are not valid and compatible numeric types: 'Bool' and 'Bool'");
}

TEST_F(ExistenceComparisonTest, multiplicationIsRejected) {
    expectExpressionError("(a)-->(b) * (b)-->(a)",
                          "Operands are not valid and compatible numeric types: 'Bool' and 'Bool'");
}

TEST_F(ExistenceComparisonTest, divisionIsRejected) {
    expectExpressionError("(a)-->(b) / (b)-->(a)",
                          "Operands are not valid and compatible numeric types: 'Bool' and 'Bool'");
}

TEST_F(ExistenceComparisonTest, moduloIsRejected) {
    expectExpressionError("(a)-->(b) % (b)-->(a)",
                          "Operands are not valid and compatible numeric types: 'Bool' and 'Bool'");
}

TEST_F(ExistenceComparisonTest, powerIsRejected) {
    expectExpressionError("(a)-->(b) ^ (b)-->(a)",
                          "Operands are not valid and compatible numeric types: 'Bool' and 'Bool'");
}

TEST_F(ExistenceComparisonTest, unaryMinusIsRejected) {
    expectExpressionError("-((a)-->(b))", "Operand must be an integer or double, not 'Bool'");
}

TEST_F(ExistenceComparisonTest, startsWithIsRejected) {
    expectExpressionError("(a)-->(b) STARTS WITH (b)-->(a)",
                          "String expressions operands must be strings, not 'Bool' and 'Bool'");
}

TEST_F(ExistenceComparisonTest, endsWithIsRejected) {
    expectExpressionError("(a)-->(b) ENDS WITH (b)-->(a)",
                          "String expressions operands must be strings, not 'Bool' and 'Bool'");
}

TEST_F(ExistenceComparisonTest, containsIsRejected) {
    expectExpressionError("(a)-->(b) CONTAINS (b)-->(a)",
                          "String expressions operands must be strings, not 'Bool' and 'Bool'");
}
