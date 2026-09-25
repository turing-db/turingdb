#include <gtest/gtest.h>

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

// A list built out of columns with a null among them hands its elements back as type-erased
// cells, and arithmetic over a cell computes in the f64 its mixed numeric tags land on. The
// analyzer reads that same list as the integers its other elements name, so an equality it
// accepts as Integer = Integer meets a double against an integer column at run time.
class TaggedCellArithmeticEqualityTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager());

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);
    }

    QueryStatus runQuery(std::string_view query, RowSink* sink) {
        QueryStatus status;
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              ChangeID::head(),
                              &_env->getMem(),
                              sink);

        return status;
    }

    void expectRows(std::string_view query, const Rows& expected) {
        RowSink sink;
        const QueryStatus status = runQuery(query, &sink);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        std::string actualText;
        describeRows(sink.rows(), actualText);

        EXPECT_EQ(sink.rows(), expected) << "query: " << query << "\nactual:\n" << actualText;
    }

    void expectRejected(std::string_view query, std::string_view reason) {
        RowSink sink;
        const QueryStatus status = runQuery(query, &sink);

        EXPECT_EQ(status.getStatus(), QueryStatus::Status::ANALYZE_ERROR) << "query: " << query;

        const std::string error = status.getError();
        EXPECT_NE(error.find(reason), std::string::npos) << "query: " << query << "\nerror: " << error;
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

TEST_F(TaggedCellArithmeticEqualityTest, comparesACellArithmeticResultAgainstAnInteger) {
    expectRows("MATCH (n:Person {name: 'Remy'}) RETURN [0, n.missing][0] - 0 = 3", {{"false"}});
}

TEST_F(TaggedCellArithmeticEqualityTest, answersTrueWhereTheTwoSidesHoldTheSameNumber) {
    expectRows("MATCH (n:Person {name: 'Remy'}) RETURN [0, n.missing][0] - 0 = 0", {{"true"}});
}

TEST_F(TaggedCellArithmeticEqualityTest, comparesWithTheIntegerOnTheLeft) {
    expectRows("MATCH (n:Person {name: 'Remy'}) RETURN 3 = [0, n.missing][0] - 0", {{"false"}});
}

TEST_F(TaggedCellArithmeticEqualityTest, comparesForInequality) {
    expectRows("MATCH (n:Person {name: 'Remy'}) RETURN [0, n.missing][0] - 0 <> 3", {{"true"}});
}

TEST_F(TaggedCellArithmeticEqualityTest, answersNullWhereTheCellHoldsNoNumber) {
    expectRows("MATCH (n:Person {name: 'Remy'}) RETURN [0, n.missing][1] - 0 = 3", {{"null"}});
}

TEST_F(TaggedCellArithmeticEqualityTest, comparesACellReadOutOfAPropertyElement) {
    expectRows("MATCH (n:Person {name: 'Remy'}) RETURN [n.age, null][0] - 0 = 32", {{"true"}});
}

TEST_F(TaggedCellArithmeticEqualityTest, comparesAMultipliedCell) {
    expectRows("MATCH (n:Person {name: 'Remy'}) RETURN [n.age, null][0] * 2 = 64", {{"true"}});
}

TEST_F(TaggedCellArithmeticEqualityTest, comparesAgainstANullableIntegerProperty) {
    expectRows("MATCH (n:Person {name: 'Remy'}) RETURN [n.age, null][0] - 0 = n.age", {{"true"}});
}

// A count is unsigned, which is the other integer width a cell's double is compared against.
TEST_F(TaggedCellArithmeticEqualityTest, comparesAgainstAnUnsignedCount) {
    expectRows("MATCH (n:Person) WITH count(n) AS personCount "
               "MATCH (m:Person {name: 'Remy'}) RETURN [m.age, null][0] - 0 = personCount",
               {{"false"}});
}

TEST_F(TaggedCellArithmeticEqualityTest, filtersOnACellArithmeticEquality) {
    expectRows("MATCH (n:Person {name: 'Remy'}) WHERE [n.age, null][0] - 0 = 32 RETURN n.name",
               {{"Remy"}});
}

TEST_F(TaggedCellArithmeticEqualityTest, comparesAnUnwoundCell) {
    expectRows("MATCH (n:Person {name: 'Remy'}) UNWIND [n.age, null] AS element "
               "RETURN element - 0 = 32",
               {{"true"}, {"null"}});
}

// The subtraction answers a double, which is the operand the comparisons above hand the
// equality: were the cell read as an integer instead, none of them would reach it.
TEST_F(TaggedCellArithmeticEqualityTest, computesTheCellArithmeticInADouble) {
    expectRows("MATCH (n:Person {name: 'Remy'}) RETURN [n.age, null][0] - 0", {{"32.000000"}});
}

// The kernels these comparisons reach are new; the rule that turns away the double
// equalities the rounding concern is about is not, and both sides of it still hold.
TEST_F(TaggedCellArithmeticEqualityTest, stillRejectsTheEqualityOfTwoDoubles) {
    expectRejected("RETURN 1.0 = 1.0", "not encouraged due to potential rounding");
}

TEST_F(TaggedCellArithmeticEqualityTest, stillRejectsADoubleTypedExpressionAgainstAnInteger) {
    expectRejected("MATCH (n:Person) RETURN avg(n.age) = 3",
                   "Operands are not valid or compatible types: 'Double' and 'Integer'");
}
