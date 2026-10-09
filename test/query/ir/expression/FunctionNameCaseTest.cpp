#include <gtest/gtest.h>

#include <string_view>
#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

// A function name is matched whatever its case, as Cypher's are: stdev is stDev and COUNT
// is count. A procedure name is not a function name and keeps its case.
class FunctionNameCaseTest : public CallV3Test {
protected:
    void expectRows(std::string_view query, const std::vector<StringRowSink::Row>& expected) {
        StringRowSink sink;
        runQuery(query, sink);

        EXPECT_EQ(sink.getRows(), expected) << query;
    }
};

TEST_F(FunctionNameCaseTest, lowerCaseAggregate) {
    expectRows("UNWIND [1.0, 3.0] AS v RETURN stdev(v) AS s", {{"1.4142135623730951"}});
}

TEST_F(FunctionNameCaseTest, upperCaseAggregate) {
    expectRows("MATCH (n) RETURN COUNT(n), Sum(n.age)", {{"18", "64"}});
}

TEST_F(FunctionNameCaseTest, mixedCaseScalarFunctions) {
    expectRows("RETURN TOUPPER('abc'), toupper('d'), SQRT(16.0), tointeger('7')", {{"ABC", "D", "4", "7"}});
}

TEST_F(FunctionNameCaseTest, percentileInAnyCase) {
    expectRows("UNWIND [10, 20, 30, 40] AS v RETURN PERCENTILECONT(v, 0.5), percentiledisc(v, 0.5)", {{"25", "20"}});
}

TEST_F(FunctionNameCaseTest, unknownFunctionInAnyCase) {
    runQueryExpectingError("RETURN NOSUCHFUNCTION(1)", "Function 'NOSUCHFUNCTION' does not exist");
}
