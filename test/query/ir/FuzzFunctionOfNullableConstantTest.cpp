#include <gtest/gtest.h>

#include <stddef.h>

#include <algorithm>
#include <memory>
#include <string>
#include <string_view>

#include "NLOutputSink.h"
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

// AFL inputs that tripped the 'typedInput' assertion of functionConstKernel: a function
// over a constant that can be null, such as the one toInteger("10") answers.
class FuzzFunctionOfNullableConstantTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager());

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);
    }

    QueryStatus runQuery(std::string_view query, NLOutputSink* sink) {
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

    void expectRowShape(std::string_view query, size_t rowCount, size_t columnCount) {
        RowSink sink;
        const QueryStatus status = runQuery(query, &sink);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        const Rows& rows = sink.rows();
        ASSERT_EQ(rows.size(), rowCount) << "query: " << query;

        const bool everyRowHasEveryColumn = std::ranges::all_of(rows, [columnCount](const Row& row) {
            return row.size() == columnCount;
        });
        EXPECT_TRUE(everyRowHasEveryColumn) << "query: " << query;
    }

    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
    std::string _graphName {"simpledb"};
    size_t _nodeCount {18};
};

TEST_F(FuzzFunctionOfNullableConstantTest, ReturnConversionOfConversion) {
    expectRows("RETURN toFloat(toInteger('10'))", {{"10.000000"}});
}

TEST_F(FuzzFunctionOfNullableConstantTest, ReturnConversionOfConversionPlusInteger) {
    expectRows("RETURN toFloat(toInteger('10') + 1)", {{"11.000000"}});
}

TEST_F(FuzzFunctionOfNullableConstantTest, ReturnConversionOfConcatenatedConversion) {
    expectRows("RETURN toInteger(toInteger('1') + '3')", {{"13"}});
}

TEST_F(FuzzFunctionOfNullableConstantTest, ReturnSizeOfConcatenatedConversion) {
    expectRows("RETURN size(toInteger('1') + '3')", {{"2"}});
}

TEST_F(FuzzFunctionOfNullableConstantTest, ReturnConversionOfConcatenatedFloatConversion) {
    expectRows("RETURN toFloat(toFloat('1.5') + '3')", {{"1.530000"}});
}

TEST_F(FuzzFunctionOfNullableConstantTest, ReturnConversionOfConversionOverRows) {
    Rows expected(_nodeCount, Row {"10.000000"});
    expectRows("MATCH (n) RETURN toFloat(toInteger('10'))", expected);
}

// toInteger('x') converts nothing, and a function of that null is null
TEST_F(FuzzFunctionOfNullableConstantTest, ReturnConversionOfFailedConversion) {
    expectRows("RETURN toFloat(toInteger('x'))", {{"null"}});
}

TEST_F(FuzzFunctionOfNullableConstantTest, ReturnConversionOfFailedConversionPlusInteger) {
    expectRows("RETURN toFloat(toInteger('x') + 1)", {{"null"}});
}

TEST_F(FuzzFunctionOfNullableConstantTest, ReturnSizeOfFailedConversion) {
    expectRows("RETURN size(toString(toInteger('x')))", {{"null"}});
}

// toString answers an owned string, which a function taking a string reads all the same
TEST_F(FuzzFunctionOfNullableConstantTest, ReturnSizeOfConvertedString) {
    expectRows("RETURN size(toString(10))", {{"2"}});
}

TEST_F(FuzzFunctionOfNullableConstantTest, ReturnConversionOfConvertedString) {
    expectRows("RETURN toInteger(toString(10))", {{"10"}});
}

TEST_F(FuzzFunctionOfNullableConstantTest, ReturnSizeOfConvertedConversion) {
    expectRows("RETURN size(toString(toInteger('10')))", {{"2"}});
}

// Grouped by 10 > n.age * 4.2: false for Remy and Adam, null for the other 16 nodes
TEST_F(FuzzFunctionOfNullableConstantTest, Return000162) {
    expectRowShape("MATCH (n), (m) WHERE m.age = 32 RETURN COUNT(n) * 1D<=-01.004200^+ toInteger(\"10\")+  COUNT(n)<=00000000000000000000000000000000000042000+ toInteger(\"10\")+ 10>  COUNT(n)<=0000< toFloat(00000042000+ toInteger(\"10\")+\"\x19" "03.421313\")% 0D<=00000000/ 3%  toFloat(\"103N421313\"), 10>n.age * 4.2000, toInteger(\"Q0\")% 0",
                   2,
                   3);
}

TEST_F(FuzzFunctionOfNullableConstantTest, Return000164) {
    expectRowShape("MATCH (n), (m) WHERE m.age = 32 RETURN COUNT(n) *  3% toInteger(\"Q0\")>=+  COUNT(n)<-01.004200^+ 0000>000000000000000000042000+ toInteger(\"10\")+ 10>  COUNT(n)<=0000< toFloat( toInteger(\"10\")+ \" 10>n.age * 4.200\x19" "-3.421313\")% 0D<=00000000/ 3%  toFloat(\"103N42131DATETIMES3\"), 10>n.age * 4.2000, toInteger(\"Q0\")= 0",
                   2,
                   3);
}
