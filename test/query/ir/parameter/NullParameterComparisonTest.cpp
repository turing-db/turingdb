#include <gtest/gtest.h>

#include <vector>

#include "ParameterMap.h"
#include "ParameterValue.h"

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace db;
using namespace turing::test;

class NullParameterComparisonTest : public CallV3Test {
};

TEST_F(NullParameterComparisonTest, aComparisonWithANullParameterMatchesNoRow) {
    ParameterMap parameters;
    parameters.set("name", ParameterValue());

    StringRowSink sink;
    runQuery("MATCH (n:Person) WHERE n.name = $name RETURN n.name", parameters, sink);

    EXPECT_TRUE(sink.getRows().empty());
}

TEST_F(NullParameterComparisonTest, aComparisonWithANullParameterIsNull) {
    ParameterMap parameters;
    parameters.set("n", ParameterValue());

    StringRowSink sink;
    runQuery("RETURN $n = 1, $n <> 'a', 1 < $n", parameters, sink);

    const std::vector<StringRowSink::Row> expected {{"null", "null", "null"}};
    EXPECT_EQ(sink.getRows(), expected);
}
