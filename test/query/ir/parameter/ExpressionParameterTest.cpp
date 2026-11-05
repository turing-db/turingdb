#include <gtest/gtest.h>

#include <vector>

#include "ParameterMap.h"
#include "ParameterValue.h"

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace db;
using namespace turing::test;

class ExpressionParameterTest : public CallV3Test {
};

TEST_F(ExpressionParameterTest, aScalarParameterIsReadAsItsValue) {
    ParameterMap parameters;
    parameters.insert("i").setInt64(41);
    parameters.insert("d").setDouble(1.5);
    parameters.insert("s").setString("hi");
    parameters.insert("b").setBool(true);

    StringRowSink sink;
    runQuery("RETURN $i + 1, $d * 2.0, $s + '!', $b AND true, $i", parameters, sink);

    const std::vector<StringRowSink::Row> expected {{"42", "3", "hi!", "true", "41"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(ExpressionParameterTest, aParameterFiltersTheRowsOfAMatch) {
    ParameterMap parameters;
    parameters.insert("name").setString("Remy");

    StringRowSink sink;
    runQuery("MATCH (n:Person) WHERE n.name = $name OR n.name = $name + 'x' RETURN n.name, $name", parameters, sink);

    const std::vector<StringRowSink::Row> expected {{"Remy", "Remy"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(ExpressionParameterTest, aParameterWritesAProperty) {
    ParameterMap parameters;
    parameters.insert("rank").setInt64(7);
    runWrite("CREATE (:Ranked {rank: $rank})", parameters);

    StringRowSink sink;
    runQuery("MATCH (n:Ranked) RETURN n.rank", sink);

    const std::vector<StringRowSink::Row> expected {{"7"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(ExpressionParameterTest, aNullOfUnknownTypeIsTheNullLiteral) {
    ParameterMap parameters;
    parameters.set("n", ParameterValue());

    StringRowSink sink;
    runQuery("RETURN $n + 1, $n IS NULL, coalesce($n, 'default')", parameters, sink);

    const std::vector<StringRowSink::Row> expected {{"null", "true", "default"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(ExpressionParameterTest, aDurationAndADateTimeAreReadAsTheirValues) {
    ParameterMap parameters;
    parameters.insert("wait").setDuration(Duration {3'600'000'000});
    parameters.insert("when").setDateTime(DateTime {1'793'887'500'000'000});

    StringRowSink sink;
    runQuery("RETURN $wait + duration({hours: 1}), $when + $wait, $when.year, $wait > duration({minutes: 1})", parameters, sink);

    const std::vector<StringRowSink::Row> expected {{"PT2H", "2026-11-05T15:05:00Z", "2026", "true"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(ExpressionParameterTest, aListParameterInAnExpressionIsNotTakenYet) {
    ParameterMap parameters;
    parameters.insert("l").setList().emplace_back().setInt64(1);

    runQueryExpectingError("RETURN size($l)", parameters, "Parameter $l is a List, which a parameter in an expression does not take yet");
}
