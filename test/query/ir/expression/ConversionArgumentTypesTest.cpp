#include <gtest/gtest.h>

#include "IRTestRows.h"
#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

class ConversionArgumentTypesTest : public WriteQueryTest {
};

TEST_F(ConversionArgumentTypesTest, toIntegerOfABoolean) {
    expectRows("RETURN toInteger(true), toInteger(false), toIntegerOrNull(true), toIntegerOrNull(false)",
               {{"1", "0", "1", "0"}});
}

TEST_F(ConversionArgumentTypesTest, toIntegerOfABooleanColumn) {
    expectRows("MATCH (p:Person {name: 'Remy'}) RETURN toInteger(p.isFrench), toIntegerOrNull(p.hasPhD)", {{"1", "1"}});
    expectRows("MATCH (p:Person {name: 'Remy'}) RETURN toInteger(p.age > 30), toIntegerOrNull(p.age < 30)", {{"1", "0"}});
}

TEST_F(ConversionArgumentTypesTest, toBooleanOfAnInteger) {
    expectRows("RETURN toBoolean(1), toBoolean(0), toBoolean(-3), toBooleanOrNull(7), toBooleanOrNull(0)",
               {{"true", "false", "true", "true", "false"}});
    expectRows("MATCH (p:Person {name: 'Remy'}) RETURN toBoolean(p.age), toBooleanOrNull(p.age - 32)", {{"true", "false"}});
}

TEST_F(ConversionArgumentTypesTest, toBooleanOfABoolean) {
    expectRows("RETURN toBoolean(true), toBoolean(false), toBooleanOrNull(true)", {{"true", "false", "true"}});
    expectRows("MATCH (p:Person {name: 'Remy'}) RETURN toBoolean(p.isFrench), toBooleanOrNull(p.age < 30)", {{"true", "false"}});
}

TEST_F(ConversionArgumentTypesTest, toStringOfADateTime) {
    expectRows("RETURN toString(datetime('2020-01-02T03:04:05Z')), toStringOrNull(datetime('2020-01-02T03:04:05.5Z'))",
               {{"2020-01-02T03:04:05Z", "2020-01-02T03:04:05.500000Z"}});
}

TEST_F(ConversionArgumentTypesTest, toStringOfADuration) {
    expectRows("RETURN toString(duration(90000000)), toStringOrNull(duration(3600000000))", {{"PT1M30S", "PT1H"}});
}

TEST_F(ConversionArgumentTypesTest, toStringOfABoundTemporal) {
    expectRows("WITH datetime(0) AS d, duration(90000000) AS e RETURN toString(d), toStringOrNull(d), toString(e), toStringOrNull(e)",
               {{"1970-01-01T00:00:00Z", "1970-01-01T00:00:00Z", "PT1M30S", "PT1M30S"}});
}

TEST_F(ConversionArgumentTypesTest, toStringOfATemporalProperty) {
    applyWrite("CREATE (:Event {at: datetime(0), lasts: duration(90000000)}), (:Event)");
    expectRows("MATCH (e:Event) RETURN toString(e.at), toStringOrNull(e.lasts)",
               {{"1970-01-01T00:00:00Z", "PT1M30S"}, {"null", "null"}});
}

TEST_F(ConversionArgumentTypesTest, toStringOfAnUnwoundTemporal) {
    expectRows("UNWIND [datetime(0), duration(90000000)] AS v RETURN toString(v), toStringOrNull(v)",
               {{"1970-01-01T00:00:00Z", "1970-01-01T00:00:00Z"}, {"PT1M30S", "PT1M30S"}});
}

TEST_F(ConversionArgumentTypesTest, typedArgumentsConvertAsTaggedCellsDo) {
    expectRows("UNWIND [true, 0] AS v RETURN toIntegerOrNull(v), toBooleanOrNull(v)", {{"1", "true"}, {"0", "false"}});
    expectRows("RETURN toIntegerOrNull(true), toBooleanOrNull(true), toIntegerOrNull(0), toBooleanOrNull(0)",
               {{"1", "true", "0", "false"}});
}

TEST_F(ConversionArgumentTypesTest, conversionsOfANonNullComparison) {
    expectRows("UNWIND [1, 5] AS x RETURN toInteger(x > 3), toBoolean(x > 3), toString(x > 3)",
               {{"0", "false", "false"}, {"1", "true", "true"}});
    expectRows("UNWIND [1, 5] AS x RETURN toIntegerOrNull(x > 3), toBooleanOrNull(x > 3), toStringOrNull(x > 3)",
               {{"0", "false", "false"}, {"1", "true", "true"}});
}

TEST_F(ConversionArgumentTypesTest, conversionsOfALabelPredicateAndAnExistsSubquery) {
    expectRows("MATCH (n:Person {name: 'Remy'}) RETURN toInteger(n:Person), toBoolean(n:Interest), toString(n:Person)",
               {{"1", "false", "true"}});
    expectRows("MATCH (n:Person {name: 'Remy'}) RETURN toIntegerOrNull(EXISTS { (n)-->() }), toStringOrNull(EXISTS { (n)-->() })",
               {{"1", "true"}});
    expectRows("MATCH (n:Person {name: 'Remy'}) RETURN toInteger(any(x IN [n.age] WHERE x > 30)), toBooleanOrNull(all(x IN [n.age] WHERE x > 40))",
               {{"1", "false"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
