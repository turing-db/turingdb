#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

class StandaloneCallTest : public CallV3Test {
};

TEST_F(StandaloneCallTest, namesAndAnswersEveryReturnValueWithoutAYield) {
    StringRowSink sink;
    runQuery("CALL db.labels()", sink);

    const std::vector<std::string> expectedNames {"id", "label"};
    EXPECT_EQ(sink.getNames(), expectedNames);

    const std::vector<StringRowSink::Row> expected {{"0", "Person"},
                                                   {"1", "SoftwareEngineering"},
                                                   {"2", "Founder"},
                                                   {"3", "Bioinformatics"},
                                                   {"4", "Interest"},
                                                   {"5", "Exotic"},
                                                   {"6", "Supernatural"},
                                                   {"7", "SleepDisturber"},
                                                   {"8", "Sales"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(StandaloneCallTest, yieldStarAnswersEveryReturnValueWithoutAReturn) {
    StringRowSink sink;
    runQuery("CALL db.propertyTypes() YIELD *", sink);

    const std::vector<std::string> expectedNames {"id", "propertyType", "valueType"};
    EXPECT_EQ(sink.getNames(), expectedNames);

    const std::vector<StringRowSink::Row> expected {{"0", "name", "String"},
                                                    {"1", "dob", "String"},
                                                    {"2", "age", "Int64"},
                                                    {"3", "isFrench", "Bool"},
                                                    {"4", "hasPhD", "Bool"},
                                                    {"5", "isReal", "Bool"},
                                                    {"6", "duration", "Int64"},
                                                    {"7", "proficiency", "String"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(StandaloneCallTest, readsALiteralArgument) {
    StringRowSink sink;
    runQuery("call db.describeCommit(\"not-a-commit\")", sink);

    const std::vector<std::string> expectedNames {"nodeCount", "edgeCount", "partCount"};
    EXPECT_EQ(sink.getNames(), expectedNames);

    const std::vector<StringRowSink::Row> expected {{"0", "0", "0"}};
    EXPECT_EQ(sink.getRows(), expected);
}
