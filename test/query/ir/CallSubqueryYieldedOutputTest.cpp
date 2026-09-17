#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

// What a standalone CALL yielded is the query's result unless the query writes. A subquery
// body that only reads writes nothing, whatever it ends on
class CallSubqueryYieldedOutputTest : public CallV3Test {
};

TEST_F(CallSubqueryYieldedOutputTest, keepsTheYieldedResultOfAReadOnlyBody) {
    StringRowSink sink;
    runQuery("CALL db.labels() YIELD label CALL { CALL db.edgeTypes() }", sink);

    const std::vector<StringRowSink::Row> expected {{"Person"},
                                                    {"SoftwareEngineering"},
                                                    {"Founder"},
                                                    {"Bioinformatics"},
                                                    {"Interest"},
                                                    {"Exotic"},
                                                    {"Supernatural"},
                                                    {"SleepDisturber"},
                                                    {"Sales"}};
    EXPECT_EQ(sink.getRows(), expected);
}

// A query that writes reports no row: its result is its RETURN, and it has none
TEST_F(CallSubqueryYieldedOutputTest, dropsTheYieldedResultOfAWritingBody) {
    StringRowSink sink;
    runWrite("CALL db.labels() YIELD label CALL { CREATE (:Marker) }", sink);

    EXPECT_TRUE(sink.getRows().empty());
}
