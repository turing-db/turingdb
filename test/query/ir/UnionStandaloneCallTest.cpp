#include <gtest/gtest.h>

#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

// A branch of a UNION that is one CALL and nothing else is as standalone as the same query
// alone: it owes no YIELD, and what it does owe is the RETURN every branch of a UNION ends
// on
class UnionStandaloneCallTest : public CallV3Test {
};

TEST_F(UnionStandaloneCallTest, namesTheMissingReturnOfAStandaloneCallBranch) {
    runQueryExpectingError("CALL db.labels() UNION CALL db.labels()",
                           "Every sub-query of a UNION must end with a RETURN clause");
}

TEST_F(UnionStandaloneCallTest, namesTheMissingReturnOfASecondStandaloneCallBranch) {
    runQueryExpectingError("CALL db.labels() YIELD label RETURN label UNION CALL db.labels()",
                           "Every sub-query of a UNION must end with a RETURN clause");
}

TEST_F(UnionStandaloneCallTest, unionsTheBranchesThatDoReturn) {
    StringRowSink sink;
    runQuery("CALL db.labels() YIELD label RETURN label "
             "UNION CALL db.labels() YIELD label RETURN label",
             sink);

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
