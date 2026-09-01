#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

// labels() and edgeType() fill columns of owned strings, so a sort keyed on one has to
// buffer and compare owned strings rather than the borrowed ones a property read yields.
class OrderByLabelsTest : public CallV3Test {
};

TEST_F(OrderByLabelsTest, ordersByTheProjectedLabels) {
    StringRowSink sink;
    runQuery("MATCH (n) RETURN labels(n) ORDER BY labels(n)", sink);

    const std::vector<StringRowSink::Row> expected {{"Interest"},
                                                    {"Interest"},
                                                    {"Interest"},
                                                    {"Interest"},
                                                    {"Interest"},
                                                    {"Interest"},
                                                    {"Interest, Exotic"},
                                                    {"Interest, Exotic, Supernatural, SleepDisturber"},
                                                    {"Interest, SleepDisturber"},
                                                    {"Person, Bioinformatics"},
                                                    {"Person, Bioinformatics"},
                                                    {"Person, Founder, Bioinformatics"},
                                                    {"Person, Sales"},
                                                    {"Person, SoftwareEngineering"},
                                                    {"Person, SoftwareEngineering"},
                                                    {"Person, SoftwareEngineering"},
                                                    {"Person, SoftwareEngineering, Founder"},
                                                    {"SoftwareEngineering, Interest"}};
    EXPECT_EQ(sink.getRows(), expected);
}

// The key is not projected, so it rides along as an extra sorted column
TEST_F(OrderByLabelsTest, ordersByTheLabelsTheReturnDoesNotCarry) {
    StringRowSink sink;
    runQuery("MATCH (n:Person) RETURN n.name ORDER BY labels(n) DESC, n.name", sink);

    const std::vector<StringRowSink::Row> expected {{"Remy"},
                                                    {"Cyrus"},
                                                    {"Luc"},
                                                    {"Suhas"},
                                                    {"Doruk"},
                                                    {"Adam"},
                                                    {"Martina"},
                                                    {"Maxime"}};
    EXPECT_EQ(sink.getRows(), expected);
}

// Remy's four out-edges: one KNOWS_WELL, three INTERESTED_IN
TEST_F(OrderByLabelsTest, ordersByTheProjectedEdgeType) {
    StringRowSink sink;
    runQuery("MATCH (n {name: 'Remy'})-[e]->(m) RETURN edgeType(e) ORDER BY edgeType(e)", sink);

    const std::vector<StringRowSink::Row> expected {{"INTERESTED_IN"}, {"INTERESTED_IN"}, {"INTERESTED_IN"}, {"KNOWS_WELL"}};
    EXPECT_EQ(sink.getRows(), expected);
}
