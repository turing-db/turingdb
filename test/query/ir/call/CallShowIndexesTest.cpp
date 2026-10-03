#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

class CallShowIndexesTest : public CallV3Test {
};

TEST_F(CallShowIndexesTest, listsNoIndexOnAFreshGraph) {
    StringRowSink sink;
    runQuery("CALL db.showIndexes() YIELD name, size RETURN name, size", sink);

    const std::vector<std::string> expectedNames {"name", "size"};
    EXPECT_EQ(sink.getNames(), expectedNames);
    EXPECT_TRUE(sink.getRows().empty());
}

// The index is created through the legacy engine, which is the only one that runs CREATE
// INDEX; the MLIR engine then lists it.
TEST_F(CallShowIndexesTest, listsACreatedIndex) {
    runLegacyWrite("CREATE INDEX ageindex FOR (n) ON n.age");

    StringRowSink sink;
    runQuery("CALL db.showIndexes() YIELD name RETURN name", sink);

    const std::vector<StringRowSink::Row> expected {{"ageindex"}};
    EXPECT_EQ(sink.getRows(), expected);
}
