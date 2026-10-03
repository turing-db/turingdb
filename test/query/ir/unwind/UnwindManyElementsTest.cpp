#include <gtest/gtest.h>

#include <stddef.h>
#include <string>
#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

class UnwindManyElementsTest : public CallV3Test {
protected:
    static constexpr size_t ELEMENT_COUNT = 100000;
};

TEST_F(UnwindManyElementsTest, matchesNodeIDsFromALongList) {
    std::string list;
    for (size_t nodeID = 0; nodeID < ELEMENT_COUNT; nodeID++) {
        list += nodeID == 0 ? "[" : ", ";
        list += std::to_string(nodeID);
    }
    list += "]";

    StringRowSink sink;
    runQuery("UNWIND " + list + " AS x MATCH (n) WHERE n = x RETURN count(n)", sink);

    const std::vector<StringRowSink::Row> expected {{"18"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(UnwindManyElementsTest, matchesStringKeysFromALongList) {
    std::string list = "['Remy', 'Adam'";
    for (size_t index = 0; index < ELEMENT_COUNT; index++) {
        list += ", 'absent" + std::to_string(index) + "'";
    }
    list += "]";

    StringRowSink sink;
    runQuery("UNWIND " + list + " AS x MATCH (n) WHERE n.name = x RETURN count(n)", sink);

    const std::vector<StringRowSink::Row> expected {{"2"}};
    EXPECT_EQ(sink.getRows(), expected);
}
