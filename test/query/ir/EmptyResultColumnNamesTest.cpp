#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

class EmptyResultColumnNamesTest : public CallV3Test {
};

TEST_F(EmptyResultColumnNamesTest, namesTheColumnsOfAnEmptyGetNodes) {
    StringRowSink sink;
    runQuery("CALL db.getNodes([]) YIELD id, labels, inEdgeCount, outEdgeCount, properties RETURN id", sink);

    const std::vector<std::string> expectedNames {"id"};
    EXPECT_EQ(sink.getNames(), expectedNames);
    EXPECT_TRUE(sink.getRows().empty());
}

TEST_F(EmptyResultColumnNamesTest, namesTheColumnsOfAnEmptyGetEdges) {
    StringRowSink sink;
    runQuery("CALL db.getEdges([]) YIELD id, src, tgt, edgeTypeID, properties RETURN id", sink);

    const std::vector<std::string> expectedNames {"id"};
    EXPECT_EQ(sink.getNames(), expectedNames);
    EXPECT_TRUE(sink.getRows().empty());
}

TEST_F(EmptyResultColumnNamesTest, namesTheColumnsOfAListNodesMatchingNothing) {
    StringRowSink sink;
    runQuery("CALL db.listNodes([], ['name'], ['zzznope'], 0, 1000) YIELD id, labels, properties RETURN id, labels, properties", sink);

    const std::vector<std::string> expectedNames {"id", "labels", "properties"};
    EXPECT_EQ(sink.getNames(), expectedNames);
    EXPECT_TRUE(sink.getRows().empty());
}

TEST_F(EmptyResultColumnNamesTest, namesTheColumnsOfAStandaloneCallMatchingNothing) {
    StringRowSink sink;
    runQuery("CALL db.getNodes([])", sink);

    const std::vector<std::string> expectedNames {"id", "labels", "inEdgeCount", "outEdgeCount", "properties"};
    EXPECT_EQ(sink.getNames(), expectedNames);
    EXPECT_TRUE(sink.getRows().empty());
}
