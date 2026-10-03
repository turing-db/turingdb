#include <gtest/gtest.h>

#include <string>
#include <string_view>
#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

class EdgeTypeFunctionTest : public CallV3Test {
protected:
    void expectRows(std::string_view query, const std::vector<StringRowSink::Row>& expected) {
        StringRowSink sink;
        runQuery(query, sink);

        std::vector<StringRowSink::Row> rows;
        sink.sortedRows(rows);

        EXPECT_EQ(rows, expected) << query;
    }
};

TEST_F(EdgeTypeFunctionTest, returnsTheTypeOfEachMatchedEdge) {
    expectRows("MATCH (n)-[e]->(m) WHERE n.name = 'Remy' RETURN m.name, type(e)",
               {
                   {"Adam", "KNOWS_WELL"},
                   {"Computers", "INTERESTED_IN"},
                   {"Eighties", "INTERESTED_IN"},
                   {"Ghosts", "INTERESTED_IN"},
               });
}

TEST_F(EdgeTypeFunctionTest, groupsOnTheEdgeType) {
    expectRows("MATCH ()-[e]->() RETURN type(e), count(e)",
               {
                   {"INTERESTED_IN", "15"},
                   {"KNOWS_WELL", "3"},
               });
}

TEST_F(EdgeTypeFunctionTest, filtersOnTheEdgeType) {
    expectRows("MATCH (n)-[e]->(m) WHERE type(e) = 'KNOWS_WELL' RETURN n.name, m.name",
               {
                   {"Adam", "Remy"},
                   {"Ghosts", "Remy"},
                   {"Remy", "Adam"},
               });
}

TEST_F(EdgeTypeFunctionTest, aliasesTheEdgeType) {
    expectRows("MATCH (n)-[e:KNOWS_WELL]->(m) RETURN type(e) AS relationship, count(*)",
               {
                   {"KNOWS_WELL", "3"},
               });
}

TEST_F(EdgeTypeFunctionTest, rejectsANodeArgument) {
    runQueryExpectingError("MATCH (n) RETURN type(n)", "Invalid arguments for function 'type'");
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
