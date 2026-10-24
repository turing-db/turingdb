#include <gtest/gtest.h>

#include <string_view>
#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

// NOT, AND, OR and XOR over a cell holding no boolean are a type error, raised only where a
// row reads the cell. No person is called Nobody, and only Remy and Adam have an age.
class LogicalOperatorOverUnreachedCellTest : public CallV3Test {
protected:
    void expectRows(std::string_view query, const std::vector<StringRowSink::Row>& expected) {
        StringRowSink sink;
        runQuery(query, sink);

        std::vector<StringRowSink::Row> rows;
        sink.sortedRows(rows);

        EXPECT_EQ(rows, expected) << query;
    }
};

TEST_F(LogicalOperatorOverUnreachedCellTest, readsNoCellWithoutARow) {
    expectRows("MATCH (p:Person) WHERE p.name = 'Nobody' RETURN NOT [1, true][0]", {});
    expectRows("MATCH (p:Person) WHERE p.name = 'Nobody' RETURN [1, true][0] AND true", {});
    expectRows("MATCH (p:Person) WHERE p.name = 'Nobody' RETURN [1, true][0] OR false", {});
    expectRows("MATCH (p:Person) WHERE p.name = 'Nobody' RETURN [1, true][0] XOR true", {});
}

TEST_F(LogicalOperatorOverUnreachedCellTest, readsNoCellInABranchNoRowTakes) {
    expectRows("MATCH (p:Person) WHERE p.name IN ['Remy', 'Luc'] "
               "RETURN p.name, CASE WHEN p.age > 1000 THEN NOT [1, true][0] ELSE false END",
               {{"Luc", "false"}, {"Remy", "false"}});
}

TEST_F(LogicalOperatorOverUnreachedCellTest, raisesWhereARowReadsTheCell) {
    runQueryExpectingError("MATCH (p:Person) WHERE p.name = 'Remy' RETURN NOT [1, true][0]",
                           "must be booleans");
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
