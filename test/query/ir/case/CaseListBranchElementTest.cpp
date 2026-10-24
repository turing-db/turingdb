#include <gtest/gtest.h>

#include <string_view>
#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

// A CASE or a coalesce whose branches are lists of one element type answers a list of that
// type, so what unwinds out of it is a node where the branches list nodes
class CaseListBranchElementTest : public CallV3Test {
protected:
    void expectRows(std::string_view query, const std::vector<StringRowSink::Row>& expected) {
        StringRowSink sink;
        runQuery(query, sink);

        std::vector<StringRowSink::Row> rows;
        sink.sortedRows(rows);

        EXPECT_EQ(rows, expected) << query;
    }
};

TEST_F(CaseListBranchElementTest, unwindsNodesACaseSelects) {
    expectRows("MATCH (n:Person), (m:Person) WHERE n.name = 'Remy' AND m.name = 'Luc' "
               "UNWIND CASE WHEN true THEN [n] ELSE [m] END AS x RETURN x.name",
               {{"Remy"}});
}

TEST_F(CaseListBranchElementTest, unwindsNodesACoalesceAnswers) {
    expectRows("MATCH (n:Person) WHERE n.name = 'Remy' "
               "UNWIND coalesce(null, [n]) AS x RETURN x.name",
               {{"Remy"}});
}

TEST_F(CaseListBranchElementTest, indexesANodeACaseSelects) {
    expectRows("MATCH (n:Person) WHERE n.name = 'Remy' "
               "RETURN (CASE WHEN true THEN [n] ELSE [n] END)[0].name",
               {{"Remy"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
