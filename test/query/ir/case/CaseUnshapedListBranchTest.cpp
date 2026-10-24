#include <gtest/gtest.h>

#include <string_view>
#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

// labels(n) is a list whose element type the analyzer does not name. A CASE or a coalesce
// mixing it with a list of integers answers a list of either, so an element read out of
// it is a string on Remy's row.
class CaseUnshapedListBranchTest : public CallV3Test {
protected:
    void expectRows(std::string_view query, const std::vector<StringRowSink::Row>& expected) {
        StringRowSink sink;
        runQuery(query, sink);

        EXPECT_EQ(sink.getRows(), expected) << query;
    }
};

TEST_F(CaseUnshapedListBranchTest, readsAStringOutOfACoalesce) {
    expectRows("MATCH (n:Person) WHERE n.name = 'Remy' RETURN coalesce(labels(n), [1, 2])[0] STARTS WITH ''",
               {{"true"}});
}

TEST_F(CaseUnshapedListBranchTest, readsAStringOutOfACase) {
    expectRows("MATCH (n:Person) WHERE n.name = 'Remy' "
               "RETURN (CASE WHEN n.age > 100 THEN [1, 2] ELSE labels(n) END)[0] STARTS WITH ''",
               {{"true"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
