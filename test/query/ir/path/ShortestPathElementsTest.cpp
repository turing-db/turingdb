#include <gtest/gtest.h>

#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

namespace {

using Rows = std::vector<StringRowSink::Row>;

}

// nodes(p), relationships(p) and length(p) over the path a SHORTESTPATH binds. The path is
// stored target-first: Adam reaches Ghosts through Remy as Ghosts(6) -e1- Remy(0) -e4- Adam(1).
class ShortestPathElementsTest : public CallV3Test {
};

TEST_F(ShortestPathElementsTest, readsTheElementsOfATwoHopPath) {
    StringRowSink sink;
    runQuery("MATCH (n {name: 'Adam'}), (m {name: 'Ghosts'}) "
             "SHORTESTPATH(n, m, duration, d, p) RETURN d, nodes(p), relationships(p), length(p)",
             sink);

    EXPECT_EQ(sink.getRows(), (Rows {{"40", "6, 0, 1", "1, 4", "2"}}));
}

TEST_F(ShortestPathElementsTest, readsTheElementsOfAZeroLengthPath) {
    StringRowSink sink;
    runQuery("MATCH (n {name: 'Remy'}), (m {name: 'Remy'}) "
             "SHORTESTPATH(n, m, duration, d, p) RETURN d, nodes(p), relationships(p), length(p)",
             sink);

    EXPECT_EQ(sink.getRows(), (Rows {{"0", "0", "", "0"}}));
}

TEST_F(ShortestPathElementsTest, readsNoRowWhenTheTargetIsUnreachable) {
    StringRowSink sink;
    runQuery("MATCH (n {name: 'Ghosts'}), (m {name: 'Cooking'}) "
             "SHORTESTPATH(n, m, duration, d, p) RETURN nodes(p)",
             sink);

    EXPECT_TRUE(sink.getRows().empty());
}
