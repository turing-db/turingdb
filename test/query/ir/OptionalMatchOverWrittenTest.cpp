#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// An optional pattern and a pattern comprehension walk from what a CREATE of the same query
// wrote, over the edges it wrote as well as the graph's
class OptionalMatchOverWrittenTest : public WriteQueryTest {
};

TEST_F(OptionalMatchOverWrittenTest, padsTheRowOfACreatedNodeWithNoEdge) {
    expectWriteRows("CREATE (a:Thing {name: 'x'}) WITH a OPTIONAL MATCH (a)-[:SEES]->(b) RETURN a.name, b.name",
                    {{"x", "null"}});
}

TEST_F(OptionalMatchOverWrittenTest, walksAnEdgeTheQueryCreated) {
    expectWriteRows("CREATE (a:Thing {name: 'y'})-[:SEES]->(:Other {name: 'z'}) "
                    "WITH a OPTIONAL MATCH (a)-[:SEES]->(c) RETURN a.name, c.name",
                    {{"y", "z"}});
}

TEST_F(OptionalMatchOverWrittenTest, walksAnEdgeTheQueryCreatedFromAMatchedNode) {
    expectWriteRows("MATCH (p:Person {name: 'Remy'}) CREATE (p)-[:OWNS]->(:Thing {name: 'w'}) "
                    "WITH p OPTIONAL MATCH (p)-[:OWNS]->(x) RETURN p.name, x.name",
                    {{"Remy", "w"}});
}

TEST_F(OptionalMatchOverWrittenTest, collectsWhatAPatternComprehensionWalksFromACreatedNode) {
    expectWriteRows("CREATE (a:Thing {name: 'y'})-[:SEES]->(:Other {name: 'z'}) "
                    "WITH a RETURN a.name, size([(a)-[:SEES]->(c) | c.name])",
                    {{"y", "1"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
