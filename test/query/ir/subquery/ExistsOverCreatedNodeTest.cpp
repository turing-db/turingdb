#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

class ExistsOverCreatedNodeTest : public WriteQueryTest {
};

TEST_F(ExistsOverCreatedNodeTest, returnsExistsOverACreatedNode) {
    expectWriteRows("CREATE (n:X) RETURN EXISTS { (n)-->() } AS x", {{"false"}});
}

TEST_F(ExistsOverCreatedNodeTest, returnsCountOverACreatedNode) {
    expectWriteRows("CREATE (n:X) RETURN COUNT { (n)-->() } AS x", {{"0"}});
}

TEST_F(ExistsOverCreatedNodeTest, returnsExistsOverACreatedNodeWithProperties) {
    expectWriteRows("CREATE (n:X {name: 'Kai'}) RETURN n.name, EXISTS { (n)-->() } AS x", {{"Kai", "false"}});
}

TEST_F(ExistsOverCreatedNodeTest, returnsExistsOverACreatedNodeInACallBody) {
    expectWriteRows("CREATE (n:X) CALL (n) { RETURN EXISTS { (n)-->() } AS x } RETURN x", {{"false"}});
}

TEST_F(ExistsOverCreatedNodeTest, returnsExistsOverACreatedNodePassedThroughWith) {
    expectWriteRows("CREATE (n:X) WITH n CALL (n) { RETURN EXISTS { (n)-->() } AS x } RETURN x", {{"false"}});
}

TEST_F(ExistsOverCreatedNodeTest, branchesOnExistsOverACreatedNode) {
    expectWriteRows("CREATE (n:X) CALL (n) { WHEN EXISTS { (n)-->() } THEN RETURN 1 AS x ELSE RETURN 2 AS x } RETURN x",
                    {{"2"}});
}

TEST_F(ExistsOverCreatedNodeTest, filtersOnExistsOverACreatedNode) {
    expectWriteRows("CREATE (n:X) WITH n WHERE NOT EXISTS { (n)-->() } RETURN count(n)", {{"1"}});
}

TEST_F(ExistsOverCreatedNodeTest, returnsExistsOverEachUnwoundCreatedNode) {
    expectWriteRows("UNWIND [1, 2] AS i CREATE (n:X {i: i}) RETURN i, EXISTS { (n)-->() } AS x",
                    {{"1", "false"}, {"2", "false"}});
}

TEST_F(ExistsOverCreatedNodeTest, returnsExistsOverAMatchedNodeBesideACreatedOne) {
    expectWriteRows("MATCH (p:Person {name: 'Remy'}) CREATE (n:X) RETURN EXISTS { (p)-->() } AS x, EXISTS { (n)-->() } AS y",
                    {{"true", "false"}});
}
