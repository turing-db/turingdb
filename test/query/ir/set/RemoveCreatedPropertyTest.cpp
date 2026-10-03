#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

class RemoveCreatedPropertyTest : public WriteQueryTest {
};

TEST_F(RemoveCreatedPropertyTest, readsNullForThePropertyRemovedFromTheNodeThePartCreated) {
    expectWriteRows("CREATE (t:Tag {name: 'x', dob: '01/01'}) REMOVE t.name RETURN t.name, t.dob",
                    {{"null", "01/01"}});
}

TEST_F(RemoveCreatedPropertyTest, readsNullForThePropertyRemovedFromTheEdgeThePartCreated) {
    expectWriteRows("CREATE (a:Tag)-[e:LINK {w: 1, v: 2}]->(b:Tag) REMOVE e.w RETURN e.w, e.v",
                    {{"null", "2"}});
}

TEST_F(RemoveCreatedPropertyTest, readsNullForThePropertyRemovedFromAnEdgeCreatedOffAMatchedNode) {
    expectWriteRows("MATCH (a:Person {name: 'Remy'}) CREATE (a)-[e:LINK {w: 1}]->(b:Tag) REMOVE e.w RETURN e.w",
                    {{"null"}});
}

TEST_F(RemoveCreatedPropertyTest, readsNullForThePropertyRemovedFromEveryNodeAnUnwindCreated) {
    expectWriteRows("UNWIND [1, 2] AS x CREATE (t:Tag {w: x, v: x}) REMOVE t.w RETURN t.w, t.v",
                    {{"null", "1"}, {"null", "2"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
