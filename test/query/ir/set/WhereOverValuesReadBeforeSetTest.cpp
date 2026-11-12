#include <gtest/gtest.h>

#include "IRTestRows.h"
#include "WriteQueryTest.h"

using namespace turing::test;

// A WHERE after a SET tests the value a property held when the query read it, whichever of its
// conjuncts reads it. Remy is 32, and Remy -> Adam lasts 20.
class WhereOverValuesReadBeforeSetTest : public WriteQueryTest {
};

TEST_F(WhereOverValuesReadBeforeSetTest, testsTheNodePropertyReadBeforeTheSet) {
    expectWriteRows("MATCH (n:Person) WITH n, n.age AS oldAge SET n.age = 100 "
                    "WITH n, oldAge WHERE n.name = 'Remy' AND oldAge > 50 RETURN n.name",
                    {});
}

TEST_F(WhereOverValuesReadBeforeSetTest, keepsTheRowTheNodePropertyReadBeforeTheSetPasses) {
    expectWriteRows("MATCH (n:Person) WITH n, n.age AS oldAge SET n.age = 100 "
                    "WITH n, oldAge WHERE n.name = 'Remy' AND oldAge < 50 RETURN n.name",
                    {{"Remy"}});
}

TEST_F(WhereOverValuesReadBeforeSetTest, testsTheEdgePropertyReadBeforeTheSet) {
    expectWriteRows("MATCH (a:Person)-[e:KNOWS_WELL]->(:Person) WITH a, e, e.duration AS oldDuration "
                    "SET e.duration = 50 "
                    "WITH a, oldDuration WHERE a.name = 'Remy' AND oldDuration > 30 RETURN a.name",
                    {});
}
