#include <gtest/gtest.h>

#include "IRTestRows.h"
#include "WriteQueryTest.h"

using namespace turing::test;

class MapValueAgainstAMaskTest : public WriteQueryTest {
};

TEST_F(MapValueAgainstAMaskTest, ordersAMapValueAgainstAComparison) {
    expectRows("MATCH (n:Person {name: 'Remy'}) UNWIND [{k: false}, {k: true}, {k: 1}] AS m "
               "RETURN m.k, m.k < (id(n) >= 0), (id(n) >= 0) >= m.k",
               {{"false", "true", "true"}, {"true", "false", "true"}, {"1", "null", "null"}});
}

TEST_F(MapValueAgainstAMaskTest, comparesAMapValueAgainstAComparison) {
    expectRows("MATCH (n:Person {name: 'Remy'}) UNWIND [{k: false}, {k: true}, {k: 1}] AS m "
               "RETURN m.k, m.k = (id(n) >= 0), (id(n) >= 0) <> m.k",
               {{"false", "false", "true"}, {"true", "true", "false"}, {"1", "false", "true"}});
}
