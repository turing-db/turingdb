#include <gtest/gtest.h>

#include "IRTestRows.h"
#include "WriteQueryTest.h"

using namespace turing::test;

class OwnedStringCellOrderingTest : public WriteQueryTest {
};

TEST_F(OwnedStringCellOrderingTest, ordersAMapValueAgainstAnEdgeType) {
    expectRows("MATCH (a:Person {name: 'Remy'})-[r:KNOWS_WELL]->() UNWIND [{k: 'A'}, {k: 'Z'}, {k: 1}] AS m "
               "RETURN m.k, m.k < type(r), type(r) <= m.k",
               {{"A", "true", "false"}, {"Z", "false", "true"}, {"1", "null", "null"}});
}

TEST_F(OwnedStringCellOrderingTest, ordersAListCellAgainstAnEdgeType) {
    expectRows("MATCH (a:Person {name: 'Remy'})-[r:KNOWS_WELL]->() UNWIND ['A', 'Z', 1] AS x "
               "RETURN x, x < type(r), type(r) >= x",
               {{"A", "true", "true"}, {"Z", "false", "false"}, {"1", "null", "null"}});
}
