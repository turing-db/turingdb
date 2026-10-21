#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// A WHEN segment whose branches only write returns nothing, so the segment after it starts
// from a single row. Remy, Adam, Luc and Martina have a PhD.
class NextWhenThatWritesTest : public WriteQueryTest {
};

TEST_F(NextWhenThatWritesTest, startsFromOneRowAfterAWhenThatWrites) {
    expectWriteRows("MATCH (p:Person) RETURN p "
                    "NEXT WHEN p.hasPhD THEN { SET p.t = 1 } ELSE { SET p.t = 2 } "
                    "NEXT RETURN 1 AS one",
                    {{"1"}});

    expectRows("MATCH (p:Person) RETURN p.t, count(*)", {{"1", "4"}, {"2", "4"}});
}
