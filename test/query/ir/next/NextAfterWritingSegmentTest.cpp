#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// A segment ending on a write returns nothing, so the segment after it starts from a
// single row however many rows the segment wrote over
class NextAfterWritingSegmentTest : public WriteQueryTest {
};

TEST_F(NextAfterWritingSegmentTest, startsFromOneRowAfterWritingOverATable) {
    expectWriteRows("UNWIND [1, 2, 3] AS x RETURN x NEXT CREATE (:Q {x: x}) NEXT RETURN 1 AS one", {{"1"}});

    expectRows("MATCH (q:Q) RETURN q.x", {{"1"}, {"2"}, {"3"}});
}

TEST_F(NextAfterWritingSegmentTest, countsOneRowAfterSettingEveryPerson) {
    expectWriteRows("MATCH (p:Person) RETURN p NEXT SET p.seen = true NEXT RETURN count(*) AS n", {{"1"}});

    expectRows("MATCH (p:Person) WHERE p.seen RETURN count(p)", {{"8"}});
}
