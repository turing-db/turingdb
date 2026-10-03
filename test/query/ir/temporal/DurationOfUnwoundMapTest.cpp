#include <gtest/gtest.h>

#include "IRTestRows.h"
#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

class DurationOfUnwoundMapTest : public WriteQueryTest {
};

TEST_F(DurationOfUnwoundMapTest, readsEachUnwoundMap) {
    expectRows("UNWIND [{days: 1}, {hours: 2}] AS m RETURN duration(m)", {{"PT24H"}, {"PT2H"}});
}

TEST_F(DurationOfUnwoundMapTest, readsANullCellAsNull) {
    expectRows("UNWIND [{days: 1}, null] AS m RETURN duration(m)", {{"PT24H"}, {"null"}});
}
