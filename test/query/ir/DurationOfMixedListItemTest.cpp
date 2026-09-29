#include <gtest/gtest.h>

#include "IRTestRows.h"
#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

class DurationOfMixedListItemTest : public WriteQueryTest {
};

TEST_F(DurationOfMixedListItemTest, readsAnIntegerOrAMapInTheSameList) {
    expectRows("UNWIND [1000000, {days: 1}, null] AS x RETURN duration(x)", {{"PT1S"}, {"PT24H"}, {"null"}});
}

TEST_F(DurationOfMixedListItemTest, readsAnIndexedItemOfAMixedList) {
    expectRows("WITH [1000000, {hours: 2}] AS l RETURN duration(l[1])", {{"PT2H"}});
}
