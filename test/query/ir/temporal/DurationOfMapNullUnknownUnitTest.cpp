#include <gtest/gtest.h>

#include "IRTestRows.h"
#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

class DurationOfMapNullUnknownUnitTest : public WriteQueryTest {
};

TEST_F(DurationOfMapNullUnknownUnitTest, rejectsAnUnknownUnitWhoseCountIsNull) {
    expectError("RETURN duration({month: null})", "Unknown duration component: month");
}
