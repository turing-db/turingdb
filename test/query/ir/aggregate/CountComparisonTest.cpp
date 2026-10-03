#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// A count is an unsigned column and an integer literal a signed one. simpledb holds 8
// people.
class CountComparisonTest : public WriteQueryTest {
};

TEST_F(CountComparisonTest, comparesACountWithANegativeNumber) {
    expectRows("MATCH (p:Person) WITH count(p) AS c "
               "RETURN c > -1, c >= -1, c < -1, c <= -1, c = -1, c <> -1, -1 < c",
               {{"true", "true", "false", "false", "false", "true", "true"}});
}

TEST_F(CountComparisonTest, comparesACountWithAPositiveNumber) {
    expectRows("MATCH (p:Person) WITH count(p) AS c "
               "RETURN c > 7, c >= 9, c < 9, c <= 7, c = 8, c <> 8",
               {{"true", "false", "true", "false", "true", "false"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
