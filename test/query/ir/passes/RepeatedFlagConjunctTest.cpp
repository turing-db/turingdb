#include <gtest/gtest.h>

#include "IRTestRows.h"
#include "WriteQueryTest.h"

using namespace turing::test;

// A WHERE testing a flag an earlier WHERE already tested, the flag a conjunction of its own.
// The stacked WHEREs merge into a mask reading the flag twice. Remy and Adam are 32; the
// other people have no age.
class RepeatedFlagConjunctTest : public WriteQueryTest {
};

TEST_F(RepeatedFlagConjunctTest, keepsTheRowsBothWheresKeep) {
    expectRows("MATCH (n:Person) WITH n, (n.age > 1 AND labels(n) <> ['x']) AS ok WHERE ok "
               "WITH n, ok WHERE ok AND n.name = 'Remy' RETURN n.name",
               {{"Remy"}});
}

TEST_F(RepeatedFlagConjunctTest, keepsTheRowsAFlagTestedTwiceInOneWhereKeeps) {
    expectRows("MATCH (n:Person) WITH n, (n.age > 1 AND labels(n) <> ['x']) AS ok WHERE ok "
               "WITH n, ok WHERE ok AND n.name = 'Remy' AND ok RETURN n.name",
               {{"Remy"}});
}

TEST_F(RepeatedFlagConjunctTest, keepsTheRowsAWhereTestingTheFlagTwiceKeeps) {
    expectRows("MATCH (n:Person) WITH n, (n.age > 1 AND labels(n) <> ['x']) AS ok WHERE n.name <> 'Luc' "
               "WITH n, ok WHERE ok AND ok RETURN n.name",
               {{"Adam"}, {"Remy"}});
}
