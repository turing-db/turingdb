#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// A chain in a CALL with no scope clause opens on a RETURN, which imports nothing
class NextInUnscopedCallTest : public WriteQueryTest {
};

TEST_F(NextInUnscopedCallTest, chainsFromAReturnOfItsOwn) {
    expectRows("CALL { RETURN 1 AS z NEXT RETURN z + 1 AS w } RETURN w", {{"2"}});
}

TEST_F(NextInUnscopedCallTest, readsNoVariableOfTheOuterQuery) {
    expectError("UNWIND [1, 2] AS y CALL { RETURN y NEXT RETURN y + 1 AS w } RETURN y, w",
                "Variable 'y' not found");
}
