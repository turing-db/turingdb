#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// What the scope clause of a CALL names stays readable after a segment of its body that
// writes and returns nothing
class NextKeepsCallImportsAfterWriteTest : public WriteQueryTest {
};

TEST_F(NextKeepsCallImportsAfterWriteTest, readsTheImportAfterASegmentThatWrites) {
    expectWriteRows("MATCH (p:Person) WHERE p.name = 'Remy' "
                    "CALL (p) { UNWIND [1, 2] AS x RETURN x NEXT CREATE (:T {x: x}) NEXT RETURN p.name AS name } "
                    "RETURN name",
                    {{"Remy"}});

    expectRows("MATCH (t:T) RETURN t.x", {{"1"}, {"2"}});
}
