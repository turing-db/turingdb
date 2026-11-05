#include <gtest/gtest.h>

#include "IRTestRows.h"
#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

class NullTemporalOperandTest : public WriteQueryTest {
};

TEST_F(NullTemporalOperandTest, rejectsANullBesideAnOperandNoValueTakes) {
    expectError("RETURN null / duration(1)",
                "Operands are not valid and compatible numeric types: 'Null' and 'Duration'");
    expectError("RETURN null % duration(1)",
                "Operands are not valid and compatible numeric types: 'Null' and 'Duration'");
    expectError("RETURN duration(1) % null",
                "Operands are not valid and compatible numeric types: 'Duration' and 'Null'");
    expectError("RETURN null ^ duration(1)",
                "Operands are not valid and compatible numeric types: 'Null' and 'Duration'");
    expectError("RETURN null / datetime('2024-01-01T00:00:00Z')",
                "Operands are not valid and compatible numeric types: 'Null' and 'DateTime'");
    expectError("RETURN datetime('2024-01-01T00:00:00Z') * null",
                "Operands are not valid and compatible numeric types: 'DateTime' and 'Null'");
}

TEST_F(NullTemporalOperandTest, answersNullBesideAnOperandSomeValueTakes) {
    expectRows("RETURN duration(1) / null, null * duration(1), null + duration(1), duration(1) - null",
               {{"null", "null", "null", "null"}});
    expectRows("RETURN datetime('2024-01-01T00:00:00Z') - null, null - datetime('2024-01-01T00:00:00Z')",
               {{"null", "null"}});
    expectRows("RETURN 2 - null, null / 2, null % 2, 2 ^ null, null + 'a', [1] + null",
               {{"null", "null", "null", "null", "null", "[1, null]"}});
}
