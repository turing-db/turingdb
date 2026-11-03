#include <gtest/gtest.h>

#include "IRTestRows.h"
#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// coalesce answers each row its first argument that is not null, in that argument's own
// type, and an argument that is never null - a test, an ID, a count - is one like any
// other. Maxime, Luc, Remy and Adam have a date of birth, the other four people have none.
// Martina is node 11, Suhas 12, Cyrus 15 and Doruk 17; Martina and Doruk have one outgoing
// edge, Suhas and Cyrus two.
class CoalesceNonNullableScalarTest : public WriteQueryTest {
};

TEST_F(CoalesceNonNullableScalarTest, keepsABooleanBesideAString) {
    expectRows("MATCH (n:Person) RETURN n.name, coalesce(n.dob, n.age IS NULL)",
               {{"Adam", "18/08"}, {"Cyrus", "true"}, {"Doruk", "true"}, {"Luc", "28/05"},
                {"Martina", "true"}, {"Maxime", "24/07"}, {"Remy", "18/01"}, {"Suhas", "true"}});
}

TEST_F(CoalesceNonNullableScalarTest, keepsAnIDBesideAString) {
    expectRows("MATCH (n:Person) RETURN n.name, coalesce(n.dob, id(n))",
               {{"Adam", "18/08"}, {"Cyrus", "15"}, {"Doruk", "17"}, {"Luc", "28/05"},
                {"Martina", "11"}, {"Maxime", "24/07"}, {"Remy", "18/01"}, {"Suhas", "12"}});
}

TEST_F(CoalesceNonNullableScalarTest, keepsACountBesideAString) {
    expectRows("MATCH (n:Person) RETURN n.name, coalesce(n.dob, COUNT { MATCH (n)-->() })",
               {{"Adam", "18/08"}, {"Cyrus", "2"}, {"Doruk", "1"}, {"Luc", "28/05"},
                {"Martina", "1"}, {"Maxime", "24/07"}, {"Remy", "18/01"}, {"Suhas", "2"}});
}
