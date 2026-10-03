#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// SET of a value expression that is null on some rows, rather than the null literal. A null
// row takes the property off its entity and every other row writes its value.
//
// Of simpledb's 8 Person nodes, Remy and Adam carry an age of 32, and Remy, Adam, Maxime
// and Luc carry a dob.
class SetPropertyNullExpressionTest : public WriteQueryTest {
};

TEST_F(SetPropertyNullExpressionTest, setsTheIntegerPropertyToNullOnTheRowACaseLeavesNull) {
    applyWrite("MATCH (p:Person) SET p.age = CASE WHEN p.name = 'Remy' THEN null ELSE 7 END");

    expectRows("MATCH (p:Person) WHERE p.age IS NULL RETURN p.name", {{"Remy"}});
    expectRows("MATCH (p:Person) RETURN count(p.age), sum(p.age)", {{"7", "49"}});
}

TEST_F(SetPropertyNullExpressionTest, keepsTheStringPropertyOnTheRowsACaseDoesNotNull) {
    applyWrite("MATCH (p:Person) SET p.dob = CASE WHEN p.name = 'Adam' THEN null ELSE p.dob END");

    expectRows("MATCH (p:Person) WHERE p.dob IS NOT NULL RETURN p.name, p.dob",
               {{"Remy", "18/01"}, {"Maxime", "24/07"}, {"Luc", "28/05"}});
}

TEST_F(SetPropertyNullExpressionTest, setsThePropertyToNullFromArithmeticOverNull) {
    expectWriteRows("MATCH (p:Person {name: 'Remy'}) SET p.age = null + 1 RETURN p.age", {{"null"}});

    expectRows("MATCH (p:Person) WHERE p.age IS NOT NULL RETURN p.name", {{"Adam"}});
}

TEST_F(SetPropertyNullExpressionTest, setsThePropertyToNullFromAFunctionCalledWithNull) {
    expectWriteRows("MATCH (p:Person {name: 'Remy'}) SET p.age = toInteger(null) RETURN p.age", {{"null"}});

    expectRows("MATCH (p:Person) WHERE p.age IS NOT NULL RETURN p.name", {{"Adam"}});
}

TEST_F(SetPropertyNullExpressionTest, setsThePropertyToNullFromACoalesceOfNulls) {
    expectWriteRows("MATCH (p:Person {name: 'Remy'}) SET p.dob = coalesce(null, null) RETURN p.dob", {{"null"}});

    expectRows("MATCH (p:Person) WHERE p.dob IS NOT NULL RETURN p.name", {{"Adam"}, {"Maxime"}, {"Luc"}});
}

TEST_F(SetPropertyNullExpressionTest, setsThePropertyToNullFromTheNullElementOfAList) {
    applyWrite("MATCH (p:Person {name: 'Remy'}) "
               "SET p.age = [1, null][1], p.dob = ['a', null][1], p.isFrench = [true, null][1]");

    expectRows("MATCH (p:Person {name: 'Remy'}) RETURN p.age, p.dob, p.isFrench", {{"null", "null", "null"}});
}

// 100000 rows span two chunks of 65536, and the odd values left sum to 50000^2
TEST_F(SetPropertyNullExpressionTest, setsThePropertyToNullOnEveryOtherRowAcrossChunks) {
    applyWrite("UNWIND range(1, 100000) AS x CREATE (:Big {v: x})");
    applyWrite("MATCH (b:Big) SET b.v = CASE WHEN b.v % 2 = 0 THEN null ELSE b.v END");

    expectRows("MATCH (b:Big) RETURN count(b.v), sum(b.v), min(b.v), max(b.v)",
               {{"50000", "2500000000", "1", "99999"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
