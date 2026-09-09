#include <gtest/gtest.h>

#include <string>
#include <string_view>
#include <vector>

#include "CallV3Test.h"
#include "IRTestRows.h"

using namespace turing::test;

namespace {

// Only Remy and Adam carry an age in simpledb, so the other six answer every null test
const Rows aged {
    {"Adam", "true"}, {"Cyrus", "false"}, {"Doruk", "false"}, {"Luc", "false"},
    {"Martina", "false"}, {"Maxime", "false"}, {"Remy", "true"}, {"Suhas", "false"},
};

const Rows ageless {
    {"Adam", "false"}, {"Cyrus", "true"}, {"Doruk", "true"}, {"Luc", "true"},
    {"Martina", "true"}, {"Maxime", "true"}, {"Remy", "false"}, {"Suhas", "true"},
};

const Rows allNull {
    {"Adam", "null"}, {"Cyrus", "null"}, {"Doruk", "null"}, {"Luc", "null"},
    {"Martina", "null"}, {"Maxime", "null"}, {"Remy", "null"}, {"Suhas", "null"},
};

}

// Comparing against null and testing for one are different operators. IS NULL asks whether a
// value is absent and answers true or false; = null is a comparison, and a comparison against
// null is null, so it is neither true nor false and matches no row.
class NullComparisonTest : public CallV3Test {
protected:
    void expectRows(std::string_view query, const Rows& expected) {
        RowSink sink;
        runQuery(query, sink);

        Rows rows;
        sink.sortedRows(rows);

        EXPECT_EQ(rows, expected) << query;
    }
};

TEST_F(NullComparisonTest, isNullAnswersTrueOrFalse) {
    expectRows("MATCH (n:Person) RETURN n.name, n.age IS NULL", ageless);
}

TEST_F(NullComparisonTest, isNotNullAnswersTrueOrFalse) {
    expectRows("MATCH (n:Person) RETURN n.name, n.age IS NOT NULL", aged);
}

TEST_F(NullComparisonTest, isNullSelectsTheRowsWithoutAValue) {
    expectRows("MATCH (n:Person) WHERE n.age IS NULL RETURN n.name",
               {{"Cyrus"}, {"Doruk"}, {"Luc"}, {"Martina"}, {"Maxime"}, {"Suhas"}});
}

TEST_F(NullComparisonTest, isNotNullSelectsTheRowsWithAValue) {
    expectRows("MATCH (n:Person) WHERE n.age IS NOT NULL RETURN n.name", {{"Adam"}, {"Remy"}});
}

TEST_F(NullComparisonTest, equalityAgainstNullIsNull) {
    expectRows("MATCH (n:Person) RETURN n.name, n.age = null", allNull);
}

TEST_F(NullComparisonTest, inequalityAgainstNullIsNull) {
    expectRows("MATCH (n:Person) RETURN n.name, n.age <> null", allNull);
}

TEST_F(NullComparisonTest, equalityAgainstNullReadsTheSameOnEitherSide) {
    expectRows("MATCH (n:Person) RETURN n.name, null = n.age", allNull);
}

// The rows an ageless person would take under an IS NULL reading of = null
TEST_F(NullComparisonTest, equalityAgainstNullMatchesNoRow) {
    expectRows("MATCH (n:Person) WHERE n.age = null RETURN n.name", {});
}

TEST_F(NullComparisonTest, inequalityAgainstNullMatchesNoRow) {
    expectRows("MATCH (n:Person) WHERE n.age <> null RETURN n.name", {});
}

TEST_F(NullComparisonTest, twoNullsCompareToNull) {
    expectRows("MATCH (n:Person {name: 'Remy'}) RETURN n.name, null = null", {{"Remy", "null"}});
}

TEST_F(NullComparisonTest, aNullIsNull) {
    expectRows("MATCH (n:Person {name: 'Remy'}) RETURN n.name, null IS NULL", {{"Remy", "true"}});
}

TEST_F(NullComparisonTest, aNullIsNotNotNull) {
    expectRows("MATCH (n:Person {name: 'Remy'}) RETURN n.name, null IS NOT NULL", {{"Remy", "false"}});
}

// The null of an absent value still propagates through an ordinary comparison
TEST_F(NullComparisonTest, comparingAnAbsentValueAgainstOneIsNull) {
    expectRows("MATCH (n:Person) RETURN n.name, n.age = 32",
               {
                   {"Adam", "true"}, {"Cyrus", "null"}, {"Doruk", "null"}, {"Luc", "null"},
                   {"Martina", "null"}, {"Maxime", "null"}, {"Remy", "true"}, {"Suhas", "null"},
               });
}

TEST_F(NullComparisonTest, rejectsAnIsTestAgainstAValue) {
    runQueryExpectingError("MATCH (n:Person) RETURN n.age IS 5", "must be NULL");
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
