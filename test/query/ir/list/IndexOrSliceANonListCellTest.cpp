#include <gtest/gtest.h>

#include <string_view>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

class IndexOrSliceANonListCellTest : public WriteQueryTest {
protected:
    void expectIndexError(std::string_view query) {
        expectError(query, "A list index reads a list, and this row holds a value that is not one");
    }

    void expectSliceError(std::string_view query) {
        expectError(query, "A list slice or a percentile reads a list, and this row holds a value that is not one");
    }
};

TEST_F(IndexOrSliceANonListCellTest, indexesNoIntegerCell) {
    expectIndexError("UNWIND [[1, 2], 3] AS v RETURN v[0]");
    expectIndexError("UNWIND [[1, 2], 3] AS v RETURN v[-1]");
}

TEST_F(IndexOrSliceANonListCellTest, indexesNoStringCell) {
    expectIndexError("UNWIND [[1, 2], 'a'] AS v RETURN v[0]");
}

TEST_F(IndexOrSliceANonListCellTest, indexesNoMapCellByPosition) {
    expectIndexError("UNWIND [[1, 2], {k: 1}] AS v RETURN v[0]");
}

TEST_F(IndexOrSliceANonListCellTest, indexesNoNodeCell) {
    expectIndexError("MATCH (n:Person {name: 'Remy'}) UNWIND [[1, 2], n] AS v RETURN v[0]");
}

TEST_F(IndexOrSliceANonListCellTest, indexesNoConstantCell) {
    expectIndexError("RETURN [[1, 2], 3][1][0]");
}

TEST_F(IndexOrSliceANonListCellTest, indexesNoCellInAComprehension) {
    expectIndexError("RETURN [x IN [[1, 2], 3] | x[0]]");
}

TEST_F(IndexOrSliceANonListCellTest, indexesNoCellNothingReads) {
    expectIndexError("UNWIND [[1, 2], 3] AS v WITH v, v[0] AS e RETURN v");
}

TEST_F(IndexOrSliceANonListCellTest, indexesTheListACellHolds) {
    expectRows("UNWIND [[1, 2], 'a'] AS v WITH v WHERE v = [1, 2] RETURN v[0], v[-1], v[5]", {{"1", "2", "null"}});
}

TEST_F(IndexOrSliceANonListCellTest, indexesACellHoldingANullAsNull) {
    expectRows("UNWIND [[1, 2], null, 'a'] AS v WITH v WHERE v IS NULL RETURN v[0]", {{"null"}});
}

TEST_F(IndexOrSliceANonListCellTest, indexesOnlyTheRowsACaseTakes) {
    expectRows("UNWIND [[1, 2], 3] AS v RETURN CASE WHEN v = 3 THEN -1 ELSE v[0] END", {{"-1"}, {"1"}});
}

TEST_F(IndexOrSliceANonListCellTest, indexesNothingWhereNoRowReaches) {
    expectRows("MATCH (n:Person) WHERE n.name = 'Nobody' RETURN [[1, 2], 3][1][0]", {});
}

TEST_F(IndexOrSliceANonListCellTest, slicesNoStringCell) {
    expectSliceError("UNWIND [[1, 2], 'a'] AS l RETURN l[0..1]");
}

TEST_F(IndexOrSliceANonListCellTest, slicesNoIntegerCell) {
    expectSliceError("UNWIND [[1, 2], 3] AS l RETURN l[1..]");
    expectSliceError("UNWIND [[1, 2], 3] AS l RETURN l[..1]");
}

TEST_F(IndexOrSliceANonListCellTest, slicesNoMapCell) {
    expectSliceError("UNWIND [[1, 2], {k: 1}] AS l RETURN l[0..1]");
}

TEST_F(IndexOrSliceANonListCellTest, slicesNoConstantCell) {
    expectSliceError("RETURN [[1, 2], 'a'][1][0..1]");
}

TEST_F(IndexOrSliceANonListCellTest, slicesNoCellNothingReads) {
    expectSliceError("UNWIND [[1, 2], 'a'] AS l WITH l, l[0..1] AS s RETURN l");
}

TEST_F(IndexOrSliceANonListCellTest, slicesTheListACellHolds) {
    expectRows("UNWIND [[1, 2], 'a'] AS l WITH l WHERE l = [1, 2] RETURN l[0..1], l[1..], l[..5]",
               {{"[1]", "[2]", "[1, 2]"}});
}

TEST_F(IndexOrSliceANonListCellTest, slicesACellHoldingANullAsNull) {
    expectRows("UNWIND [[1, 2], null, 'a'] AS l WITH l WHERE l IS NULL RETURN l[0..1]", {{"null"}});
}

TEST_F(IndexOrSliceANonListCellTest, slicesOnlyTheRowsACaseTakes) {
    expectRows("UNWIND [[1, 2], 'a'] AS l RETURN CASE WHEN l = 'a' THEN [] ELSE l[1..] END", {{"[2]"}, {"[]"}});
}

TEST_F(IndexOrSliceANonListCellTest, slicesNothingWhereNoRowReaches) {
    expectRows("MATCH (n:Person) WHERE n.name = 'Nobody' RETURN [[1, 2], 'a'][1][0..1]", {});
}
