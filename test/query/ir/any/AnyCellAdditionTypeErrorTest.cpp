#include <gtest/gtest.h>

#include <string_view>

#include "QueryStatus.h"

#include "CallV3Test.h"
#include "IRTestRows.h"

using namespace db;
using namespace turing::test;

class AnyCellAdditionTypeErrorTest : public CallV3Test {
protected:
    void expectRows(std::string_view query, const Rows& expected) {
        RowSink sink;
        runQuery(query, sink);

        Rows rows;
        sink.sortedRows(rows);

        EXPECT_EQ(rows, expected) << query;
    }

    // A type-erased cell is typed on each row, so the addition raises at run time
    void expectRunTimeError(std::string_view query) {
        runQueryExpectingError(query, QueryStatus::Status::EXEC_ERROR, "Operands are not valid and compatible types");
    }
};

TEST_F(AnyCellAdditionTypeErrorTest, addsNoBooleanToTheNumberARowHolds) {
    expectRunTimeError("UNWIND [1, 'a'] AS x RETURN x + true");
    expectRunTimeError("UNWIND [1, 'a'] AS x RETURN true + x");
}

TEST_F(AnyCellAdditionTypeErrorTest, addsNoBooleanToANumberCell) {
    expectRunTimeError("RETURN [1, 'a'][0] + true");
    expectRunTimeError("RETURN [1.5, 'a'][0] + false");
    expectRunTimeError("RETURN false + [1.5, 'a'][0]");
}

TEST_F(AnyCellAdditionTypeErrorTest, addsNoNumberToABooleanCell) {
    expectRunTimeError("RETURN [true, 'a'][0] + 1");
    expectRunTimeError("RETURN 1.5 + [true, 'a'][0]");
}

TEST_F(AnyCellAdditionTypeErrorTest, addsNoBooleanToABooleanCell) {
    expectRunTimeError("RETURN [true, 'a'][0] + true");
    expectRunTimeError("RETURN [true, 'a'][0] + [false, 'a'][0]");
}

TEST_F(AnyCellAdditionTypeErrorTest, addsNoBooleanCellToANumberCell) {
    expectRunTimeError("RETURN [true, 'a'][0] + [1, 'a'][0]");
    expectRunTimeError("RETURN [1, 'a'][0] + [true, 'a'][0]");
}

TEST_F(AnyCellAdditionTypeErrorTest, addsNoBooleanPropertyToANumberCell) {
    expectRunTimeError("MATCH (p:Person {name: 'Remy'}) RETURN [1, 'a'][0] + p.isFrench");
}

TEST_F(AnyCellAdditionTypeErrorTest, addsNoBooleanMapValueToANumberMapValue) {
    expectRunTimeError("WITH {a: 1, b: true} AS m RETURN m.a + m.b");
}

TEST_F(AnyCellAdditionTypeErrorTest, addsNoMapCell) {
    expectRunTimeError("RETURN [{k: 1}, 'a'][0] + 1");
    expectRunTimeError("RETURN 'x' + [{k: 1}, 'a'][0]");
    expectRunTimeError("RETURN [{k: 1}, 'a'][0] + true");
    expectRunTimeError("RETURN [{k: 1}, 'a'][0] + [{k: 2}, 'a'][0]");
}

TEST_F(AnyCellAdditionTypeErrorTest, addsNoNodeCell) {
    expectRunTimeError("MATCH (n:Person {name: 'Remy'}) RETURN [n, 'a'][0] + 1");
    expectRunTimeError("MATCH (n:Person {name: 'Remy'}) RETURN 'x' + [n, 'a'][0]");
}

TEST_F(AnyCellAdditionTypeErrorTest, addsNoEdgeCell) {
    expectRunTimeError("MATCH (:Person {name: 'Remy'})-[e]->() WITH e LIMIT 1 RETURN [e, 'a'][0] + 1");
    expectRunTimeError("MATCH (:Person {name: 'Remy'})-[e]->() WITH e LIMIT 1 RETURN [e, 'a'][0] + 'x'");
}

TEST_F(AnyCellAdditionTypeErrorTest, addsNothingButADurationToATemporalCell) {
    expectRunTimeError("RETURN [datetime('2024-01-01T00:00:00Z'), 'a'][0] + true");
    expectRunTimeError("RETURN true + [duration(3600000000), 'a'][0]");
    expectRunTimeError("RETURN [datetime('2024-01-01T00:00:00Z'), 'a'][0] + 1");
    expectRunTimeError("RETURN 1 + [duration(3600000000), 'a'][0]");
    expectRunTimeError("RETURN [datetime('2024-01-01T00:00:00Z'), 'a'][0] + datetime('2024-01-01T00:00:00Z')");
}

TEST_F(AnyCellAdditionTypeErrorTest, addsNoBooleanInAReduceBody) {
    expectRunTimeError("RETURN reduce(acc = 0, y IN [1, true] | acc + y)");
}

TEST_F(AnyCellAdditionTypeErrorTest, addsNoBooleanInAComprehension) {
    expectRunTimeError("RETURN [x IN [1, true] | x + 1]");
}

TEST_F(AnyCellAdditionTypeErrorTest, addsABooleanToAStringCell) {
    expectRows("RETURN ['a', 1][0] + true AS a, true + ['a', 1][0] AS b", {{"atrue", "truea"}});
}

TEST_F(AnyCellAdditionTypeErrorTest, addsAStringToABooleanCell) {
    expectRows("RETURN [true, 1][0] + 'x' AS a, 'x' + [true, 1][0] AS b", {{"truex", "xtrue"}});
    expectRows("RETURN [true, 1][0] + ['a', 1][0]", {{"truea"}});
}

TEST_F(AnyCellAdditionTypeErrorTest, addsABooleanPropertyToAStringCell) {
    expectRows("MATCH (p:Person {name: 'Remy'}) RETURN ['a', 1][0] + p.isFrench", {{"atrue"}});
}

TEST_F(AnyCellAdditionTypeErrorTest, addsTheValueEachRowHolds) {
    expectRows("UNWIND ['a', [1]] AS x RETURN x + true", {{"[1, true]"}, {"atrue"}});
    expectRows("UNWIND [1, 'a'] AS x RETURN x + 1", {{"2"}, {"a1"}});
}

TEST_F(AnyCellAdditionTypeErrorTest, joinsABooleanCellToAList) {
    expectRows("RETURN [[1], 'a'][0] + true AS a, true + [[1], 'a'][0] AS b", {{"[1, true]", "[true, 1]"}});
    expectRows("RETURN [true, 'a'][0] + [1]", {{"[true, 1]"}});
}

TEST_F(AnyCellAdditionTypeErrorTest, joinsAMapOrANodeCellToAList) {
    expectRows("RETURN [{k: 1}, 'a'][0] + [1]", {{"[{k: 1}, 1]"}});
    expectRows("MATCH (n:Person {name: 'Remy'}) RETURN [n, 'a'][0] + [1]", {{"[0, 1]"}});
}

TEST_F(AnyCellAdditionTypeErrorTest, addsNullToABooleanCell) {
    expectRows("RETURN [true, 'a'][0] + null AS a, null + [true, 'a'][0] AS b", {{"null", "null"}});
    expectRows("RETURN [null, 1, 'a'][0] + true", {{"null"}});
}

TEST_F(AnyCellAdditionTypeErrorTest, addsADurationToATemporalCell) {
    expectRows("RETURN [datetime('2024-01-01T00:00:00Z'), 'a'][0] + duration(3600000000)", {{"2024-01-01T01:00:00Z"}});
    expectRows("RETURN [duration(3600000000), 'a'][0] + duration(3600000000)", {{"PT2H"}});
}

TEST_F(AnyCellAdditionTypeErrorTest, addsATemporalToAStringCell) {
    expectRows("RETURN ['a', 1][0] + datetime('2024-01-01T00:00:00Z') AS a, [duration(3600000000), 1][0] + 'x' AS b",
               {{"a2024-01-01T00:00:00Z", "PT1Hx"}});
}

TEST_F(AnyCellAdditionTypeErrorTest, addsNothingWhereNoRowReaches) {
    expectRows("MATCH (p:Person) WHERE p.name = 'Nobody' RETURN [1, 'a'][0] + true", {});
    expectRows("UNWIND [] AS x RETURN [1, 'a'][0] + true", {});
}

TEST_F(AnyCellAdditionTypeErrorTest, addsOnlyTheRowsAFilterKeeps) {
    expectRows("UNWIND [1, 'a'] AS x WITH x WHERE x = 'a' RETURN x + true", {{"atrue"}});
    expectRows("UNWIND [1, 'a'] AS x RETURN CASE WHEN x = 'a' THEN x + true ELSE 'none' END", {{"atrue"}, {"none"}});
    expectRows("UNWIND [1, 'a'] AS x RETURN [y IN [x] WHERE y = 'a' | y + true]", {{"[]"}, {"[atrue]"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
