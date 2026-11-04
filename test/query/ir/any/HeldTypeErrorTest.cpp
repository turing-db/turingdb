#include <gtest/gtest.h>

#include <string>
#include <string_view>

#include "QueryStatus.h"
#include "versioning/ChangeID.h"

#include "IRTestRows.h"
#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

namespace {

constexpr std::string_view REMY = "MATCH (p:Person {name: 'Remy'}) ";
constexpr std::string_view DATETIME = "datetime('2020-01-01T00:00:00Z')";

}

class HeldTypeErrorTest : public WriteQueryTest {
protected:
    void expectMessage(std::string_view query, std::string_view message) {
        RowSink sink;
        const QueryStatus status = runQuery(query, &sink);

        ASSERT_FALSE(status.isOk()) << "query: " << query;
        EXPECT_EQ(status.getError(), message) << "query: " << query;
    }

    void expectWriteMessage(std::string_view query, std::string_view message) {
        ChangeID changeID;
        openChange(changeID);

        const QueryStatus status = runWrite(query, changeID);

        ASSERT_FALSE(status.isOk()) << "query: " << query;
        EXPECT_EQ(status.getError(), message) << "query: " << query;
    }
};

TEST_F(HeldTypeErrorTest, slicesAValueThatIsNoList) {
    expectMessage("UNWIND [[1, 2], 1] AS l RETURN l[0..1]", "A list slice or a percentile reads a list, not 'Integer'");
    expectMessage("UNWIND [[1, 2], {a: 1}] AS l RETURN l[0..1]", "A list slice or a percentile reads a list, not 'Map'");
}

TEST_F(HeldTypeErrorTest, boundsARangeOrASliceByAValueThatIsNoInteger) {
    expectMessage("UNWIND [1, 'a'] AS b RETURN range(0, b)", "A range or a list slice reads an integer bound, not 'String'");
    expectMessage("UNWIND [1, 1.5] AS b RETURN [1, 2, 3][0..b]", "A range or a list slice reads an integer bound, not 'Double'");
}

TEST_F(HeldTypeErrorTest, testsAConditionThatIsNoBoolean) {
    expectMessage("UNWIND [true, 1] AS x WITH x WHERE x RETURN x", "A condition must be a boolean, not 'Integer'");
    expectMessage("UNWIND [true, 'a'] AS x RETURN CASE WHEN x THEN 1 END", "A condition must be a boolean, not 'String'");
}

TEST_F(HeldTypeErrorTest, aggregatesAValueThatIsNoNumber) {
    expectMessage("UNWIND [1, 'a'] AS x RETURN sum(x)", "sum() and avg() read numbers or durations, not 'String'");
    expectMessage("UNWIND [1, true] AS x RETURN avg(x)", "sum() and avg() read numbers or durations, not 'Bool'");
}

TEST_F(HeldTypeErrorTest, takesThePercentileOfAValueThatIsNoNumber) {
    expectMessage("UNWIND [1, 'a'] AS x RETURN percentileDisc(x, 0.5)",
                  "percentileDisc() takes the percentile of numbers, not 'String'");
    expectMessage("UNWIND [1, [2]] AS x RETURN percentileCont(x, 0.5)",
                  "percentileCont() takes the percentile of numbers, not 'List'");
}

TEST_F(HeldTypeErrorTest, connectsAValueThatIsNoBoolean) {
    expectMessage("UNWIND [true, 1] AS x RETURN x AND true", "A logical operand must be a boolean, not 'Integer'");
    expectMessage("UNWIND [true, [1]] AS x RETURN NOT x", "A logical operand must be a boolean, not 'List'");
}

TEST_F(HeldTypeErrorTest, computesOverValuesOfIncompatibleTypes) {
    expectMessage("UNWIND [" + std::string(DATETIME) + ", 1] AS x RETURN x * 2",
                  "Operands are not valid and compatible numeric types: 'DateTime' and 'Integer'");
    expectMessage("UNWIND [true, 1] AS x RETURN x + 1",
                  "Operands are not valid and compatible types for '+': 'Bool' and 'Integer'");
    expectMessage("UNWIND [" + std::string(DATETIME) + ", 1] AS x RETURN x + x",
                  "Operands are not valid and compatible types for '+': 'DateTime' and 'DateTime'");
    expectMessage("UNWIND [{a: 1}, 1] AS x RETURN x + 'a'",
                  "Operands are not valid and compatible types for '+': 'Map' and 'String'");
}

TEST_F(HeldTypeErrorTest, concatenatesValuesThatAreNeitherStringsNorLists) {
    expectMessage("UNWIND [1, 'a'] AS x RETURN x || x",
                  "Operands are not valid and compatible types for '||': 'Integer' and 'Integer'");
    expectMessage("UNWIND [1.5, 'a'] AS x RETURN x || 'b'",
                  "Operands are not valid and compatible types for '||': 'Double' and 'String'");
}

TEST_F(HeldTypeErrorTest, readsTheEndsOfAValueThatIsNoList) {
    expectMessage("UNWIND [[1], 'a'] AS x RETURN head(x)", "head(), last() and tail() read a list, not 'String'");
}

TEST_F(HeldTypeErrorTest, readsTheNodesOfAValueThatIsNoEdge) {
    expectMessage("UNWIND [1, 'a'] AS x RETURN startNode(x)", "startNode() and endNode() read an edge, not 'Integer'");
    expectMessage(std::string(REMY) + "UNWIND [p, 1] AS x RETURN endNode(x)",
                  "startNode() and endNode() read an edge, not 'NodePattern'");
}

TEST_F(HeldTypeErrorTest, sizesAValueThatIsNeitherAListNorAString) {
    expectMessage("UNWIND [1, 'a'] AS x RETURN size(x)", "size() and length() read a list, a string or a path, not 'Integer'");
    expectMessage("UNWIND ['a', {a: 1}] AS x RETURN size(x)", "size() and length() read a list, a string or a path, not 'Map'");
}

TEST_F(HeldTypeErrorTest, testsTheEmptinessOfAValueThatHasNone) {
    expectMessage("UNWIND [1, 'a'] AS x RETURN isEmpty(x)", "isEmpty() reads a list, a string or a map, not 'Integer'");
}

TEST_F(HeldTypeErrorTest, readsTextOutOfAValueThatIsNoString) {
    expectMessage("UNWIND [1, 'a'] AS x RETURN toUpper(x)", "toUpper() reads a string, not 'Integer'");
}

TEST_F(HeldTypeErrorTest, readsAPositionOutOfAValueThatIsNoInteger) {
    expectMessage("UNWIND [1, 'a'] AS x RETURN substring('abc', x)", "substring() reads an integer, not 'String'");
}

TEST_F(HeldTypeErrorTest, readsANumberOutOfAValueThatIsNone) {
    expectMessage("UNWIND [1, 'a'] AS x RETURN sqrt(x)", "sqrt() reads a number, not 'String'");
    expectMessage("UNWIND [1, true] AS x RETURN abs(x)", "abs() reads a number, not 'Bool'");
}

TEST_F(HeldTypeErrorTest, reversesAValueThatIsNeitherAStringNorAList) {
    expectMessage("UNWIND [1, 'a'] AS x RETURN reverse(x)", "reverse() reads a string or a list, not 'Integer'");
}

TEST_F(HeldTypeErrorTest, readsTheEntriesOfAValueThatHasNone) {
    expectMessage("UNWIND [{a: 1}, 1] AS x RETURN keys(x)", "keys() reads a node, a relationship or a map, not 'Integer'");
    expectMessage("UNWIND [{a: 1}, 1] AS x RETURN labels(x)", "labels() reads a node, not 'Map'");
    expectMessage(std::string(REMY) + "UNWIND [p, 1] AS x RETURN type(x)", "type() reads a relationship, not 'NodePattern'");
    expectMessage("UNWIND [[1], 1] AS x RETURN properties(x)", "properties() reads a node, a relationship or a map, not 'List'");
}

TEST_F(HeldTypeErrorTest, convertsAValueThatHasNoConversion) {
    expectMessage("UNWIND [[1], 'a'] AS x RETURN toInteger(x)", "toInteger() has no conversion for 'List'");
    expectMessage("UNWIND [{a: 1}, 'a'] AS x RETURN toString(x)", "toString() has no conversion for 'Map'");
}

TEST_F(HeldTypeErrorTest, signsAValueThatIsNoNumber) {
    expectMessage("UNWIND [[1], 1] AS x RETURN -x",
                  "The operand of a unary minus must be an integer, a double or a duration, not 'List'");
    expectMessage("UNWIND ['a', 1] AS x RETURN +x",
                  "The operand of a unary plus must be an integer, a double or a duration, not 'String'");
}

TEST_F(HeldTypeErrorTest, readsTheIdOfAValueThatIsNoEntity) {
    expectMessage("UNWIND [true, 'a'] AS x RETURN id(x)", "id() reads a node or an edge, not 'Bool'");
}

TEST_F(HeldTypeErrorTest, makesADurationOutOfAValueThatIsNeitherAnIntegerNorAMap) {
    expectMessage("UNWIND [1, 'a'] AS x RETURN duration(x)", "duration() reads an integer or a map, not 'String'");
    expectMessage("UNWIND [1, " + std::string(DATETIME) + "] AS x RETURN duration(x)",
                  "duration() reads an integer or a map, not 'DateTime'");
}

TEST_F(HeldTypeErrorTest, makesADurationOutOfAUnitThatIsNoNumber) {
    expectMessage("UNWIND [{hours: 1}, {hours: 'a'}] AS m RETURN duration(m)",
                  "duration() reads a number for the unit 'hours', not 'String'");
}

TEST_F(HeldTypeErrorTest, setsAPropertyToAValueOfAnotherType) {
    expectWriteMessage("WITH ['Remy', 5] AS pair MATCH (p:Person {name: pair[0]}) SET p.name = pair[1]",
                       "Cannot set a property of type 'String' to a value of type 'Integer'");
    expectWriteMessage("MATCH (q:Person {name: 'Adam'}) WITH ['Remy', q] AS pair "
                       "MATCH (p:Person {name: pair[0]}) SET p.name = pair[1]",
                       "Cannot set a property of type 'String' to a value of type 'NodePattern'");
}
