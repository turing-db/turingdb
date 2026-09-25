#include <gtest/gtest.h>

#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

// An element of a list holding more than one type carries its type per row, so a SET
// writing it to a typed property checks the type on the row it writes.
class SetListElementPropertyTest : public CallV3Test {
};

TEST_F(SetListElementPropertyTest, setsANodePropertyToAnElementOfAMixedList) {
    StringRowSink written;
    runWrite("WITH ['Remy', 5] AS pair MATCH (p:Person {name: pair[0]}) SET p.age = pair[1] RETURN p.age", written);

    const std::vector<StringRowSink::Row> expectedWritten {{"5"}};
    EXPECT_EQ(written.getRows(), expectedWritten);

    StringRowSink read;
    runQuery("MATCH (p:Person {name: 'Remy'}) RETURN p.age", read);

    const std::vector<StringRowSink::Row> expectedRead {{"5"}};
    EXPECT_EQ(read.getRows(), expectedRead);
}

TEST_F(SetListElementPropertyTest, setsAnEdgePropertyToAnElementOfAMixedList) {
    runWrite("WITH ['Remy', 3] AS pair "
             "MATCH (a:Person {name: pair[0]})-[e:KNOWS_WELL]->(b:Person {name: 'Adam'}) "
             "SET e.duration = pair[1]");

    StringRowSink read;
    runQuery("MATCH (a:Person {name: 'Remy'})-[e:KNOWS_WELL]->(b:Person {name: 'Adam'}) RETURN e.duration", read);

    const std::vector<StringRowSink::Row> expectedRead {{"3"}};
    EXPECT_EQ(read.getRows(), expectedRead);
}

TEST_F(SetListElementPropertyTest, setsAStringPropertyToAnElementOfAMixedList) {
    runWrite("WITH [1, 'Remi'] AS pair MATCH (p:Person {name: 'Remy'}) SET p.name = pair[1]");

    StringRowSink read;
    runQuery("MATCH (p:Person {name: 'Remi'}) RETURN p.age", read);

    const std::vector<StringRowSink::Row> expectedRead {{"32"}};
    EXPECT_EQ(read.getRows(), expectedRead);
}

TEST_F(SetListElementPropertyTest, setsAPropertyToANullElementOfAMixedList) {
    runWrite("WITH ['Remy', 5, null] AS triple MATCH (p:Person {name: triple[0]}) SET p.age = triple[2]");

    StringRowSink read;
    runQuery("MATCH (p:Person {name: 'Remy'}) RETURN p.age", read);

    const std::vector<StringRowSink::Row> expectedRead {{"null"}};
    EXPECT_EQ(read.getRows(), expectedRead);
}

TEST_F(SetListElementPropertyTest, setsAPropertyToAnElementPastTheEndOfAMixedList) {
    runWrite("WITH ['Remy', 5] AS pair MATCH (p:Person {name: pair[0]}) SET p.age = pair[2]");

    StringRowSink read;
    runQuery("MATCH (p:Person {name: 'Remy'}) RETURN p.age", read);

    const std::vector<StringRowSink::Row> expectedRead {{"null"}};
    EXPECT_EQ(read.getRows(), expectedRead);
}

TEST_F(SetListElementPropertyTest, setsAPropertyPerRowFromUnwoundMixedLists) {
    runWrite("UNWIND [['Remy', 7], ['Adam', 8]] AS pair "
             "MATCH (p:Person {name: pair[0]}) "
             "SET p.age = pair[1]");

    StringRowSink read;
    runQuery("MATCH (p:Person) WHERE p.age IS NOT NULL RETURN p.name, p.age", read);

    const std::vector<StringRowSink::Row> expectedRead {{"Remy", "7"}, {"Adam", "8"}};
    EXPECT_EQ(read.getRows(), expectedRead);
}

TEST_F(SetListElementPropertyTest, widensAnIntegerElementWrittenToADoubleProperty) {
    runWrite("MATCH (p:Person {name: 'Remy'}) SET p.height = 1.5");
    runWrite("WITH ['Remy', 2] AS pair MATCH (p:Person {name: pair[0]}) SET p.height = pair[1]");

    StringRowSink read;
    runQuery("MATCH (p:Person {name: 'Remy'}) RETURN p.height + 0.25", read);

    const std::vector<StringRowSink::Row> expectedRead {{"2.25"}};
    EXPECT_EQ(read.getRows(), expectedRead);
}

TEST_F(SetListElementPropertyTest, setsAListPropertyToAListElementOfAMixedList) {
    runWrite("MATCH (p:Person {name: 'Remy'}) SET p.tags = [0]");
    runWrite("WITH ['Remy', [1, 'x']] AS pair MATCH (p:Person {name: pair[0]}) SET p.tags = pair[1]");

    StringRowSink read;
    runQuery("MATCH (p:Person {name: 'Remy'}) RETURN size(p.tags), p.tags[1]", read);

    const std::vector<StringRowSink::Row> expectedRead {{"2", "x"}};
    EXPECT_EQ(read.getRows(), expectedRead);
}

TEST_F(SetListElementPropertyTest, rejectsAnElementWhoseTypeThePropertyDoesNotHold) {
    runWriteExpectingError("WITH ['Remy', 5] AS pair MATCH (p:Person {name: pair[0]}) SET p.age = pair[0]",
                           "Cannot set a property of type 'Int64' to a list element of another type");
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
