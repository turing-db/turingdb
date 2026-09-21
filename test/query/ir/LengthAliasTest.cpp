#include <gtest/gtest.h>

#include <string>
#include <string_view>

#include "QueryStatus.h"

#include "IRTestRows.h"
#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// length() is size() under its other Cypher name, so it answers over the same three
// arguments: a list, a string, and the type-erased cell either rides.
class LengthAliasTest : public WriteQueryTest {
protected:
    void expectRejected(std::string_view query, std::string_view messagePart) {
        RowSink sink;
        const QueryStatus status = runQuery(query, &sink);

        ASSERT_FALSE(status.isOk()) << "query: " << query << " was accepted";
        EXPECT_NE(status.getError().find(messagePart), std::string::npos)
            << "query: " << query << "\nerror: " << status.getError();
    }
};

TEST_F(LengthAliasTest, lengthsAStringLiteral) {
    expectRows("RETURN length('hello')", {{"5"}});
}

TEST_F(LengthAliasTest, lengthsAMultibyteStringByItsCharacters) {
    expectRows("RETURN length('héllo')", {{"5"}});
}

TEST_F(LengthAliasTest, lengthsALiteralList) {
    expectRows("RETURN length([1, 2, 3])", {{"3"}});
}

TEST_F(LengthAliasTest, lengthsAnEmptyList) {
    expectRows("RETURN length([])", {{"0"}});
}

TEST_F(LengthAliasTest, lengthsAStoredStringProperty) {
    expectRows("MATCH (n:Person {name: 'Remy'}) RETURN length(n.name)", {{"4"}});
}

TEST_F(LengthAliasTest, lengthsAStoredList) {
    applyWrite("CREATE (n:Tagged {name: 'a', tags: [1, 2, 3]})");

    expectRows("MATCH (n:Tagged) RETURN length(n.tags)", {{"3"}});
}

TEST_F(LengthAliasTest, lengthsACollectedList) {
    expectRows("MATCH (n:Person) RETURN length(collect(n.name))", {{"8"}});
}

TEST_F(LengthAliasTest, lengthsAStringHeldInATaggedCell) {
    expectRows("RETURN length(head(['abcd', 2]))", {{"4"}});
}

TEST_F(LengthAliasTest, lengthsANullCellAsNull) {
    expectRows("RETURN length(head([]))", {{"null"}});
}

TEST_F(LengthAliasTest, lengthsNullWhereTheNodeHasNoString) {
    applyWrite("CREATE (a:Tagged {name: 'a', text: 'abcd'})");
    applyWrite("CREATE (b:Tagged {name: 'b'})");

    expectRows("MATCH (n:Tagged) RETURN n.name, length(n.text)",
               {{"a", "4"}, {"b", "null"}});
}

TEST_F(LengthAliasTest, answersWhatSizeAnswers) {
    expectRows("MATCH (n:Person) WHERE length(n.name) = size(n.name) RETURN count(*)", {{"8"}});
}

TEST_F(LengthAliasTest, rejectsTheLengthOfANumber) {
    expectRejected("RETURN length(1)", "length");
}

TEST_F(LengthAliasTest, rejectsTheLengthOfACellHoldingNeitherAListNorAString) {
    expectRejected("RETURN length(head([1, 2]))", "size() and length() read a list or a string");
}
