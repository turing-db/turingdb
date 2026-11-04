#include <gtest/gtest.h>

#include <string_view>

#include "QueryStatus.h"

#include "IRTestRows.h"
#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

namespace {

constexpr std::string_view EVERY_PROPERTY_OF_A_LIST = "Reading every property needs a node, a relationship or a map, not 'List'";
constexpr std::string_view EVERY_PROPERTY_OF_A_DATETIME = "Reading every property needs a node, a relationship or a map, not 'DateTime'";
constexpr std::string_view EVERY_PROPERTY_OF_AN_INTEGER = "Reading every property needs a node, a relationship or a map, not 'Integer'";

}

class SharedOpHeldTypeErrorTest : public WriteQueryTest {
protected:
    void expectMessage(std::string_view query, std::string_view message) {
        RowSink sink;
        const QueryStatus status = runQuery(query, &sink);

        ASSERT_FALSE(status.isOk()) << "query: " << query;
        EXPECT_EQ(status.getError(), message) << "query: " << query;
    }
};

TEST_F(SharedOpHeldTypeErrorTest, projectsEveryPropertyOfAList) {
    expectMessage("UNWIND [[1], 1] AS x RETURN x {.*}", EVERY_PROPERTY_OF_A_LIST);
}

TEST_F(SharedOpHeldTypeErrorTest, projectsEveryPropertyOfADateTimeReadOffAMap) {
    expectMessage("WITH {d: datetime('2020-03-04T00:00:00Z')} AS m WITH m.d AS d RETURN d {.*}", EVERY_PROPERTY_OF_A_DATETIME);
}

TEST_F(SharedOpHeldTypeErrorTest, projectsEveryPropertyOfADateTime) {
    expectMessage("UNWIND [{a: 1}, datetime('2020-03-04T00:00:00Z')] AS x RETURN x {.*}", EVERY_PROPERTY_OF_A_DATETIME);
}

TEST_F(SharedOpHeldTypeErrorTest, projectsEveryPropertyOfAnInteger) {
    expectMessage("UNWIND [{a: 1}, 1] AS x RETURN x {.*}", EVERY_PROPERTY_OF_AN_INTEGER);
    expectMessage("UNWIND [{a: 1}, 1] AS x RETURN x {.*, b: 2}", EVERY_PROPERTY_OF_AN_INTEGER);
}

TEST_F(SharedOpHeldTypeErrorTest, readsThePropertiesOfAList) {
    expectMessage("UNWIND [[1], 1] AS x RETURN properties(x)", EVERY_PROPERTY_OF_A_LIST);
}

TEST_F(SharedOpHeldTypeErrorTest, countsTheCharactersOfAValueThatIsNoString) {
    expectMessage("UNWIND ['a', 1] AS x RETURN char_length(x)",
                  "char_length() and character_length() read a string, not 'Integer'");
    expectMessage("UNWIND ['a', [1]] AS x RETURN character_length(x)",
                  "char_length() and character_length() read a string, not 'List'");
}

TEST_F(SharedOpHeldTypeErrorTest, readsTheIdOfAValueThatIsNoEntity) {
    expectMessage("UNWIND [true, 'a'] AS x RETURN id(x)", "id() and elementId() read a node or an edge, not 'Bool'");
    expectMessage("UNWIND [true, 'a'] AS x RETURN elementId(x)", "id() and elementId() read a node or an edge, not 'Bool'");
}
