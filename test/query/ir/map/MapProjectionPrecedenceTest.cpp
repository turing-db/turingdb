#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// An entry a map projection writes wins over the property `.*` reads under the same key,
// wherever the `.*` stands. Remy's stored name is Remy.
class MapProjectionPrecedenceTest : public WriteQueryTest {
};

TEST_F(MapProjectionPrecedenceTest, keepsAnEntryWrittenBeforeEveryProperty) {
    expectRows("MATCH (n:Person {name: 'Remy'}) WITH n {name: 1, .*} AS m RETURN m.name, m.age", {{"1", "32"}});
}

TEST_F(MapProjectionPrecedenceTest, keepsAnEntryWrittenAfterEveryProperty) {
    expectRows("MATCH (n:Person {name: 'Remy'}) WITH n {.*, name: 1} AS m RETURN m.name, m.age", {{"1", "32"}});
}

TEST_F(MapProjectionPrecedenceTest, keepsAnEntryWrittenBetweenTwoReadsOfEveryProperty) {
    expectRows("MATCH (n:Person {name: 'Remy'}) WITH n {.*, name: 1, .*} AS m RETURN m.name, m.age", {{"1", "32"}});
}
