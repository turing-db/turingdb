#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// A dotted chain reads as its parenthesized spelling, `m.a.b.c` as `((m.a).b).c`, which is
// how Neo4j 5.26 parses it. The answers are Neo4j's.
class DottedPropertyChainQueryTest : public WriteQueryTest {
};

TEST_F(DottedPropertyChainQueryTest, readsAKeyAtEveryDepth) {
    expectRows("WITH {d: {e: 1}} AS m RETURN m.d.e", {{"1"}});
    expectRows("WITH {d: {e: {f: 2}}} AS m RETURN m.d.e.f", {{"2"}});
}

TEST_F(DottedPropertyChainQueryTest, readsNullThroughANullValue) {
    expectRows("WITH {d: null} AS m RETURN m.d.e", {{"null"}});
    expectRows("WITH {a: 1} AS m RETURN m.nosuch.x", {{"null"}});
}

TEST_F(DottedPropertyChainQueryTest, readsNullThroughAPropertyNoNodeCarries) {
    const Rows nullPerPerson(8, {"null"});

    expectRows("MATCH (n:Person) RETURN n.nosuch.x", nullPerPerson);
    expectRows("MATCH (n:Person) RETURN (n.nosuch).x", nullPerPerson);
}

TEST_F(DottedPropertyChainQueryTest, readsAComponentOfADatetimeAMapHolds) {
    expectRows("WITH {d: datetime('2020-03-04T00:00:00Z')} AS m RETURN m.d.year", {{"2020"}});
}

TEST_F(DottedPropertyChainQueryTest, raisesOnAKeyOfAScalarAMapHolds) {
    expectError("WITH {a: 1} AS m RETURN m.a.x", "Type mismatch: property 'x' is read off a map");
    expectError("WITH {s: 'abc'} AS m RETURN m.s.x", "Type mismatch: property 'x' is read off a map");
}

TEST_F(DottedPropertyChainQueryTest, rejectsAKeyOfAComponent) {
    expectError("WITH datetime('2020-01-01T00:00:00Z') AS d RETURN d.year.x",
                "A value of type 'Integer' has no property 'x'");
}

TEST_F(DottedPropertyChainQueryTest, rejectsAKeyOfAStringProperty) {
    expectError("MATCH (n:Person) RETURN n.name.year", "A value of type 'String' has no property 'year'");
    expectError("MATCH (n:Person) RETURN (n.name).year", "A value of type 'String' has no property 'year'");
}

TEST_F(DottedPropertyChainQueryTest, ordersByAKeyOfAGroupingKey) {
    expectRowsInOrder("UNWIND [{d: {e: 2}}, {d: {e: 1}}] AS m RETURN m.d AS d, count(*) AS c ORDER BY m.d.e",
                      {{"{e: 1}", "1"}, {"{e: 2}", "1"}});
}

TEST_F(DottedPropertyChainQueryTest, ordersByTheGroupingKeyInEitherSpelling) {
    expectRowsInOrder("UNWIND [{d: {e: 2}}, {d: {e: 1}}] AS m RETURN m.d.e AS e, count(*) AS c ORDER BY m.d.e",
                      {{"1", "1"}, {"2", "1"}});
    expectRowsInOrder("UNWIND [{d: {e: 2}}, {d: {e: 1}}] AS m RETURN (m.d).e AS e, count(*) AS c ORDER BY m.d.e",
                      {{"1", "1"}, {"2", "1"}});
    expectRowsInOrder("UNWIND [{d: {e: 2}}, {d: {e: 1}}] AS m RETURN m.d.e AS e, count(*) AS c ORDER BY (m.d).e",
                      {{"1", "1"}, {"2", "1"}});
}
