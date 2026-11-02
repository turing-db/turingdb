#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

class NestedMapKeyTest : public WriteQueryTest {
};

TEST_F(NestedMapKeyTest, readsAKeyOfAMapAMapVariableHolds) {
    expectRows("WITH {d: {e: 1}} AS m RETURN m.d.e", {{"1"}});
}

TEST_F(NestedMapKeyTest, readsAComponentOfADatetimeAMapVariableHolds) {
    expectRows("WITH {d: datetime('2020-03-04T00:00:00Z')} AS m RETURN m.d.year, m.d.month", {{"2020", "3"}});
}

TEST_F(NestedMapKeyTest, readsAComponentOfADurationAMapVariableHolds) {
    expectRows("WITH {d: duration({hours: 25, minutes: 1})} AS m RETURN m.d.hours, m.d.minutesOfHour", {{"25", "1"}});
}

TEST_F(NestedMapKeyTest, readsKeysAtEveryDepth) {
    expectRows("WITH {a: {b: {c: 3}}} AS m RETURN m.a.b.c", {{"3"}});
    expectRows("WITH {a: {b: {c: {d: 4}}}} AS m RETURN m.a.b.c.d, m.a.b.c", {{"4", "{d: 4}"}});
}

TEST_F(NestedMapKeyTest, readsAComponentDeepInsideAMap) {
    expectRows("WITH {a: {b: {at: datetime('2020-03-04T00:00:00Z')}}} AS m RETURN m.a.b.at.day", {{"4"}});
}

TEST_F(NestedMapKeyTest, readsNullWhereAKeyOnTheWayIsMissing) {
    expectRows("WITH {a: {b: 1}} AS m RETURN m.x.b, m.a.x, m.x.y.z", {{"null", "null", "null"}});
}

TEST_F(NestedMapKeyTest, readsNullThroughANullValue) {
    expectRows("WITH {a: null} AS m RETURN m.a.b, m.a.b.c", {{"null", "null"}});
}

TEST_F(NestedMapKeyTest, readsAPropertyOfANodeANestedMapHolds) {
    expectRows("MATCH (p:Person {name: 'Remy'}) WITH {who: p} AS m RETURN m.who.name", {{"Remy"}});
    expectRows("MATCH (p:Person {name: 'Remy'}) WITH {d: {who: p}} AS m RETURN m.d.who.name", {{"Remy"}});
}

TEST_F(NestedMapKeyTest, readsAKeyOfAMapBuiltPerRow) {
    expectRows("MATCH (p:Person) WHERE p.name IN ['Remy', 'Adam'] WITH {d: {name: p.name}} AS m RETURN m.d.name",
               {{"Adam"}, {"Remy"}});
}

TEST_F(NestedMapKeyTest, readsAKeyOfAMapCarriedThroughWith) {
    expectRows("WITH {d: {e: 1}} AS m WITH m WHERE m.d.e = 1 RETURN m.d.e", {{"1"}});
    expectRows("WITH {d: {e: 1}} AS m WITH m.d AS d RETURN d.e", {{"1"}});
}

TEST_F(NestedMapKeyTest, readsAKeyOfAnUnwoundMap) {
    expectRows("UNWIND [{d: {e: 1}}, {d: {e: 2}}, {d: null}] AS m RETURN m.d.e", {{"1"}, {"2"}, {"null"}});
}

TEST_F(NestedMapKeyTest, readsAKeyOfAMapProjection) {
    expectRows("MATCH (p:Person {name: 'Remy'}) WITH p {.name, d: {age: p.age}} AS m RETURN m.d.age", {{"32"}});
    expectRows("MATCH (p:Person {name: 'Remy'}) RETURN p {d: {age: p.age}}.d.age", {{"32"}});
}

TEST_F(NestedMapKeyTest, readsANestedKeyOfAStoredMapProperty) {
    applyWrite("CREATE (:Tagged {name: 'a', attrs: {d: {e: 1, f: {g: 'deep'}}}})");

    expectRows("MATCH (t:Tagged) RETURN t.attrs.d.e, t.attrs.d.f.g, t.attrs.d.x, t.attrs.x.y",
               {{"1", "deep", "null", "null"}});
    expectRows("MATCH (t:Tagged) WITH t.attrs AS m RETURN m.d.e, m.d.f.g", {{"1", "deep"}});
}

TEST_F(NestedMapKeyTest, ordersByANestedKeyOfAGroupingKey) {
    expectRowsInOrder("UNWIND [{d: {e: 2}}, {d: {e: 1}}, {d: {e: 2}}] AS m "
                      "RETURN m.d AS d, count(*) AS c ORDER BY m.d.e",
                      {{"{e: 1}", "1"}, {"{e: 2}", "2"}});
    expectRowsInOrder("UNWIND [{p: {e: 2}}, {p: {e: 1}}] AS m WITH {d: m} AS m "
                      "RETURN m.d AS d, count(*) AS c ORDER BY m.d.p.e",
                      {{"{p: {e: 1}}", "1"}, {"{p: {e: 2}}", "1"}});
}

TEST_F(NestedMapKeyTest, ordersByANestedKeyOfADistinctKey) {
    expectRowsInOrder("UNWIND [{d: {e: 2}}, {d: {e: 1}}, {d: {e: 2}}] AS m RETURN DISTINCT m.d AS d ORDER BY m.d.e",
                      {{"{e: 1}"}, {"{e: 2}"}});
}

TEST_F(NestedMapKeyTest, raisesOnAKeyOfAValueThatHoldsNone) {
    expectError("WITH {d: 1} AS m RETURN m.d.e", "Type mismatch: property 'e' is read off a map");
    expectError("WITH {d: [1]} AS m RETURN m.d.e", "Type mismatch: property 'e' is read off a map");
    expectError("WITH {a: {b: 'x'}} AS m RETURN m.a.b.c", "Type mismatch: property 'c' is read off a map");
    expectError("WITH {d: datetime('2020-03-04T00:00:00Z')} AS m RETURN m.d.year.x",
                "Type mismatch: property 'x' is read off a map");
}

TEST_F(NestedMapKeyTest, raisesOnANameNoComponentOfANestedDatetimeHas) {
    expectError("WITH {d: datetime('2020-03-04T00:00:00Z')} AS m RETURN m.d.nosuch",
                "'nosuch' is not a component of a datetime");
}

TEST_F(NestedMapKeyTest, rejectsAPropertyChainThroughAStringProperty) {
    expectError("MATCH (n:Person) RETURN n.name.year.x",
                "Property 'name' is 'String', only a datetime or a map has components");
}

TEST_F(NestedMapKeyTest, rejectsWritingANestedKey) {
    expectError("WITH {a: {b: 1}} AS m SET m.a.b = 2", "A map key is a value inside a property");
    expectError("WITH {a: {b: 1}} AS m REMOVE m.a.b", "A map key is a value inside a property");
}
