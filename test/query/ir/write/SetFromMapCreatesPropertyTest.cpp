#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// A map entry is a value like m.v in SET n.k = m.v, so a map write creates the property a
// key names, typed from the values of the first chunk that hold one, as a write of cells is.
// Neo4j 5.26 writes every form below.
class SetFromMapCreatesPropertyTest : public WriteQueryTest {
};

TEST_F(SetFromMapCreatesPropertyTest, createsAPropertyFromAMapReadPerRow) {
    applyWrite("UNWIND [{fresh: 5}] AS m MATCH (n:Person {name: 'Remy'}) SET n += m");
    expectRows("MATCH (n:Person {name: 'Remy'}) RETURN n.fresh", {{"5"}});
}

TEST_F(SetFromMapCreatesPropertyTest, createsAPropertyWhenAMapReadPerRowReplacesTheProperties) {
    applyWrite("UNWIND [{name: 'Remy', fresh: 5}] AS m MATCH (n:Person {name: 'Remy'}) SET n = m");
    expectRows("MATCH (n:Person {name: 'Remy'}) RETURN n.fresh, size(keys(n))", {{"5", "2"}});
}

TEST_F(SetFromMapCreatesPropertyTest, createsAPropertyFromAMapBoundToAVariable) {
    applyWrite("WITH {name: 'Remy', fresh: 5} AS m MATCH (n:Person {name: 'Remy'}) SET n = m");
    expectRows("MATCH (n:Person {name: 'Remy'}) RETURN n.fresh", {{"5"}});
}

TEST_F(SetFromMapCreatesPropertyTest, createsAPropertyFromAMapLiteralEntryTypedPerRow) {
    applyWrite("UNWIND [{v: 5}] AS m MATCH (n:Person {name: 'Remy'}) SET n += {fresh: m.v}");
    applyWrite("UNWIND [{v: 'x'}] AS m MATCH (n:Person {name: 'Remy'}) SET n = {name: 'Remy', other: m.v}");
    expectRows("MATCH (n:Person {name: 'Remy'}) RETURN n.fresh, n.other", {{"null", "x"}});
}

TEST_F(SetFromMapCreatesPropertyTest, widensAnIntegerBesideADouble) {
    applyWrite("UNWIND [{name: 'Remy', fresh: 1}, {name: 'Adam', fresh: 2.5}] AS m "
               "MATCH (n:Person {name: m.name}) SET n += m");
    expectRows("MATCH (n:Person) WHERE n.fresh IS NOT NULL RETURN n.name, n.fresh",
               {{"Remy", "1.000000"}, {"Adam", "2.500000"}});
}

TEST_F(SetFromMapCreatesPropertyTest, typesThePropertyFromTheRowsThatHoldAValue) {
    applyWrite("UNWIND [{name: 'Remy', fresh: null}, {name: 'Adam', fresh: 3}] AS m "
               "MATCH (n:Person {name: m.name}) SET n += m");
    expectRows("MATCH (n:Person) WHERE n.name IN ['Remy', 'Adam'] RETURN n.name, n.fresh",
               {{"Remy", "null"}, {"Adam", "3"}});
}

TEST_F(SetFromMapCreatesPropertyTest, rejectsTwoTypesForOneNewProperty) {
    expectWriteError("UNWIND [{name: 'Remy', fresh: 1}, {name: 'Adam', fresh: 'x'}] AS m "
                     "MATCH (n:Person {name: m.name}) SET n += m",
                     "One write gives the new property 'fresh' two types");
}
