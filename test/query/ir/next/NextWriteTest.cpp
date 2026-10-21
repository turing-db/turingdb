#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// NEXT chains whose segments write
class NextWriteTest : public WriteQueryTest {
};

TEST_F(NextWriteTest, writesOverTheRowsTheSegmentBeforeReturned) {
    applyWrite("MATCH (p:Person) WHERE p.isFrench RETURN p NEXT SET p.seen = true");

    expectRows("MATCH (p:Person) WHERE p.seen RETURN p.name",
               {{"Remy"}, {"Adam"}, {"Maxime"}, {"Luc"}});
}

// A segment ending on a write returns nothing, so the one after it starts from one row
TEST_F(NextWriteTest, startsAfreshAfterASegmentThatWrites) {
    expectWriteRows("UNWIND [1, 2, 3] AS x CREATE (:Tag {x: x}) NEXT RETURN 1 AS one", {{"1"}});

    expectRows("MATCH (t:Tag) RETURN t.x", {{"1"}, {"2"}, {"3"}});
}

// Remy, Adam, Luc and Martina have a PhD
TEST_F(NextWriteTest, writesInTheWhenBranchEachRowTakes) {
    applyWrite("MATCH (p:Person) RETURN p "
               "NEXT WHEN p.hasPhD THEN { SET p.title = 'doctor' } ELSE { SET p.title = 'student' }");

    expectRows("MATCH (p:Person) RETURN p.title, count(*)", {{"doctor", "4"}, {"student", "4"}});
}
