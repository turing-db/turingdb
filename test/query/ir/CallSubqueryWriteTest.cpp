#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// A unit subquery - a body ending on an updating clause - writes once per row in flight
// and leaves those rows as they were
class CallSubqueryWriteTest : public WriteQueryTest {
};

TEST_F(CallSubqueryWriteTest, writesOncePerRowAndPassesTheRowsThrough) {
    expectWriteRows("MATCH (p:Person) "
                    "CALL (p) { CREATE (p)-[:VISITED]->(:Place {name: 'Paris'}) } "
                    "RETURN count(p)",
                    {{"8"}});

    expectRows("MATCH (:Person)-[:VISITED]->(place:Place) RETURN count(place)", {{"8"}});
    expectRows("MATCH (p:Person)-[:VISITED]->(place:Place) RETURN p.name, place.name",
               {{"Remy", "Paris"},
                {"Adam", "Paris"},
                {"Maxime", "Paris"},
                {"Luc", "Paris"},
                {"Martina", "Paris"},
                {"Suhas", "Paris"},
                {"Cyrus", "Paris"},
                {"Doruk", "Paris"}});
}

// The body walks 15 edges but the rows in flight are the 8 Persons still
TEST_F(CallSubqueryWriteTest, aFanningOutBodyLeavesTheRowCountAlone) {
    expectWriteRows("MATCH (p:Person) "
                    "CALL (p) { MATCH (p)-[:INTERESTED_IN]->(i) CREATE (i)-[:LIKED_BY]->(p) } "
                    "RETURN count(p)",
                    {{"8"}});

    expectRows("MATCH (:Interest)-[:LIKED_BY]->(:Person) RETURN count(*)", {{"15"}});
}

TEST_F(CallSubqueryWriteTest, setsAPropertyThroughAnImportedVariable) {
    expectWriteRows("MATCH (p:Person) "
                    "CALL (p) { SET p.visited = true } "
                    "RETURN count(p)",
                    {{"8"}});

    expectRows("MATCH (p:Person) WHERE p.visited = true RETURN count(p)", {{"8"}});
}

// A body importing nothing still writes once per row in flight
TEST_F(CallSubqueryWriteTest, aBodyImportingNothingWritesOncePerRow) {
    expectWriteRows("MATCH (p:Person) "
                    "CALL () { CREATE (:Place {name: 'Paris'}) } "
                    "RETURN count(p)",
                    {{"8"}});

    expectRows("MATCH (place:Place) RETURN count(place)", {{"8"}});
}
