#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// CALL (*) { ... }: the body imports every variable in scope
class CallSubqueryImportEverythingTest : public WriteQueryTest {
};

TEST_F(CallSubqueryImportEverythingTest, readsTheRowsVariablesPerInputRow) {
    expectRows("MATCH (p:Person) "
               "CALL (*) { MATCH (p)-[:INTERESTED_IN]->(i) RETURN count(i) AS interests } "
               "RETURN p.name, interests",
               {{"Remy", "3"},
                {"Adam", "2"},
                {"Maxime", "2"},
                {"Luc", "2"},
                {"Martina", "1"},
                {"Suhas", "2"},
                {"Cyrus", "2"},
                {"Doruk", "1"}});
}

TEST_F(CallSubqueryImportEverythingTest, importsWhatAWithProjected) {
    expectRows("MATCH (p:Person {name: 'Remy'}) WITH p, 3 AS k "
               "CALL (*) { RETURN p.name AS name, k + 1 AS next } "
               "RETURN name, next",
               {{"Remy", "4"}});
}

TEST_F(CallSubqueryImportEverythingTest, importsNothingAWithDropped) {
    expectError("MATCH (p:Person)-[:INTERESTED_IN]->(i) WITH p "
                "CALL (*) { RETURN i AS interest } "
                "RETURN interest",
                "'i'");
}

TEST_F(CallSubqueryImportEverythingTest, writesOncePerRow) {
    expectWriteRows("MATCH (p:Person) "
                    "CALL (*) { CREATE (p)-[:VISITED]->(:Place) } "
                    "RETURN count(p)",
                    {{"8"}});

    expectRows("MATCH (:Person)-[:VISITED]->(place:Place) RETURN count(place)", {{"8"}});
}

TEST_F(CallSubqueryImportEverythingTest, padsTheRowsAnOptionalBodyYieldsNothingFor) {
    expectRows("MATCH (p:Person) "
               "OPTIONAL CALL (*) { MATCH (p)-[:KNOWS_WELL]->(k) RETURN k.name AS known } "
               "RETURN p.name, known",
               {{"Remy", "Adam"},
                {"Adam", "Remy"},
                {"Maxime", "null"},
                {"Luc", "null"},
                {"Martina", "null"},
                {"Suhas", "null"},
                {"Cyrus", "null"},
                {"Doruk", "null"}});
}
