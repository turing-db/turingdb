#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace turing::test;

// A label test and a null test compute their truth value, a boolean property reads a
// stored one, and the two meet in one expression. Of the 8 people, Remy and Adam are
// founders with an age; isFrench is true for Remy, Adam, Maxime and Luc, and hasPhD for
// Remy, Adam, Luc and Martina.
class MaskMeetsBoolPropertyTest : public WriteQueryTest {
};

TEST_F(MaskMeetsBoolPropertyTest, takesACaseBranchOfEither) {
    expectRows("MATCH (p:Person) "
               "RETURN p.name, CASE WHEN p:Founder THEN p.hasPhD ELSE p:Bioinformatics END",
               {
                   {"Remy", "true"},
                   {"Adam", "true"},
                   {"Maxime", "true"},
                   {"Luc", "false"},
                   {"Martina", "true"},
                   {"Suhas", "false"},
                   {"Cyrus", "false"},
                   {"Doruk", "false"},
               });
}

TEST_F(MaskMeetsBoolPropertyTest, coalescesAPropertyWithALabelTest) {
    expectRows("MATCH (p:Person) OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f) "
               "RETURN p.name, coalesce(p.age > 40, p:SoftwareEngineering), coalesce(f.isFrench, p:SoftwareEngineering)",
               {
                   {"Remy", "false", "true"},
                   {"Adam", "false", "true"},
                   {"Maxime", "false", "false"},
                   {"Luc", "true", "true"},
                   {"Martina", "false", "false"},
                   {"Suhas", "true", "true"},
                   {"Cyrus", "true", "true"},
                   {"Doruk", "false", "false"},
               });
}

TEST_F(MaskMeetsBoolPropertyTest, returnsALabelTestOfEveryNode) {
    Rows expected(8, {"true"});
    expected.insert(expected.end(), 10, {"false"});

    expectRows("MATCH (n) RETURN n:Person", expected);
}

TEST_F(MaskMeetsBoolPropertyTest, searchesAListForALabelTest) {
    expectRows("MATCH (p:Person) "
               "RETURN p.name, p:Founder IN [true], p:Founder IN [false, null], p.isFrench IN [p:Founder]",
               {
                   {"Remy", "true", "null", "true"},
                   {"Adam", "true", "null", "true"},
                   {"Maxime", "false", "true", "false"},
                   {"Luc", "false", "true", "false"},
                   {"Martina", "false", "true", "true"},
                   {"Suhas", "false", "true", "true"},
                   {"Cyrus", "false", "true", "true"},
                   {"Doruk", "false", "true", "true"},
               });
}

TEST_F(MaskMeetsBoolPropertyTest, listsBoth) {
    expectRows("MATCH (p:Person) RETURN p.name, [p:Founder, p.hasPhD, p.age IS NULL]",
               {
                   {"Remy", "[true, true, false]"},
                   {"Adam", "[true, true, false]"},
                   {"Maxime", "[false, false, true]"},
                   {"Luc", "[false, true, true]"},
                   {"Martina", "[false, true, true]"},
                   {"Suhas", "[false, false, true]"},
                   {"Cyrus", "[false, false, true]"},
                   {"Doruk", "[false, false, true]"},
               });
}

TEST_F(MaskMeetsBoolPropertyTest, keepsTheDistinctPairs) {
    expectRows("MATCH (p:Person) RETURN DISTINCT p:Founder, p.isFrench",
               {
                   {"true", "true"},
                   {"false", "true"},
                   {"false", "false"},
               });
}

TEST_F(MaskMeetsBoolPropertyTest, ordersOnBoth) {
    expectRowsInOrder("MATCH (p:Person) RETURN p.name ORDER BY p:Founder DESC, p.hasPhD DESC, p.name",
                      {
                          {"Adam"},
                          {"Remy"},
                          {"Luc"},
                          {"Martina"},
                          {"Cyrus"},
                          {"Doruk"},
                          {"Maxime"},
                          {"Suhas"},
                      });
}

TEST_F(MaskMeetsBoolPropertyTest, groupsOnBoth) {
    expectRows("MATCH (p:Person) RETURN p:Founder, p.isFrench, count(*), count(DISTINCT p.age IS NULL)",
               {
                   {"true", "true", "2", "1"},
                   {"false", "true", "2", "1"},
                   {"false", "false", "4", "1"},
               });
}

TEST_F(MaskMeetsBoolPropertyTest, comparesOneWithTheOther) {
    expectRows("MATCH (p:Person) "
               "RETURN p.name, p:Founder = p.hasPhD, p:SoftwareEngineering <> p.isFrench, p.hasPhD = p:Founder",
               {
                   {"Remy", "true", "false", "true"},
                   {"Adam", "true", "true", "true"},
                   {"Maxime", "true", "true", "true"},
                   {"Luc", "false", "false", "false"},
                   {"Martina", "false", "false", "false"},
                   {"Suhas", "true", "true", "true"},
                   {"Cyrus", "true", "true", "true"},
                   {"Doruk", "true", "false", "true"},
               });
}

TEST_F(MaskMeetsBoolPropertyTest, carriesANullTestAsAValue) {
    expectRows("MATCH (p:Person) WITH p, p.age IS NULL AS noAge "
               "WHERE noAge = p.hasPhD OR p:Founder "
               "RETURN p.name, noAge, noAge AND p.hasPhD, NOT noAge",
               {
                   {"Remy", "false", "false", "true"},
                   {"Adam", "false", "false", "true"},
                   {"Luc", "true", "true", "false"},
                   {"Martina", "true", "true", "false"},
               });
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
