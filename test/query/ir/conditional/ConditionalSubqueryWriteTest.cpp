#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// Branches of a conditional CALL that write: each row in flight writes what the branch it
// takes writes, and nothing for a branch it does not take
class ConditionalSubqueryWriteTest : public WriteQueryTest {
};

TEST_F(ConditionalSubqueryWriteTest, setsAPropertyPerBranch) {
    expectWriteRows("MATCH (p:Person) "
                    "CALL (p) { WHEN p.isFrench THEN { SET p.group = 'fr' } ELSE { SET p.group = 'other' } } "
                    "RETURN count(p)",
                    {{"8"}});

    expectRows("MATCH (p:Person) RETURN p.group, count(*)", {{"fr", "4"}, {"other", "4"}});
}

// Remy, Adam, Luc and Martina have a PhD. A unit body passes all 8 rows through.
TEST_F(ConditionalSubqueryWriteTest, writesOnlyInTheBranchTaken) {
    expectWriteRows("MATCH (p:Person) "
                    "CALL (p) { WHEN p.hasPhD THEN { CREATE (p)-[:HOLDS]->(:Degree) } } "
                    "RETURN count(p)",
                    {{"8"}});

    expectRows("MATCH (p:Person)-[:HOLDS]->(:Degree) RETURN p.name",
               {{"Remy"}, {"Adam"}, {"Luc"}, {"Martina"}});
}

TEST_F(ConditionalSubqueryWriteTest, returnsFromABranchThatWrites) {
    expectWriteRows("MATCH (p:Person) "
                    "CALL (p) { WHEN p.isFrench THEN { CREATE (:Badge) RETURN p.name AS badge } } "
                    "RETURN badge",
                    {{"Remy"}, {"Adam"}, {"Maxime"}, {"Luc"}});

    expectRows("MATCH (b:Badge) RETURN count(b)", {{"4"}});
}

TEST_F(ConditionalSubqueryWriteTest, writesInAStandaloneConditional) {
    applyWrite("WHEN true THEN CREATE (:Marker)");
    applyWrite("WHEN false THEN { CREATE (:Marker) } ELSE { CREATE (:Other) }");

    expectRows("MATCH (m:Marker) RETURN count(m)", {{"1"}});
    expectRows("MATCH (o:Other) RETURN count(o)", {{"1"}});
}

TEST_F(ConditionalSubqueryWriteTest, returnsFromAStandaloneConditionalThatWrites) {
    expectWriteRows("WHEN true THEN { CREATE (m:Marker {name: 'x'}) RETURN m.name AS name }",
                    {{"x"}});

    expectRows("MATCH (m:Marker) RETURN m.name", {{"x"}});
}
