#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// P1 reaches DNA repair through both TP53 and BRCA1, P5 is both Basal and LumA, and no
// Basal patient reaches Hormone.
class CountDistinctCaseTest : public WriteQueryTest {
protected:
    void initialize() override {
        WriteQueryTest::initialize();

        applyWrite("CREATE (basal:Subtype {name: 'Basal'}), (luma:Subtype {name: 'LumA'}), (her2:Subtype {name: 'Her2'}), "
                   "(p1:Patient {name: 'P1'}), (p2:Patient {name: 'P2'}), (p3:Patient {name: 'P3'}), "
                   "(p4:Patient {name: 'P4'}), (p5:Patient {name: 'P5'}), "
                   "(tp53:Gene {name: 'TP53'}), (brca1:Gene {name: 'BRCA1'}), "
                   "(pik3ca:Gene {name: 'PIK3CA'}), (esr1:Gene {name: 'ESR1'}), "
                   "(w1:Pathway {name: 'DNA repair'}), (w2:Pathway {name: 'PI3K'}), "
                   "(w3:Pathway {name: 'Apoptosis'}), (w4:Pathway {name: 'Hormone'}), "
                   "(p1)-[:HAS_SUBTYPE]->(basal), (p2)-[:HAS_SUBTYPE]->(basal), (p3)-[:HAS_SUBTYPE]->(luma), "
                   "(p4)-[:HAS_SUBTYPE]->(her2), (p5)-[:HAS_SUBTYPE]->(basal), (p5)-[:HAS_SUBTYPE]->(luma), "
                   "(p1)-[:HAS_ALTERATION]->(tp53), (p1)-[:HAS_ALTERATION]->(brca1), "
                   "(p2)-[:HAS_ALTERATION]->(pik3ca), (p3)-[:HAS_ALTERATION]->(tp53), "
                   "(p3)-[:HAS_ALTERATION]->(pik3ca), (p3)-[:HAS_ALTERATION]->(esr1), "
                   "(p4)-[:HAS_ALTERATION]->(brca1), (p5)-[:HAS_ALTERATION]->(tp53), "
                   "(tp53)-[:IN_PATHWAY]->(w1), (tp53)-[:IN_PATHWAY]->(w3), (brca1)-[:IN_PATHWAY]->(w1), "
                   "(pik3ca)-[:IN_PATHWAY]->(w2), (esr1)-[:IN_PATHWAY]->(w4)");
    }
};

TEST_F(CountDistinctCaseTest, countsEachPatientOncePerSide) {
    expectRows("MATCH (s:Subtype)<-[:HAS_SUBTYPE]-(p:Patient)"
               "-[:HAS_ALTERATION]->(:Gene)-[:IN_PATHWAY]->(w:Pathway) "
               "RETURN w.name AS pathway, "
               "count(DISTINCT CASE WHEN s.name = 'Basal' THEN p END) AS basal, "
               "count(DISTINCT CASE WHEN s.name <> 'Basal' THEN p END) AS otherSubtypes "
               "ORDER BY basal DESC",
               {{"DNA repair", "2", "3"}, {"Apoptosis", "2", "2"}, {"PI3K", "1", "1"}, {"Hormone", "0", "1"}});
}

TEST_F(CountDistinctCaseTest, ordersByTheCount) {
    expectRowsInOrder("MATCH (s:Subtype)<-[:HAS_SUBTYPE]-(p:Patient)"
                      "-[:HAS_ALTERATION]->(:Gene)-[:IN_PATHWAY]->(w:Pathway) "
                      "RETURN w.name AS pathway, "
                      "count(DISTINCT CASE WHEN s.name = 'Basal' THEN p END) AS basal, "
                      "count(DISTINCT CASE WHEN s.name <> 'Basal' THEN p END) AS otherSubtypes "
                      "ORDER BY basal DESC, pathway",
                      {{"Apoptosis", "2", "2"}, {"DNA repair", "2", "3"}, {"PI3K", "1", "1"}, {"Hormone", "0", "1"}});
}

TEST_F(CountDistinctCaseTest, countsEveryPathWithoutDistinct) {
    expectRows("MATCH (s:Subtype)<-[:HAS_SUBTYPE]-(p:Patient)"
               "-[:HAS_ALTERATION]->(:Gene)-[:IN_PATHWAY]->(w:Pathway) "
               "RETURN w.name, "
               "count(CASE WHEN s.name = 'Basal' THEN p END), "
               "count(CASE WHEN s.name <> 'Basal' THEN p END)",
               {{"DNA repair", "3", "3"}, {"Apoptosis", "2", "2"}, {"PI3K", "1", "1"}, {"Hormone", "0", "1"}});
}

TEST_F(CountDistinctCaseTest, countsTheSimpleForm) {
    expectRows("MATCH (s:Subtype)<-[:HAS_SUBTYPE]-(p:Patient)"
               "-[:HAS_ALTERATION]->(:Gene)-[:IN_PATHWAY]->(w:Pathway) "
               "RETURN w.name, count(DISTINCT CASE s.name WHEN 'Basal' THEN p ELSE null END)",
               {{"DNA repair", "2"}, {"Apoptosis", "2"}, {"PI3K", "1"}, {"Hormone", "0"}});
}
