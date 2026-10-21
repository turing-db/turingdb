#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// A CALL's import read after a UNION segment one of whose branches projects through a WITH.
// Remy and Adam have an age; the table holds the 8 Persons.
class NextImportPastBranchWithTest : public WriteQueryTest {
};

TEST_F(NextImportPastBranchWithTest, readsTheImportAfterABranchWithAWith) {
    expectRows("MATCH (p:Person) WHERE p.name = 'Remy' "
               "CALL (p) { MATCH (q:Person) RETURN q AS q "
               "NEXT WITH q WHERE q.age > 1 RETURN q.name AS n UNION ALL RETURN 'x' AS n "
               "NEXT RETURN p.name AS pn, n } "
               "RETURN pn, n",
               {{"Remy", "Remy"}, {"Remy", "Adam"},
                {"Remy", "x"}, {"Remy", "x"}, {"Remy", "x"}, {"Remy", "x"},
                {"Remy", "x"}, {"Remy", "x"}, {"Remy", "x"}, {"Remy", "x"}});
}
