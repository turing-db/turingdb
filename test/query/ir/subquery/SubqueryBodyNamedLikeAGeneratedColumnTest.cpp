#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// An unaliased item gets a generated name, v0 for the first. A body declaring a variable of
// that name declares a variable of its own. Out-edges of each person: Remy 4, Adam 3,
// Maxime 2, Luc 2, Martina 1, Suhas 2, Cyrus 2, Doruk 1.
class SubqueryBodyNamedLikeAGeneratedColumnTest : public WriteQueryTest {
};

TEST_F(SubqueryBodyNamedLikeAGeneratedColumnTest, ordersOnACountOverItsOwnVariable) {
    expectRowsInOrder("MATCH (p:Person) RETURN p.name "
                      "ORDER BY COUNT { MATCH (v0:Person)-->() WHERE v0.name = p.name } DESC, p.name",
                      {{"Remy"}, {"Adam"}, {"Cyrus"}, {"Luc"},
                       {"Maxime"}, {"Suhas"}, {"Doruk"}, {"Martina"}});
}

TEST_F(SubqueryBodyNamedLikeAGeneratedColumnTest, ordersOnAnExistsOverItsOwnVariable) {
    expectRowsInOrder("MATCH (p:Person) RETURN p.name "
                      "ORDER BY EXISTS { MATCH (v0:Person)-[:KNOWS_WELL]->() WHERE v0.name = p.name } DESC, p.name",
                      {{"Adam"}, {"Remy"}, {"Cyrus"}, {"Doruk"},
                       {"Luc"}, {"Martina"}, {"Maxime"}, {"Suhas"}});
}
