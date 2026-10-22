#include <gtest/gtest.h>

#include "IRTestRows.h"
#include "WriteQueryTest.h"

using namespace turing::test;

class CaseOrderingAcrossTypesTest : public WriteQueryTest {
};

TEST_F(CaseOrderingAcrossTypesTest, skipsAnOrderingOfANode) {
    expectRows("MATCH (n:Person {name: 'Remy'}) RETURN CASE n WHEN < 5 THEN 'a' ELSE 'b' END", {{"b"}});
}

TEST_F(CaseOrderingAcrossTypesTest, skipsAnOrderingAgainstANode) {
    expectRows("MATCH (n:Person {name: 'Remy'}) RETURN CASE 5 WHEN < n THEN 'a' ELSE 'b' END", {{"b"}});
}

TEST_F(CaseOrderingAcrossTypesTest, skipsAnOrderingOfAScalarOfAnotherType) {
    expectRows("RETURN CASE 5 WHEN > 'a' THEN 'a' ELSE 'b' END", {{"b"}});
}

TEST_F(CaseOrderingAcrossTypesTest, ordersACellAgainstAMapValue) {
    expectRows("UNWIND [1, 'a'] AS x WITH {k: 2} AS m, x RETURN x, CASE x WHEN < m.k THEN 'y' ELSE 'n' END",
               {{"1", "y"}, {"a", "n"}});
}

TEST_F(CaseOrderingAcrossTypesTest, ordersAMapValueSubject) {
    expectRows("UNWIND [{k: 1}, {k: 'a'}] AS m RETURN m.k, CASE m.k WHEN < 5 THEN 'y' WHEN < 'b' THEN 'z' ELSE 'n' END",
               {{"1", "y"}, {"a", "z"}});
}

TEST_F(CaseOrderingAcrossTypesTest, comparesAMapValueSubject) {
    expectRows("UNWIND [{k: 1}, {k: 'a'}] AS m RETURN m.k, CASE m.k WHEN 1 THEN 'y' ELSE 'n' END",
               {{"1", "y"}, {"a", "n"}});
}
