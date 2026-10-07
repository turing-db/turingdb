#include <gtest/gtest.h>

#include <string_view>

#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/OwningOpRef.h"

#include "DBOps.h"

#include "HashJoinQueryTest.h"
#include "IRTestOps.h"
#include "ProductPlacement.h"

using namespace turing::test;

// TUR-173. A WHERE over a three-component comma pattern cuts a nested product from above,
// so a cut relating two components in one factor ran over every row of the whole product:
// MATCH (a:Item), (b:Item), (c:Item) WHERE b.id = c.id took 13 s on 1000 items, where
// WHERE a.id = c.id took 14 ms.
class NestedProductFilterTest : public HashJoinQueryTest {
protected:
    void expectJoinedCount(std::string_view query, size_t joins, size_t expected) {
        mlir::OwningOpRef<mlir::ModuleOp> module;
        dbModule(query, module);

        EXPECT_EQ(countFiltersOverOneFactor(module.get()), 0u) << "query: " << query;
        EXPECT_EQ(countOps<mlir::db::HashJoin>(module.get()), joins) << "query: " << query;

        expectCount(query, expected);
    }
};

// SimpleGraph has 8 people and 10 interests
TEST_F(NestedProductFilterTest, joinsTwoComponentsOfOneFactor) {
    expectJoinedCount("MATCH (a:Person), (b:Interest), (c:Interest) WHERE b.name = c.name RETURN count(*)", 1, 80);
}

TEST_F(NestedProductFilterTest, joinsTheOuterComponentWithAnInnerOne) {
    expectJoinedCount("MATCH (a:Person), (b:Interest), (c:Person) WHERE a.name = c.name RETURN count(*)", 1, 80);
}

TEST_F(NestedProductFilterTest, joinsAnEarlierPartWithOneComponent) {
    expectJoinedCount("MATCH (x:Person {name: 'Remy'}) WITH x "
                      "MATCH (a:Person), (b:Interest) WHERE a.name = x.name RETURN count(*)",
                      1,
                      10);
}

TEST_F(NestedProductFilterTest, joinsTwoPairsOfComponents) {
    expectJoinedCount("MATCH (a:Person), (b:Interest), (c:Interest), (d:Person) "
                      "WHERE b.name = c.name AND a.name = d.name RETURN count(*)",
                      2,
                      80);
}

TEST_F(NestedProductFilterTest, splitsAConjunctionAcrossFactors) {
    expectJoinedCount("MATCH (a:Person), (b:Interest), (c:Interest) "
                      "WHERE b.name = c.name AND a.name = 'Remy' RETURN count(*)",
                      1,
                      10);
}

// 45 ordered pairs of distinct interests for each of the 8 people
TEST_F(NestedProductFilterTest, cutsTwoComponentsByAComparison) {
    expectJoinedCount("MATCH (a:Person), (b:Interest), (c:Interest) WHERE b.name < c.name RETURN count(*)", 0, 360);
}

TEST_F(NestedProductFilterTest, keepsTheColumnsOfEveryComponent) {
    expectRows("MATCH (a:Person), (b:Interest), (c:Interest) "
               "WHERE b.name = c.name AND a.name = 'Remy' AND b.name STARTS WITH 'C' "
               "RETURN a.name, b.name, c.name ORDER BY b.name",
               {{"Remy", "Computers", "Computers"}, {"Remy", "Cooking", "Cooking"}});
}
