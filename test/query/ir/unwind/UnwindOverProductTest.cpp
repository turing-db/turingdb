#include <gtest/gtest.h>

#include <string_view>

#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/OwningOpRef.h"

#include "HashJoinQueryTest.h"
#include "ProductPlacement.h"

using namespace turing::test;

// TUR-173. Codegen unwinds a non-literal list over the product of every pattern component:
// UNWIND range(1, 1000) AS i MATCH (a:Item {id: i}) MATCH (b:Item) on 1000 items unwound
// 1e9 rows to keep 1e6. The unwind belongs in the factor its list reads, or in a factor of
// its own when its list reads no row.
class UnwindOverProductTest : public HashJoinQueryTest {
protected:
    void expectPlacedCount(std::string_view query, size_t expected) {
        mlir::OwningOpRef<mlir::ModuleOp> module;
        dbModule(query, module);

        EXPECT_EQ(countUnwindsOverAProduct(module.get()), 0u) << "query: " << query;
        EXPECT_EQ(countFiltersOverOneFactor(module.get()), 0u) << "query: " << query;

        expectCount(query, expected);
    }
};

// Remy and Adam are 32 and SimpleGraph has 10 interests
TEST_F(UnwindOverProductTest, unwindsAConstantListBesideTheProduct) {
    expectPlacedCount("UNWIND range(30, 35) AS i MATCH (a:Person {age: i}) MATCH (b:Interest) RETURN count(*)", 20);
}

TEST_F(UnwindOverProductTest, unwindsAConstantListUnderACommaPattern) {
    expectPlacedCount("UNWIND range(30, 35) AS i MATCH (b:Interest), (a:Person) WHERE a.age = i RETURN count(*)", 20);
}

TEST_F(UnwindOverProductTest, unwindsAListThePatternDoesNotRead) {
    expectPlacedCount("UNWIND range(1, 3) AS i MATCH (a:Person {name: 'Remy'}) MATCH (b:Interest) RETURN count(*)", 30);
}

TEST_F(UnwindOverProductTest, unwindsAConstantListBesideASinglePattern) {
    expectPlacedCount("UNWIND range(30, 35) AS i MATCH (a:Person {age: i}) RETURN count(*)", 2);
}

TEST_F(UnwindOverProductTest, unwindsAListCollectedByAnEarlierPart) {
    expectPlacedCount("MATCH (p:Person) WITH collect(p.name) AS names "
                      "UNWIND names AS n MATCH (a:Person {name: n}) MATCH (b:Interest) RETURN count(*)",
                      80);
}

// j = 1 unwinds 31 and 32, j = 2 unwinds 31 to 33: Remy and Adam match once for each j
TEST_F(UnwindOverProductTest, unwindsAListReadOffAnotherUnwind) {
    expectPlacedCount("UNWIND range(1, 2) AS j UNWIND range(31, 31 + j) AS i "
                      "MATCH (a:Person {age: i}) MATCH (b:Interest) RETURN count(*)",
                      40);
}

TEST_F(UnwindOverProductTest, unwindsTwoListsCollectedByAnEarlierPart) {
    expectPlacedCount("MATCH (p:Person) WITH collect(p.name) AS names "
                      "UNWIND names AS n MATCH (a:Person {name: n}) MATCH (b:Person {name: n}) RETURN count(*)",
                      8);
}

TEST_F(UnwindOverProductTest, unwindsAListReadThroughAFilter) {
    expectPlacedCount("MATCH (a:Person), (b:Interest) WHERE a.name = 'Remy' "
                      "UNWIND range(1, a.age) AS i RETURN count(*)",
                      320);
}

// 30 pairs of a person and an interest whose name sorts after the person's
TEST_F(UnwindOverProductTest, unwindsAListReadThroughAFilterOfBothFactors) {
    expectPlacedCount("MATCH (a:Person), (b:Interest) WHERE a.name < b.name "
                      "UNWIND [a.name, a.name] AS n RETURN count(*)",
                      60);
}

TEST_F(UnwindOverProductTest, keepsTheColumnsOfFilteredFactors) {
    expectRows("MATCH (a:Person), (b:Interest) WHERE a.name = 'Luc' AND b.name STARTS WITH 'C' "
               "UNWIND [a.name + '1', a.name + '2'] AS n RETURN b.name, n ORDER BY b.name, n",
               {{"Computers", "Luc1"}, {"Computers", "Luc2"}, {"Cooking", "Luc1"}, {"Cooking", "Luc2"}});
}

TEST_F(UnwindOverProductTest, keepsTheColumnsOfEveryFactor) {
    expectRows("UNWIND range(31, 33) AS i MATCH (a:Person {age: i}) MATCH (b:Interest {name: 'Bio'}) "
               "RETURN a.name, i, b.name ORDER BY a.name",
               {{"Adam", "32", "Bio"}, {"Remy", "32", "Bio"}});
}

TEST_F(UnwindOverProductTest, unwindsAListReadFromBothFactors) {
    expectRows("MATCH (a:Person {name: 'Remy'}), (b:Interest {name: 'Bio'}) "
               "UNWIND [a.name, b.name] AS n RETURN n ORDER BY n",
               {{"Bio"}, {"Remy"}});
}
