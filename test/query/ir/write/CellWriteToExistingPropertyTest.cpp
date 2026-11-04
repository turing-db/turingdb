#include <gtest/gtest.h>

#include <string>
#include <string_view>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// A CREATE or a MERGE writing a value held in a type-erased cell to a property the graph
// already holds, which checks each row against the property's type as a SET does. Remy and
// Adam are 32, and KNOWS_WELL's duration is an integer. Expected values are Neo4j 5.26's.
class CellWriteToExistingPropertyTest : public WriteQueryTest {
protected:
    void expectWriteError(std::string_view query, std::string_view expectedError) {
        ChangeID changeID;
        openChange(changeID);

        const QueryStatus status = runWrite(query, changeID);
        ASSERT_FALSE(status.isOk()) << "query: " << query << "\nexpected it to fail";
        EXPECT_NE(status.getError().find(expectedError), std::string::npos)
            << "query: " << query << "\nerror: " << status.getError();
    }
};

TEST_F(CellWriteToExistingPropertyTest, createsANodeWithAnExistingProperty) {
    applyWrite("UNWIND [[30, 'a'][0]] AS x CREATE (:Person {name: 'New', age: x})");
    expectRows("MATCH (p:Person {name: 'New'}) RETURN p.age", {{"30"}});
}

TEST_F(CellWriteToExistingPropertyTest, returnsTheExistingPropertyItCreated) {
    expectWriteRows("UNWIND [[30, 'a'][0]] AS x CREATE (n:Person {name: 'New', age: x}) RETURN n.age, n.age + 1",
                    {{"30", "31"}});
}

TEST_F(CellWriteToExistingPropertyTest, createsAPropertyInOneChangeAndWritesItInTheNext) {
    applyWrite("UNWIND [1, 2.5] AS x CREATE (:T {v: x})");
    applyWrite("UNWIND [1, 2.5] AS x CREATE (:T {v: x})");
    expectRows("MATCH (t:T) RETURN t.v", {{"1.000000"}, {"2.500000"}, {"1.000000"}, {"2.500000"}});
}

TEST_F(CellWriteToExistingPropertyTest, createsAnEdgeWithAnExistingProperty) {
    applyWrite("MATCH (a:Person {name: 'Remy'}), (b:Person {name: 'Adam'}) "
               "CREATE (a)-[:KNOWS_WELL {duration: [5, 'a'][0]}]->(b)");
    expectRows("MATCH (:Person {name: 'Remy'})-[e:KNOWS_WELL]->(:Person {name: 'Adam'}) RETURN e.duration",
               {{"20"}, {"5"}});
}

TEST_F(CellWriteToExistingPropertyTest, mergesTheNodesHoldingTheValue) {
    expectWriteRows("UNWIND [[32, 'a']] AS pair MERGE (p:Person {age: pair[0]}) RETURN p.name", {{"Remy"}, {"Adam"}});
    expectRows("MATCH (p:Person) RETURN count(p)", {{"8"}});
}

TEST_F(CellWriteToExistingPropertyTest, mergesANodeNoneHolds) {
    applyWrite("UNWIND [[40, 'a']] AS pair MERGE (:Person {age: pair[0]})");
    expectRows("MATCH (p:Person) WHERE p.age = 40 RETURN count(p)", {{"1"}});
}

TEST_F(CellWriteToExistingPropertyTest, widensAnIntegerToAnExistingDoubleProperty) {
    applyWrite("CREATE (:T {v: 1.5})");
    applyWrite("UNWIND [[2, 'a']] AS pair CREATE (:T {v: pair[0]})");
    expectRows("MATCH (t:T) RETURN t.v", {{"1.500000"}, {"2.000000"}});
}

TEST_F(CellWriteToExistingPropertyTest, raisesOnAValueTheExistingPropertyCannotHold) {
    expectWriteError("UNWIND [['x', 1]] AS pair CREATE (:Person {age: pair[0]})",
                     "Cannot set a property of type 'Int64' to a value of type 'String'");
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
