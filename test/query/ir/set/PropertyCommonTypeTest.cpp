#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// A value whose type varies per row - a CASE, a coalesce, a UNION or WHEN column - creates
// a property of the type its values convert to without loss: an integer beside a float
// makes a Double. Remy, Adam, Maxime and Luc are French, Remy and Adam are 32, and Remy,
// Adam, Luc and Martina have a PhD. KNOWS_WELL leaves Remy for Adam and Adam for Remy.
class PropertyCommonTypeTest : public WriteQueryTest {
protected:
    void expectWriteError(std::string_view query, std::string_view expectedError) {
        ChangeID changeID;
        openChange(changeID);

        const QueryStatus status = runWrite(query, changeID);
        ASSERT_FALSE(status.isOk()) << "query: " << query << "\nexpected it to fail";
        EXPECT_NE(status.getError().find(expectedError), std::string::npos)
            << "query: " << query << "\nerror: " << status.getError();
    }

    const Rows _frenchOrNot {{"Remy", "1.000000"},
                             {"Adam", "1.000000"},
                             {"Maxime", "1.000000"},
                             {"Luc", "1.000000"},
                             {"Martina", "0.500000"},
                             {"Suhas", "0.500000"},
                             {"Cyrus", "0.500000"},
                             {"Doruk", "0.500000"}};
};

TEST_F(PropertyCommonTypeTest, setsANodePropertyFromACase) {
    applyWrite("MATCH (n:Person) SET n.score = CASE WHEN n.isFrench THEN 1 ELSE 0.5 END");
    expectRows("MATCH (n:Person) RETURN n.name, n.score", _frenchOrNot);
}

TEST_F(PropertyCommonTypeTest, setsANodePropertyFromACoalesce) {
    applyWrite("MATCH (n:Person) WHERE n.hasPhD SET n.weight = coalesce(n.age, 0.5)");
    expectRows("MATCH (n:Person) WHERE n.hasPhD RETURN n.name, n.weight",
               {{"Remy", "32.000000"}, {"Adam", "32.000000"}, {"Luc", "0.500000"}, {"Martina", "0.500000"}});
}

TEST_F(PropertyCommonTypeTest, setsAnEdgePropertyFromACase) {
    applyWrite("MATCH (a:Person)-[e:KNOWS_WELL]->() SET e.strength = CASE WHEN a.name = 'Remy' THEN 1 ELSE 0.5 END");
    expectRows("MATCH (a:Person)-[e:KNOWS_WELL]->() RETURN a.name, e.strength",
               {{"Remy", "1.000000"}, {"Adam", "0.500000"}});
}

TEST_F(PropertyCommonTypeTest, setsAPropertyFromAWhenColumn) {
    applyWrite("MATCH (n:Person) CALL (n) { WHEN n.isFrench THEN RETURN 1 AS x ELSE RETURN 0.5 AS x } SET n.score = x");
    expectRows("MATCH (n:Person) RETURN n.name, n.score", _frenchOrNot);
}

TEST_F(PropertyCommonTypeTest, keepsTheCommonTypeThroughAWith) {
    applyWrite("MATCH (n:Person) WITH n, CASE WHEN n.isFrench THEN 1 ELSE 0.5 END AS v SET n.score = v");
    expectRows("MATCH (n:Person) RETURN n.name, n.score", _frenchOrNot);
}

TEST_F(PropertyCommonTypeTest, createsANodePropertyFromACase) {
    applyWrite("CREATE (:Thing {v: CASE WHEN true THEN 1 ELSE 0.5 END})");
    expectRows("MATCH (t:Thing) RETURN t.v", {{"1.000000"}});
}

TEST_F(PropertyCommonTypeTest, createsANodePropertyFromAUnionColumn) {
    applyWrite("CALL { RETURN 1 AS x UNION ALL RETURN 2.5 AS x } CREATE (:Thing {v: x})");
    expectRows("MATCH (t:Thing) RETURN t.v", {{"1.000000"}, {"2.500000"}});
}

TEST_F(PropertyCommonTypeTest, createsANodePropertyFromAnImportedCase) {
    applyWrite("WITH CASE WHEN true THEN 1 ELSE 0.5 END AS v CALL (v) { CREATE (:Thing {v: v}) }");
    expectRows("MATCH (t:Thing) RETURN t.v", {{"1.000000"}});
}

TEST_F(PropertyCommonTypeTest, createsAnEdgePropertyFromACase) {
    applyWrite("MATCH (a:Person {name: 'Remy'}), (b:Person {name: 'Adam'}) "
               "CREATE (a)-[:RATES {v: CASE WHEN true THEN 1 ELSE 0.5 END}]->(b)");
    expectRows("MATCH ()-[e:RATES]->() RETURN e.v", {{"1.000000"}});
}

TEST_F(PropertyCommonTypeTest, mergesANodePropertyFromACase) {
    applyWrite("MERGE (:Thing {v: CASE WHEN true THEN 1 ELSE 0.5 END})");
    expectRows("MATCH (t:Thing) RETURN t.v", {{"1.000000"}});
}

TEST_F(PropertyCommonTypeTest, rejectsCreatingAPropertyFromValuesSharingNoType) {
    const std::string_view error = "Cannot create property 'v' from a value whose type is only known per row";

    expectWriteError("MATCH (n:Person) SET n.v = CASE WHEN n.isFrench THEN 1 ELSE 'a' END", error);
    expectWriteError("UNWIND [1, 'a'] AS x CREATE (:Thing {v: x})", error);
}

TEST_F(PropertyCommonTypeTest, namesTheValueTypeAnExistingPropertyCannotHold) {
    expectWriteError("MATCH (n:Person) SET n.age = CASE WHEN n.isFrench THEN 1 ELSE 0.5 END",
                     "Cannot set a property of type 'Int64' to a value of type 'Double'");
}
