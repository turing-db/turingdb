#include <gtest/gtest.h>

#include "IRTestRows.h"
#include "WriteQueryTest.h"

using namespace turing::test;

class IntegerDoubleEqualityTest : public WriteQueryTest {
protected:
    void initialize() override {
        WriteQueryTest::initialize();

        applyWrite("MATCH (n:Interest {name: 'Ghosts'}) SET n.score = 32.0");
        applyWrite("MATCH (n:Interest {name: 'Ghosts'}) SET n.bigFloat = 9007199254740992.0");
        applyWrite("MATCH (n:Person {name: 'Luc'}) SET n.big = 9007199254740993");
    }
};

TEST_F(IntegerDoubleEqualityTest, comparesLiteralsByValue) {
    expectRows("RETURN 1 = 1.0, 1.0 = 1, 1 <> 1.0, 1.0 <> 1", {{"true", "true", "false", "false"}});
    expectRows("RETURN 1 = 1.5, 1.5 = 1, 1 <> 1.5, 1.5 <> 1", {{"false", "false", "true", "true"}});
}

TEST_F(IntegerDoubleEqualityTest, comparesExactlyPastTheDoublePrecision) {
    expectRows("RETURN 9007199254740993 = 9007199254740992.0, 9007199254740993 <> 9007199254740992.0",
               {{"false", "true"}});
    expectRows("RETURN 9007199254740992 = 9007199254740992.0, 9007199254740992.0 <> 9007199254740992",
               {{"true", "false"}});
}

TEST_F(IntegerDoubleEqualityTest, comparesBoundValues) {
    expectRows("WITH 2 AS i, 2.0 AS d, 2.5 AS h RETURN i = d, d <> i, i = h, h <> i",
               {{"true", "false", "false", "true"}});
}

TEST_F(IntegerDoubleEqualityTest, filtersAnIntegerPropertyOnAFloatLiteral) {
    expectRows("MATCH (n) WHERE n.age = 32.0 RETURN n.name", {{"Remy"}, {"Adam"}});
    expectRows("MATCH (n:Person) WHERE 32.0 = n.age RETURN n.name", {{"Remy"}, {"Adam"}});
    expectRows("MATCH (n) WHERE n.age = 32.5 RETURN n.name", {});
    expectRows("MATCH (n:Person) WHERE n.age <> 32.5 RETURN n.name", {{"Remy"}, {"Adam"}});
}

TEST_F(IntegerDoubleEqualityTest, filtersAFloatPropertyOnAnIntegerLiteral) {
    expectRows("MATCH (n) WHERE n.score = 32 RETURN n.name", {{"Ghosts"}});
    expectRows("MATCH (n:Interest) WHERE 32 = n.score RETURN n.name", {{"Ghosts"}});
    expectRows("MATCH (n) WHERE n.score = 33 RETURN n.name", {});
}

TEST_F(IntegerDoubleEqualityTest, filtersAPropertyExactlyPastTheDoublePrecision) {
    expectRows("MATCH (n) WHERE n.big = 9007199254740992.0 RETURN n.name", {});
    expectRows("MATCH (n) WHERE n.big <> 9007199254740992.0 RETURN n.name", {{"Luc"}});
    expectRows("MATCH (n) WHERE n.bigFloat = 9007199254740993 RETURN n.name", {});
    expectRows("MATCH (n) WHERE n.bigFloat = 9007199254740992 RETURN n.name", {{"Ghosts"}});
}

TEST_F(IntegerDoubleEqualityTest, matchesAnInlinePropertyOfTheOtherNumberType) {
    expectRows("MATCH (n {age: 32.0}) RETURN n.name", {{"Remy"}, {"Adam"}});
    expectRows("MATCH (n:Person {age: 32.5}) RETURN n.name", {});
    expectRows("MATCH (n {score: 32}) RETURN n.name", {{"Ghosts"}});
}

TEST_F(IntegerDoubleEqualityTest, projectsTheComparisonOfAProperty) {
    expectRows("MATCH (n:Person) WHERE n.name = 'Remy' RETURN n.age = 32.0, n.age <> 32.0, n.age = 32.5",
               {{"true", "false", "false"}});
}

TEST_F(IntegerDoubleEqualityTest, filtersAnEdgePropertyOnAFloatLiteral) {
    expectRows("MATCH ()-[e]->() WHERE e.duration = 20.0 RETURN e.name",
               {{"Remy -> Adam"}, {"Adam -> Remy"}, {"Remy -> Ghosts"}, {"Remy -> Eighties"}, {"Luc -> Animals"}});
}

TEST_F(IntegerDoubleEqualityTest, joinsAnIntegerPropertyOnAFloatProperty) {
    expectRows("MATCH (a:Person), (b:Interest) WHERE a.age = b.score RETURN a.name, b.name",
               {{"Remy", "Ghosts"}, {"Adam", "Ghosts"}});
    expectRows("MATCH (a), (b) WHERE a.big = b.bigFloat RETURN a.name, b.name", {});
}

TEST_F(IntegerDoubleEqualityTest, comparesAnAggregate) {
    expectRows("MATCH (n:Person) RETURN count(n) = 8.0, count(n) <> 8.0", {{"true", "false"}});
    expectRows("MATCH (n:Person) RETURN avg(n.age) = 32, avg(n.age) <> 32", {{"true", "false"}});
}

TEST_F(IntegerDoubleEqualityTest, matchesASimpleCaseOfTheOtherNumberType) {
    expectRows("UNWIND [1, 2] AS x RETURN x, CASE x WHEN 1.0 THEN 'one' ELSE 'other' END",
               {{"1", "one"}, {"2", "other"}});
    expectRows("UNWIND [1.0, 2.5] AS x RETURN CASE x WHEN 1 THEN 'one' ELSE 'other' END",
               {{"one"}, {"other"}});
}

TEST_F(IntegerDoubleEqualityTest, keepsTheUnwoundValueItEquals) {
    expectRows("UNWIND [32.0, 40.0] AS x MATCH (n:Person) WHERE n.age = x RETURN n.name, x",
               {{"Remy", "32.000000"}, {"Adam", "32.000000"}});
    expectRows("UNWIND [32, 40] AS x MATCH (n:Interest) WHERE n.score = x RETURN n.name, x",
               {{"Ghosts", "32"}});
    expectRows("UNWIND [32, 40] AS x MATCH (n:Interest) WHERE [n.score, 'a'][0] = x RETURN n.name, x",
               {{"Ghosts", "32"}});
    expectRows("UNWIND [32.0, 40.0] AS x MATCH (n:Person) WHERE n.age = x RETURN n.name, x / 3 > 10.5",
               {{"Remy", "true"}, {"Adam", "true"}});
    expectRows("UNWIND [32, 40] AS x MATCH (n:Interest) WHERE n.score * 1 = x RETURN n.name, x",
               {{"Ghosts", "32"}});
    expectRows("UNWIND [-0.0] AS x MATCH (n:Person) WHERE n.age - 32 = x RETURN n.name, x",
               {{"Remy", "-0.000000"}, {"Adam", "-0.000000"}});
    expectRows("UNWIND [0, 1] AS i MATCH (n) WHERE id(n) = i RETURN i, n.name", {{"0", "Remy"}, {"1", "Adam"}});
}

TEST_F(IntegerDoubleEqualityTest, filtersAnIDOnAFloat) {
    expectRows("MATCH (n) WHERE id(n) = 1.0 RETURN n.name", {{"Adam"}});
    expectRows("MATCH (n) WHERE id(n) = 1.5 RETURN n.name", {});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
