#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// The branches of a UNION or a WHEN fill one column. Where they return different types, each
// row keeps the type of the value its branch returned, as Neo4j answers it. Remy is node 0,
// Remy, Adam, Maxime and Luc are French, and Remy is interested in Ghosts, Computers and
// Eighties, Adam in Bio and Cooking, Maxime in Bio and Padel, Luc in Animals and Computers.
class UnionBranchValueTypeTest : public WriteQueryTest {
};

TEST_F(UnionBranchValueTypeTest, keepsAnIntegerBesideAFloat) {
    expectRows("RETURN 1 AS x UNION ALL RETURN 2.5 AS x", {{"1"}, {"2.500000"}});
    expectRows("CALL { RETURN 1 AS x UNION ALL RETURN 2.5 AS x } RETURN x + 1", {{"2"}, {"3.500000"}});
}

TEST_F(UnionBranchValueTypeTest, mixesAStringAndANumber) {
    expectRows("RETURN 1 AS x UNION ALL RETURN 'a' AS x", {{"1"}, {"a"}});
    expectRows("CALL { RETURN 1 AS x UNION ALL RETURN 'a' AS x } WITH x WHERE x = 1 RETURN x", {{"1"}});
}

TEST_F(UnionBranchValueTypeTest, dedupsAcrossBranchesOfDifferentTypes) {
    expectRows("RETURN 1 AS x UNION RETURN 'a' AS x UNION RETURN 1 AS x", {{"1"}, {"a"}});
    expectRows("UNWIND [1, 2] AS i RETURN i AS x UNION RETURN 'a' AS x UNION RETURN 2 AS x",
               {{"1"}, {"2"}, {"a"}});
}

TEST_F(UnionBranchValueTypeTest, mixesListsOfDifferentElements) {
    expectRows("RETURN ['a'] AS l UNION ALL RETURN [] AS l", {{"[a]"}, {"[]"}});
    expectRows("RETURN ['a'] AS l UNION ALL RETURN [1] AS l", {{"[a]"}, {"[1]"}});
}

TEST_F(UnionBranchValueTypeTest, fillsANodeColumnWithNull) {
    expectRows("MATCH (p:Person {name: 'Remy'}) RETURN p AS q UNION ALL RETURN null AS q", {{"0"}, {"null"}});
    expectRows("CALL { MATCH (p:Person {name: 'Remy'}) RETURN p AS q UNION ALL RETURN null AS q } RETURN q.name",
               {{"Remy"}, {"null"}});
}

TEST_F(UnionBranchValueTypeTest, holdsANullBesideValuesOfDifferentTypes) {
    expectRows("CALL { RETURN 1 AS x UNION ALL RETURN null AS x UNION ALL RETURN 'a' AS x } RETURN x",
               {{"1"}, {"null"}, {"a"}});
    expectRows("CALL { RETURN 1 AS x UNION ALL RETURN null AS x UNION ALL RETURN 'a' AS x } RETURN count(x)",
               {{"2"}});
}

TEST_F(UnionBranchValueTypeTest, holdsANullBesideAListOrADateTime) {
    expectRows("RETURN [1] AS l UNION ALL RETURN null AS l", {{"[1]"}, {"null"}});
    expectRows("RETURN datetime('2020-01-01T00:00:00Z') AS d UNION ALL RETURN null AS d",
               {{"2020-01-01T00:00:00Z"}, {"null"}});
}

TEST_F(UnionBranchValueTypeTest, mixesPropertiesOfDifferentTypes) {
    expectRows("MATCH (p:Person) WHERE p.name = 'Remy' RETURN p.age AS v "
               "UNION ALL "
               "MATCH (i:Interest) WHERE i.name = 'Gym' RETURN i.name AS v",
               {{"32"}, {"Gym"}});
}

TEST_F(UnionBranchValueTypeTest, rejectsAPathBesideAnInteger) {
    expectError("MATCH p = (a:Person)-[:KNOWS_WELL]->(b) RETURN p AS x UNION ALL RETURN 1 AS x",
                "Column 'x' cannot hold both 'GraphPath' and 'Integer' values");
}

TEST_F(UnionBranchValueTypeTest, mixesANodeAndAScalar) {
    expectRows("MATCH (p:Person {name: 'Remy'}) RETURN p AS q UNION ALL RETURN 1 AS q", {{"0"}, {"1"}});
}

TEST_F(UnionBranchValueTypeTest, mixesTheBranchesOfAWhen) {
    expectRows("WHEN false THEN RETURN 1 AS x ELSE RETURN 'a' AS x", {{"a"}});

    expectRows("MATCH (p:Person) "
               "CALL (p) { WHEN p.isFrench THEN RETURN p.name AS v ELSE RETURN 0 AS v } "
               "RETURN p.name, v",
               {{"Remy", "Remy"},
                {"Adam", "Adam"},
                {"Maxime", "Maxime"},
                {"Luc", "Luc"},
                {"Martina", "0"},
                {"Suhas", "0"},
                {"Cyrus", "0"},
                {"Doruk", "0"}});

    expectRows("MATCH (p:Person) "
               "CALL (p) { WHEN p.isFrench THEN RETURN p AS q ELSE RETURN null AS q } "
               "RETURN p.name, q.name",
               {{"Remy", "Remy"},
                {"Adam", "Adam"},
                {"Maxime", "Maxime"},
                {"Luc", "Luc"},
                {"Martina", "null"},
                {"Suhas", "null"},
                {"Cyrus", "null"},
                {"Doruk", "null"}});
}

TEST_F(UnionBranchValueTypeTest, mixesACollectAndAnEmptyList) {
    expectRows("MATCH (p:Person) "
               "CALL (p) { "
               "  WHEN p.isFrench THEN RETURN COLLECT { MATCH (p)-[:INTERESTED_IN]->(i) RETURN i.name } AS l "
               "  ELSE RETURN [] AS l "
               "} "
               "RETURN p.name, l",
               {{"Remy", "[Ghosts, Computers, Eighties]"},
                {"Adam", "[Bio, Cooking]"},
                {"Maxime", "[Bio, Padel]"},
                {"Luc", "[Animals, Computers]"},
                {"Martina", "[]"},
                {"Suhas", "[]"},
                {"Cyrus", "[]"},
                {"Doruk", "[]"}});

    expectRows("MATCH (p:Person) WHERE p.name = 'Adam' "
               "CALL (p) { RETURN COLLECT { MATCH (p)-[:INTERESTED_IN]->(i) RETURN i.name } AS l UNION ALL RETURN [] AS l } "
               "RETURN p.name, l",
               {{"Adam", "[Bio, Cooking]"}, {"Adam", "[]"}});
}

TEST_F(UnionBranchValueTypeTest, collectsBranchesOfDifferentTypes) {
    expectRows("RETURN COLLECT { RETURN 1 AS x UNION RETURN 'a' AS x } AS l", {{"[1, a]"}});
    expectRows("RETURN COLLECT { RETURN 1 AS x UNION ALL RETURN 'a' AS x } AS l", {{"[1, a]"}});
}
