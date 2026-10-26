#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// Every { } holds a single query, a UNION, a WHEN or a NEXT chain, nested in any of the
// others. Remy, Adam, Maxime and Luc are French; Remy, Adam, Luc and Martina have a PhD.
// Remy is interested in Ghosts, Computers and Eighties, Adam in Bio and Cooking, Maxime in
// Bio and Padel, Luc in Animals and Computers, Martina in Cooking, Suhas in Gym and JiuJitsu,
// Cyrus in Gym and Travel and Doruk in Gym. KNOWS_WELL leaves Remy for Adam and Adam for Remy.
class SubqueryBodyFormsTest : public WriteQueryTest {
};

TEST_F(SubqueryBodyFormsTest, whenInAnExistsBody) {
    expectRows("MATCH (p:Person) RETURN p.name, EXISTS { WHEN p.isFrench THEN MATCH (p)-[:KNOWS_WELL]->() }",
               {{"Remy", "true"},
                {"Adam", "true"},
                {"Maxime", "false"},
                {"Luc", "false"},
                {"Martina", "false"},
                {"Suhas", "false"},
                {"Cyrus", "false"},
                {"Doruk", "false"}});

    expectRows("MATCH (p:Person) "
               "WHERE EXISTS { "
               "  WHEN p.isFrench THEN { MATCH (p)-[:KNOWS_WELL]->(f) RETURN f.name AS name } "
               "  ELSE { MATCH (p)-[:INTERESTED_IN]->(i {name: 'Gym'}) RETURN i.name AS name } "
               "} "
               "RETURN p.name",
               {{"Remy"}, {"Adam"}, {"Suhas"}, {"Cyrus"}, {"Doruk"}});
}

TEST_F(SubqueryBodyFormsTest, whenInACountBody) {
    expectRows("MATCH (p:Person) RETURN p.name, COUNT { WHEN p.isFrench THEN MATCH (p)-[:INTERESTED_IN]->() }",
               {{"Remy", "3"},
                {"Adam", "2"},
                {"Maxime", "2"},
                {"Luc", "2"},
                {"Martina", "0"},
                {"Suhas", "0"},
                {"Cyrus", "0"},
                {"Doruk", "0"}});

    expectRows("MATCH (p:Person) "
               "RETURN p.name, COUNT { "
               "  WHEN p.isFrench THEN { MATCH (p)-[:INTERESTED_IN]->(i) RETURN i.name AS n } "
               "  ELSE { UNWIND ['a', 'b'] AS n RETURN n } "
               "}",
               {{"Remy", "3"},
                {"Adam", "2"},
                {"Maxime", "2"},
                {"Luc", "2"},
                {"Martina", "2"},
                {"Suhas", "2"},
                {"Cyrus", "2"},
                {"Doruk", "2"}});
}

TEST_F(SubqueryBodyFormsTest, whenInACollectBody) {
    expectRows("MATCH (p:Person) "
               "RETURN p.name, COLLECT { "
               "  WHEN p.isFrench THEN { MATCH (p)-[:INTERESTED_IN]->(i) RETURN i.name AS n } "
               "  ELSE { RETURN 'none' AS n } "
               "}",
               {{"Remy", "[Ghosts, Computers, Eighties]"},
                {"Adam", "[Bio, Cooking]"},
                {"Maxime", "[Bio, Padel]"},
                {"Luc", "[Animals, Computers]"},
                {"Martina", "[none]"},
                {"Suhas", "[none]"},
                {"Cyrus", "[none]"},
                {"Doruk", "[none]"}});

    expectRows("MATCH (p:Person) RETURN p.name, COLLECT { WHEN p.hasPhD THEN RETURN p.name AS n }",
               {{"Remy", "[Remy]"},
                {"Adam", "[Adam]"},
                {"Maxime", "[]"},
                {"Luc", "[Luc]"},
                {"Martina", "[Martina]"},
                {"Suhas", "[]"},
                {"Cyrus", "[]"},
                {"Doruk", "[]"}});
}

TEST_F(SubqueryBodyFormsTest, unionInAWhenBranch) {
    expectRows("WHEN true THEN { RETURN 1 AS x UNION RETURN 2 AS x } ELSE { RETURN 3 AS x }", {{"1"}, {"2"}});
    expectRows("WHEN false THEN { RETURN 1 AS x } ELSE { RETURN 2 AS x UNION ALL RETURN 2 AS x }", {{"2"}, {"2"}});

    expectRows("MATCH (p:Person) "
               "CALL (p) { "
               "  WHEN p.isFrench THEN { "
               "    MATCH (p)-[:INTERESTED_IN]->(i) RETURN i.name AS t "
               "    UNION "
               "    MATCH (p)-[:KNOWS_WELL]->(f) RETURN f.name AS t "
               "  } "
               "  ELSE { RETURN 'none' AS t } "
               "} "
               "RETURN p.name, t",
               {{"Remy", "Ghosts"},
                {"Remy", "Computers"},
                {"Remy", "Eighties"},
                {"Remy", "Adam"},
                {"Adam", "Bio"},
                {"Adam", "Cooking"},
                {"Adam", "Remy"},
                {"Maxime", "Bio"},
                {"Maxime", "Padel"},
                {"Luc", "Animals"},
                {"Luc", "Computers"},
                {"Martina", "none"},
                {"Suhas", "none"},
                {"Cyrus", "none"},
                {"Doruk", "none"}});
}

TEST_F(SubqueryBodyFormsTest, whenInAWhenBranch) {
    expectRows("WHEN true THEN { WHEN false THEN RETURN 1 AS x ELSE RETURN 2 AS x } ELSE RETURN 3 AS x", {{"2"}});

    expectRows("MATCH (p:Person) "
               "CALL (p) { "
               "  WHEN p.isFrench THEN { "
               "    WHEN p.hasPhD THEN RETURN 'french doctor' AS t "
               "    ELSE RETURN 'french' AS t "
               "  } "
               "  ELSE RETURN 'other' AS t "
               "} "
               "RETURN p.name, t",
               {{"Remy", "french doctor"},
                {"Adam", "french doctor"},
                {"Maxime", "french"},
                {"Luc", "french doctor"},
                {"Martina", "other"},
                {"Suhas", "other"},
                {"Cyrus", "other"},
                {"Doruk", "other"}});
}

TEST_F(SubqueryBodyFormsTest, whenInAUnionInAWhenBranch) {
    expectRows("MATCH (p:Person) "
               "CALL (p) { "
               "  WHEN p.isFrench THEN { { WHEN p.hasPhD THEN RETURN 'doctor' AS t } UNION RETURN 'french' AS t } "
               "  ELSE RETURN 'other' AS t "
               "} "
               "RETURN p.name, t",
               {{"Remy", "doctor"},
                {"Remy", "french"},
                {"Adam", "doctor"},
                {"Adam", "french"},
                {"Maxime", "french"},
                {"Luc", "doctor"},
                {"Luc", "french"},
                {"Martina", "other"},
                {"Suhas", "other"},
                {"Cyrus", "other"},
                {"Doruk", "other"}});
}

TEST_F(SubqueryBodyFormsTest, padsTheRowsANestedWhenTakesNoBranchFor) {
    expectRows("MATCH (p:Person) "
               "OPTIONAL CALL (p) { WHEN p.isFrench THEN { WHEN p.hasPhD THEN RETURN 'doctor' AS t } } "
               "RETURN p.name, t",
               {{"Remy", "doctor"},
                {"Adam", "doctor"},
                {"Maxime", "null"},
                {"Luc", "doctor"},
                {"Martina", "null"},
                {"Suhas", "null"},
                {"Cyrus", "null"},
                {"Doruk", "null"}});
}

// Maxime is the one French without a KNOWS_WELL edge who is interested in Padel
TEST_F(SubqueryBodyFormsTest, answersForTheRowsOfAUnionReturningNothing) {
    expectRows("MATCH (p:Person) "
               "RETURN p.name, EXISTS { "
               "  WHEN p.isFrench THEN { MATCH (p)-[:KNOWS_WELL]->() UNION MATCH (p)-[:INTERESTED_IN]->({name: 'Padel'}) } "
               "}",
               {{"Remy", "true"},
                {"Adam", "true"},
                {"Maxime", "true"},
                {"Luc", "false"},
                {"Martina", "false"},
                {"Suhas", "false"},
                {"Cyrus", "false"},
                {"Doruk", "false"}});

    expectRows("MATCH (p:Person) "
               "RETURN p.name, COUNT { "
               "  WHEN p.isFrench THEN { MATCH (p)-[:KNOWS_WELL]->() UNION ALL MATCH (p)-[:INTERESTED_IN]->() } "
               "  ELSE { MATCH (p)-[:INTERESTED_IN]->() } "
               "}",
               {{"Remy", "4"},
                {"Adam", "3"},
                {"Maxime", "2"},
                {"Luc", "2"},
                {"Martina", "1"},
                {"Suhas", "2"},
                {"Cyrus", "2"},
                {"Doruk", "1"}});
}

TEST_F(SubqueryBodyFormsTest, writesInAWhenBranchOfAWhenBranch) {
    expectWriteRows("MATCH (p:Person) "
                    "CALL (p) { WHEN p.isFrench THEN { WHEN p.hasPhD THEN { CREATE (:Doctor {name: p.name}) } } }",
                    {});

    expectRows("MATCH (d:Doctor) RETURN d.name", {{"Remy"}, {"Adam"}, {"Luc"}});
}

TEST_F(SubqueryBodyFormsTest, bracedQueriesAsTheSidesOfAUnion) {
    expectRows("{ RETURN 1 AS x } UNION RETURN 2 AS x", {{"1"}, {"2"}});
    expectRows("{ RETURN 1 AS x UNION ALL RETURN 1 AS x } UNION ALL RETURN 2 AS x", {{"1"}, {"1"}, {"2"}});
    expectRows("{ RETURN 1 AS x UNION ALL RETURN 1 AS x } UNION RETURN 2 AS x", {{"1"}, {"2"}});
    expectRows("RETURN 1 AS x UNION ALL { RETURN 2 AS x UNION RETURN 2 AS x }", {{"1"}, {"2"}});
}

TEST_F(SubqueryBodyFormsTest, whenSidesOfAUnionInCorrelatedBodies) {
    expectRows("MATCH (p:Person) "
               "RETURN p.name, EXISTS { { WHEN p.isFrench THEN RETURN 1 AS x } UNION { WHEN p.hasPhD THEN RETURN 2 AS x } }",
               {{"Remy", "true"},
                {"Adam", "true"},
                {"Maxime", "true"},
                {"Luc", "true"},
                {"Martina", "true"},
                {"Suhas", "false"},
                {"Cyrus", "false"},
                {"Doruk", "false"}});

    expectRows("MATCH (p:Person) "
               "RETURN p.name, COUNT { { WHEN p.isFrench THEN RETURN 1 AS x } UNION ALL { WHEN p.hasPhD THEN RETURN 2 AS x } }",
               {{"Remy", "2"},
                {"Adam", "2"},
                {"Maxime", "1"},
                {"Luc", "2"},
                {"Martina", "1"},
                {"Suhas", "0"},
                {"Cyrus", "0"},
                {"Doruk", "0"}});

    expectRows("MATCH (p:Person) "
               "RETURN p.name, COLLECT { { WHEN p.isFrench THEN RETURN 1 AS x } UNION ALL RETURN 2 AS x }",
               {{"Remy", "[1, 2]"},
                {"Adam", "[1, 2]"},
                {"Maxime", "[1, 2]"},
                {"Luc", "[1, 2]"},
                {"Martina", "[2]"},
                {"Suhas", "[2]"},
                {"Cyrus", "[2]"},
                {"Doruk", "[2]"}});
}

TEST_F(SubqueryBodyFormsTest, nextInACollectBody) {
    expectRows("MATCH (p:Person) "
               "RETURN p.name, COLLECT { MATCH (p)-[:INTERESTED_IN]->(i) RETURN i NEXT RETURN i.name AS n }",
               {{"Remy", "[Ghosts, Computers, Eighties]"},
                {"Adam", "[Bio, Cooking]"},
                {"Maxime", "[Bio, Padel]"},
                {"Luc", "[Animals, Computers]"},
                {"Martina", "[Cooking]"},
                {"Suhas", "[Gym, JiuJitsu]"},
                {"Cyrus", "[Gym, Travel]"},
                {"Doruk", "[Gym]"}});
}

TEST_F(SubqueryBodyFormsTest, rejectsAWriteInACorrelatedWhenBranch) {
    expectError("MATCH (p:Person) RETURN EXISTS { WHEN true THEN { CREATE (:Tag) RETURN 1 AS x } }",
                "An EXISTS subquery is read-only: its body cannot write to the graph");
}

TEST_F(SubqueryBodyFormsTest, rejectsCorrelatedWhenBranchesReturningDifferentColumns) {
    expectError("MATCH (p:Person) RETURN COUNT { WHEN p.isFrench THEN RETURN 1 AS x ELSE RETURN 2 AS y }",
                "All branches of a conditional query must return the same column names");
}
