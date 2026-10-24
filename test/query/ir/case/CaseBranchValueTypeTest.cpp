#include <gtest/gtest.h>

#include <string>
#include <string_view>
#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

// A CASE branch selects a value of any type Cypher has, and branches need not agree on
// one. Remy and Adam are 32; the other six people have no age.
class CaseBranchValueTypeTest : public CallV3Test {
protected:
    void expectRows(std::string_view query, const std::vector<StringRowSink::Row>& expected) {
        StringRowSink sink;
        runQuery(query, sink);

        std::vector<StringRowSink::Row> rows;
        sink.sortedRows(rows);

        EXPECT_EQ(rows, expected) << query;
    }

    void expectOrderedRows(std::string_view query, const std::vector<StringRowSink::Row>& expected) {
        StringRowSink sink;
        runQuery(query, sink);

        EXPECT_EQ(sink.getRows(), expected) << query;
    }
};

TEST_F(CaseBranchValueTypeTest, selectsAListPerRow) {
    expectRows("MATCH (p:Person) "
               "RETURN p.name, CASE WHEN p.age IS NULL THEN ['none'] ELSE [p.age, p.name] END",
               {
                   {"Adam", "32, Adam"}, {"Cyrus", "none"}, {"Doruk", "none"}, {"Luc", "none"},
                   {"Martina", "none"}, {"Maxime", "none"}, {"Remy", "32, Remy"}, {"Suhas", "none"},
               });
}

TEST_F(CaseBranchValueTypeTest, leavesAnUnmatchedListRowNull) {
    expectRows("MATCH (p:Person) RETURN p.name, CASE WHEN p.age IS NOT NULL THEN [p.age] END",
               {
                   {"Adam", "32"}, {"Cyrus", "null"}, {"Doruk", "null"}, {"Luc", "null"},
                   {"Martina", "null"}, {"Maxime", "null"}, {"Remy", "32"}, {"Suhas", "null"},
               });
}

TEST_F(CaseBranchValueTypeTest, selectsAListLiteral) {
    expectRows("RETURN CASE WHEN true THEN [1, 2] ELSE [3] END", {{"1, 2"}});
}

TEST_F(CaseBranchValueTypeTest, selectsAMapPerRow) {
    expectRows("MATCH (p:Person) "
               "WITH p, CASE WHEN p.age IS NULL THEN {none: true} ELSE {age: p.age, name: p.name} END AS m "
               "RETURN p.name, keys(m)",
               {
                   {"Adam", "age, name"}, {"Cyrus", "none"}, {"Doruk", "none"}, {"Luc", "none"},
                   {"Martina", "none"}, {"Maxime", "none"}, {"Remy", "age, name"}, {"Suhas", "none"},
               });
}

TEST_F(CaseBranchValueTypeTest, selectsADateTimePerRow) {
    expectRows("MATCH (p:Person) RETURN p.name, CASE WHEN p.age IS NULL THEN null "
               "ELSE datetime('2024-01-01T00:00:00Z') + duration({days: p.age}) END",
               {
                   {"Adam", "2024-02-02T00:00:00Z"}, {"Cyrus", "null"}, {"Doruk", "null"},
                   {"Luc", "null"}, {"Martina", "null"}, {"Maxime", "null"},
                   {"Remy", "2024-02-02T00:00:00Z"}, {"Suhas", "null"},
               });
}

TEST_F(CaseBranchValueTypeTest, selectsADateTimeLiteral) {
    expectRows("RETURN CASE WHEN true THEN datetime('2020-01-01T00:00:00Z') ELSE null END",
               {{"2020-01-01T00:00:00Z"}});
}

TEST_F(CaseBranchValueTypeTest, selectsADurationPerRow) {
    expectRows("MATCH (p:Person) "
               "RETURN p.name, CASE WHEN p.age IS NULL THEN duration(0) ELSE duration({days: p.age}) END",
               {
                   {"Adam", "PT768H"}, {"Cyrus", "PT0S"}, {"Doruk", "PT0S"}, {"Luc", "PT0S"},
                   {"Martina", "PT0S"}, {"Maxime", "PT0S"}, {"Remy", "PT768H"}, {"Suhas", "PT0S"},
               });
}

TEST_F(CaseBranchValueTypeTest, selectsBranchesOfDifferentTypes) {
    expectRows("MATCH (p:Person) RETURN p.name, CASE WHEN p.age IS NULL THEN p.name ELSE p.age END",
               {
                   {"Adam", "32"}, {"Cyrus", "Cyrus"}, {"Doruk", "Doruk"}, {"Luc", "Luc"},
                   {"Martina", "Martina"}, {"Maxime", "Maxime"}, {"Remy", "32"}, {"Suhas", "Suhas"},
               });
}

TEST_F(CaseBranchValueTypeTest, selectsLiteralBranchesOfDifferentTypes) {
    expectRows("RETURN CASE WHEN true THEN 1 ELSE 'none' END", {{"1"}});
    expectRows("RETURN CASE WHEN false THEN 1 ELSE 'none' END", {{"none"}});
}

TEST_F(CaseBranchValueTypeTest, selectsANodeOrAScalar) {
    expectRows("MATCH (p:Person) WHERE p.name = 'Remy' OR p.name = 'Luc' "
               "RETURN p.name, CASE WHEN p.age IS NULL THEN 'nobody' ELSE p END",
               {{"Luc", "nobody"}, {"Remy", "0"}});
}

TEST_F(CaseBranchValueTypeTest, keepsEachRowsNumberType) {
    expectRows("MATCH (p:Person) WHERE p.name IN ['Remy', 'Luc'] "
               "RETURN p.name, CASE WHEN p.age > 30 THEN p.age + 0.5 WHEN p.age IS NULL THEN 'none' ELSE 1 END",
               {{"Luc", "none"}, {"Remy", "32.5"}});
}

TEST_F(CaseBranchValueTypeTest, selectsATaggedBranchBesideATypedOne) {
    expectRows("UNWIND [1, 'a', 2.5] AS x RETURN CASE WHEN x = 1 THEN x ELSE true END",
               {{"1"}, {"true"}, {"true"}});
}

// Cypher orders a string before a number
TEST_F(CaseBranchValueTypeTest, ordersBranchesOfDifferentTypes) {
    expectOrderedRows("MATCH (p:Person) WHERE p.name IN ['Remy', 'Luc', 'Adam', 'Cyrus'] "
                      "RETURN CASE WHEN p.age IS NULL THEN p.name ELSE p.age END AS v ORDER BY v",
                      {{"Cyrus"}, {"Luc"}, {"32"}, {"32"}});
}

TEST_F(CaseBranchValueTypeTest, groupsOnBranchesOfDifferentTypes) {
    expectRows("MATCH (p:Person) RETURN CASE WHEN p.age IS NULL THEN 'none' ELSE p.age END AS v, count(*)",
               {{"32", "2"}, {"none", "6"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
