#include <gtest/gtest.h>

#include <string>
#include <string_view>
#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

// coalesce answers its first non-null argument, whatever type that argument has. Remy and
// Adam are 32; the other six people have no age.
class CoalesceArgumentValueTypeTest : public CallV3Test {
protected:
    void expectRows(std::string_view query, const std::vector<StringRowSink::Row>& expected) {
        StringRowSink sink;
        runQuery(query, sink);

        std::vector<StringRowSink::Row> rows;
        sink.sortedRows(rows);

        EXPECT_EQ(rows, expected) << query;
    }
};

TEST_F(CoalesceArgumentValueTypeTest, answersArgumentsOfDifferentTypes) {
    expectRows("MATCH (p:Person) RETURN p.name, coalesce(p.age, 'none')",
               {
                   {"Adam", "32"}, {"Cyrus", "none"}, {"Doruk", "none"}, {"Luc", "none"},
                   {"Martina", "none"}, {"Maxime", "none"}, {"Remy", "32"}, {"Suhas", "none"},
               });
}

TEST_F(CoalesceArgumentValueTypeTest, answersANodeOrAScalar) {
    expectRows("MATCH (p:Person) WHERE p.name = 'Remy' OR p.name = 'Luc' "
               "OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f) RETURN p.name, coalesce(f, 'nobody')",
               {{"Luc", "nobody"}, {"Remy", "1"}});
}

TEST_F(CoalesceArgumentValueTypeTest, answersAList) {
    expectRows("MATCH (p:Person) "
               "RETURN p.name, coalesce(CASE WHEN p.age IS NOT NULL THEN [p.age] END, ['none'])",
               {
                   {"Adam", "32"}, {"Cyrus", "none"}, {"Doruk", "none"}, {"Luc", "none"},
                   {"Martina", "none"}, {"Maxime", "none"}, {"Remy", "32"}, {"Suhas", "none"},
               });
}

TEST_F(CoalesceArgumentValueTypeTest, answersADateTime) {
    expectRows("MATCH (p:Person) RETURN p.name, coalesce(CASE WHEN p.age IS NOT NULL "
               "THEN datetime('2024-01-01T00:00:00Z') + duration({days: p.age}) END, datetime(0))",
               {
                   {"Adam", "2024-02-02T00:00:00Z"}, {"Cyrus", "1970-01-01T00:00:00Z"},
                   {"Doruk", "1970-01-01T00:00:00Z"}, {"Luc", "1970-01-01T00:00:00Z"},
                   {"Martina", "1970-01-01T00:00:00Z"}, {"Maxime", "1970-01-01T00:00:00Z"},
                   {"Remy", "2024-02-02T00:00:00Z"}, {"Suhas", "1970-01-01T00:00:00Z"},
               });
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
