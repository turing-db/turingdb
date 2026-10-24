#include <gtest/gtest.h>

#include <string_view>
#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

// A datetime or a duration a function computes is present on every row, and joins a column
// of values of other types as any other value does. Remy is 32 and Luc has no age.
class CaseComputedTemporalBranchTest : public CallV3Test {
protected:
    void expectRows(std::string_view query, const std::vector<StringRowSink::Row>& expected) {
        StringRowSink sink;
        runQuery(query, sink);

        std::vector<StringRowSink::Row> rows;
        sink.sortedRows(rows);

        EXPECT_EQ(rows, expected) << query;
    }
};

TEST_F(CaseComputedTemporalBranchTest, selectsADateTimeOrAnInteger) {
    expectRows("MATCH (p:Person) WHERE p.name IN ['Remy', 'Luc'] "
               "RETURN p.name, CASE WHEN p.age IS NULL THEN 1 ELSE datetime('2020-01-01T00:00:00Z') END",
               {{"Luc", "1"}, {"Remy", "2020-01-01T00:00:00Z"}});
}

TEST_F(CaseComputedTemporalBranchTest, selectsADurationOrAString) {
    expectRows("MATCH (p:Person) WHERE p.name IN ['Remy', 'Luc'] "
               "RETURN p.name, CASE WHEN p.age IS NULL THEN 'none' ELSE duration({days: p.age}) END",
               {{"Luc", "none"}, {"Remy", "PT768H"}});
}

TEST_F(CaseComputedTemporalBranchTest, coalescesToADateTime) {
    expectRows("MATCH (p:Person) WHERE p.name IN ['Remy', 'Luc'] "
               "RETURN p.name, coalesce(p.age, datetime('2020-01-01T00:00:00Z'))",
               {{"Luc", "2020-01-01T00:00:00Z"}, {"Remy", "32"}});
    expectRows("RETURN coalesce(null, datetime('2020-01-01T00:00:00Z'), 1)", {{"2020-01-01T00:00:00Z"}});
}

TEST_F(CaseComputedTemporalBranchTest, selectsTheCurrentInstantOrAnInteger) {
    expectRows("MATCH (p:Person) WHERE p.name IN ['Remy', 'Luc'] "
               "RETURN p.name, CASE WHEN p.age IS NULL THEN 1 ELSE datetime() END IS NULL",
               {{"Luc", "false"}, {"Remy", "false"}});
    expectRows("RETURN CASE WHEN coalesce(null, datetime(), 1) IS NULL THEN 'null' ELSE 'set' END", {{"set"}});
    expectRows("RETURN [datetime(), 1][1]", {{"1"}});
}

TEST_F(CaseComputedTemporalBranchTest, listsADateTimeBesideAnInteger) {
    expectRows("MATCH (p:Person) WHERE p.name IN ['Remy', 'Luc'] "
               "RETURN p.name, [p.age, datetime('2020-01-01T00:00:00Z'), duration(5)]",
               {{"Luc", "null, 2020-01-01T00:00:00Z, PT0.000005S"}, {"Remy", "32, 2020-01-01T00:00:00Z, PT0.000005S"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
