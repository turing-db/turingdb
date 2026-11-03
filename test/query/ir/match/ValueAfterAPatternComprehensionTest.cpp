#include <gtest/gtest.h>

#include <string_view>
#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

class ValueAfterAPatternComprehensionTest : public CallV3Test {
protected:
    void expectRows(std::string_view query, const std::vector<StringRowSink::Row>& expected) {
        StringRowSink sink;
        runQuery(query, sink);

        std::vector<StringRowSink::Row> rows;
        sink.sortedRows(rows);

        EXPECT_EQ(rows, expected) << query;
    }
};

TEST_F(ValueAfterAPatternComprehensionTest, readsAPropertyOfTheValueAfterTheComprehension) {
    expectRows("MATCH (n:Person {name: 'Remy'}) WITH {person: n} AS m WITH m.person AS p "
               "RETURN [(p)-[:KNOWS_WELL]->(x) | x.name], p.name",
               {{"Adam", "Remy"}});
}

TEST_F(ValueAfterAPatternComprehensionTest, returnsTheValueAfterTheComprehension) {
    expectRows("MATCH (n:Person {name: 'Remy'}) WITH {person: n} AS m WITH m.person AS p "
               "RETURN [(p)-[:KNOWS_WELL]->(x) | x.name] AS l, p",
               {{"Adam", "0"}});
}

TEST_F(ValueAfterAPatternComprehensionTest, readsTheValueAfterAComprehensionInTheWhere) {
    expectRows("MATCH (n:Person) WHERE n.name IN ['Remy', 'Luc'] WITH {person: n} AS m WITH m.person AS p "
               "WHERE size([(p)-[:KNOWS_WELL]->(x) | x]) > 0 RETURN p.name",
               {{"Remy"}});
}

TEST_F(ValueAfterAPatternComprehensionTest, readsTheValueInTwoComprehensions) {
    expectRows("MATCH (n:Person {name: 'Remy'}) WITH {person: n} AS m WITH m.person AS p "
               "RETURN size([(p)-[:KNOWS_WELL]->(x) | x]), size([(p)-[:INTERESTED_IN]->(i) | i])",
               {{"1", "3"}});
}

TEST_F(ValueAfterAPatternComprehensionTest, readsTheNodeInTheProjectionAndTheValueAfterIt) {
    expectRows("MATCH (n:Person {name: 'Remy'}) WITH {person: n} AS m WITH m.person AS p "
               "RETURN [(p)-[:KNOWS_WELL]->(x) | p.name + '->' + x.name], p.name",
               {{"Remy->Adam", "Remy"}});
}

TEST_F(ValueAfterAPatternComprehensionTest, setsAPropertyFromTheNodeTheProjectionReads) {
    runWrite("MATCH (n:Person {name: 'Remy'}) WITH {person: n} AS m WITH m.person AS p "
             "MATCH (q:Person {name: 'Luc'}) SET q.friend = head([(p)-[:KNOWS_WELL]->(f) | p.name + '->' + f.name])");

    expectRows("MATCH (q:Person {name: 'Luc'}) RETURN q.friend", {{"Remy->Adam"}});
}
