#include <gtest/gtest.h>

#include <algorithm>
#include <string>
#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

namespace {

using Rows = std::vector<StringRowSink::Row>;

}

// simpledb's KNOWS_WELL edges are Remy -> Adam, Adam -> Remy and Ghosts -> Remy. Of its eight
// people only Remy and Adam leave by one, so the other six come back with null groups. Of
// Remy's and Adam's interests only Adam's include Cooking.
class QuantifiedGroupAcrossScopesTest : public CallV3Test {
protected:
    void expectSortedRows(const std::string& query, Rows expected) {
        StringRowSink sink;
        runQuery(query, sink);

        Rows rows = sink.getRows();
        std::sort(rows.begin(), rows.end());
        std::sort(expected.begin(), expected.end());

        EXPECT_EQ(rows, expected) << query;
    }
};

TEST_F(QuantifiedGroupAcrossScopesTest, bindsTheGroupsOfAOneHopBody) {
    expectSortedRows("MATCH (n:Person) OPTIONAL MATCH (n)((x)-[:KNOWS_WELL]->(y)){1,2}(c) "
                     "RETURN n.name, [v IN x | v.name], [v IN y | v.name], c.name",
                     {{"Remy", "Remy", "Adam", "Adam"},
                      {"Remy", "Remy, Adam", "Adam, Remy", "Remy"},
                      {"Adam", "Adam", "Remy", "Remy"},
                      {"Adam", "Adam, Remy", "Remy, Adam", "Adam"},
                      {"Maxime", "null", "null", "null"},
                      {"Luc", "null", "null", "null"},
                      {"Martina", "null", "null", "null"},
                      {"Suhas", "null", "null", "null"},
                      {"Cyrus", "null", "null", "null"},
                      {"Doruk", "null", "null", "null"}});
}

TEST_F(QuantifiedGroupAcrossScopesTest, bindsTheGroupsOfASeveralHopBody) {
    expectSortedRows("MATCH (n:Person) OPTIONAL MATCH (n)((x)-[k:KNOWS_WELL]->(y)-[:KNOWS_WELL]->(z)){1,1}(c) "
                     "RETURN n.name, [v IN y | v.name], [e IN k | e.name], c.name",
                     {{"Remy", "Adam", "Remy -> Adam", "Remy"},
                      {"Adam", "Remy", "Adam -> Remy", "Adam"},
                      {"Maxime", "null", "null", "null"},
                      {"Luc", "null", "null", "null"},
                      {"Martina", "null", "null", "null"},
                      {"Suhas", "null", "null", "null"},
                      {"Cyrus", "null", "null", "null"},
                      {"Doruk", "null", "null", "null"}});
}

TEST_F(QuantifiedGroupAcrossScopesTest, carriesTheGroupsPastALaterOptionalMatch) {
    expectSortedRows("MATCH (n:Person {name: 'Remy'}) ((x)-[:KNOWS_WELL]->(y)){1,2} (c) "
                     "OPTIONAL MATCH (c)-[:INTERESTED_IN]->(i:Interest {name: 'Cooking'}) "
                     "RETURN [v IN y | v.name], c.name, i.name",
                     {{"Adam", "Adam", "Cooking"},
                      {"Adam, Remy", "Remy", "null"}});
}

TEST_F(QuantifiedGroupAcrossScopesTest, importsAGroupIntoACallSubquery) {
    expectSortedRows("MATCH (n:Person {name: 'Remy'}) ((x)-[:KNOWS_WELL]->(y)){1,2} (c) "
                     "CALL { WITH y RETURN size(y) AS s } "
                     "RETURN [v IN y | v.name], s",
                     {{"Adam", "1"},
                      {"Adam, Remy", "2"}});
}

TEST_F(QuantifiedGroupAcrossScopesTest, carriesTheGroupsPastACallSubquery) {
    expectSortedRows("MATCH (n:Person {name: 'Remy'}) ((x)-[:KNOWS_WELL]->(y)){1,2} (c) "
                     "CALL { WITH c MATCH (c)-[:INTERESTED_IN]->(i:Interest {name: 'Cooking'}) RETURN i } "
                     "RETURN [v IN y | v.name], i.name",
                     {{"Adam", "Cooking"}});
}

TEST_F(QuantifiedGroupAcrossScopesTest, carriesTheGroupsPastAWithStar) {
    expectSortedRows("MATCH (n:Person {name: 'Remy'}) ((x)-[k:KNOWS_WELL]->(y)-[:KNOWS_WELL]->(z)){1,1} (c) "
                     "WITH * RETURN [v IN y | v.name], [e IN k | e.name], [v IN z | v.name]",
                     {{"Adam", "Remy -> Adam", "Remy"}});
}

TEST_F(QuantifiedGroupAcrossScopesTest, carriesTheGroupsOfAWalkSeededFromItsEndPastAWithStar) {
    expectSortedRows("MATCH (c:Person {name: 'Remy'}) MATCH (n)((x)-[:KNOWS_WELL]->(y)){1,1}(c) "
                     "WITH * RETURN n.name, [v IN x | v.name], [v IN y | v.name]",
                     {{"Adam", "Adam", "Remy"},
                      {"Ghosts", "Ghosts", "Remy"}});
}
