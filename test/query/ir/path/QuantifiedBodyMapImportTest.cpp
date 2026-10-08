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

// simpledb's KNOWS_WELL edges are Remy -> Adam and Adam -> Remy, both of duration 20, and
// Ghosts -> Remy, of duration 200; Remy and Adam are both 32
class QuantifiedBodyMapImportTest : public CallV3Test {
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

TEST_F(QuantifiedBodyMapImportTest, aRelationshipMapReadsAnOuterVariable) {
    expectSortedRows("MATCH ({name: 'Ghosts'})-[x:KNOWS_WELL]->() "
                     "MATCH (n)((a)-[e:KNOWS_WELL {duration: x.duration}]->(b)){1,1}(m) RETURN n.name, m.name",
                     {{"Ghosts", "Remy"}});
}

TEST_F(QuantifiedBodyMapImportTest, aNodeMapReadsAnOuterVariable) {
    expectSortedRows("MATCH (p:Person {name: 'Adam'}) "
                     "MATCH (n)((a)-[:KNOWS_WELL]->(b {age: p.age})-[:KNOWS_WELL]->(c)){1,1}(m) RETURN n.name, m.name",
                     {{"Remy", "Remy"}, {"Adam", "Adam"}, {"Ghosts", "Adam"}});
}
