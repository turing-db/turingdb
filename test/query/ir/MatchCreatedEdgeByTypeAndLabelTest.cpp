#include <gtest/gtest.h>

#include <string>
#include <string_view>
#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace db;
using namespace turing::test;

class MatchCreatedEdgeByTypeAndLabelTest : public CallV3Test {
protected:
    void expectPlanHolds(std::string_view query, std::string_view opName) {
        StringRowSink sink;
        runWrite(std::string("EXPLAIN (db) ") + std::string(query), sink);

        std::string_view program;
        for (const StringRowSink::Row& row : sink.getRows()) {
            if (row.front() == "db") {
                program = row.back();
            }
        }

        EXPECT_NE(program.find(opName), std::string_view::npos) << program;
    }
};

// Cy is reached over the wrong type and Dee is no Person, so only Bo is kept.
TEST_F(MatchCreatedEdgeByTypeAndLabelTest, walksACreatedEdgeOfTheTypeArrivingAtTheLabels) {
    const std::string_view query = "CREATE (a:Person {name: 'Ana'})-[:KNOWS_WELL]->(:Person {name: 'Bo'}), "
                                   "(a)-[:INTERESTED_IN]->(:Person {name: 'Cy'}), "
                                   "(a)-[:KNOWS_WELL]->(:Interest {name: 'Dee'}) "
                                   "WITH a MATCH (a)-[:KNOWS_WELL]->(m:Person) RETURN m.name";

    expectPlanHolds(query, "db.get_out_edges_by_type_and_label");

    StringRowSink sink;
    runWrite(query, sink);

    const std::vector<StringRowSink::Row> expected {{"Bo"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(MatchCreatedEdgeByTypeAndLabelTest, walksACreatedEdgeOfTheTypeBackwardsFromTheLabels) {
    const std::string_view query = "CREATE (:Person {name: 'Ana'})-[:KNOWS_WELL]->(b:Person {name: 'Bo'}), "
                                   "(:Interest {name: 'Ivy'})-[:KNOWS_WELL]->(b), "
                                   "(:Person {name: 'Cy'})-[:INTERESTED_IN]->(b) "
                                   "WITH b MATCH (b)<-[:KNOWS_WELL]-(m:Person) RETURN m.name";

    expectPlanHolds(query, "db.get_in_edges_by_type_and_label");

    StringRowSink sink;
    runWrite(query, sink);

    const std::vector<StringRowSink::Row> expected {{"Ana"}};
    EXPECT_EQ(sink.getRows(), expected);
}

// Remy's committed INTERESTED_IN edges arrive at Computers, Eighties and Ghosts, of which
// Eighties and Ghosts are Exotic; the pending edge to Bo is kept beside them.
TEST_F(MatchCreatedEdgeByTypeAndLabelTest, walksACreatedEdgeBesideTheCommittedOnes) {
    const std::string_view query = "MATCH (p:Person {name: 'Remy'}) "
                                   "CREATE (p)-[:INTERESTED_IN]->(:Interest:Exotic {name: 'Bo'}) "
                                   "WITH p MATCH (p)-[:INTERESTED_IN]->(m:Exotic) RETURN m.name";

    expectPlanHolds(query, "db.get_out_edges_by_type_and_label");

    StringRowSink sink;
    runWrite(query, sink);

    std::vector<StringRowSink::Row> rows;
    sink.sortedRows(rows);

    const std::vector<StringRowSink::Row> expected {{"Bo"}, {"Eighties"}, {"Ghosts"}};
    EXPECT_EQ(rows, expected);
}
