#include <gtest/gtest.h>

#include <string>
#include <string_view>
#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace db;
using namespace turing::test;

class ScanEdgesByEndpointLabelsTest : public CallV3Test {
protected:
    void explain(std::string_view query, bool write, std::string& program) {
        const std::string explained = std::string("EXPLAIN (db) ") + std::string(query);

        StringRowSink sink;
        if (write) {
            runWrite(explained, sink);
        } else {
            runQuery(explained, sink);
        }

        for (const StringRowSink::Row& row : sink.getRows()) {
            if (row.front() == "db") {
                program = row.back();
            }
        }
    }

    void expectScan(std::string_view query, std::string_view scan, bool write = false) {
        std::string program;
        explain(query, write, program);

        EXPECT_NE(program.find(scan), std::string::npos) << program;
        EXPECT_EQ(program.find("db.check_label_constraint"), std::string::npos) << program;
    }

    void expectRows(std::string_view query, const std::vector<StringRowSink::Row>& expected) {
        StringRowSink sink;
        runQuery(query, sink);

        std::vector<StringRowSink::Row> rows;
        sink.sortedRows(rows);

        EXPECT_EQ(rows, expected) << query;
    }

    void expectWrittenRows(std::string_view query, const std::vector<StringRowSink::Row>& expected) {
        StringRowSink sink;
        runWrite(query, sink);

        std::vector<StringRowSink::Row> rows;
        sink.sortedRows(rows);

        EXPECT_EQ(rows, expected) << query;
    }
};

// Remy is the one SoftwareEngineering node with an edge to an Exotic one.
TEST_F(ScanEdgesByEndpointLabelsTest, labelledOutHopReadsTheSourceIndex) {
    const std::string_view query = "MATCH (n:SoftwareEngineering)-->(m:Exotic) RETURN n.name, m.name";

    expectScan(query, "db.scan_out_edges_by_label_src_tgt([\"SoftwareEngineering\"], [\"Exotic\"])");
    expectRows(query, {{"Remy", "Eighties"}, {"Remy", "Ghosts"}});
}

TEST_F(ScanEdgesByEndpointLabelsTest, labelledInHopReadsTheTargetIndex) {
    const std::string_view query = "MATCH (n:Exotic)<--(m:Founder) RETURN n.name, m.name";

    expectScan(query, "db.scan_in_edges_by_label_src_tgt([\"Founder\"], [\"Exotic\"])");
    expectRows(query, {{"Eighties", "Remy"}, {"Ghosts", "Remy"}});
}

// The pattern labels the target, so the scan keeps reading the target's index and checks
// the source the WHERE labels.
TEST_F(ScanEdgesByEndpointLabelsTest, whereOnTheSourceOfATargetLabelledScanKeepsTheTargetIndex) {
    const std::string_view query = "MATCH (n)-->(m:Interest) WHERE n:Bioinformatics RETURN n.name, m.name";

    expectScan(query, "db.scan_in_edges_by_label_src_tgt([\"Bioinformatics\"], [\"Interest\"])");
    expectRows(query,
               {{"Adam", "Bio"},
                {"Adam", "Cooking"},
                {"Martina", "Cooking"},
                {"Maxime", "Bio"},
                {"Maxime", "Padel"}});
}

TEST_F(ScanEdgesByEndpointLabelsTest, whereOnTheTargetOfASourceLabelledInHopKeepsTheSourceIndex) {
    const std::string_view query = "MATCH (n)<--(m:Exotic) WHERE n:Person RETURN n.name, m.name";

    expectScan(query, "db.scan_out_edges_by_label_src_tgt([\"Exotic\"], [\"Person\"])");
    expectRows(query, {{"Remy", "Ghosts"}});
}

// The WHERE label lands on the end the scan already reads by its labels.
TEST_F(ScanEdgesByEndpointLabelsTest, whereOnTheLabelledSourceWidensItsLabels) {
    const std::string_view query = "MATCH (n:Person)-->(m:Interest) WHERE n:Founder RETURN n.name, m.name";

    expectScan(query, "db.scan_out_edges_by_label_src_tgt([\"Person\", \"Founder\"], [\"Interest\"])");
    expectRows(query,
               {{"Adam", "Bio"},
                {"Adam", "Cooking"},
                {"Remy", "Computers"},
                {"Remy", "Eighties"},
                {"Remy", "Ghosts"}});
}

TEST_F(ScanEdgesByEndpointLabelsTest, whereOnBothEndsOfASourceLabelledScan) {
    const std::string_view query = "MATCH (n:Person)-->(m) WHERE n:Founder AND m:Exotic RETURN n.name, m.name";

    expectScan(query, "db.scan_out_edges_by_label_src_tgt([\"Person\", \"Founder\"], [\"Exotic\"])");
    expectRows(query, {{"Remy", "Eighties"}, {"Remy", "Ghosts"}});
}

TEST_F(ScanEdgesByEndpointLabelsTest, countsTheEdgesBetweenTheLabels) {
    const std::string_view query = "MATCH (n:Person)-->(m:Interest) RETURN count(*)";

    expectScan(query, "db.scan_out_edges_by_label_src_tgt([\"Person\"], [\"Interest\"])");
    expectRows(query, {{"15"}});
}

TEST_F(ScanEdgesByEndpointLabelsTest, hopsOnFromTheLabelledTarget) {
    const std::string_view query = "MATCH (n:Founder)-->(m:Exotic)-->(k) RETURN n.name, m.name, k.name";

    expectScan(query, "db.scan_out_edges_by_label_src_tgt([\"Founder\"], [\"Exotic\"])");
    expectRows(query, {{"Remy", "Ghosts", "Remy"}});
}

// A disjunction of labels is no label set an index holds, so it stays a check.
TEST_F(ScanEdgesByEndpointLabelsTest, disjunctionOnTheTargetStaysACheck) {
    const std::string_view query = "MATCH (n:Person)-->(m:Interest) WHERE m:Exotic OR m:SleepDisturber RETURN n.name, m.name";

    std::string program;
    explain(query, false, program);
    EXPECT_NE(program.find("db.scan_out_edges_by_label_src(["), std::string::npos) << program;
    EXPECT_NE(program.find("db.check_label_constraint"), std::string::npos) << program;

    expectRows(query, {{"Luc", "Animals"}, {"Remy", "Eighties"}, {"Remy", "Ghosts"}});
}

// Chess is no Exotic and Bo no Founder, so of the edges the CREATE wrote only Ana's to
// Opera is kept beside the two committed ones.
TEST_F(ScanEdgesByEndpointLabelsTest, keepsTheCreatedEdgesBetweenTheLabels) {
    const std::string_view query = "CREATE (a:Person:Founder {name: 'Ana'})-[:INTERESTED_IN]->(o:Interest:Exotic {name: 'Opera'}), "
                                   "(a)-[:INTERESTED_IN]->(:Interest {name: 'Chess'}), "
                                   "(:Person {name: 'Bo'})-[:INTERESTED_IN]->(o) "
                                   "WITH count(*) AS created MATCH (n:Founder)-->(m:Exotic) RETURN n.name, m.name";

    expectScan(query, "db.scan_out_edges_by_label_src_tgt([\"Founder\"], [\"Exotic\"])", true);
    expectWrittenRows(query, {{"Ana", "Opera"}, {"Remy", "Eighties"}, {"Remy", "Ghosts"}});
}

TEST_F(ScanEdgesByEndpointLabelsTest, keepsTheCreatedEdgesBetweenTheLabelsOffTheTargetIndex) {
    const std::string_view query = "CREATE (a:Person:Founder {name: 'Ana'})-[:INTERESTED_IN]->(o:Interest:Exotic {name: 'Opera'}), "
                                   "(a)-[:INTERESTED_IN]->(:Interest {name: 'Chess'}), "
                                   "(:Person {name: 'Bo'})-[:INTERESTED_IN]->(o) "
                                   "WITH count(*) AS created MATCH (m:Exotic)<--(n:Founder) RETURN n.name, m.name";

    expectScan(query, "db.scan_in_edges_by_label_src_tgt([\"Founder\"], [\"Exotic\"])", true);
    expectWrittenRows(query, {{"Ana", "Opera"}, {"Remy", "Eighties"}, {"Remy", "Ghosts"}});
}
