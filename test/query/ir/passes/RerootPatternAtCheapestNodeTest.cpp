#include <gtest/gtest.h>

#include <algorithm>
#include <string>
#include <string_view>
#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

namespace {

using Rows = std::vector<StringRowSink::Row>;

bool contains(std::string_view text, std::string_view needle) {
    return text.find(needle) != std::string_view::npos;
}

// The line of the op the program's rows come off first: a scan, or a fetch of IDs
std::string_view firstSourceOf(std::string_view program) {
    const size_t scan = program.find("db.scan");
    const size_t fetch = program.find("db.fetch_nodes");
    const size_t source = std::min(scan, fetch);
    if (source == std::string_view::npos) {
        return {};
    }

    const size_t lineStart = program.rfind('\n', source) + 1;
    const size_t lineEnd = program.find('\n', source);

    return program.substr(lineStart, lineEnd - lineStart);
}

// Remy's interests are Ghosts, Computers and Eighties, and Luc shares Computers
const Rows sharingRemysInterests {
    {"Luc", "Computers"},
    {"Remy", "Computers"},
    {"Remy", "Eighties"},
    {"Remy", "Ghosts"},
};

}

class RerootPatternAtCheapestNodeTest : public CallV3Test {
protected:
    void dbProgramOf(std::string_view query, std::string& program) {
        StringRowSink sink;
        runQuery(std::string("EXPLAIN (db) ") + std::string(query), sink);

        ASSERT_EQ(sink.getRows().size(), 1u);
        program = sink.getRows().front().back();
    }

    void expectWalkedFrom(std::string_view query, const std::vector<std::string_view>& firstSource, const Rows& expected) {
        std::string program;
        dbProgramOf(query, program);

        const std::string_view source = firstSourceOf(program);
        for (const std::string_view part : firstSource) {
            EXPECT_TRUE(contains(source, part)) << part << " in " << program;
        }

        EXPECT_FALSE(contains(program, "db.fetch_nodes")) << program;
        EXPECT_FALSE(contains(program, "db.cross_product")) << program;
        EXPECT_FALSE(contains(program, "db.check_edge_type_constraint")) << program;

        StringRowSink sink;
        runQuery(query, sink);

        Rows rows;
        sink.sortedRows(rows);

        EXPECT_EQ(rows, expected) << query;
    }
};

TEST_F(RerootPatternAtCheapestNodeTest, joinsTwoPatternsFromTheFilteredOne) {
    expectWalkedFrom("MATCH (p:Person)-[:INTERESTED_IN]->(i1:Interest), (q:Person)-[:INTERESTED_IN]->(i2:Interest) "
                     "WHERE i1 = i2 AND p.name = 'Remy' RETURN q.name, i1.name",
                     {"db.scan_nodes_by_property_value", "Remy"},
                     sharingRemysInterests);
}

TEST_F(RerootPatternAtCheapestNodeTest, joinsTwoPatternsFromTheFilteredOneWhicheverSideTheEqualityNames) {
    expectWalkedFrom("MATCH (p:Person)-[:INTERESTED_IN]->(i1:Interest), (q:Person)-[:INTERESTED_IN]->(i2:Interest) "
                     "WHERE i2 = i1 AND p.name = 'Remy' RETURN q.name, i1.name",
                     {"db.scan_nodes_by_property_value", "Remy"},
                     sharingRemysInterests);
}

TEST_F(RerootPatternAtCheapestNodeTest, walksASharedNodeFromTheFilteredPattern) {
    expectWalkedFrom("MATCH (q:Person)-[:INTERESTED_IN]->(i:Interest), (p:Person)-[:INTERESTED_IN]->(i) "
                     "WHERE p.name = 'Remy' RETURN q.name, i.name",
                     {"db.scan_nodes_by_property_value", "Remy"},
                     sharingRemysInterests);
}

TEST_F(RerootPatternAtCheapestNodeTest, walksAPathFromItsFilteredEnd) {
    expectWalkedFrom("MATCH (q:Person)-[:INTERESTED_IN]->(i:Interest)<-[:INTERESTED_IN]-(p:Person {name: 'Remy'}) "
                     "RETURN q.name, i.name",
                     {"db.scan_nodes_by_property_value", "Remy"},
                     sharingRemysInterests);
}

TEST_F(RerootPatternAtCheapestNodeTest, walksAPathFromItsMiddle) {
    expectWalkedFrom("MATCH (q)-[:INTERESTED_IN]->(i:Interest {name: 'Gym'})<-[:INTERESTED_IN]-(p) "
                     "RETURN q.name, p.name",
                     {"db.scan_nodes_by_property_value", "Gym"},
                     {
                         {"Cyrus", "Cyrus"},
                         {"Cyrus", "Doruk"},
                         {"Cyrus", "Suhas"},
                         {"Doruk", "Cyrus"},
                         {"Doruk", "Doruk"},
                         {"Doruk", "Suhas"},
                         {"Suhas", "Cyrus"},
                         {"Suhas", "Doruk"},
                         {"Suhas", "Suhas"},
                     });
}

// Two Founders against the 18 nodes the pattern would otherwise start from, edge scan
// included, whichever side of the equality names the matched one
TEST_F(RerootPatternAtCheapestNodeTest, walksFromTheMatchedSeedWhicheverSideTheEqualityNames) {
    const Rows expected {
        {"Adam", "Bio"},
        {"Adam", "Cooking"},
        {"Adam", "Remy"},
        {"Remy", "Adam"},
        {"Remy", "Computers"},
        {"Remy", "Eighties"},
        {"Remy", "Ghosts"},
    };

    expectWalkedFrom("MATCH (s:Founder) WITH s MATCH (n)-[e]->(m) WHERE n = s RETURN s.name, m.name", {"Founder"}, expected);
    expectWalkedFrom("MATCH (s:Founder) WITH s MATCH (n)-[e]->(m) WHERE s = n RETURN s.name, m.name", {"Founder"}, expected);
}
