#include <gtest/gtest.h>

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

std::string withConstant(std::string_view query, std::string_view constant) {
    std::string result(query);
    const size_t at = result.find("$X");
    result.replace(at, 2, constant);

    return result;
}

// A shape a pass rewrites, the op the rewrite leaves in the program, and the rows it binds
struct PassCase {
    std::string_view _match;
    std::string_view _fusedOp;
    size_t _rowCount {0};
};

}

// An op reading constants alone runs over the rows in flight, which no use-def chain shows,
// so a pass that drops, replaces or moves a row column must leave those rows as they were.
// Each pass must still fire, and the constant must still go out once per row.
class ConstantRowsPassesTest : public CallV3Test {
protected:
    void programOf(std::string_view query, std::string& program) {
        StringRowSink sink;
        runQuery(std::string("EXPLAIN (db) ") + std::string(query), sink);

        ASSERT_EQ(sink.getRows().size(), 1u);
        program = sink.getRows().front().back();
    }

    void rowsOf(std::string_view query, Rows& rows) {
        StringRowSink sink;
        runQuery(query, sink);

        sink.sortedRows(rows);
    }

    // MATCH ... RETURN 1 emits the rows count(*) tallies, through the op the pass fuses
    void expectFusedConstantRows(const PassCase& passCase) {
        const std::string match(passCase._match);

        StringRowSink counted;
        runQuery(match + " RETURN count(*)", counted);
        ASSERT_EQ(counted.getRows().size(), 1u);
        EXPECT_EQ(counted.getRows().front().front(), std::to_string(passCase._rowCount)) << match;

        const std::string query = match + " RETURN 1";

        StringRowSink sink;
        runQuery(query, sink);
        EXPECT_EQ(sink.getRows().size(), passCase._rowCount) << query;

        std::string program;
        programOf(query, program);
        EXPECT_TRUE(contains(program, passCase._fusedOp)) << passCase._fusedOp << "\n" << program;
    }

    // The query with a constant that may raise in place of $X emits the rows it does with the
    // literal 1, which is computed once
    void expectRowsOfTheLiteral(std::string_view query) {
        const std::string literal = withConstant(query, "1");
        const std::string raising = withConstant(query, "(size('ab') - 1)");

        Rows expected;
        rowsOf(literal, expected);

        Rows rows;
        rowsOf(raising, rows);

        EXPECT_FALSE(expected.empty()) << literal;
        EXPECT_EQ(rows, expected) << raising;
    }
};

TEST_F(ConstantRowsPassesTest, fusesNodeScansUnderAConstantProjection) {
    const PassCase cases[] = {
        {"MATCH (n:Person)", "db.scan_nodes_by_label", 8},
        {"MATCH (n) WHERE n:Person AND n:Founder", "db.scan_nodes_by_label", 2},
        {"MATCH (n:Person {name: 'Remy'})", "db.scan_nodes_by_property_value", 1},
        {"MATCH (n:Person) WHERE n.name = 'Remy'", "db.scan_nodes_by_property_value", 1},
        {"MATCH (n) WHERE id(n) = 1", "db.const_scan_nodes", 1},
    };

    for (const PassCase& passCase : cases) {
        expectFusedConstantRows(passCase);
    }
}

TEST_F(ConstantRowsPassesTest, fusesEdgeScansUnderAConstantProjection) {
    const PassCase cases[] = {
        {"MATCH (a)-[e]->(b)", "db.scan_edges", 18},
        {"MATCH (a)-[e:KNOWS_WELL]->(b)", "db.scan_edges_by_type", 3},
        {"MATCH (a:Person)-[e:KNOWS_WELL]->(b)", "db.get_out_edges_by_type", 2},
        {"MATCH (a:Person)-[e]->(b)", "db.scan_out_edges_by_label_src", 17},
        {"MATCH (a)-[e]->(b:Interest)", "db.scan_out_edges_by_label_tgt", 15},
        {"MATCH (a {name: 'Remy'})-[e]->(b:Interest)", "db.get_out_edges_by_label", 3},
        {"MATCH (a:Person)-[e:KNOWS_WELL]->(b:Person)", "db.get_out_edges_by_type_and_label", 2},
    };

    for (const PassCase& passCase : cases) {
        expectFusedConstantRows(passCase);
    }
}

TEST_F(ConstantRowsPassesTest, placesFactorsAndReRootsUnderAConstantProjection) {
    const PassCase cases[] = {
        {"MATCH (a:Person), (b:Interest) WHERE b.name = 'Bio'", "db.scan_nodes_by_property_value", 8},
        {"MATCH (a)-->(b:Person {name: 'Remy'})", "db.scan_out_edges_by_label_tgt", 2},
        {"MATCH (a:Interest)<--(b:Person {name: 'Adam'})", "db.scan_in_edges_by_label_src_tgt", 2},
    };

    for (const PassCase& passCase : cases) {
        expectFusedConstantRows(passCase);
    }
}

TEST_F(ConstantRowsPassesTest, fusesUnwoundIDsUnderAConstantProjection) {
    const PassCase cases[] = {
        {"UNWIND [0, 1] AS i MATCH (n) WHERE id(n) = i", "db.const_scan_nodes", 2},
        {"UNWIND range(1, 5) AS i", "db.unwind_range", 5},
        {"UNWIND [[0, 1], [2, 3]] AS r MATCH (a), (b) WHERE a = r[0] AND b = r[1]", "db.fetch_nodes", 2},
        {"WITH [0, 2] AS ss, [1, 3] AS ds UNWIND range(0, size(ss) - 1) AS i "
         "MATCH (a), (b) WHERE a = ss[i] AND b = ds[i]",
         "db.fetch_nodes",
         2},
        {"MATCH (n:Person) WHERE n.age = 32 UNWIND [1, 2] AS x", "db.unwind_const", 4},
    };

    for (const PassCase& passCase : cases) {
        expectFusedConstantRows(passCase);
    }
}

TEST_F(ConstantRowsPassesTest, fusesExplorationsUnderAConstantProjection) {
    const PassCase cases[] = {
        {"MATCH (a:Person)-[e*1..3]->(b:Interest)", "end_labels", 27},
        {"MATCH (a:Person)((x)-[r]->(y:Person)){1,3}(b)", "hop_labels", 4},
        {"MATCH (a:Person)-[e*1..3]->(b), (a)-->(b)", "end_column", 26},
        {"MATCH (a:Person)-[e*1..3]->(b), (b:Interest {name: 'Bio'})", "end_nodes", 4},
        {"MATCH (a:Person)-[e*1..3]->(b {name: 'Bio'})", "end_nodes", 4},
        {"MATCH (a:Person)-[e*1..3]->(b)-->(c {name: 'Bio'})", "end_nodes", 3},
    };

    for (const PassCase& passCase : cases) {
        expectFusedConstantRows(passCase);
    }
}

TEST_F(ConstantRowsPassesTest, fusesAnOptionalHopUnderAConstantProjection) {
    expectFusedConstantRows({"MATCH (a:Person) OPTIONAL MATCH (a)-[:KNOWS_WELL]->(b)", "db.get_out_edges_by_type", 8});
}

TEST_F(ConstantRowsPassesTest, readsARaisingConstantOverTheRowsOfAPattern) {
    expectRowsOfTheLiteral("MATCH (n:Person) WHERE n.age > $X RETURN n.name");
    expectRowsOfTheLiteral("MATCH (n:Person {name: 'Remy'}) WHERE n.age > $X RETURN n.name");
    expectRowsOfTheLiteral("MATCH (a:Person)-[e]->(b:Interest) WHERE a.age > $X RETURN a.name, b.name");
    expectRowsOfTheLiteral("MATCH (a:Person), (b:Interest) WHERE a.age > $X AND b.name = 'Bio' RETURN a.name, b.name");
    expectRowsOfTheLiteral("MATCH (a)-->(b:Person {name: 'Remy'}) WHERE a.age > $X RETURN a.name");
    expectRowsOfTheLiteral("UNWIND [0, 1] AS i MATCH (n) WHERE id(n) = i AND n.age > $X RETURN n.name");
    expectRowsOfTheLiteral("UNWIND [[0, 1], [2, 3]] AS r MATCH (a), (b) WHERE a = r[0] AND b = r[1] AND a.age > $X "
                           "RETURN a.name, b.name");
    expectRowsOfTheLiteral("MATCH (a:Person)-[e*1..3]->(b {name: 'Bio'}) WHERE a.age > $X RETURN a.name");
    expectRowsOfTheLiteral("MATCH (a:Person)-[e*1..3]->(b) WHERE a.age > $X RETURN DISTINCT a.name, b.name");
}

// A raising constant in the WHERE is computed over the walk's rows one for one, so the rows
// the DISTINCT dedups are still a set of the walk's ends
TEST_F(ConstantRowsPassesTest, exploresDistinctlyPastARaisingConstant) {
    const std::string_view query = "MATCH (a:Person)-[e*1..3]->(b) WHERE a.age > (size('ab') - 1) RETURN DISTINCT a.name, b.name";

    std::string program;
    programOf(query, program);

    const size_t exploration = program.find("db.explore_paths");
    ASSERT_NE(exploration, std::string::npos) << program;

    const std::string_view explorationLine = std::string_view(program).substr(exploration, program.find('\n', exploration) - exploration);
    EXPECT_TRUE(contains(explorationLine, " distinct")) << program;
}

TEST_F(ConstantRowsPassesTest, readsARaisingConstantOverTheRowsOfItsRegion) {
    expectRowsOfTheLiteral("MATCH (a:Person)((x)-[r]->(y) WHERE y.age > $X){1,3}(b) RETURN a.name, b.name");
    expectRowsOfTheLiteral("MATCH (a:Person)((x)-[r]->(y) WHERE $X > 0){1,3}(b) RETURN a.name, b.name");
    expectRowsOfTheLiteral("MATCH (a:Person) RETURN a.name, size([(a)-->(b) WHERE $X > 0 | b.name]) AS s");
    expectRowsOfTheLiteral("MATCH (a:Person) OPTIONAL MATCH (a)-->(b) WHERE $X > 0 RETURN a.name, b.name");
    expectRowsOfTheLiteral("MATCH (a:Person) RETURN a.name, COUNT { MATCH (a)-->(b) WHERE $X > 0 } AS c");
    expectRowsOfTheLiteral("MATCH (a:Person) CALL (a) { MATCH (a)-->(b) WHERE $X > 0 RETURN b } RETURN a.name, b.name");
    expectRowsOfTheLiteral("MATCH (a:Person) RETURN a.name, [x IN [1, 2] WHERE x > $X | x] AS l");
    expectRowsOfTheLiteral("MATCH (a:Person) RETURN a.name, CASE WHEN a.age > 30 THEN $X ELSE 0 END AS c");
    expectRowsOfTheLiteral("MATCH (a:Person) RETURN a.name AS n UNION ALL MATCH (b:Interest) WHERE $X > 0 RETURN b.name AS n");
}
