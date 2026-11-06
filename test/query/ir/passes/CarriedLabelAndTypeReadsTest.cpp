#include <gtest/gtest.h>

#include <stddef.h>

#include <string>
#include <string_view>
#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

namespace {

using Rows = std::vector<StringRowSink::Row>;

size_t countOccurrences(std::string_view text, std::string_view needle) {
    size_t count = 0;
    size_t position = text.find(needle);
    while (position != std::string_view::npos) {
        count++;
        position = text.find(needle, position + needle.size());
    }

    return count;
}

}

class CarriedLabelAndTypeReadsTest : public CallV3Test {
protected:
    void expectReads(std::string_view query, std::string_view read, size_t expectedReads) {
        StringRowSink explained;
        runQuery(std::string("EXPLAIN (nl) ") + std::string(query), explained);

        const Rows& rows = explained.getRows();
        ASSERT_EQ(rows.size(), 1u) << query;

        const std::string& program = rows.front().back();
        EXPECT_EQ(countOccurrences(program, read), expectedReads) << query << "\n" << program;
    }

    void expectRows(std::string_view query, const Rows& expected) {
        StringRowSink sink;
        runQuery(query, sink);

        Rows rows;
        sink.sortedRows(rows);

        EXPECT_EQ(rows, expected) << query;
    }
};

// Remy and Adam are the two Founders and the only nodes with an age. The 6 other people
// and the 10 interests have no age, so the CASE is null for them.
TEST_F(CarriedLabelAndTypeReadsTest, readsTheLabelSetOnceIntoACaseBranch) {
    const std::string_view query = "MATCH (n) RETURN n:Person, CASE WHEN n.age > 3 THEN n:Founder END";
    expectReads(query, "nl.get_node_label_set(", 1);

    Rows expected;
    expected.insert(expected.end(), 10, {"false", "null"});
    expected.insert(expected.end(), 6, {"true", "null"});
    expected.insert(expected.end(), 2, {"true", "true"});
    expectRows(query, expected);
}

// Remy, Adam, Maxime and Luc are French. Adam and Maxime work in Bioinformatics, Remy and
// Luc in SoftwareEngineering.
TEST_F(CarriedLabelAndTypeReadsTest, testsTheLabelSetsOfTheRowsTheConditionKeeps) {
    const std::string_view query = "MATCH (n:Person) RETURN n.name, n:Founder, "
                                   "CASE WHEN n.isFrench THEN n:Bioinformatics END";
    expectReads(query, "nl.get_node_label_set(", 1);

    expectRows(query, {
        {"Adam", "true", "true"},
        {"Cyrus", "false", "null"},
        {"Doruk", "false", "null"},
        {"Luc", "false", "false"},
        {"Martina", "false", "null"},
        {"Maxime", "false", "true"},
        {"Remy", "true", "false"},
        {"Suhas", "false", "null"},
    });
}

// Bio, Cooking and Padel are the interests with no isReal, and Eighties the one whose
// isReal is false.
TEST_F(CarriedLabelAndTypeReadsTest, readsTheLabelSetOnceIntoACoalesceArgument) {
    const std::string_view query = "MATCH (n) RETURN n.name, n:Founder, coalesce(n.isReal, n:Person)";
    expectReads(query, "nl.get_node_label_set(", 1);

    expectRows(query, {
        {"Adam", "true", "true"},
        {"Animals", "false", "true"},
        {"Bio", "false", "false"},
        {"Computers", "false", "true"},
        {"Cooking", "false", "false"},
        {"Cyrus", "false", "true"},
        {"Doruk", "false", "true"},
        {"Eighties", "false", "false"},
        {"Ghosts", "false", "true"},
        {"Gym", "false", "true"},
        {"JiuJitsu", "false", "true"},
        {"Luc", "false", "true"},
        {"Martina", "false", "true"},
        {"Maxime", "false", "true"},
        {"Padel", "false", "false"},
        {"Remy", "true", "true"},
        {"Suhas", "false", "true"},
        {"Travel", "false", "true"},
    });
}

// Adam, Maxime and Martina work in Bioinformatics.
TEST_F(CarriedLabelAndTypeReadsTest, readsTheLabelSetOnceIntoAListPredicate) {
    const std::string_view query = "MATCH (n:Person) RETURN n.name, n:Founder, "
                                   "any(x IN [1, 2] WHERE x > 1 AND n:Bioinformatics)";
    expectReads(query, "nl.get_node_label_set(", 1);

    expectRows(query, {
        {"Adam", "true", "true"},
        {"Cyrus", "false", "false"},
        {"Doruk", "false", "false"},
        {"Luc", "false", "false"},
        {"Martina", "false", "true"},
        {"Maxime", "false", "true"},
        {"Remy", "true", "false"},
        {"Suhas", "false", "false"},
    });
}

// The first filter keeps the 8 people and Eighties, the second the 4 French people and
// Eighties, which is Exotic.
TEST_F(CarriedLabelAndTypeReadsTest, readsTheLabelSetOnceAcrossAFilter) {
    const std::string_view query = "MATCH (n) WHERE n:Person OR n.isReal = false "
                                   "WITH n WHERE n.isFrench OR n:Exotic RETURN n.name";
    expectReads(query, "nl.get_node_label_set(", 1);

    expectRows(query, {
        {"Adam"},
        {"Eighties"},
        {"Luc"},
        {"Maxime"},
        {"Remy"},
    });
}

// Ghosts is the one real interest with an out-edge, to Remy.
TEST_F(CarriedLabelAndTypeReadsTest, readsTheLabelSetOnceAcrossAHop) {
    const std::string_view query = "MATCH (a)-->(b) WHERE a:Founder OR a.isReal RETURN a.name, a:Person, b.name";
    expectReads(query, "nl.get_node_label_set(", 1);

    expectRows(query, {
        {"Adam", "true", "Bio"},
        {"Adam", "true", "Cooking"},
        {"Adam", "true", "Remy"},
        {"Ghosts", "false", "Remy"},
        {"Remy", "true", "Adam"},
        {"Remy", "true", "Computers"},
        {"Remy", "true", "Eighties"},
        {"Remy", "true", "Ghosts"},
    });
}

// A walk uses no edge twice. Remy reaches Adam, Ghosts, Computers and Eighties in one hop,
// and Remy, Bio, Cooking and Remy again in two; Adam reaches 3 nodes, then 4 through Remy;
// Ghosts reaches Remy, then Remy's 4 targets.
TEST_F(CarriedLabelAndTypeReadsTest, readsTheLabelSetOnceAcrossAVariableLengthWalk) {
    const std::string_view query = "MATCH (n) WHERE n:Founder OR n.isReal "
                                   "MATCH (n)-[*1..2]->(m) RETURN n.name, n:Person, count(m)";
    expectReads(query, "nl.get_node_label_set(", 1);

    expectRows(query, {
        {"Adam", "true", "7"},
        {"Ghosts", "false", "5"},
        {"Remy", "true", "8"},
    });
}

// Suhas, Remy and Maxime are the last 3 of the 8 people and Eighties by name.
TEST_F(CarriedLabelAndTypeReadsTest, readsTheLabelSetOnceAcrossAnOrderByAndALimit) {
    const std::string_view query = "MATCH (n) WHERE n:Person OR n.isReal = false "
                                   "WITH n ORDER BY n.name DESC LIMIT 3 RETURN n.name, n:Founder";
    expectReads(query, "nl.get_node_label_set(", 1);

    expectRows(query, {
        {"Maxime", "false"},
        {"Remy", "true"},
        {"Suhas", "false"},
    });
}

TEST_F(CarriedLabelAndTypeReadsTest, readsTheLabelSetOnceAcrossAnUnwind) {
    const std::string_view query = "MATCH (n) WHERE n:Founder OR n.isReal = false "
                                   "UNWIND [1, 2] AS x RETURN n.name, x, n:Person";
    expectReads(query, "nl.get_node_label_set(", 1);

    expectRows(query, {
        {"Adam", "1", "true"},
        {"Adam", "2", "true"},
        {"Eighties", "1", "false"},
        {"Eighties", "2", "false"},
        {"Remy", "1", "true"},
        {"Remy", "2", "true"},
    });
}

// Remy has 4 out-edges and Adam 3. Ghosts has one, but is not a Person.
TEST_F(CarriedLabelAndTypeReadsTest, readsTheLabelSetOnceIntoACountSubquery) {
    const std::string_view query = "MATCH (n) WHERE n:Founder OR n.isReal "
                                   "RETURN n.name, COUNT { MATCH (n)-->(m) WHERE n:Person }";
    expectReads(query, "nl.get_node_label_set(", 1);

    expectRows(query, {
        {"Adam", "3"},
        {"Animals", "0"},
        {"Computers", "0"},
        {"Ghosts", "0"},
        {"Gym", "0"},
        {"JiuJitsu", "0"},
        {"Remy", "4"},
        {"Travel", "0"},
    });
}

TEST_F(CarriedLabelAndTypeReadsTest, readsTheLabelSetOnceIntoACallBody) {
    const std::string_view query = "MATCH (n) WHERE n:Founder OR n.isReal "
                                   "CALL (n) { MATCH (n)-->(m) WHERE n:Person RETURN m } RETURN n.name, m.name";
    expectReads(query, "nl.get_node_label_set(", 1);

    expectRows(query, {
        {"Adam", "Bio"},
        {"Adam", "Cooking"},
        {"Adam", "Remy"},
        {"Remy", "Adam"},
        {"Remy", "Computers"},
        {"Remy", "Eighties"},
        {"Remy", "Ghosts"},
    });
}

// The ORDER BY and the LIMIT run the body once per row. Adam's first neighbour by name is
// Bio and Remy's is Adam; Ghosts is not a Person.
TEST_F(CarriedLabelAndTypeReadsTest, readsTheLabelSetOnceIntoABodyRunPerRow) {
    const std::string_view query = "MATCH (n) WHERE n:Founder OR n.isReal "
                                   "CALL (n) { MATCH (n)-->(m) WHERE n:Person RETURN m ORDER BY m.name LIMIT 1 } "
                                   "RETURN n.name, m.name";
    expectReads(query, "nl.get_node_label_set(", 1);

    expectRows(query, {
        {"Adam", "Bio"},
        {"Remy", "Adam"},
    });
}

TEST_F(CarriedLabelAndTypeReadsTest, readsTheLabelSetOnceAcrossACall) {
    const std::string_view query = "MATCH (n) WHERE n:Founder OR n.isReal "
                                   "CALL (n) { MATCH (n)-->(m) RETURN m } RETURN n.name, n:Person, m.name";
    expectReads(query, "nl.get_node_label_set(", 1);

    expectRows(query, {
        {"Adam", "true", "Bio"},
        {"Adam", "true", "Cooking"},
        {"Adam", "true", "Remy"},
        {"Ghosts", "false", "Remy"},
        {"Remy", "true", "Adam"},
        {"Remy", "true", "Computers"},
        {"Remy", "true", "Eighties"},
        {"Remy", "true", "Ghosts"},
    });
}

// Eighties has no out-edge, so its row comes back with no m.
TEST_F(CarriedLabelAndTypeReadsTest, readsTheLabelSetOnceAcrossAnOptionalMatch) {
    const std::string_view query = "MATCH (n) WHERE n:Founder OR n.isReal = false "
                                   "OPTIONAL MATCH (n)-->(m:Interest) RETURN n.name, n:Person, m.name";
    expectReads(query, "nl.get_node_label_set(", 1);

    expectRows(query, {
        {"Adam", "true", "Bio"},
        {"Adam", "true", "Cooking"},
        {"Eighties", "false", "null"},
        {"Remy", "true", "Computers"},
        {"Remy", "true", "Eighties"},
        {"Remy", "true", "Ghosts"},
    });
}

TEST_F(CarriedLabelAndTypeReadsTest, readsTheLabelSetOnceOutOfACrossProductFactor) {
    const std::string_view query = "MATCH (a), (b) WHERE (a:Founder OR a.isReal = false) AND b.name = 'Gym' "
                                   "RETURN a.name, a:Person, b.name";
    expectReads(query, "nl.get_node_label_set(", 1);

    expectRows(query, {
        {"Adam", "true", "Gym"},
        {"Eighties", "false", "Gym"},
        {"Remy", "true", "Gym"},
    });
}

// Each of Remy, Adam and Eighties joins the one node of its name.
TEST_F(CarriedLabelAndTypeReadsTest, readsTheLabelSetOnceOutOfAHashJoin) {
    const std::string_view query = "MATCH (a), (b) WHERE (a:Founder OR a.isReal = false) AND a.name = b.name "
                                   "RETURN a.name, a:Person, b.name";
    expectReads(query, "nl.get_node_label_set(", 1);

    expectRows(query, {
        {"Adam", "true", "Adam"},
        {"Eighties", "false", "Eighties"},
        {"Remy", "true", "Remy"},
    });
}

// The 3 KNOWS_WELL edges are the ones lasting 20 or 200. Remy -> Ghosts, Remy -> Eighties
// and Luc -> Animals are the INTERESTED_IN edges lasting over 15; Luc -> Computers lasts 15,
// Martina -> Cooking 10, and the 10 others have no duration.
TEST_F(CarriedLabelAndTypeReadsTest, readsTheEdgeTypesOnceIntoACaseBranch) {
    const std::string_view query = "MATCH ()-[e]->() WITH e "
                                   "RETURN e.name, e:KNOWS_WELL, CASE WHEN e.duration > 15 THEN e:INTERESTED_IN END";
    expectReads(query, "nl.get_edge_types(", 0);

    expectRows(query, {
        {"Adam -> Bio", "false", "null"},
        {"Adam -> Cooking", "false", "null"},
        {"Adam -> Remy", "true", "false"},
        {"Cyrus -> Gym", "false", "null"},
        {"Cyrus -> Travel", "false", "null"},
        {"Doruk -> Gym", "false", "null"},
        {"Ghosts -> Remy", "true", "false"},
        {"Luc -> Animals", "false", "true"},
        {"Luc -> Computers", "false", "null"},
        {"Martina -> Cooking", "false", "null"},
        {"Maxime -> Bio", "false", "null"},
        {"Maxime -> Padel", "false", "null"},
        {"Remy -> Adam", "true", "false"},
        {"Remy -> Computers", "false", "null"},
        {"Remy -> Eighties", "false", "true"},
        {"Remy -> Ghosts", "false", "true"},
        {"Suhas -> Gym", "false", "null"},
        {"Suhas -> Jiutjitsu", "false", "null"},
    });
}

// The 3 KNOWS_WELL edges and Martina -> Cooking, the one edge lasting under 15, sorted by
// name: the INTERESTED_IN edge comes third.
TEST_F(CarriedLabelAndTypeReadsTest, readsTheEdgeTypesOnceAcrossAnOrderByAndALimit) {
    const std::string_view query = "MATCH ()-[e]->() WITH e WHERE e:KNOWS_WELL OR e.duration < 15 "
                                   "WITH e ORDER BY e.name LIMIT 3 RETURN e.name, e:INTERESTED_IN";
    expectReads(query, "nl.get_edge_types(", 0);

    expectRows(query, {
        {"Adam -> Remy", "false"},
        {"Ghosts -> Remy", "false"},
        {"Martina -> Cooking", "true"},
    });
}

// Martina -> Cooking is the one INTERESTED_IN edge lasting under 15, and no KNOWS_WELL edge
// is INTERESTED_IN.
TEST_F(CarriedLabelAndTypeReadsTest, readsTheEdgeTypesOnceIntoABodyRunPerRow) {
    const std::string_view query = "MATCH ()-[e]->() WITH e WHERE e:KNOWS_WELL OR e.duration < 15 "
                                   "CALL (e) { WITH e WHERE e:INTERESTED_IN RETURN e.name AS name ORDER BY name LIMIT 1 } "
                                   "RETURN name";
    expectReads(query, "nl.get_edge_types(", 0);

    expectRows(query, {{"Martina -> Cooking"}});
}
