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

// A read after an op whose region reads the same of the same rows shares one read with it
class ReadsAfterARegionTest : public CallV3Test {
protected:
    void expectReads(std::string_view query, std::string_view read, size_t expectedReads) {
        StringRowSink explained;
        runQuery(std::string("EXPLAIN (db) ") + std::string(query), explained);

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
TEST_F(ReadsAfterARegionTest, readsTheLabelSetOnceAfterACaseBranch) {
    const std::string_view query = "MATCH (n) RETURN CASE WHEN n.age > 3 THEN n:Founder END, n:Person";
    expectReads(query, "db.get_node_label_set(", 1);

    Rows expected;
    expected.insert(expected.end(), 10, {"null", "false"});
    expected.insert(expected.end(), 6, {"null", "true"});
    expected.insert(expected.end(), 2, {"true", "true"});
    expectRows(query, expected);
}

TEST_F(ReadsAfterARegionTest, readsTheEdgeTypesOnceAfterACaseBranch) {
    const std::string_view query = "MATCH (:Person {name: 'Remy'})-[e]->() WITH DISTINCT e "
                                   "RETURN e.name, CASE WHEN e.duration > 15 THEN e:KNOWS_WELL END, e:INTERESTED_IN";
    expectReads(query, "db.get_edge_types(", 1);

    expectRows(query, {
        {"Remy -> Adam", "true", "false"},
        {"Remy -> Computers", "null", "true"},
        {"Remy -> Eighties", "false", "true"},
        {"Remy -> Ghosts", "false", "true"},
    });
}

// Maxime has a dob and no PhD, Martina a PhD and no dob
TEST_F(ReadsAfterARegionTest, readsAPropertyOnceAfterACaseBranch) {
    const std::string_view query = "MATCH (n:Person) RETURN n.name, CASE WHEN n.hasPhD THEN n.dob END, n.dob";
    expectReads(query, ", \"dob\")", 1);

    expectRows(query, {
        {"Adam", "18/08", "18/08"},
        {"Cyrus", "null", "null"},
        {"Doruk", "null", "null"},
        {"Luc", "28/05", "28/05"},
        {"Martina", "null", "null"},
        {"Maxime", "null", "24/07"},
        {"Remy", "18/01", "18/01"},
        {"Suhas", "null", "null"},
    });
}

TEST_F(ReadsAfterARegionTest, readsAPropertyOnceAfterACoalesceArgument) {
    const std::string_view query = "MATCH (n:Person) RETURN n.name, coalesce(n.age, n.dob), n.dob";
    expectReads(query, ", \"dob\")", 1);

    expectRows(query, {
        {"Adam", "32", "18/08"},
        {"Cyrus", "null", "null"},
        {"Doruk", "null", "null"},
        {"Luc", "28/05", "28/05"},
        {"Martina", "null", "null"},
        {"Maxime", "24/07", "24/07"},
        {"Remy", "32", "18/01"},
        {"Suhas", "null", "null"},
    });
}

TEST_F(ReadsAfterARegionTest, readsAPropertyOnceAfterNestedCases) {
    const std::string_view query = "MATCH (n:Person) RETURN n.name, "
                                   "CASE WHEN n.isFrench THEN CASE WHEN n.hasPhD THEN n.dob END END, n.dob";
    expectReads(query, ", \"dob\")", 1);

    expectRows(query, {
        {"Adam", "18/08", "18/08"},
        {"Cyrus", "null", "null"},
        {"Doruk", "null", "null"},
        {"Luc", "28/05", "28/05"},
        {"Martina", "null", "null"},
        {"Maxime", "null", "24/07"},
        {"Remy", "18/01", "18/01"},
        {"Suhas", "null", "null"},
    });
}

// Remy's out-edges to Ghosts and Computers are expert, to Eighties moderate
TEST_F(ReadsAfterARegionTest, readsAnEdgePropertyOnceAfterACaseBranch) {
    const std::string_view query = "MATCH (:Person {name: 'Remy'})-[e]->() "
                                   "RETURN e.name, CASE WHEN e.duration > 15 THEN e.proficiency END, e.proficiency";
    expectReads(query, ", \"proficiency\")", 1);

    expectRows(query, {
        {"Remy -> Adam", "null", "null"},
        {"Remy -> Computers", "null", "expert"},
        {"Remy -> Eighties", "moderate", "moderate"},
        {"Remy -> Ghosts", "expert", "expert"},
    });
}

// Adam, Maxime and Martina work in Bioinformatics
TEST_F(ReadsAfterARegionTest, readsTheLabelSetOnceAfterAListPredicate) {
    const std::string_view query = "MATCH (n:Person) RETURN n.name, "
                                   "any(x IN [1, 2] WHERE x > 1 AND n:Bioinformatics), n:Founder";
    expectReads(query, "db.get_node_label_set(", 1);

    expectRows(query, {
        {"Adam", "true", "true"},
        {"Cyrus", "false", "false"},
        {"Doruk", "false", "false"},
        {"Luc", "false", "false"},
        {"Martina", "true", "false"},
        {"Maxime", "true", "false"},
        {"Remy", "false", "true"},
        {"Suhas", "false", "false"},
    });
}

// Remy has 4 out-edges and Adam 3
TEST_F(ReadsAfterARegionTest, readsTheLabelSetOnceAfterACountSubquery) {
    const std::string_view query = "MATCH (n:Person) RETURN n.name, COUNT { MATCH (n)-->(m) WHERE n:Founder }, n:Founder";
    expectReads(query, "db.get_node_label_set(", 1);

    expectRows(query, {
        {"Adam", "3", "true"},
        {"Cyrus", "0", "false"},
        {"Doruk", "0", "false"},
        {"Luc", "0", "false"},
        {"Martina", "0", "false"},
        {"Maxime", "0", "false"},
        {"Remy", "4", "true"},
        {"Suhas", "0", "false"},
    });
}

// The inner CASE and the test beside it share the read in the outer CASE's branch, and
// that read shares the one after the outer CASE. Remy, Adam, Maxime and Luc are French,
// and Maxime the only one of them with no PhD.
TEST_F(ReadsAfterARegionTest, readsTheLabelSetOnceAfterARegionInsideARegion) {
    const std::string_view query = "MATCH (n:Person) RETURN n.name, "
                                   "CASE WHEN n.isFrench THEN CASE WHEN n.hasPhD THEN n:Founder END = n:Bioinformatics END, "
                                   "n:SoftwareEngineering";
    expectReads(query, "db.get_node_label_set(", 1);

    expectRows(query, {
        {"Adam", "true", "false"},
        {"Cyrus", "null", "true"},
        {"Doruk", "null", "false"},
        {"Luc", "true", "true"},
        {"Martina", "null", "false"},
        {"Maxime", "null", "false"},
        {"Remy", "false", "true"},
        {"Suhas", "null", "true"},
    });
}
