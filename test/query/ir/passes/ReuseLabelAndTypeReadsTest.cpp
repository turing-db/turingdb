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

class ReuseLabelAndTypeReadsTest : public CallV3Test {
protected:
    void nlProgram(std::string_view query, std::string& program) {
        StringRowSink explained;
        runQuery(std::string("EXPLAIN (nl) ") + std::string(query), explained);

        const Rows& rows = explained.getRows();
        ASSERT_EQ(rows.size(), 1u) << query;

        program = rows.front().back();
    }

    void expectRows(std::string_view query, const Rows& expected) {
        StringRowSink sink;
        runQuery(query, sink);

        Rows rows;
        sink.sortedRows(rows);

        EXPECT_EQ(rows, expected) << query;
    }
};

// The age between the two label tests keeps the disjunction from fusing into one check.
// Every Person passes, Remy and Adam are the only nodes with an age, and every Founder is a
// Person, so the 8 people come back and none of the 10 interests.
TEST_F(ReuseLabelAndTypeReadsTest, readsTheLabelSetOnceForADisjunctionAroundAProperty) {
    const std::string_view query = "MATCH (n) WHERE n:Person OR n.age > 30 OR n:Founder RETURN n.name";

    std::string program;
    nlProgram(query, program);
    EXPECT_EQ(countOccurrences(program, "nl.get_node_label_set("), 1u) << program;

    expectRows(query, {
        {"Adam"},
        {"Cyrus"},
        {"Doruk"},
        {"Luc"},
        {"Martina"},
        {"Maxime"},
        {"Remy"},
        {"Suhas"},
    });
}

// Remy and Adam are the two Founders, both 32. Eighties is the one interest whose isReal is
// false.
TEST_F(ReuseLabelAndTypeReadsTest, readsTheLabelSetOnceForADisjunctionOfConjunctions) {
    const std::string_view query = "MATCH (n) WHERE (n:Founder AND n.age > 30) OR (n:Interest AND n.isReal = false) "
                                   "RETURN n.name";

    std::string program;
    nlProgram(query, program);
    EXPECT_EQ(countOccurrences(program, "nl.get_node_label_set("), 1u) << program;

    expectRows(query, {
        {"Adam"},
        {"Eighties"},
        {"Remy"},
    });
}

// Maxime, Luc, Martina, Suhas, Cyrus and Doruk are the people who did not found anything.
TEST_F(ReuseLabelAndTypeReadsTest, readsTheLabelSetOnceForAnExclusiveDisjunction) {
    const std::string_view query = "MATCH (n) WHERE n:Person XOR n:Founder RETURN n.name";

    std::string program;
    nlProgram(query, program);
    EXPECT_EQ(countOccurrences(program, "nl.get_node_label_set("), 1u) << program;

    expectRows(query, {
        {"Cyrus"},
        {"Doruk"},
        {"Luc"},
        {"Martina"},
        {"Maxime"},
        {"Suhas"},
    });
}

// 2 Founders, 6 other people and 10 interests.
TEST_F(ReuseLabelAndTypeReadsTest, readsTheLabelSetOnceForTwoProjectedTests) {
    const std::string_view query = "MATCH (n) RETURN n:Person AS person, n:Founder AS founder, count(*)";

    std::string program;
    nlProgram(query, program);
    EXPECT_EQ(countOccurrences(program, "nl.get_node_label_set("), 1u) << program;

    expectRows(query, {
        {"false", "false", "10"},
        {"true", "false", "6"},
        {"true", "true", "2"},
    });
}

// The nodes come back out of a list, so no scan publishes anything about them.
TEST_F(ReuseLabelAndTypeReadsTest, readsTheLabelSetOnceForUnwoundNodes) {
    const std::string_view query = "MATCH (n) WITH collect(n) AS nodes UNWIND nodes AS m "
                                   "RETURN m:Person AS person, m:Founder AS founder, count(*)";

    std::string program;
    nlProgram(query, program);
    EXPECT_EQ(countOccurrences(program, "nl.get_node_label_set("), 1u) << program;

    expectRows(query, {
        {"false", "false", "10"},
        {"true", "false", "6"},
        {"true", "true", "2"},
    });
}

// The source and the target are two columns, so each keeps a read of its own. Remy has 4
// out-edges and Adam 3, Ghosts -> Remy is the one more edge into a Founder, and Doruk -> Gym
// the one edge out of Sales.
TEST_F(ReuseLabelAndTypeReadsTest, readsTheLabelSetOnceForEachEndOfAHop) {
    const std::string_view query = "MATCH (n)-->(m) WHERE n:Founder OR m:Founder OR n:Sales RETURN count(*)";

    std::string program;
    nlProgram(query, program);
    EXPECT_EQ(countOccurrences(program, "nl.get_node_label_set("), 2u) << program;

    expectRows(query, {{"9"}});
}

// A WITH republishes the edges without their type column, so the type tests after it read
// the types again. Ghosts -> Remy is the one KNOWS_WELL edge lasting over 100, and
// Martina -> Cooking the one INTERESTED_IN edge lasting under 15.
TEST_F(ReuseLabelAndTypeReadsTest, readsTheEdgeTypesOnceForADisjunctionOfConjunctions) {
    const std::string_view query = "MATCH ()-[e]->() WITH e "
                                   "WHERE (e:KNOWS_WELL AND e.duration > 100) OR (e:INTERESTED_IN AND e.duration < 15) "
                                   "RETURN e.name";

    std::string program;
    nlProgram(query, program);
    EXPECT_EQ(countOccurrences(program, "nl.get_edge_types("), 1u) << program;

    expectRows(query, {
        {"Ghosts -> Remy"},
        {"Martina -> Cooking"},
    });
}

// 3 KNOWS_WELL edges, and the other 15 are INTERESTED_IN.
TEST_F(ReuseLabelAndTypeReadsTest, readsTheEdgeTypesOnceForUnwoundEdges) {
    const std::string_view query = "MATCH ()-[e]->() WITH collect(e) AS edges UNWIND edges AS f "
                                   "RETURN f:KNOWS_WELL AS knows, f:INTERESTED_IN AS interested, count(*)";

    std::string program;
    nlProgram(query, program);
    EXPECT_EQ(countOccurrences(program, "nl.get_edge_types("), 1u) << program;

    expectRows(query, {
        {"false", "true", "15"},
        {"true", "false", "3"},
    });
}

// With one read left, the two tests read one type column and fold into one check. Every one
// of the 18 edges is KNOWS_WELL or INTERESTED_IN.
TEST_F(ReuseLabelAndTypeReadsTest, foldsTheTypeTestsOfRepublishedEdgesIntoOneCheck) {
    const std::string_view query = "MATCH ()-[e]->() WITH e WHERE e:KNOWS_WELL OR e:INTERESTED_IN RETURN count(e)";

    std::string program;
    nlProgram(query, program);
    EXPECT_EQ(countOccurrences(program, "nl.get_edge_types("), 1u) << program;
    EXPECT_EQ(countOccurrences(program, "nl.check_edge_type_constraint("), 1u) << program;

    expectRows(query, {{"18"}});
}
