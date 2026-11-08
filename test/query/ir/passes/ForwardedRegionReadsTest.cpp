#include <gtest/gtest.h>

#include <stddef.h>
#include <stdint.h>

#include <algorithm>
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

// A region of a CASE or a coalesce shares a read with a condition or an argument region
// before it, which sees every row it sees and forwards the column it read
class ForwardedRegionReadsTest : public CallV3Test {
protected:
    // More rows than one chunk holds. Item i holds v = i where i % 3 = 0 and no v elsewhere.
    static constexpr int64_t ITEM_COUNT = 3 * 65536 + 7;

    void expectReads(std::string_view query, std::string_view read, size_t expectedReads) {
        StringRowSink explained;
        runQuery(std::string("EXPLAIN (db) ") + std::string(query), explained);

        const Rows& rows = explained.getRows();
        ASSERT_EQ(rows.size(), 1u) << query;

        const std::string& program = rows.front().back();
        EXPECT_EQ(countOccurrences(program, read), expectedReads) << query << "\n" << program;
    }

    void expectRows(std::string_view query, Rows expected) {
        StringRowSink sink;
        runQuery(query, sink);

        Rows rows;
        sink.sortedRows(rows);
        std::sort(expected.begin(), expected.end());

        ASSERT_EQ(rows.size(), expected.size()) << query;

        const auto [actualIt, expectedIt] = std::mismatch(rows.begin(), rows.end(), expected.begin());
        if (actualIt != rows.end()) {
            ADD_FAILURE() << query << "\nfirst differing row starts with " << actualIt->front()
                          << ", expected a row starting with " << expectedIt->front();
        }
    }

    void createItems() {
        runWrite("UNWIND range(0, " + std::to_string(ITEM_COUNT - 1) + ") AS i "
                 "CREATE (:Item {i: i, v: CASE WHEN i % 3 = 0 THEN i END})");
    }

    // -1 where i % 4 = 0, else v where it is even, v + 1 where it is odd, and null where
    // there is no v
    static void fillItemRows(Rows& rows) {
        for (int64_t i = 0; i < ITEM_COUNT; i++) {
            std::string value = "null";
            if (i % 4 == 0) {
                value = "-1";
            } else if (i % 3 == 0) {
                value = std::to_string(i % 2 == 0 ? i : i + 1);
            }

            rows.push_back({std::to_string(i), value});
        }
    }
};

// Remy, Adam, Maxime and Luc are French, Martina works in Bioinformatics, and the 10
// interests have no isFrench
TEST_F(ForwardedRegionReadsTest, readsTheLabelSetOnceInALaterConditionAndTheElse) {
    const std::string_view query = "MATCH (n) RETURN n.name, CASE WHEN n.isFrench THEN 'french' "
                                   "WHEN n:Bioinformatics THEN 'bio' ELSE CASE WHEN n:Interest THEN 'interest' END END";
    expectReads(query, "db.get_node_label_set(", 1);

    expectRows(query, {
        {"Adam", "french"},
        {"Animals", "interest"},
        {"Bio", "interest"},
        {"Computers", "interest"},
        {"Cooking", "interest"},
        {"Cyrus", "null"},
        {"Doruk", "null"},
        {"Eighties", "interest"},
        {"Ghosts", "interest"},
        {"Gym", "interest"},
        {"JiuJitsu", "interest"},
        {"Luc", "french"},
        {"Martina", "bio"},
        {"Maxime", "french"},
        {"Padel", "interest"},
        {"Remy", "french"},
        {"Suhas", "null"},
        {"Travel", "interest"},
    });
}

// Eighties and Ghosts are the exotic interests
TEST_F(ForwardedRegionReadsTest, readsTheLabelSetOnceInAConditionAndItsValue) {
    const std::string_view query = "MATCH (n) RETURN n.name, CASE WHEN n.isFrench THEN 'french' "
                                   "WHEN n:Interest THEN CASE WHEN n:Exotic THEN 'exotic' ELSE 'plain' END END";
    expectReads(query, "db.get_node_label_set(", 1);

    expectRows(query, {
        {"Adam", "french"},
        {"Animals", "plain"},
        {"Bio", "plain"},
        {"Computers", "plain"},
        {"Cooking", "plain"},
        {"Cyrus", "null"},
        {"Doruk", "null"},
        {"Eighties", "exotic"},
        {"Ghosts", "exotic"},
        {"Gym", "plain"},
        {"JiuJitsu", "plain"},
        {"Luc", "french"},
        {"Martina", "null"},
        {"Maxime", "french"},
        {"Padel", "plain"},
        {"Remy", "french"},
        {"Suhas", "null"},
        {"Travel", "plain"},
    });
}

// Eighties is the one interest whose isReal is false, and Bio, Cooking and Padel have none
TEST_F(ForwardedRegionReadsTest, readsTheLabelSetOnceInARegionNestedInTheElse) {
    const std::string_view query = "MATCH (n) RETURN n.name, CASE WHEN n.isFrench THEN 'french' "
                                   "WHEN n:Bioinformatics THEN 'bio' "
                                   "ELSE CASE WHEN n.isReal THEN CASE WHEN n:Exotic THEN 'exotic' ELSE 'real' END END END";
    expectReads(query, "db.get_node_label_set(", 1);

    expectRows(query, {
        {"Adam", "french"},
        {"Animals", "real"},
        {"Bio", "null"},
        {"Computers", "real"},
        {"Cooking", "null"},
        {"Cyrus", "null"},
        {"Doruk", "null"},
        {"Eighties", "null"},
        {"Ghosts", "exotic"},
        {"Gym", "real"},
        {"JiuJitsu", "real"},
        {"Luc", "french"},
        {"Martina", "bio"},
        {"Maxime", "french"},
        {"Padel", "null"},
        {"Remy", "french"},
        {"Suhas", "null"},
        {"Travel", "real"},
    });
}

TEST_F(ForwardedRegionReadsTest, readsTheLabelSetOnceInLaterCoalesceArguments) {
    const std::string_view query = "MATCH (n) RETURN n.name, coalesce(n.isReal, CASE WHEN n:Exotic THEN true END, n:Interest)";
    expectReads(query, "db.get_node_label_set(", 1);

    expectRows(query, {
        {"Adam", "false"},
        {"Animals", "true"},
        {"Bio", "true"},
        {"Computers", "true"},
        {"Cooking", "true"},
        {"Cyrus", "false"},
        {"Doruk", "false"},
        {"Eighties", "false"},
        {"Ghosts", "true"},
        {"Gym", "true"},
        {"JiuJitsu", "true"},
        {"Luc", "false"},
        {"Martina", "false"},
        {"Maxime", "false"},
        {"Padel", "true"},
        {"Remy", "false"},
        {"Suhas", "false"},
        {"Travel", "true"},
    });
}

// Luc -> Computers lasts 15 and Martina -> Cooking 10, Ghosts -> Remy 200. Remy -> Eighties
// is the one moderate edge.
TEST_F(ForwardedRegionReadsTest, readsAnEdgePropertyOnceInTwoLaterConditions) {
    const std::string_view query = "MATCH ()-[e]->() RETURN e.name, CASE WHEN e.proficiency = 'moderate' THEN 'moderate' "
                                   "WHEN e.duration < 18 THEN 'short' WHEN e.duration < 65 THEN 'medium' ELSE 'long' END";
    expectReads(query, ", \"duration\")", 1);

    expectRows(query, {
        {"Adam -> Bio", "long"},
        {"Adam -> Cooking", "long"},
        {"Adam -> Remy", "medium"},
        {"Cyrus -> Gym", "long"},
        {"Cyrus -> Travel", "long"},
        {"Doruk -> Gym", "long"},
        {"Ghosts -> Remy", "long"},
        {"Luc -> Animals", "medium"},
        {"Luc -> Computers", "short"},
        {"Martina -> Cooking", "short"},
        {"Maxime -> Bio", "long"},
        {"Maxime -> Padel", "long"},
        {"Remy -> Adam", "medium"},
        {"Remy -> Computers", "long"},
        {"Remy -> Eighties", "moderate"},
        {"Remy -> Ghosts", "medium"},
        {"Suhas -> Gym", "long"},
        {"Suhas -> Jiutjitsu", "long"},
    });
}

// Remy and Adam have an age, Luc's dob is past '25' and Maxime's is not
TEST_F(ForwardedRegionReadsTest, readsAPropertyOnceInLaterCoalesceArguments) {
    const std::string_view query = "MATCH (n:Person) RETURN n.name, coalesce(n.age, CASE WHEN n.dob > '25' THEN n.dob END, n.dob + '!')";
    expectReads(query, ", \"dob\")", 1);

    expectRows(query, {
        {"Adam", "32"},
        {"Cyrus", "null"},
        {"Doruk", "null"},
        {"Luc", "28/05"},
        {"Martina", "null"},
        {"Maxime", "24/07!"},
        {"Remy", "32"},
        {"Suhas", "null"},
    });
}

TEST_F(ForwardedRegionReadsTest, cutsTheForwardedColumnOfACaseAcrossChunks) {
    createItems();

    const std::string_view query = "MATCH (n:Item) RETURN n.i, "
                                   "CASE WHEN n.i % 4 = 0 THEN -1 WHEN n.v % 2 = 0 THEN n.v ELSE n.v + 1 END";
    expectReads(query, ", \"v\")", 1);

    Rows expected;
    fillItemRows(expected);
    expectRows(query, expected);
}

TEST_F(ForwardedRegionReadsTest, cutsTheForwardedColumnOfACoalesceAcrossChunks) {
    createItems();

    const std::string_view query = "MATCH (n:Item) RETURN n.i, "
                                   "coalesce(CASE WHEN n.i % 4 = 0 THEN -1 END, CASE WHEN n.v % 2 = 0 THEN n.v END, n.v + 1)";
    expectReads(query, ", \"v\")", 1);

    Rows expected;
    fillItemRows(expected);
    expectRows(query, expected);
}
