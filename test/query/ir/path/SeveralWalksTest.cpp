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

const std::vector<std::string> personWalks {
    "(0), [0], (1)",
    "(0), [0], (1), [4], (0)",
    "(0), [1], (6), [7], (0)",
    "(0), [1], (6), [7], (0), [0], (1)",
    "(0), [0], (1), [4], (0), [1], (6), [7], (0)",
    "(0), [1], (6), [7], (0), [0], (1), [4], (0)",
    "(1), [4], (0)",
    "(1), [4], (0), [0], (1)",
    "(1), [4], (0), [1], (6), [7], (0)",
    "(1), [4], (0), [1], (6), [7], (0), [0], (1)",
};

void chainedWalks(Rows& rows) {
    rows.clear();

    for (const std::string& first : personWalks) {
        const std::string_view end = std::string_view(first).substr(first.rfind('('));

        for (const std::string& second : personWalks) {
            if (second.starts_with(end)) {
                rows.push_back({first, second});
            }
        }
    }

    std::sort(rows.begin(), rows.end());
}

}

// Every walk of a query writes its paths into an arena of its own in the one PathTrie the
// query holds, so a walk seeded from another's rows or crossed with them leaves theirs intact
class SeveralWalksTest : public CallV3Test {
};

TEST_F(SeveralWalksTest, seedsAWalkFromTheEndOfAnother) {
    StringRowSink sink;
    runQuery("MATCH p = (n:Person)-[e]->+(m:Person) MATCH q = (m)-[f]->+(k:Person) RETURN p, q", sink);

    Rows rows;
    sink.sortedRows(rows);

    Rows expected;
    chainedWalks(expected);

    EXPECT_EQ(rows.size(), 52u);
    EXPECT_EQ(rows, expected);
}

TEST_F(SeveralWalksTest, sortsTheRowsTwoWalksRideOn) {
    StringRowSink sink;
    runQuery("MATCH p = (n:Person)-[e]->+(m:Person) MATCH q = (m)-[f]->+(k:Person) "
             "WITH p, q, k ORDER BY k.name RETURN p, q",
             sink);

    Rows rows;
    sink.sortedRows(rows);

    Rows expected;
    chainedWalks(expected);

    EXPECT_EQ(rows.size(), 52u);
    EXPECT_EQ(rows, expected);
}

TEST_F(SeveralWalksTest, pairsTwoWalksOverDisjointVariables) {
    StringRowSink sink;
    runQuery("MATCH p = (n:Person)-[e]->+(m:Person) MATCH q = (a:Person)-[f]->+(b:Person) RETURN p, q", sink);

    Rows rows;
    sink.sortedRows(rows);

    Rows expected;
    for (const std::string& first : personWalks) {
        for (const std::string& second : personWalks) {
            expected.push_back({first, second});
        }
    }
    std::sort(expected.begin(), expected.end());

    EXPECT_EQ(rows.size(), 100u);
    EXPECT_EQ(rows, expected);
}
