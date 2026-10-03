#include <gtest/gtest.h>

#include <algorithm>
#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

namespace {

using Rows = std::vector<StringRowSink::Row>;

Rows sorted(Rows rows) {
    std::sort(rows.begin(), rows.end());
    return rows;
}

}

class SumOverPathRelationshipsTest : public CallV3Test {
};

TEST_F(SumOverPathRelationshipsTest, sumsTheRelationshipsOfAFixedHop) {
    StringRowSink sink;
    runQuery("MATCH p = (n:Person)-[e]->(m:Person) UNWIND relationships(p) AS r WITH p, sum(r.duration) AS total RETURN relationships(p), total", sink);

    Rows rows;
    sink.sortedRows(rows);

    EXPECT_EQ(rows, sorted(Rows {
        {"0", "20"},
        {"4", "20"},
    }));
}

TEST_F(SumOverPathRelationshipsTest, sumsTheRelationshipsOfAWalk) {
    StringRowSink sink;
    runQuery("MATCH p = (n:Person)-[e]->+(m:Person) UNWIND relationships(p) AS r WITH p, sum(r.duration) AS total RETURN relationships(p), total", sink);

    Rows rows;
    sink.sortedRows(rows);

    EXPECT_EQ(rows, sorted(Rows {
        {"0", "20"},
        {"4", "20"},
        {"0, 4", "40"},
        {"4, 0", "40"},
        {"1, 7", "220"},
        {"1, 7, 0", "240"},
        {"4, 1, 7", "240"},
        {"0, 4, 1, 7", "260"},
        {"1, 7, 0, 4", "260"},
        {"4, 1, 7, 0", "260"},
    }));
}

TEST_F(SumOverPathRelationshipsTest, returnsThePathBesideTheSum) {
    StringRowSink sink;
    runQuery("MATCH p = (n:Person)-[e]->(m:Person) UNWIND relationships(p) AS r WITH p, sum(r.duration) AS total RETURN p, total", sink);

    Rows rows;
    sink.sortedRows(rows);

    EXPECT_EQ(rows, sorted(Rows {
        {"(0), [0], (1)", "20"},
        {"(1), [4], (0)", "20"},
    }));
}

TEST_F(SumOverPathRelationshipsTest, keepsTheDistinctPaths) {
    StringRowSink sink;
    runQuery("MATCH p = (n:Person)-[e]->+(m:Person) UNWIND relationships(p) AS r WITH DISTINCT p WHERE length(p) = 2 RETURN relationships(p)", sink);

    Rows rows;
    sink.sortedRows(rows);

    EXPECT_EQ(rows, sorted(Rows {
        {"0, 4"},
        {"4, 0"},
        {"1, 7"},
    }));
}

TEST_F(SumOverPathRelationshipsTest, groupsTheMissedPathsAsOneNull) {
    StringRowSink sink;
    runQuery("MATCH (n:Person) OPTIONAL MATCH p = (n)-[e]->(m:Person) RETURN p, count(n)", sink);

    Rows rows;
    sink.sortedRows(rows);

    EXPECT_EQ(rows, sorted(Rows {
        {"(0), [0], (1)", "1"},
        {"(1), [4], (0)", "1"},
        {"null", "6"},
    }));
}
