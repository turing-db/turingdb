#include <gtest/gtest.h>

#include <string_view>
#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

namespace {

using Rows = std::vector<StringRowSink::Row>;

}

// 70000 paths, more than one chunk holds, so the groups, the lists and the extremes of
// paths carry across the steps of the scan that finds them
class PathAggregatesAcrossChunksTest : public CallV3Test {
protected:
    void initialize() override {
        CallV3Test::initialize();
        runWrite("UNWIND range(1, 70000) AS i CREATE (:Start {i: i})-[:STEP]->(:End {k: i % 7})");
    }

    void expectRows(std::string_view query, const Rows& expected) {
        StringRowSink sink;
        runQuery(query, sink);

        EXPECT_EQ(sink.getRows(), expected) << query;
    }

    void rowsOf(std::string_view query, Rows& rows) {
        StringRowSink sink;
        runQuery(query, sink);

        rows = sink.getRows();
    }
};

TEST_F(PathAggregatesAcrossChunksTest, countsAndCollectsEveryPathOnce) {
    expectRows("MATCH p = (a:Start)-[e:STEP]->(b) RETURN count(*), count(DISTINCT p), size(collect(p)), size(collect(DISTINCT p))",
               {{"70000", "70000", "70000", "70000"}});
}

TEST_F(PathAggregatesAcrossChunksTest, groupsEachPathAlone) {
    expectRows("MATCH p = (a:Start)-[e:STEP]->(b) WITH p, count(*) AS c RETURN count(*), min(c), max(c)",
               {{"70000", "1", "1"}});
}

TEST_F(PathAggregatesAcrossChunksTest, groupsTheCopiesOfAPathTogether) {
    expectRows("MATCH p = (a:Start)-[e:STEP]->(b) UNWIND [1, 2] AS x WITH p, count(*) AS c RETURN count(*), min(c), max(c)",
               {{"70000", "2", "2"}});
}

TEST_F(PathAggregatesAcrossChunksTest, dedupsTheCopiesOfAPath) {
    expectRows("MATCH p = (a:Start)-[e:STEP]->(b) UNWIND [1, 2] AS x WITH DISTINCT p RETURN count(*)", {{"70000"}});
    expectRows("MATCH p = (a:Start)-[e:STEP]->(b) UNWIND [1, 2] AS x RETURN count(*), count(DISTINCT p)", {{"140000", "70000"}});
}

TEST_F(PathAggregatesAcrossChunksTest, reducesToThePathsTheSortPutsFirst) {
    Rows extremes;
    rowsOf("MATCH p = (a:Start)-[e:STEP]->(b) RETURN min(p), max(p)", extremes);

    Rows first;
    rowsOf("MATCH p = (a:Start)-[e:STEP]->(b) RETURN p ORDER BY p LIMIT 1", first);

    Rows last;
    rowsOf("MATCH p = (a:Start)-[e:STEP]->(b) RETURN p ORDER BY p DESC LIMIT 1", last);

    ASSERT_EQ(extremes.size(), 1u);
    ASSERT_EQ(first.size(), 1u);
    ASSERT_EQ(last.size(), 1u);

    EXPECT_EQ(extremes.front()[0], first.front()[0]);
    EXPECT_EQ(extremes.front()[1], last.front()[0]);
}

TEST_F(PathAggregatesAcrossChunksTest, collectsEachGroupsPaths) {
    expectRows("MATCH p = (a:Start)-[e:STEP]->(b) RETURN b.k, count(DISTINCT p), size(collect(p)), length(max(p)) ORDER BY b.k",
               {{"0", "10000", "10000", "1"},
                {"1", "10000", "10000", "1"},
                {"2", "10000", "10000", "1"},
                {"3", "10000", "10000", "1"},
                {"4", "10000", "10000", "1"},
                {"5", "10000", "10000", "1"},
                {"6", "10000", "10000", "1"}});
}

TEST_F(PathAggregatesAcrossChunksTest, unwindsTheCollectedPaths) {
    expectRows("MATCH p = (a:Start)-[e:STEP]->(b) WITH collect(p) AS paths UNWIND paths AS q RETURN count(q), count(DISTINCT q), sum(length(q))",
               {{"70000", "70000", "70000"}});
}

TEST_F(PathAggregatesAcrossChunksTest, indexesTheLastCollectedPath) {
    expectRows("MATCH p = (a:Start)-[e:STEP]->(b) WITH collect(p) AS paths RETURN length(paths[69999]), paths[70000], nodes(paths[-1]) = nodes(last(paths))",
               {{"1", "null", "true"}});
}
