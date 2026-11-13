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

}

// A WHERE that may raise stays above the product of the seed and the pattern, so the seed
// reaches the equality through its filter. Remy and Adam are the Founders and the only nodes
// with an age. They know each other, and Ghosts (6) knows Remy.
class RerootSeedThroughAFilterTest : public CallV3Test {
protected:
    void expectRows(std::string_view query, const Rows& expected) {
        StringRowSink sink;
        runQuery(query, sink);

        Rows rows;
        sink.sortedRows(rows);

        EXPECT_EQ(rows, expected) << query;
    }

    void expectRerootedAtTheSeed(std::string_view query, bool rerooted) {
        StringRowSink sink;
        runQuery("EXPLAIN (passes) " + std::string(query), sink);

        const bool reroots = std::ranges::any_of(sink.getRows(), [](const StringRowSink::Row& stage) {
            return stage.front() == "after reroot_pattern_at_seed";
        });

        EXPECT_EQ(reroots, rerooted) << query;
    }
};

TEST_F(RerootSeedThroughAFilterTest, carriesTheSeedThroughAFilterThatMayRaise) {
    const std::string_view query = "MATCH (s:Founder) MATCH (n)-->(m) WITH s, n, m WHERE 100 / m.age > 1 "
                                   "WITH s, n, m WHERE n = s RETURN n.name, m.name";

    expectRerootedAtTheSeed(query, true);
    expectRows(query, {{"Adam", "Remy"}, {"Remy", "Adam"}});
}

TEST_F(RerootSeedThroughAFilterTest, computesTheSeedIDsFromTheColumnsAFilterCarries) {
    const std::string_view query = "WITH [0, 1, 6, 99] AS ss UNWIND range(0, 3) AS i MATCH (n)-->(m) "
                                   "WITH ss, i, n, m WHERE 100 / m.age > 1 "
                                   "WITH ss, i, n, m WHERE n = ss[i] RETURN n.name, m.name";

    expectRerootedAtTheSeed(query, true);
    expectRows(query, {{"Adam", "Remy"}, {"Ghosts", "Remy"}, {"Remy", "Adam"}});
}

TEST_F(RerootSeedThroughAFilterTest, readsAMaskThroughTheFilterBelowIt) {
    const std::string_view query = "MATCH (s:Founder) MATCH (n)-->(m) WITH s, n, m WHERE 100 / m.age > 1 "
                                   "WITH s, n, m WHERE 100 / n.age > 1 "
                                   "WITH s, n, m WHERE n = s RETURN n.name, m.name";

    expectRerootedAtTheSeed(query, true);
    expectRows(query, {{"Adam", "Remy"}, {"Remy", "Adam"}});
}

TEST_F(RerootSeedThroughAFilterTest, keepsTheRootWhereAFilterReadsTheSeed) {
    const std::string_view query = "MATCH (s:Founder) MATCH (n)-->(m) WITH s, n, m WHERE m.age > s.age - 1 "
                                   "WITH s, n, m WHERE n = s RETURN n.name, m.name";

    expectRerootedAtTheSeed(query, false);
    expectRows(query, {{"Adam", "Remy"}, {"Remy", "Adam"}});
}

TEST_F(RerootSeedThroughAFilterTest, keepsTheRootWhereAMaskReadsTheSeedThroughAFilter) {
    const std::string_view query = "MATCH (s:Founder) MATCH (n)-->(m) WITH s, n, m WHERE 100 / m.age > 1 "
                                   "WITH s, n, m WHERE m.age > s.age - 1 "
                                   "WITH s, n, m WHERE n = s RETURN n.name, m.name";

    expectRerootedAtTheSeed(query, false);
    expectRows(query, {{"Adam", "Remy"}, {"Remy", "Adam"}});
}
