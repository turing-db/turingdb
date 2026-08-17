#include <gtest/gtest.h>

#include <stdint.h>

#include <algorithm>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "NLOutputSink.h"
#include "QueryInterpreterV3.h"
#include "QueryStatus.h"

#include "Graph.h"
#include "SimpleGraph.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "columns/ColumnOptVector.h"
#include "columns/ColumnVector.h"
#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"

#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

namespace {

using Counts = std::vector<uint64_t>;
using Sums = std::vector<std::optional<int64_t>>;

// The eight Person nodes of simpledb are what every count below is taken over
constexpr uint64_t personCount = 8;

// Its eighteen nodes of every label, which MATCH (n) matches
constexpr uint64_t nodeCount = 18;

// Collects the ui64 column a grouped count emits. The aggregates come behind the
// grouping keys, so the count is the last chunk whichever key is grouped on.
class CountSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_FALSE(chunks.empty());

        const auto* counts = dynamic_cast<const ColumnVector<uint64_t>*>(chunks.back());
        ASSERT_NE(counts, nullptr);

        const auto& countRaw = counts->getRaw();
        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            _counts.push_back(countRaw[rowIndex]);
        }
    }

    void sortedCounts(Counts& counts) const {
        counts = _counts;
        std::sort(counts.begin(), counts.end());
    }

private:
    Counts _counts;
};

// The same for a sum, whose value column is a nullable i64 rather than a ui64 count
class SumSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_FALSE(chunks.empty());

        const auto* sums = dynamic_cast<const ColumnOptVector<int64_t>*>(chunks.back());
        ASSERT_NE(sums, nullptr);

        const auto& sumRaw = sums->getRaw();
        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            _sums.push_back(sumRaw[rowIndex]);
        }
    }

    void sortedSums(Sums& sums) const {
        sums = _sums;
        std::sort(sums.begin(), sums.end());
    }

private:
    Sums _sums;
};

}

// An aggregate taken over the alias of a constant item - the count(x) of
// RETURN 1 AS x, count(x). The alias is a second name for a column holding one value
// standing for every row, so the aggregate folds as many values as the match produced:
// what the argument names is a constant, but how many times it is folded is not.
class AggregateOverConstantAliasTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager());

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);
    }

    QueryStatus runQuery(std::string_view query, NLOutputSink* sink) {
        QueryStatus status;
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              ChangeID::head(),
                              &_env->getMem(),
                              sink);

        return status;
    }

    // The counts the query emits, one per group, compared order-independently
    void expectCounts(std::string_view query, const Counts& expected) {
        CountSink sink;
        const QueryStatus status = runQuery(query, &sink);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        Counts actual;
        sink.sortedCounts(actual);

        EXPECT_EQ(actual, expected) << "query: " << query;
    }

    void expectSums(std::string_view query, const Sums& expected) {
        SumSink sink;
        const QueryStatus status = runQuery(query, &sink);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        Sums actual;
        sink.sortedSums(actual);

        EXPECT_EQ(actual, expected) << "query: " << query;
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

// The constant is the only grouping key, so every match falls in one group and the count
// is the whole match: one row holding the number of Person nodes.
TEST_F(AggregateOverConstantAliasTest, countsOverAConstantAlias) {
    expectCounts("MATCH (n:Person) RETURN 1 AS x, count(x)", {personCount});
}

// The sum reads the values the count only tallied: eight rows of the constant 1.
TEST_F(AggregateOverConstantAliasTest, sumsOverAConstantAlias) {
    expectSums("MATCH (n:Person) RETURN 1 AS x, sum(x)", {personCount});
}

// The same aggregate beside a key that does group the rows: the eight distinct names of
// simpledb's Person nodes split the match into eight groups of one.
TEST_F(AggregateOverConstantAliasTest, countsOverAConstantAliasBesideARowKey) {
    expectCounts("MATCH (n:Person) RETURN n.name, 1 AS x, count(x)", Counts(personCount, 1));
}

// The value reduction of the same shape: each group holds one row of the constant, so
// each group's sum is the constant itself.
TEST_F(AggregateOverConstantAliasTest, sumsOverAConstantAliasBesideARowKey) {
    expectSums("MATCH (n:Person) RETURN n.name, 1 AS x, sum(x)", Sums(personCount, 1));
}

// The alias is what the aggregate is taken over, not what it is grouped by: counting the
// traversal variable under the same constant key must reach the same answer as counting
// the alias of that key.
TEST_F(AggregateOverConstantAliasTest, countsATraversalVariableUnderAConstantKey) {
    expectCounts("MATCH (n:Person) RETURN 1 AS x, count(n)", {personCount});
}

// The same projection with nothing driving it: a constant stands for every row of the
// relation the query matched, and a query that matched none is one row of its own - so
// the count is that one row rather than the graph's.
TEST_F(AggregateOverConstantAliasTest, countsOverAConstantAliasWithoutAMatch) {
    expectCounts("RETURN 1 AS x, count(x)", {1});
}

// The alias is only a name for the constant behind it, so the aggregate spelled over the
// constant itself is the same aggregate: 42 is a value in every matched row, and every
// node of simpledb is matched.
TEST_F(AggregateOverConstantAliasTest, countsASpelledOutConstant) {
    expectCounts("MATCH (n) RETURN count(42)", {nodeCount});
}

// Both spellings answer the same way with nothing driving them, too
TEST_F(AggregateOverConstantAliasTest, countsASpelledOutConstantWithoutAMatch) {
    expectCounts("RETURN count(42)", {1});
}

// The rows the constant stands for are the rows the match left standing, so a filter is
// what the aggregate folds over: one node of simpledb is named Remy.
TEST_F(AggregateOverConstantAliasTest, countsASpelledOutConstantUnderAFilter) {
    expectCounts("MATCH (n) WHERE n.name = 'Remy' RETURN count(42)", {1});
}

// The value reduction of that same one row
TEST_F(AggregateOverConstantAliasTest, sumsOverAConstantAliasUnderAFilter) {
    expectSums("MATCH (n) WHERE n.name = 'Remy' RETURN 1 AS x, sum(x)", {1});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
