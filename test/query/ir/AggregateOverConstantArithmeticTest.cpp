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
using DoubleSums = std::vector<std::optional<double>>;

// The eighteen nodes of simpledb, which MATCH (n) matches
constexpr uint64_t nodeCount = 18;

// Its eight Person nodes, whose distinct names split a grouped aggregate into eight groups
constexpr uint64_t personCount = 8;

// The four of them holding a PhD, which the filtered count is taken over
constexpr uint64_t phdCount = 4;

// Collects the ui64 column a count emits. The aggregates come behind the grouping keys,
// so the count is the last chunk whichever key is grouped on.
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

// The same for the sum of a power, whose value column is a nullable f64: the operator
// promotes what it computes to a double whatever it reads.
class DoubleSumSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_FALSE(chunks.empty());

        const auto* sums = dynamic_cast<const ColumnOptVector<double>*>(chunks.back());
        ASSERT_NE(sums, nullptr);

        const auto& sumRaw = sums->getRaw();
        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            _sums.push_back(sumRaw[rowIndex]);
        }
    }

    void sortedSums(DoubleSums& sums) const {
        sums = _sums;
        std::sort(sums.begin(), sums.end());
    }

private:
    DoubleSums _sums;
};

}

// An aggregate taken over an expression computed from constants alone through the two
// operators spelled with a symbol of their own - 40 % 3 and 2 ^ 3. The expression is a
// column holding one value standing for every row, so what the aggregate folds is the
// relation the match left standing, not the single row the expression computes.
class AggregateOverConstantArithmeticTest : public TuringTest {
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

    void expectDoubleSums(std::string_view query, const DoubleSums& expected) {
        DoubleSumSink sink;
        const QueryStatus status = runQuery(query, &sink);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        DoubleSums actual;
        sink.sortedSums(actual);

        ASSERT_EQ(actual.size(), expected.size()) << "query: " << query;

        for (size_t rowIndex = 0; rowIndex < expected.size(); rowIndex++) {
            ASSERT_EQ(actual[rowIndex].has_value(), expected[rowIndex].has_value()) << "query: " << query;

            if (expected[rowIndex].has_value()) {
                EXPECT_DOUBLE_EQ(*actual[rowIndex], *expected[rowIndex]) << "query: " << query;
            }
        }
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

// The modulo is a value in every matched row, so the tally is the whole match rather than
// the one row the expression computes.
TEST_F(AggregateOverConstantArithmeticTest, countsAConstantModulo) {
    expectCounts("MATCH (n) RETURN count(40 % 3)", {nodeCount});
}

// The power the same way: what it is counted over is the driving relation.
TEST_F(AggregateOverConstantArithmeticTest, countsAConstantPower) {
    expectCounts("MATCH (n) RETURN count(2 ^ 3)", {nodeCount});
}

// The rows the expression stands for are the rows the match left standing, so a filter is
// what the count folds over: four nodes of simpledb hold a PhD.
TEST_F(AggregateOverConstantArithmeticTest, countsAConstantModuloUnderAFilter) {
    expectCounts("MATCH (n) WHERE n.hasPhD RETURN count(40 % 3)", {phdCount});
}

// The value reduction of the same shape: eighteen rows of 8.0 fold to 144.0, not to the
// 8.0 the power computes once.
TEST_F(AggregateOverConstantArithmeticTest, sumsAConstantPower) {
    expectDoubleSums("MATCH (n) RETURN sum(2 ^ 3)", {nodeCount * 8.0});
}

// The grouped sibling, whose input is laid out over the relation before it is grouped:
// the eight distinct names of simpledb's Person nodes split the match into groups of one.
TEST_F(AggregateOverConstantArithmeticTest, countsAConstantModuloPerGroup) {
    expectCounts("MATCH (n:Person) RETURN n.name, count(2 % 2)", Counts(personCount, 1));
}

// The value reduction of that grouped shape
TEST_F(AggregateOverConstantArithmeticTest, sumsAConstantPowerPerGroup) {
    expectDoubleSums("MATCH (n:Person) RETURN n.name, sum(2 ^ 3)", DoubleSums(personCount, 8.0));
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
