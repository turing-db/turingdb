#include <gtest/gtest.h>

#include <stddef.h>
#include <stdint.h>

#include <map>
#include <memory>
#include <set>
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
#include "columns/ColumnVector.h"
#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"

#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

namespace {

// Collects the single unsigned column an aggregate emits, one value per group.
class CountSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 1u);

        const auto* counts = dynamic_cast<const ColumnVector<uint64_t>*>(chunks[0]);
        ASSERT_NE(counts, nullptr);

        const auto& raw = counts->getRaw();
        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            _counts.push_back(raw[rowIndex]);
        }
    }

    const std::vector<uint64_t>& getCounts() const { return _counts; }

private:
    std::vector<uint64_t> _counts;
};

// Collects the distinct strings of a single yielded string column, so a test can derive
// what count(DISTINCT) over that same column has to answer.
class DistinctStringSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 1u);

        const auto* values = dynamic_cast<const ColumnVector<std::string_view>*>(chunks[0]);
        ASSERT_NE(values, nullptr);

        const auto& raw = values->getRaw();
        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            _values.emplace(raw[rowIndex]);
            _rowCount++;
        }
    }

    size_t getDistinctCount() const { return _values.size(); }

    size_t getRowCount() const { return _rowCount; }

private:
    std::set<std::string> _values;
    size_t _rowCount {0};
};

// Collects the (node, count) rows of an aggregate grouped on a matched node, refusing a
// group that arrives twice.
class NodeCountSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 2u);

        const auto* nodes = dynamic_cast<const ColumnVector<NodeID>*>(chunks[0]);
        const auto* counts = dynamic_cast<const ColumnVector<uint64_t>*>(chunks[1]);
        ASSERT_NE(nodes, nullptr);
        ASSERT_NE(counts, nullptr);
        ASSERT_EQ(nodes->size(), counts->size());

        const auto& nodeRaw = nodes->getRaw();
        const auto& countRaw = counts->getRaw();
        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            const auto inserted = _counts.emplace(nodeRaw[rowIndex].getValue(), countRaw[rowIndex]);
            ASSERT_TRUE(inserted.second);
        }
    }

    const std::map<NodeID::Type, uint64_t>& getCounts() const { return _counts; }

private:
    std::map<NodeID::Type, uint64_t> _counts;
};

}

// A column a procedure yields is never null, so count(DISTINCT) over one keys on the value
// itself rather than through a nullable's present flag. Only ID columns could be keyed on
// that way, which left a yielded string counting nothing - through the DISTINCT filter an
// ungrouped count reduces with, and through the group aggregate's fold under a key.
class CallYieldCountDistinctTest : public TuringTest {
public:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);

        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager());
    }

protected:
    void runQuery(std::string_view query, NLOutputSink& sink) {
        QueryStatus status;
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              ChangeID::head(),
                              &_env->getMem(),
                              &sink);

        ASSERT_EQ(status.getStatus(), QueryStatus::Status::OK) << status.getError();
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

TEST_F(CallYieldCountDistinctTest, countsDistinctYieldedStrings) {
    DistinctStringSink labels;
    runQuery("CALL db.labels() YIELD label RETURN label", labels);

    ASSERT_GT(labels.getDistinctCount(), 1u);

    CountSink counted;
    runQuery("CALL db.labels() YIELD label RETURN count(DISTINCT label)", counted);

    const std::vector<uint64_t> expected {labels.getDistinctCount()};
    EXPECT_EQ(counted.getCounts(), expected);
}

// The crossed call repeats the whole label set once per matched person, so count(*) would
// answer the product. Only a fold that actually dedups brings it back to the label count.
TEST_F(CallYieldCountDistinctTest, dedupsAYieldedStringRepeatedByACross) {
    DistinctStringSink crossed;
    runQuery("MATCH (n:Person) CALL db.labels() YIELD label RETURN label", crossed);

    ASSERT_GT(crossed.getRowCount(), crossed.getDistinctCount());

    CountSink counted;
    runQuery("MATCH (n:Person) CALL db.labels() YIELD label RETURN count(DISTINCT label)", counted);

    const std::vector<uint64_t> expected {crossed.getDistinctCount()};
    EXPECT_EQ(counted.getCounts(), expected);
}

// The ungrouped counts above reduce through a DISTINCT filter ahead of the count; a
// grouping key sends them through the group aggregate's own per-group fold instead. Every
// person sees the whole label set the crossed call repeats, so each group counts them all.
TEST_F(CallYieldCountDistinctTest, countsDistinctYieldedStringsPerGroup) {
    DistinctStringSink labels;
    runQuery("CALL db.labels() YIELD label RETURN label", labels);

    ASSERT_GT(labels.getDistinctCount(), 1u);

    NodeCountSink grouped;
    runQuery("MATCH (n:Person) CALL db.labels() YIELD label RETURN n, count(DISTINCT label)", grouped);

    ASSERT_FALSE(grouped.getCounts().empty());
    for (const auto& [nodeID, count] : grouped.getCounts()) {
        EXPECT_EQ(count, labels.getDistinctCount());
    }
}
