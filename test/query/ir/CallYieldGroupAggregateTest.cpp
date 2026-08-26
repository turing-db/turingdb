#include <gtest/gtest.h>

#include <stddef.h>
#include <stdint.h>

#include <map>
#include <memory>
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

using EdgeTypeCounts = std::map<EdgeTypeID::Type, uint64_t>;

// Collects the single edge type ID column a YIELD of db.edgeTypes' id emits, tallying how
// many rows each ID arrives on.
class EdgeTypeSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 1u);

        const auto* ids = dynamic_cast<const ColumnVector<EdgeTypeID>*>(chunks[0]);
        ASSERT_NE(ids, nullptr);

        const auto& raw = ids->getRaw();
        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            _counts[raw[rowIndex].getValue()]++;
        }
    }

    const EdgeTypeCounts& getCounts() const { return _counts; }

private:
    EdgeTypeCounts _counts;
};

// Collects the (edge type ID, count) rows a grouped count over that same yielded column
// emits, refusing a group that arrives twice.
class EdgeTypeCountSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 2u);

        const auto* ids = dynamic_cast<const ColumnVector<EdgeTypeID>*>(chunks[0]);
        const auto* counts = dynamic_cast<const ColumnVector<uint64_t>*>(chunks[1]);
        ASSERT_NE(ids, nullptr);
        ASSERT_NE(counts, nullptr);
        ASSERT_EQ(ids->size(), counts->size());

        const auto& idRaw = ids->getRaw();
        const auto& countRaw = counts->getRaw();
        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            const auto inserted = _counts.emplace(idRaw[rowIndex].getValue(), countRaw[rowIndex]);
            ASSERT_TRUE(inserted.second);
        }
    }

    const EdgeTypeCounts& getCounts() const { return _counts; }

private:
    EdgeTypeCounts _counts;
};


// Counts the rows of a single yielded column, whatever it holds, so a test can derive what
// an aggregate over that same column has to answer.
class YieldedRowCountSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 1u);

        _rowCount += rowCount;
    }

    uint64_t getRowCount() const { return _rowCount; }

private:
    uint64_t _rowCount {0};
};

// Collects the single unsigned column an ungrouped aggregate emits.
class AggregateSink : public NLOutputSink {
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

// Collects the (label ID, count) rows of a count grouped on a yielded label ID column.
class LabelCountSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 2u);

        const auto* ids = dynamic_cast<const ColumnVector<LabelID>*>(chunks[0]);
        const auto* counts = dynamic_cast<const ColumnVector<uint64_t>*>(chunks[1]);
        ASSERT_NE(ids, nullptr);
        ASSERT_NE(counts, nullptr);
        ASSERT_EQ(ids->size(), counts->size());

        const auto& idRaw = ids->getRaw();
        const auto& countRaw = counts->getRaw();
        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            const auto inserted = _counts.emplace(idRaw[rowIndex].getValue(), countRaw[rowIndex]);
            ASSERT_TRUE(inserted.second);
        }
    }

    const std::map<LabelID::Type, uint64_t>& getCounts() const { return _counts; }

private:
    std::map<LabelID::Type, uint64_t> _counts;
};

}

// A CALL's yielded columns are variables of the query like any other, so a RETURN may
// group on one of them. The grouped aggregate resolves its keys through a map of its own,
// and a key the YIELD bound is only in it if the yields are put there too.
class CallYieldGroupAggregateTest : public TuringTest {
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

TEST_F(CallYieldGroupAggregateTest, groupsOnAYieldedColumn) {
    EdgeTypeSink yielded;
    runQuery("CALL db.edgeTypes() YIELD id RETURN id", yielded);

    ASSERT_FALSE(yielded.getCounts().empty());

    EdgeTypeCountSink grouped;
    runQuery("CALL db.edgeTypes() YIELD id RETURN id, count(*)", grouped);

    // The schema numbers every edge type once, so each is a group of one row.
    EXPECT_EQ(grouped.getCounts(), yielded.getCounts());
}

// Crossing the CALL with a MATCH replicates every edge type once per matched node, so
// each is one group of as many rows as the same query reports without the aggregate: a
// projection reading the rows the CALL emitted rather than the grouped column would
// answer with those rows instead.
TEST_F(CallYieldGroupAggregateTest, groupsRepeatedYieldedValues) {
    EdgeTypeSink yielded;
    runQuery("MATCH (n:Person) CALL db.edgeTypes() YIELD id RETURN id", yielded);

    ASSERT_FALSE(yielded.getCounts().empty());
    EXPECT_GT(yielded.getCounts().begin()->second, 1u);

    EdgeTypeCountSink grouped;
    runQuery("MATCH (n:Person) CALL db.edgeTypes() YIELD id RETURN id, count(*)", grouped);

    EXPECT_EQ(grouped.getCounts(), yielded.getCounts());
}

// Grouping on one yielded column while aggregating another: the key and the aggregate's
// input are both bound by the same CALL, so the grouped key has to replace what the
// projection reads back or it reads a column the aggregate's loop never defined.
TEST_F(CallYieldGroupAggregateTest, countsOneYieldedColumnGroupedByAnother) {
    LabelCountSink grouped;
    runQuery("CALL db.labels() YIELD id, label RETURN id, count(label)", grouped);

    ASSERT_FALSE(grouped.getCounts().empty());

    // The schema names every label once, so each ID is a group of one row.
    for (const auto& [labelID, count] : grouped.getCounts()) {
        EXPECT_EQ(count, 1u);
    }
}

// count over the types a procedure yields beside the scalars: a label ID, a property type
// and the value type of a property are all columns of their own, countable like any other.
TEST_F(CallYieldGroupAggregateTest, countsAYieldedLabelIDColumn) {
    YieldedRowCountSink yielded;
    runQuery("CALL db.labels() YIELD id RETURN id", yielded);

    ASSERT_GT(yielded.getRowCount(), 0u);

    AggregateSink counted;
    runQuery("CALL db.labels() YIELD id RETURN count(id)", counted);

    const std::vector<uint64_t> expected {yielded.getRowCount()};
    EXPECT_EQ(counted.getCounts(), expected);
}

TEST_F(CallYieldGroupAggregateTest, countsAYieldedValueTypeColumn) {
    YieldedRowCountSink yielded;
    runQuery("CALL db.propertyTypes() YIELD valueType RETURN valueType", yielded);

    ASSERT_GT(yielded.getRowCount(), 0u);

    AggregateSink counted;
    runQuery("CALL db.propertyTypes() YIELD valueType RETURN count(valueType)", counted);

    const std::vector<uint64_t> expected {yielded.getRowCount()};
    EXPECT_EQ(counted.getCounts(), expected);
}

TEST_F(CallYieldGroupAggregateTest, countsAYieldedPropertyTypeColumn) {
    YieldedRowCountSink yielded;
    runQuery("CALL db.propertyTypes() YIELD id RETURN id", yielded);

    ASSERT_GT(yielded.getRowCount(), 0u);

    AggregateSink counted;
    runQuery("CALL db.propertyTypes() YIELD id RETURN count(id)", counted);

    const std::vector<uint64_t> expected {yielded.getRowCount()};
    EXPECT_EQ(counted.getCounts(), expected);
}

TEST_F(CallYieldGroupAggregateTest, countsAYieldedEdgeTypeColumn) {
    YieldedRowCountSink yielded;
    runQuery("CALL db.edgeTypes() YIELD id RETURN id", yielded);

    ASSERT_GT(yielded.getRowCount(), 0u);

    AggregateSink counted;
    runQuery("CALL db.edgeTypes() YIELD id RETURN count(id)", counted);

    const std::vector<uint64_t> expected {yielded.getRowCount()};
    EXPECT_EQ(counted.getCounts(), expected);
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
