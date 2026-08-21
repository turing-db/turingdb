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

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
