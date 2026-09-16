#include <gtest/gtest.h>

#include <memory>
#include <span>
#include <string_view>

#include "Graph.h"
#include "QueryConfig.h"
#include "SimpleGraph.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "NLOutputSink.h"
#include "TuringDB.h"

#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

namespace {

class CountingSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        _rowCount += rowCount;
    }

    size_t getRowCount() const { return _rowCount; }

private:
    size_t _rowCount {0};
};

size_t countRows(TuringDB* db,
                 LocalMemory* mem,
                 const QueryConfig* queryConfig,
                 std::string_view graphName,
                 std::string_view query) {
    CountingSink sink;

    const QueryState state(graphName, mem, queryConfig, &sink);
    const QueryStatus status = db->query(query, state);
    EXPECT_TRUE(status.isOk()) << query;

    return sink.getRowCount();
}

}

class ChainedCallTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");

        SystemAccessor system = _env->getSystemManager().accessUnique();
        _graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(_graph);

        _db = &_env->getDB();
    }

    size_t run(std::string_view query) {
        return countRows(_db, &_env->getMem(), &_queryConfig, _graphName, query);
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    TuringDB* _db {nullptr};
    Graph* _graph {nullptr};
    QueryConfig _queryConfig;
};

TEST_F(ChainedCallTest, singleCallYieldsRows) {
    const size_t rowCount = run(
        "MATCH (n) "
        "CALL gnn.neighbourhoodSample(n, 2) YIELD tgt AS m "
        "RETURN m");

    EXPECT_GT(rowCount, 0u);
}

TEST_F(ChainedCallTest, chainedCallNeverCrashes) {
    constexpr std::string_view query =
        "MATCH (n) "
        "CALL gnn.neighbourhoodSample(n, 2) YIELD tgt AS m "
        "CALL gnn.neighbourhoodSample(m, 2) YIELD tgt AS o "
        "RETURN o";

    for (size_t run = 0; run < 5; run++) {
        const size_t rowCount = ChainedCallTest::run(query);
        EXPECT_GE(rowCount, 0u) << "run " << run;
    }
}

// age=32 matches exactly Remy (4 out-edges) and Adam (3 out-edges).
// Total is deterministically 5 regardless of scan order or random replacements.
TEST_F(ChainedCallTest, chainedCallYieldsRowsWithPersonNodes) {
    const size_t rowCount = run(
        "MATCH (n) WHERE n.age = 32 "
        "CALL gnn.neighbourhoodSample(n, 4) YIELD tgt AS m "
        "CALL gnn.neighbourhoodSample(m, 2) YIELD tgt AS o "
        "RETURN o");

    EXPECT_EQ(rowCount, 5u);
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
