#include <gtest/gtest.h>

#include <memory>
#include <span>
#include <unordered_set>

#include "TuringTest.h"
#include "TuringTestEnv.h"

#include "NLOutputSink.h"
#include "QueryConfig.h"
#include "SystemManager.h"
#include "metadata/PropertyType.h"
#include "versioning/Tombstones.h"
#include "Graph.h"
#include "dump/GraphLoader.h"
#include "columns/ColumnOptVector.h"
#include "versioning/Change.h"
#include "versioning/Transaction.h"
#include "columns/ColumnIDs.h"
#include "Panic.h"

using namespace db;
using namespace turing::test;

namespace {

using ColumnIDProp = ColumnOptVector<types::Int64::Primitive>;

class IDCollectingNLSink : public NLOutputSink {
public:
    explicit IDCollectingNLSink(std::unordered_set<size_t>& ids)
        : _ids(ids)
    {
    }

    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 1u);

        const ColumnIDProp* idColumn = static_cast<const ColumnIDProp*>(chunks.front());
        for (size_t row = offset; row < offset + rowCount; row++) {
            _ids.insert(idColumn->at(row).value());
        }
    }

private:
    std::unordered_set<size_t>& _ids;
};

class CountingNLSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        _columnCount = chunks.size();
        _rowCount += rowCount;
    }

    size_t getColumnCount() const { return _columnCount; }
    size_t getRowCount() const { return _rowCount; }

private:
    size_t _columnCount {0};
    size_t _rowCount {0};
};

}

class TombstoneSerialisationTest : public TuringTest {
public:
    void initialize() override {
        _env = TuringTestEnv::createSyncedOnDisk(fs::Path {_outDir} / "turing");

        {
            SystemAccessor system = _env->getSystemManager().accessUnique();
            _builtGraph = system.createGraph(_workingGraphName);
        }

        populateAndDump();
        applyDeletesAndDump();
    }

    void populateAndDump() {
        Change* change {nullptr};
        {
            SystemAccessor system = _env->getSystemManager().accessUnique();
            auto res = system.newChange(_workingGraphName);
            if (!res) {
                panic("Failed to make change in populate().");
            }
            change = res.value();
        }

        // populate the graph
        for (size_t i = 0; i < NUM_EDGES; i++) {
            const size_t origin = i;
            const size_t target = NUM_EDGES+i;
            const std::string queryStr = "create (n:Person{id:"+std::to_string(origin)
                                      +"})-[e:FRIENDSWITH{id: "+std::to_string(i)
                                      +"}]->(m:Person{id:"+std::to_string(target)+"})";
            const auto res = query(queryStr, change->id());
            ASSERT_TRUE(res);
        }

        spdlog::info("Ran create queries");

        // implicit dump on change submit
        ASSERT_TRUE(query("change submit", change->id()));
        spdlog::info("Submitted change");

        CountingNLSink populatedSink;
        ASSERT_TRUE(query("match (n) return n", ChangeID::head(), &populatedSink));

        if (populatedSink.getColumnCount() == 0 || populatedSink.getRowCount() != NUM_NODES) {
            panic("Failed to populate graph.");
        }

        spdlog::info("Successfully populated graph");
    }

    void applyDeletesAndDump() {
        Change* delChange {nullptr};
        {
            SystemAccessor system = _env->getSystemManager().accessUnique();
            auto delRes = system.newChange(_workingGraphName);
            if (!delRes) {
                panic("Failed to make change in populate().");
            }
            delChange = delRes.value();
        }

        for (size_t edge : DELETED_EDGES) {
            const std::string queryStr = "match (n)-[e{id: "+ std::to_string(edge)+"}]->(m) delete e";
            const auto res = query(queryStr, delChange->id());
            spdlog::info(queryStr);
            ASSERT_TRUE(res) << res.getError();
        }

        ASSERT_TRUE(query("commit", delChange->id()));

        for (size_t node : DELETED_NODES) {
            const std::string queryStr = "match (n{id: " + std::to_string(node) + "}) delete n";
            const auto res = query(queryStr, delChange->id());
            spdlog::info(queryStr);
            ASSERT_TRUE(res) << res.getError();
        }
        // implicit dump on change submit
        ASSERT_TRUE(query("change submit", delChange->id()));

        spdlog::info("Submitted deletions change");
    }

    QueryStatus query(std::string_view q, ChangeID changeID, NLOutputSink* sink = nullptr) {
        const QueryState state(_workingGraphName, &_env->getMem(), &_queryConfig, sink, CommitHash::head(), changeID);
        return _env->getDB().query(q, state);
    }

protected:
    const std::string _workingGraphName {"tombstonegraph"};

    std::unique_ptr<TuringTestEnv> _env;
    Graph* _builtGraph {nullptr};
    std::unique_ptr<Graph> _loadedGraph;
    LocalMemory _mem;
    QueryConfig _queryConfig;

    static constexpr size_t NUM_EDGES = 10;
    static constexpr size_t NUM_NODES = 2 * NUM_EDGES;

    static constexpr std::array<size_t, 3> DELETED_NODES = {0, NUM_NODES - 1, 2};
    static constexpr std::array<size_t, 5> DELETED_EDGES = {0, NUM_EDGES - 1, 2, 1, 4};
};

TEST_F(TombstoneSerialisationTest, deleteNodesThenLoad) {
    _loadedGraph = Graph::create();
    {
        SystemAccessor system = _env->getSystemManager().accessShared();
        const auto res = GraphLoader::load(_loadedGraph.get(),
            system.getGraph(_workingGraphName)->getPath());
        ASSERT_TRUE(res);
    }

    const Tombstones& tombstones =
        _loadedGraph->openTransaction().viewGraph().tombstones();

    const Tombstones::NodeTombstones& nodeTombstones = tombstones.nodeTombstones();
    const Tombstones::EdgeTombstones& edgeTombstones = tombstones.edgeTombstones();

    ASSERT_EQ(DELETED_NODES.size(), nodeTombstones.size());
    ASSERT_EQ(DELETED_EDGES.size(), edgeTombstones.size());

    std::unordered_set<size_t> actualNodes;
    std::unordered_set<size_t> actualEdges;
    std::unordered_set<size_t> expectedNodes;
    std::unordered_set<size_t> expectedEdges;

    // Compute expected node and edges in the dumbest way possible
    for (size_t i = 0; i < NUM_NODES; i++) {
        expectedNodes.insert(i);
    }

    for (size_t i = 0; i < NUM_EDGES; i++) {
        expectedEdges.insert(i);
    }

    for (auto node : DELETED_NODES) {
        expectedNodes.erase(node);
    }

    for (auto edge : DELETED_EDGES) {
        expectedEdges.erase(edge);
    }

    // Get actual nodes & edges
    {
        IDCollectingNLSink nodeSink(actualNodes);

        const auto res = query("match (n) return n.id", ChangeID::head(), &nodeSink);
        ASSERT_TRUE(res);
        ASSERT_TRUE(!actualNodes.empty());
        ASSERT_EQ(actualNodes.size(), NUM_NODES-DELETED_NODES.size());
    }
    {
        IDCollectingNLSink edgeSink(actualEdges);

        const auto res = query("match (n)-[e]->(m) return e.id", ChangeID::head(), &edgeSink);
        ASSERT_TRUE(res);
        ASSERT_TRUE(!actualEdges.empty());
        ASSERT_EQ(actualEdges.size(), NUM_EDGES-DELETED_EDGES.size());
    }

    ASSERT_EQ(actualNodes, expectedNodes);
    ASSERT_EQ(actualEdges, expectedEdges);
}

int main(int argc, char** argv) {
    return turingTestMain(argc, argv);
}
