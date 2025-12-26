#include <gtest/gtest.h>

#include <memory>
#include <unordered_set>

#include "TuringTest.h"
#include "TuringTestEnv.h"

#include "SystemManager.h"
#include "metadata/PropertyType.h"
#include "versioning/Tombstones.h"
#include "Graph.h"
#include "dump/GraphLoader.h"
#include "dataframe/Dataframe.h"
#include "versioning/Change.h"
#include "versioning/Transaction.h"
#include "columns/ColumnIDs.h"
#include "Panic.h"

using namespace db;
using namespace turing::test;

class TombstoneSerialisationTest : public TuringTest {
public:
    void initialize() override {
        _env = TuringTestEnv::createSyncedOnDisk(fs::Path {_outDir} / "turing");

        _builtGraph = _env->getSystemManager().createGraph(_workingGraphName);

        populateAndDump();
        applyDeletesAndDump();
    }

    void populateAndDump() {
        auto& db = _env->getDB();
        auto& sysMan = _env->getSystemManager();

        auto res = sysMan.newChange(_workingGraphName);
        if (!res) {
            panic("Failed to make change in populate().");
        }
        Change* change = res.value();

        // populate the graph
        for (size_t i = 0; i < NUM_EDGES; i++) {
            const size_t origin = i;
            const size_t target = NUM_EDGES+i;
            const std::string queryStr = "create (n:Person{id:"+std::to_string(origin)
                                      +"})-[e:FRIENDSWITH{id: "+std::to_string(i)
                                      +"}]->(m:Person{id:"+std::to_string(target)+"})";
            const auto res = db.query(queryStr,
                                      _workingGraphName,
                                      &_env->getMem(),
                                      CommitHash::head(),
                                      change->id());
            ASSERT_TRUE(res);
        }

        spdlog::info("Ran create queries");

        // implicit dump on change submit
        ASSERT_TRUE(db.query("change submit", _workingGraphName, &_env->getMem(),
                             CommitHash::head(), change->id()));
        spdlog::info("Submitted change");

        const auto VERIFY = [](const Dataframe* df) {
            if (df->size() == 0) {
                panic("Failed to populate graph.");
            }
            if (df->cols().front()->getColumn()->size() != NUM_NODES) {
                panic("Failed to populate graph.");
            }
        };

        ASSERT_TRUE(db.query("match (n) return n", _workingGraphName, &_env->getMem(),
                             VERIFY, CommitHash::head(), ChangeID::head()));

        spdlog::info("Successfully populated graph");
    }

    void applyDeletesAndDump() {
        auto& db = _env->getDB();
        auto& sysMan = _env->getSystemManager();

        auto delRes = sysMan.newChange(_workingGraphName);
        if (!delRes) {
            panic("Failed to make change in populate().");
        }
        Change* delChange = delRes.value();

        for (size_t node : DELETED_NODES) {
            const std::string queryStr = "match (n{id: " + std::to_string(node) + "}) delete n";
            const auto res = db.query(queryStr,
                                      _workingGraphName,
                                      &_env->getMem(),
                                      CommitHash::head(),
                                      delChange->id());
            spdlog::info(queryStr);
            ASSERT_TRUE(res);
        }
        for (size_t edge : DELETED_EDGES) {
            const std::string queryStr = "match (n)-[e{id: "+ std::to_string(edge)+"}]->(m) delete e";
            const auto res = db.query(queryStr,
                                      _workingGraphName,
                                      &_env->getMem(),
                                      CommitHash::head(),
                                      delChange->id());
            spdlog::info(queryStr);
            ASSERT_TRUE(res);
        }
        // implicit dump on change submit
        ASSERT_TRUE(db.query("change submit", _workingGraphName, &_env->getMem(),
                             CommitHash::head(), delChange->id()));

        spdlog::info("Submitted deletions change");
    }

protected:
    const std::string _workingGraphName {"tombstonegraph"};

    std::unique_ptr<TuringTestEnv> _env;
    Graph* _builtGraph {nullptr};
    std::unique_ptr<Graph> _loadedGraph;
    LocalMemory _mem;

    static constexpr size_t NUM_EDGES = 10;
    static constexpr size_t NUM_NODES = 2 * NUM_EDGES;

    static constexpr std::array<size_t, 3> DELETED_NODES = {0, NUM_NODES - 1, 2};
    static constexpr std::array<size_t, 5> DELETED_EDGES = {0, NUM_EDGES - 1, 2, 1, 4};
};

TEST_F(TombstoneSerialisationTest, deleteNodesThenLoad) {
    _loadedGraph = Graph::create();
    const auto res = GraphLoader::load(_loadedGraph.get(),
        _env->getSystemManager().getGraph(_workingGraphName)->getPath());
    ASSERT_TRUE(res);

    const Tombstones& tombstones =
        _loadedGraph->openTransaction().viewGraph().commits().back().tombstones();

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
        TuringDB& db = _env->getDB();

        using ColumnIDProp = ColumnOptVector<types::Int64::Primitive>;
        auto callback = [&actualNodes](const Dataframe* df) {
            ASSERT_TRUE(df->size() == 1);
            ColumnIDProp* nodes = df->cols().front()->as<ColumnIDProp>();
            ASSERT_TRUE(nodes);

            for (const auto& id : *nodes) {
                actualNodes.insert(id.value());
            }
        };

        const auto res = db.query("match (n) return n.id",
                                  _workingGraphName,
                                  &_env->getMem(),
                                  callback);
        ASSERT_TRUE(res);
        ASSERT_TRUE(!actualNodes.empty());
        ASSERT_EQ(actualNodes.size(), NUM_NODES-DELETED_NODES.size());
    }
    {
        TuringDB& db = _env->getDB();

        using ColumnIDProp = ColumnOptVector<types::Int64::Primitive>;
        auto callback = [&actualEdges](const Dataframe* df) {
            ASSERT_TRUE(df->size() == 1);
            ColumnIDProp* edges = df->cols().front()->as<ColumnIDProp>();
            ASSERT_TRUE(edges);

            for (const auto& id : *edges) {
                actualEdges.insert(id.value());
            }
        };

        const auto res = db.query("match (n)-[e]->(m) return e.id",
                                  _workingGraphName,
                                  &_env->getMem(),
                                  callback);
        ASSERT_TRUE(res);
        ASSERT_TRUE(!actualEdges.empty());
        ASSERT_EQ(actualEdges.size(), NUM_EDGES-DELETED_EDGES.size());
    }

    ASSERT_EQ(actualNodes, expectedNodes);
    ASSERT_EQ(actualEdges, expectedEdges);
}

int main(int argc, char** argv) {
    return turingTestMain(argc, argv, [] {
        testing::GTEST_FLAG(repeat) = 1;
    });
}
