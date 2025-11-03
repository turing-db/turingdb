#include <gtest/gtest.h>

#include <memory>

#include "TuringTest.h"
#include "TuringTestEnv.h"

#include "SystemManager.h"
#include "Graph.h"
#include "dump/GraphLoader.h"
#include "views/GraphView.h"
#include "columns/Block.h"
#include "versioning/Tombstones.h"
#include "versioning/Transaction.h"

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

        auto change = sysMan.newChange(_workingGraphName);
        if (!change) {
            panic("Failed to make change in populate().");
        }

        // populate the graph
        for (size_t i = 0; i < NUM_EDGES; i++) {
            ASSERT_TRUE(db.query("create (n:Person)-[e:FRIENDSWITH]-(m:Person)",
                                 _workingGraphName, &_env->getMem(), CommitHash::head(),
                                 change->getID()));
        }
        spdlog::info("Ran create queries");

        // implicit dump on change submit
        ASSERT_TRUE(db.query("change submit", _workingGraphName, &_env->getMem(),
                             CommitHash::head(), change->getID()));
        spdlog::info("Submitted change");

        const auto VERIFY = [](const Block& block) {
            if (block.empty()) {
                panic("Failed to populate graph.");
            }
            if (block.columns().front()->size() != NUM_NODES) {
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

        auto change = sysMan.newChange(_workingGraphName);
        if (!change) {
            panic("Failed to make change in populate().");
        }

        for (size_t node : DELETED_NODES) {
            db.query("delete nodes " + std::to_string(node), _workingGraphName,
                     &_env->getMem(), CommitHash::head(), change->getID());
        }
        for (size_t node : DELETED_EDGES) {
            db.query("delete edges " + std::to_string(node), _workingGraphName,
                     &_env->getMem(), CommitHash::head(), change->getID());
        }
        // implicit dump on change submit
        ASSERT_TRUE(db.query("change submit", _workingGraphName, &_env->getMem(),
                             CommitHash::head(), change->getID()));

        spdlog::info("Submitted deletions change");
    }

protected:
    const std::string _workingGraphName {"tombstonegraph"};

    std::unique_ptr<TuringTestEnv> _env;
    Graph* _builtGraph {nullptr};
    std::unique_ptr<Graph> _loadedGraph;
    LocalMemory _mem;
    Block _outBlock;

    static constexpr size_t NUM_EDGES = 10;
    static constexpr size_t NUM_NODES = 2 * NUM_EDGES;

    static constexpr std::array<size_t, 3> DELETED_NODES = {0, NUM_NODES - 1, 2};
    static constexpr std::array<size_t, 4> DELETED_EDGES = {0, NUM_EDGES - 1, 1, 4};
};

TEST_F(TombstoneSerialisationTest, deleteNodesThenLoad) {
    _loadedGraph = Graph::create();
    auto res = GraphLoader::load(
        _loadedGraph.get(),
        _env->getSystemManager().getGraph(_workingGraphName)->getPath());
    ASSERT_TRUE(res);


    const Tombstones& tombstones =
        _loadedGraph->openTransaction().viewGraph().commits().back().tombstones();

    const Tombstones::NodeTombstones& nodeTombstones = tombstones.nodeTombstones();
    const Tombstones::EdgeTombstones& edgeTombstones = tombstones.edgeTombstones();

    ASSERT_EQ(DELETED_NODES.size(), nodeTombstones.size());
    ASSERT_EQ(DELETED_EDGES.size(), edgeTombstones.size());

    for (const NodeID deletedNode : DELETED_NODES) {
        ASSERT_TRUE(nodeTombstones.contains(deletedNode));
    }

    for (const EdgeID deletedEdge : DELETED_EDGES) {
        ASSERT_TRUE(edgeTombstones.contains(deletedEdge));
    }
}

int main(int argc, char** argv) {
    return turingTestMain(argc, argv, [] {
        testing::GTEST_FLAG(repeat) = 1;
    });
}
