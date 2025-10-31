#include <gtest/gtest.h>

#include "Panic.h"
#include "SystemManager.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"
#include "columns/Block.h"
#include "versioning/Change.h"
#include "Graph.h"

using namespace db;
using namespace turing::test;

class TombstoneSerialisationTest : public TuringTest {
public:
    void initialize() override {
        _env = TuringTestEnv::createSyncedOnDisk(fs::Path {_outDir} / "turing");

        _builtGraph = _env->getSystemManager().createGraph(_workingGraphName);

        populate();
    }

    

    void populate() {
        auto& db = _env->getDB();
        auto& sysMan = _env->getSystemManager();

        auto _callback = [this](const Block& block) {
            this->_outBlock.assignFrom(block);
        };

        auto res = sysMan.newChange(_workingGraphName);
        if (!res) {
            panic("Failed to make change in populate().");
        }
        Change* change = res.value();

        // populate the graph
        for (size_t i = 0; i < NUM_EDGES; i++) {
            db.query("create (n:Person)-[e:FRIENDSWITH]-(m:Person)",
                     _workingGraphName,
                     &_env->getMem(),
                     CommitHash::head(),
                     change->id());
        }
        spdlog::info("Ran create queries");

        db.query("change submit",
                 _workingGraphName,
                 &_env->getMem(),
                 CommitHash::head(),
                 change->id());
        spdlog::info("Submitted change");

        db.query("match (n) return n", _workingGraphName, &_env->getMem(), _callback,
                 CommitHash::head(), ChangeID::head());
        if (_outBlock.empty()) {
            panic("Failed to populate graph.");
        }
        spdlog::info("Successfully populated graph");
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
};

TEST_F(TombstoneSerialisationTest, deleteNodesAfterLoad) {
    ASSERT_TRUE(false);
}
