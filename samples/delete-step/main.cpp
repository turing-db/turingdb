#include "DeleteStep.h"
#include "spdlog/spdlog.h"

#include "LocalMemory.h"
#include "StopStep.h"
#include "SystemManager.h"
#include "TuringDB.h"
#include "Pipeline.h"
#include "ExecutionContext.h"
#include "Executor.h"
#include "reader/GraphReader.h"
#include "versioning/CommitHash.h"
#include "versioning/Transaction.h"
#include "versioning/Change.h"
#include "views/GraphView.h"

using namespace db;

const std::string GRAPHNAME = "simpledb";

int deleteStep() {
    

    TuringDB db;
    db.getSystemManager().loadGraph(GRAPHNAME, db.getJobSystem());

    GraphView view = db.getSystemManager().openTransaction(GRAPHNAME, CommitHash::head(), ChangeID::head())->viewGraph();
    {
        auto readRes = db.getSystemManager().openTransaction(
            GRAPHNAME, CommitHash::head(), ChangeID::head());
        if (!readRes) {
            spdlog::error("Failed to read graph after deletion");
            return -1;
        }
        auto& readTx = readRes.value();
        auto reader = readTx.readGraph();
        size_t numParts = reader.getDatapartCount();
        spdlog::info("Graph has {} dps priort to deletion", numParts);
        for (size_t i {0}; i < numParts; i++) {
            auto nodes = reader.dataparts()[i]->getNodeCount();
            auto edges = reader.dataparts()[i]->getEdgeCount();
            spdlog::info("DP{} has {} nodes and {} edges prior to deletion.", i, nodes,
                         edges);
        }
    }

    auto changeRes = db.getSystemManager().newChange(GRAPHNAME);
    if (!changeRes) {
        spdlog::error("Failed to make a change");
        return -1;
    }
    [[maybe_unused]] Change* change = changeRes.value();

    auto changeIDRes = ChangeID::fromString("0");
    if (!changeIDRes) {
        spdlog::error("Failed to make a ChangeID");
        return -1;
    }
    ChangeID id = changeIDRes.value();

    // Scope this to prevent deadlock
    {
        auto txRes =
            db.getSystemManager().openTransaction(GRAPHNAME, CommitHash::head(), id);
        if (!txRes) {
            spdlog::error("Failed to open a transaction");
            return -1;
        }
        auto& tx = txRes.value();

        [[maybe_unused]] auto ctxt =
            ExecutionContext(&db.getSystemManager(), &db.getJobSystem(), view, GRAPHNAME,
                             CommitHash::head(), id, &tx);

        Pipeline pipe;

        pipe.add<StopStep>();
        pipe.add<DeleteStep>(std::set<NodeID> {5}, std::set<EdgeID> {});
        pipe.add<EndStep>();

        Executor ex;

        ex.run(&ctxt, &pipe);
    }

    // Submit the delete change
    auto res = change->access().submit(db.getJobSystem());
    if (!res) {
        spdlog::info("Failed to submit change");
        return -1;
    }

    auto readRes = db.getSystemManager().openTransaction(GRAPHNAME, CommitHash::head(), ChangeID::head());
    if (!readRes) {
        spdlog::error("Failed to read graph after deletion");
        return -1;
    }
    auto& readTx = readRes.value();
    auto reader = readTx.readGraph();
    size_t numParts = reader.getDatapartCount();
    spdlog::info("Graph has {} dps post deletion", numParts);
    for (size_t i {0}; i < numParts; i++) {
        auto nodes = reader.dataparts()[i]->getNodeCount();
        auto edges = reader.dataparts()[i]->getEdgeCount();
        spdlog::info("DP{} has {} nodes and {} edges post deletion.", i, nodes, edges);
    }
    
    return 0;
}


int deleteStepWithRebase() {
    TuringDB db;
    db.getSystemManager().loadGraph(GRAPHNAME, db.getJobSystem());

    GraphView view = db.getSystemManager().openTransaction(GRAPHNAME, CommitHash::head(), ChangeID::head())->viewGraph();
    {
        auto readRes = db.getSystemManager().openTransaction(
            GRAPHNAME, CommitHash::head(), ChangeID::head());
        if (!readRes) {
            spdlog::error("Failed to read graph after deletion");
            return -1;
        }
        auto& readTx = readRes.value();
        auto reader = readTx.readGraph();
        size_t numParts = reader.getDatapartCount();
        spdlog::info("Graph has {} dps priort to deletion", numParts);
        for (size_t i {0}; i < numParts; i++) {
            auto nodes = reader.dataparts()[i]->getNodeCount();
            auto edges = reader.dataparts()[i]->getEdgeCount();
            spdlog::info("DP{} has {} nodes and {} edges prior to deletion.", i, nodes,
                         edges);
        }
    }

    auto changeRes = db.getSystemManager().newChange(GRAPHNAME);
    if (!changeRes) {
        spdlog::error("Failed to make a change");
        return -1;
    }
    [[maybe_unused]] Change* change = changeRes.value();

    auto changeIDRes = ChangeID::fromString("0");
    if (!changeIDRes) {
        spdlog::error("Failed to make a ChangeID");
        return -1;
    }
    ChangeID id = changeIDRes.value();

    // Scope this to prevent deadlock
    {
        auto txRes =
            db.getSystemManager().openTransaction(GRAPHNAME, CommitHash::head(), id);
        if (!txRes) {
            spdlog::error("Failed to open a transaction");
            return -1;
        }
        auto& tx = txRes.value();

        [[maybe_unused]] auto ctxt =
            ExecutionContext(&db.getSystemManager(), &db.getJobSystem(), view, GRAPHNAME,
                             CommitHash::head(), id, &tx);

        Pipeline pipe;

        pipe.add<StopStep>();
        pipe.add<DeleteStep>(std::set<NodeID> {5}, std::set<EdgeID> {});
        pipe.add<EndStep>();

        Executor ex;

        ex.run(&ctxt, &pipe);
    }

    // Create a new change to set the head of main ahead of where the delete change was made
    /*
    {
        auto changeNewRes = db.getSystemManager().newChange(GRAPHNAME);
        if (!changeRes) {
            spdlog::error("Failed to make a change");
            return -1;
        }
        [[maybe_unused]] Change* changeNew = changeNewRes.value();
        auto resNew = changeNew->access().submit(db.getJobSystem());
        if (!resNew) {
            spdlog::info("Failed to submit change");
            return -1;
        }
    }
    */

    // Submit the delete change
    auto res = change->access().submit(db.getJobSystem());
    if (!res) {
        spdlog::info("Failed to submit change");
        return -1;
    }

    auto readRes = db.getSystemManager().openTransaction(GRAPHNAME, CommitHash::head(), ChangeID::head());
    if (!readRes) {
        spdlog::error("Failed to read graph after deletion");
        return -1;
    }
    auto& readTx = readRes.value();
    auto reader = readTx.readGraph();
    size_t numParts = reader.getDatapartCount();
    spdlog::info("Graph has {} dps post deletion", numParts);
    for (size_t i {0}; i < numParts; i++) {
        auto nodes = reader.dataparts()[i]->getNodeCount();
        auto edges = reader.dataparts()[i]->getEdgeCount();
        spdlog::info("DP{} has {} nodes and {} edges post deletion.", i, nodes, edges);
    }
    
    return 0;
}

int main() {
    deleteStepWithRebase();
}
