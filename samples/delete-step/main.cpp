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

int main() {
    TuringDB db;
    db.getSystemManager().loadGraph(GRAPHNAME, db.getJobSystem());

    GraphView view = db.getSystemManager().openTransaction(GRAPHNAME, CommitHash::head(), ChangeID::head())->viewGraph();

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

    auto txRes = db.getSystemManager().openTransaction(GRAPHNAME, CommitHash::head(), id);
    if (!txRes) {
        spdlog::error("Failed to open a transaction");
        return -1;
    }
    auto& tx = txRes.value();

    [[maybe_unused]] auto ctxt =
        ExecutionContext(&db.getSystemManager(), &db.getJobSystem(), view, GRAPHNAME,
                         CommitHash::head(), id, &tx);

    GraphReader reader = tx.readGraph();
    spdlog::info("Graph has {} nodes and {} edges prior to deletion.",
                 reader.getNodeCount(), reader.getEdgeCount());

    Pipeline pipe;

    pipe.add<StopStep>();
    pipe.add<DeleteStep>(std::set<NodeID> {0}, std::set<EdgeID> {});
    pipe.add<EndStep>();

    Executor ex;

    ex.run(&ctxt, &pipe);

    auto readRes = db.getSystemManager().openTransaction(GRAPHNAME, CommitHash::head(), id);
    if (!readRes) {
        spdlog::error("Failed to read graph after deletion");
        return -1;
    }

    auto& readTx = readRes.value();
    GraphReader readerNew = readTx.readGraph();
    spdlog::info("Graph has {} nodes and {} edges prior to deletion.",
                 readerNew.getNodeCount(), readerNew.getEdgeCount());
}
