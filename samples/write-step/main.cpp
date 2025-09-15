#include "DataPart.h"
#include "ExecutionContext.h"
#include "Executor.h"
#include "LocalMemory.h"
#include "Pipeline.h"
#include "SystemManager.h"
#include "TuringConfig.h"
#include "TuringDB.h"
#include "reader/GraphReader.h"
#include "spdlog/spdlog.h"
#include "versioning/Change.h"
#include "versioning/ChangeResult.h"
#include "versioning/CommitHash.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"

using namespace db;

const std::string GRAPHNAME = "simpledb";


int writeStep() {
    TuringConfig config;
    LocalMemory mem;
    TuringDB db(config);
    bool res = db.getSystemManager().loadGraph(GRAPHNAME, db.getJobSystem());
    if (!res) {
        spdlog::error("Failed to load graph");
        return -1;
    }

    auto txRes = db.getSystemManager().openTransaction(GRAPHNAME, CommitHash::head(),
                                                       ChangeID::head());
    if (!txRes) {
        spdlog::error("Failed to open transaction on graph");
        return -1;
    }
    auto& tx = txRes.value();

    auto view = tx.viewGraph();

    view.isValid();

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

    {
        // Trigger a WriteStep
        db.query("CREATE (n:NEWNODE{name=\"new name\"})", GRAPHNAME, &mem, CommitHash::head(), id);
    }

    return 0;
}



int main () {
    writeStep();
}
