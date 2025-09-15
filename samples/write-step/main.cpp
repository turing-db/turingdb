#include "DataPart.h"
#include "ExecutionContext.h"
#include "Executor.h"
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
    TuringDB db(TuringConfig {});
    db.getSystemManager().loadGraph(GRAPHNAME, db.getJobSystem());

    auto txRes = db.getSystemManager().openTransaction(GRAPHNAME, CommitHash::head(),
                                                       ChangeID::head());
    if (!txRes) {
        spdlog::info("Failed to open transaction on graph");
    }
    auto& tx = txRes.value();

    auto view = tx.viewGraph();

    view.isValid();

    return 0;
}



int main () {
    writeStep();
}
