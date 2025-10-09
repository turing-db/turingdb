#include "SystemManager.h"
#include "TuringConfig.h"
#include "TuringDB.h"
#include "LocalMemory.h"
#include "spdlog/spdlog.h"
#include "versioning/Change.h"
#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"
#include <string_view>

using namespace db;

static const std::string GRAPH_NAME = "working_reactome";

int main() {
    TuringConfig config = TuringConfig::createDefault();
    config.setSyncedOnDisk(false);
    TuringDB db(&config);
    LocalMemory mem;

    static const auto query = [&](std::string_view query, std::string_view graph, ChangeID id=ChangeID::head()) {
        return db.query(query, graph, &mem, CommitHash::head(), id);
    };

    db.getSystemManager().loadGraph("working_reactome");

    {
        auto res = query(
            R"(match (n{`displayName (String)`~="Autophagy"}) return n.`displayName (String)`)",
            GRAPH_NAME);

        if (!res.isOk()) {
            spdlog::error("Query failed to execute.");
            return -1;
        }
    }

    auto changeRes = db.getSystemManager().newChange(GRAPH_NAME);
    if (changeRes) {
        spdlog::error("Cannot make change: {}", changeRes.error().fmtMessage());
        return -1;
    }
    ChangeID chID = changeRes.value()->id();

    auto res = query(R"(delete nodes 2000)", GRAPH_NAME, chID);
    if (!res.isOk()) {
        spdlog::error("Query failed to execute: {}", res.getError());
        return -1;
    }

    return 0;
}
