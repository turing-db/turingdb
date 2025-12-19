#pragma once

#include "procedures/ProcedureBlueprint.h"
#include "ProcedureData.h"

namespace db {
class ScanNodesProcessor;
}

namespace db::v2 {

struct ShortestPathProcedure {
    struct Data : public ProcedureData {
        std::unique_ptr<ScanNodesProcessor> _it;
    };

    static std::unique_ptr<ProcedureData> allocData();
    static void execute(Procedure& proc);

    static ProcedureBlueprint createBlueprint() noexcept {
        return {
            ._name = "db.shortestPath",
            ._execCallback = &execute,
            ._allocCallback = &allocData,
            ._returnValues = {{"numHops", ProcedureReturnType::INT64}},
        };
    }
};

}
