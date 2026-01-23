#pragma once

#include "procedures/ProcedureBlueprint.h"
#include "ProcedureData.h"

namespace db {

struct HistoryProcedure {
    static std::unique_ptr<ProcedureData> allocData();
    static void execute(Procedure& proc);

    static ProcedureBlueprint createBlueprint() noexcept {
        return {
            ._name = "db.history",
            ._execCallback = &execute,
            ._allocCallback = &allocData,
            ._returnValues = {{"commit", ProcedureType::STRING},
                              {"nodeCount", ProcedureType::UINT_64},
                              {"edgeCount", ProcedureType::UINT_64},
                              {"partCount", ProcedureType::UINT_64}},
        };
    }
};

}
