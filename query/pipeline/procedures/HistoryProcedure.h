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
            ._returnValues = {{"commit", ProcedureReturnType::STRING},
                              {"nodeCount", ProcedureReturnType::UINT_64},
                              {"edgeCount", ProcedureReturnType::UINT_64},
                              {"partCount", ProcedureReturnType::UINT_64}},
        };
    }
};

}
