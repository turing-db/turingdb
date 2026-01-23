#pragma once

#include "procedures/ProcedureBlueprint.h"
#include "ProcedureData.h"

namespace db {

struct ProceduresProcedure {
    static std::unique_ptr<ProcedureData> allocData();
    static void execute(Procedure& proc);

    static ProcedureBlueprint createBlueprint() noexcept {
        return {
            ._name = "db.procedures",
            ._execCallback = &execute,
            ._allocCallback = &allocData,
            ._returnValues = {{"name", ProcedureType::STRING_VIEW},
                              {"signature", ProcedureType::STRING}},
        };
    }
};

}
