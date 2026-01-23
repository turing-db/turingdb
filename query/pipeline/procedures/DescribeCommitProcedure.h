#pragma once

#include "procedures/ProcedureBlueprint.h"
#include "ProcedureData.h"

namespace db {

struct DescribeCommitProcedure {
    static std::unique_ptr<ProcedureData> allocData();
    static void execute(Procedure& proc);

    static ProcedureBlueprint createBlueprint() noexcept {
        return {
            ._name = "db.describeCommit",
            ._execCallback = &execute,
            ._allocCallback = &allocData,
            ._returnValues = {{"nodeCount", ProcedureType::UINT_64},
                              {"edgeCount", ProcedureType::UINT_64},
                              {"partCount", ProcedureType::UINT_64}},
            ._argumentTypes = {{"commit", ProcedureType::STRING}},
        };
    }
};

}
