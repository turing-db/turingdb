#pragma once

#include "procedures/ProcedureBlueprint.h"
#include "ProcedureData.h"
#include "iterators/ScanLabelsIterator.h"

namespace db {

struct LabelsProcedure {
    struct Data : public ProcedureData {
        std::unique_ptr<ScanLabelsChunkWriter> _it;
    };

    static std::unique_ptr<ProcedureData> allocData();
    static void execute(Procedure& proc);

    static ProcedureBlueprint createBlueprint() noexcept {
        return {
            ._name = "db.labels",
            ._execCallback = &execute,
            ._allocCallback = &allocData,
            ._returnValues = {{"id", ProcedureReturnType::LABEL_ID},
                              {"label", ProcedureReturnType::STRING_VIEW}},
        };
    }
};

}
