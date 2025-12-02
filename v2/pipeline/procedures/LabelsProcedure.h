#pragma once

#include "procedures/ProcedureBlueprint.h"
#include "ProcedureData.h"

namespace db {
class ScanLabelsChunkWriter;
}

namespace db::v2 {

struct LabelsProcedure {
    struct Data : public ProcedureData {
        std::unique_ptr<ScanLabelsChunkWriter> _it;
    };

    static std::unique_ptr<ProcedureData> allocData();
    static void execute(Procedure& proc);

    static constexpr ProcedureBlueprint Blueprint {
        ._name = "db.labels",
        ._execCallback = &execute,
        ._allocCallback = &allocData,
        ._returnValues = {
                          ProcedureData::ReturnValue {"id", ProcedureData::ReturnType::LABEL_ID},
                          ProcedureData::ReturnValue {"label", ProcedureData::ReturnType::STRING}},
    };
};

}
