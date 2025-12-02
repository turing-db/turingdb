#pragma once

#include "procedures/ProcedureBlueprint.h"
#include "ProcedureData.h"

namespace db {
class ScanEdgeTypesChunkWriter;
}

namespace db::v2 {

struct EdgeTypesProcedure {
    struct Data : public ProcedureData {
        std::unique_ptr<ScanEdgeTypesChunkWriter> _it;
    };

    static std::unique_ptr<ProcedureData> allocData();
    static void execute(Procedure& proc);

    static constexpr ProcedureBlueprint Blueprint {
        ._name = "db.edgeTypes",
        ._execCallback = &execute,
        ._allocCallback = &allocData,
        ._returnValues = {
                          ProcedureData::ReturnValue {"id", ProcedureData::ReturnType::LABEL_ID},
                          ProcedureData::ReturnValue {"edgeType", ProcedureData::ReturnType::STRING}},
    };
};

}
