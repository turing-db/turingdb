#pragma once

#include "procedures/ProcedureBlueprint.h"
#include "ProcedureData.h"

namespace db {
class ScanPropertyTypesChunkWriter;
}

namespace db::v2 {

struct PropertyTypesProcedure {
    struct Data : public ProcedureData {
        std::unique_ptr<ScanPropertyTypesChunkWriter> _it;
    };

    static std::unique_ptr<ProcedureData> allocData();
    static void execute(Procedure& proc);

    static constexpr ProcedureBlueprint Blueprint {
        ._name = "db.propertyTypes",
        ._execCallback = &execute,
        ._allocCallback = &allocData,
        ._returnValues = {
                          ProcedureData::ReturnValue {"id", ProcedureData::ReturnType::PROPERTY_TYPE_ID},
                          ProcedureData::ReturnValue {"propertyType", ProcedureData::ReturnType::STRING},
                          ProcedureData::ReturnValue {"valueType", ProcedureData::ReturnType::VALUE_TYPE}},
    };
};

}
