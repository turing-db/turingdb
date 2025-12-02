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

    static ProcedureBlueprint createBlueprint() noexcept {
        try {
            return {
                ._name = "db.propertyTypes",
                ._execCallback = &execute,
                ._allocCallback = &allocData,
                ._returnValues = {{"id", ProcedureReturnType::PROPERTY_TYPE_ID},
                                  {"propertyType", ProcedureReturnType::STRING},
                                  {"valueType", ProcedureReturnType::VALUE_TYPE}},
                ._valid = true,
            };
        } catch (const std::exception& e) {
            return {};
        }
    }
};

}
