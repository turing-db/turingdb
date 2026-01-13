#pragma once

#include "procedures/ProcedureBlueprint.h"
#include "ProcedureData.h"
#include "iterators/ScanPropertyTypesIterator.h"

namespace db {

struct PropertyTypesProcedure {
    struct Data : public ProcedureData {
        std::unique_ptr<ScanPropertyTypesChunkWriter> _it;
    };

    static std::unique_ptr<ProcedureData> allocData();
    static void execute(Procedure& proc);

    static ProcedureBlueprint createBlueprint() noexcept {
        return {
            ._name = "db.propertyTypes",
            ._execCallback = &execute,
            ._allocCallback = &allocData,
            ._returnValues = {{"id", ProcedureReturnType::PROPERTY_TYPE_ID},
                              {"propertyType", ProcedureReturnType::STRING_VIEW},
                              {"valueType", ProcedureReturnType::VALUE_TYPE}},
        };
    }
};

}
