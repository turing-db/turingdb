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

    static ProcedureBlueprint createBlueprint() noexcept {
        try {
            return {
                ._name = "db.edgeTypes",
                ._execCallback = &execute,
                ._allocCallback = &allocData,
                ._returnValues = {{"id", ProcedureReturnType::EDGE_TYPE_ID},
                                  {"edgeType", ProcedureReturnType::STRING}},
                ._valid = true,
            };
        } catch (const std::exception& e) {
            return {};
        }
    }
};

}
