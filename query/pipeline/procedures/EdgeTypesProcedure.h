#pragma once

#include "procedures/ProcedureBlueprint.h"
#include "ProcedureData.h"
#include "iterators/ScanEdgeTypesIterator.h"

namespace db {

struct EdgeTypesProcedure {
    struct Data : public ProcedureData {
        std::unique_ptr<ScanEdgeTypesChunkWriter> _it;
    };

    static std::unique_ptr<ProcedureData> allocData();
    static void execute(Procedure& proc);

    static ProcedureBlueprint createBlueprint() noexcept {
        return {
            ._name = "db.edgeTypes",
            ._execCallback = &execute,
            ._allocCallback = &allocData,
            ._returnValues = {{"id", ProcedureType::EDGE_TYPE_ID},
                              {"edgeType", ProcedureType::STRING_VIEW}},
        };
    }
};

}
