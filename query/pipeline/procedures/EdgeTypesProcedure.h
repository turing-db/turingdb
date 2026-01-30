#pragma once

#include "ProcedureData.h"
#include "iterators/ScanEdgeTypesIterator.h"

namespace db {

class ProcedureState;
class ProcedureNamespace;

struct EdgeTypesProcedure {
    struct Data : public ProcedureData {
        std::unique_ptr<ScanEdgeTypesChunkWriter> _it;
    };

    static ProcedureData* allocData();
    static void deallocData(ProcedureData* data);
    static void execute(ProcedureState& proc);
    static void registerProcedure(ProcedureNamespace* ns);
};

}
