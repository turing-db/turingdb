#pragma once

#include "ProcedureData.h"
#include "iterators/ScanPropertyTypesIterator.h"

namespace db {

class ProcedureState;
class ProcedureNamespace;

struct PropertyTypesProcedure {
    struct Data : public ProcedureData {
        std::unique_ptr<ScanPropertyTypesChunkWriter> _it;
    };

    static ProcedureData* allocData();
    static void deallocData(ProcedureData* data);
    static void execute(ProcedureState& proc);
    static void registerProcedure(ProcedureNamespace* ns);
};

}
