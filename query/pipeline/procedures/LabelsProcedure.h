#pragma once

#include "ProcedureData.h"
#include "iterators/ScanLabelsIterator.h"

namespace db {

class ProcedureState;
class ProcedureNamespace;

struct LabelsProcedure {
    struct Data : public ProcedureData {
        std::unique_ptr<ScanLabelsChunkWriter> _it;
    };

    static ProcedureData* allocData();
    static void deallocData(ProcedureData* data);
    static void execute(ProcedureState& proc);
    static void registerProcedure(ProcedureNamespace* ns);
};

}
