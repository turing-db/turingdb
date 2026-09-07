#pragma once

#include "Procedure.h"
#include "ProcedureTypeVector.h"

namespace db {

class ProcedureData;
class ProcedureState;
class ProcedureNamespace;

struct HistoryProcedure {
    static ProcedureData* allocData();
    static void deallocData(ProcedureData* data);
    static void execute(ProcedureState* proc);
    static void registerProcedure(ProcedureNamespace* ns);

    static constexpr Procedure::ReturnItems<4> items {
        {{._name = "commit", ._type = ProcedureType::STRING_VIEW},
         {._name = "nodeCount", ._type = ProcedureType::UINT_64},
         {._name = "edgeCount", ._type = ProcedureType::UINT_64},
         {._name = "partCount", ._type = ProcedureType::UINT_64}}
    };
};

}
