#pragma once

#include "Procedure.h"
#include "ProcedureData.h"
#include "ProcedureTypeVector.h"

namespace db {

class ProcedureState;
class ProcedureNamespace;

struct HistoryProcedure {
    static ProcedureData* allocData();
    static void deallocData(ProcedureData* data);
    static void execute(ProcedureState* proc);
    static void registerProcedure(ProcedureNamespace* ns);

    template <size_t I>
    static constexpr auto* getReturnColumn(ProcedureData* data) {
        Column* col = data->getReturnColumn(I);
        using Type = ProcedureTypeType_t<_returnItems[I]._type>;
        return static_cast<ColumnVector<Type>*>(col);
    }

    static constexpr size_t numReturnItems = 4;
    static constexpr Procedure::ReturnItems<numReturnItems> _returnItems {{
        {._name = "commit", ._type = ProcedureType::STRING_VIEW},
        {._name = "nodeCount", ._type = ProcedureType::UINT_64},
        {._name = "edgeCount", ._type = ProcedureType::UINT_64},
        {._name = "partCount", ._type = ProcedureType::UINT_64},
    }};
};

}
