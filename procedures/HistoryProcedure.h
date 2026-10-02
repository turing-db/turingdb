#pragma once

#include <array>
#include <string_view>
#include <tuple>

#include "columns/ColumnVector.h"

namespace db {

class ProcedureState;
class ProcedureNamespace;

struct HistoryProcedure {
    struct Data;

    static void execute(ProcedureState* proc);
    static void registerProcedure(ProcedureNamespace* ns);

    using Returns = std::tuple<
        ColumnVector<types::String::Primitive>,
        ColumnVector<types::UInt64::Primitive>,
        ColumnVector<types::UInt64::Primitive>,
        ColumnVector<types::UInt64::Primitive>
    >;

    static constexpr size_t numReturns = std::tuple_size_v<Returns>;
    using ReturnNames = std::array<std::string_view, numReturns>;
    static constexpr ReturnNames _returnNames {
        "commit",
        "nodeCount",
        "edgeCount",
        "partCount",
    };
};

}
