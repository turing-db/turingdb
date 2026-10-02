#pragma once

#include <array>
#include <string_view>
#include <tuple>

#include "ID.h"
#include "columns/ColumnOptVector.h"

#include "samplers/GraphSAGESampler.h"

#include <stddef.h>

namespace db {

class ProcedureState;
class ProcedureNamespace;

struct GraphSAGEProcedure {
    static void execute(ProcedureState* proc);
    static void registerProcedure(ProcedureNamespace* ns);

    static constexpr size_t numHops = GraphSAGESampler::hops;

    using Returns = std::tuple<
        ColumnOptVector<NodeID>,
        ColumnOptVector<NodeID>,
        ColumnOptVector<NodeID>,
        ColumnOptVector<NodeID>,
        ColumnOptVector<NodeID>,
        ColumnOptVector<NodeID>,
        ColumnOptVector<NodeID>,
        ColumnOptVector<NodeID>,
        ColumnOptVector<NodeID>
    >;

    static constexpr size_t numReturns = std::tuple_size_v<Returns>;
    using ReturnNames = std::array<std::string_view, numReturns>;
    static constexpr ReturnNames _returnNames {
        "dst_nodes0", "src_nodes0", "tgt_nodes0",
        "dst_nodes1", "src_nodes1", "tgt_nodes1",
        "dst_nodes2", "src_nodes2", "tgt_nodes2",
    };
};

}
