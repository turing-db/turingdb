#pragma once

#include <array>
#include <string_view>
#include <tuple>

#include "columns/ColumnVector.h"
#include "metadata/PropertyType.h"

namespace db {

class ProcedureState;
class ProcedureNamespace;

// Given a labelset return every possible label that forms a valid label sub-set
// with the count of nodes that contain new label sub-set.
//
// For example if we have nodes with the label sets
// - ABC - node count 2
// - ABDE -3
// - ABWZU - 5
// - ABWXY - 10
// - AXCD -100
//
// and we provide [A,B] as the input labelsets - we will get output
//
// C - 2
// D - 3
// E - 3
// W - 15
// Z - 5
// U - 5
// X - 10
// Y - 10
//
struct HierarchicalLabelCountsProcedure {
    static void execute(ProcedureState* proc);
    static void registerProcedure(ProcedureNamespace* ns);

    using Returns = std::tuple<
        ColumnVector<types::String::Primitive>,
        ColumnVector<types::UInt64::Primitive>
    >;

    static constexpr size_t numReturns = std::tuple_size_v<Returns>;
    using ReturnNames = std::array<std::string_view, numReturns>;
    static constexpr ReturnNames _returnNames {
        "label",
        "nodeCount",
    };
};

}
