#include "EvalProgram.h"

#include <spdlog/fmt/fmt.h>

#include "columns/ColumnOperators.h"
#include "columns/ColumnKind.h"
#include "metadata/LabelSet.h"

#include "PipelineException.h"

using namespace db;
using namespace db::v2;

namespace {

constexpr ColumnKind::ColumnKindCode getBaseFromKind(ColumnKind::ColumnKindCode kind) {
    return kind / ColumnKind::getInternalTypeKindCount();
}

constexpr ColumnKind::ColumnKindCode getInternalFromKind(ColumnKind::ColumnKindCode kind) {
    return kind % ColumnKind::getInternalTypeKindCount();
}

constexpr ColumnKind::ColumnKindCode getPairKind(ColumnKind::ColumnKindCode lhs,
                                                 ColumnKind::ColumnKindCode rhs) {
    // Comparing each pair of the same internal type (ex: Vector<size_t> and Const<size_t>
    const auto baseLhsCode = getBaseFromKind(lhs);
    const auto baseRhsCode = getBaseFromKind(rhs);
    const auto baseCount = ColumnKind::getBaseColumnKindCount();
    const auto typeCount = ColumnKind::getInternalTypeKindCount();
    const auto basePairCount = baseCount * baseCount;
    const auto typePairCount = typeCount * typeCount;
    const auto counts = basePairCount + typePairCount;
    const auto internalLhsCode = getInternalFromKind(lhs);
    const auto internalRhsCode = getInternalFromKind(rhs);
    return baseLhsCode * counts + baseRhsCode * typePairCount + internalLhsCode * typeCount + internalRhsCode;
}

constexpr ColumnKind::ColumnKindCode getOpCase(ColumnOperator op,
                                               ColumnKind::ColumnKindCode lhs,
                                               ColumnKind::ColumnKindCode rhs) {
    const auto internalCount = ColumnKind::getInternalTypeKindCount();
    const auto basePairCount = ColumnKind::getBasePairColumnCount();
    return op * basePairCount * internalCount + getPairKind(lhs, rhs);
}

template <ColumnOperator Op, typename Lhs, typename Rhs>
static constexpr ColumnKind::ColumnKindCode OpCase = getOpCase(Op,
                                                               Lhs::staticKind(),
                                                               Rhs::staticKind());

}

#define EQUAL_CASE(Lhs, Rhs)                      \
    case OpCase<OP_EQUAL, Lhs, Rhs>: {            \
        ColumnOperators::equal(                   \
            static_cast<ColumnMask*>(instr._res), \
            static_cast<const Lhs*>(instr._lhs),  \
            static_cast<const Rhs*>(instr._rhs)); \
        break;                                    \
    }

#define AND_CASE(Lhs, Rhs)                        \
    case OpCase<OP_AND, Lhs, Rhs>: {              \
        ColumnOperators::andOp(                   \
            static_cast<ColumnMask*>(instr._res), \
            static_cast<const Lhs*>(instr._lhs),  \
            static_cast<const Rhs*>(instr._rhs)); \
        break;                                    \
    }

#define OR_CASE(Lhs, Rhs)                         \
    case OpCase<OP_OR, Lhs, Rhs>: {               \
        ColumnOperators::orOp(                    \
            static_cast<ColumnMask*>(instr._res), \
            static_cast<const Lhs*>(instr._lhs), \
            static_cast<const Rhs*>(instr._rhs)); \
        break;                                    \
    }

#define PROJECT_CASE(Lhs, Rhs)                    \
    case OpCase<OP_PROJECT, Lhs, Rhs>: {          \
        ColumnOperators::projectOp(               \
            static_cast<ColumnMask*>(instr._res), \
            static_cast<const Lhs*>(instr._lhs),  \
            static_cast<const Rhs*>(instr._rhs)); \
        break;                                    \
    }

#define IN_CASE(Lhs, Rhs)                         \
    case OpCase<OP_IN, Lhs, Rhs>: {               \
        ColumnOperators::inOp(                    \
            static_cast<ColumnMask*>(instr._res), \
            static_cast<const Lhs*>(instr._lhs),  \
            static_cast<const Rhs*>(instr._rhs)); \
        break;                                    \
    }


EvalProgram::EvalProgram()
{
}

EvalProgram::~EvalProgram() {
}

void EvalProgram::execute() {
    for (const Instruction& instr : _instrs) {
        evalInstr(instr);
    }
}

void EvalProgram::evalInstr(const Instruction& instr) {
    const ColumnOperator op = instr._op;
    const Column* lhs = instr._lhs;
    const Column* rhs = instr._rhs;
    switch (getOpCase(op, lhs->getKind(), rhs->getKind())) {
        EQUAL_CASE(ColumnVector<size_t>, ColumnVector<size_t>)
        EQUAL_CASE(ColumnVector<size_t>, ColumnConst<size_t>)
        EQUAL_CASE(ColumnConst<size_t>, ColumnVector<size_t>)
        EQUAL_CASE(ColumnConst<size_t>, ColumnConst<size_t>)
        EQUAL_CASE(ColumnVector<EntityID>, ColumnVector<EntityID>)
        EQUAL_CASE(ColumnVector<EntityID>, ColumnConst<EntityID>)
        EQUAL_CASE(ColumnConst<EntityID>, ColumnVector<EntityID>)
        EQUAL_CASE(ColumnConst<EntityID>, ColumnConst<EntityID>)
        EQUAL_CASE(ColumnVector<NodeID>, ColumnVector<NodeID>)
        EQUAL_CASE(ColumnVector<NodeID>, ColumnConst<NodeID>)
        EQUAL_CASE(ColumnConst<NodeID>, ColumnVector<NodeID>)
        EQUAL_CASE(ColumnConst<NodeID>, ColumnConst<NodeID>)
        EQUAL_CASE(ColumnVector<EdgeID>, ColumnVector<EdgeID>)
        EQUAL_CASE(ColumnVector<EdgeID>, ColumnConst<EdgeID>)
        EQUAL_CASE(ColumnConst<EdgeID>, ColumnVector<EdgeID>)
        EQUAL_CASE(ColumnConst<EdgeID>, ColumnConst<EdgeID>)
        EQUAL_CASE(ColumnVector<LabelSetID>, ColumnVector<LabelSetID>)
        EQUAL_CASE(ColumnVector<LabelSetID>, ColumnConst<LabelSetID>)
        EQUAL_CASE(ColumnConst<LabelSetID>, ColumnVector<LabelSetID>)
        EQUAL_CASE(ColumnConst<LabelSetID>, ColumnConst<LabelSetID>)
        EQUAL_CASE(ColumnVector<LabelSet>, ColumnVector<LabelSet>)
        EQUAL_CASE(ColumnVector<LabelSet>, ColumnConst<LabelSet>)
        EQUAL_CASE(ColumnConst<LabelSet>, ColumnVector<LabelSet>)
        EQUAL_CASE(ColumnConst<LabelSet>, ColumnConst<LabelSet>)
        EQUAL_CASE(ColumnVector<std::string>, ColumnVector<std::string>)
        EQUAL_CASE(ColumnVector<std::string>, ColumnConst<std::string>)
        EQUAL_CASE(ColumnConst<std::string>, ColumnVector<std::string>)
        EQUAL_CASE(ColumnConst<std::string>, ColumnConst<std::string>)
        EQUAL_CASE(ColumnVector<std::string_view>, ColumnVector<std::string_view>)
        EQUAL_CASE(ColumnVector<std::string_view>, ColumnConst<std::string_view>)
        EQUAL_CASE(ColumnConst<std::string_view>, ColumnVector<std::string_view>)
        EQUAL_CASE(ColumnConst<std::string_view>, ColumnConst<std::string_view>)
        EQUAL_CASE(ColumnVector<std::string_view>, ColumnVector<std::string>)
        EQUAL_CASE(ColumnVector<std::string_view>, ColumnConst<std::string>)
        EQUAL_CASE(ColumnConst<std::string_view>, ColumnVector<std::string>)
        EQUAL_CASE(ColumnConst<std::string_view>, ColumnConst<std::string>)
        EQUAL_CASE(ColumnVector<std::string>, ColumnVector<std::string_view>)
        EQUAL_CASE(ColumnVector<std::string>, ColumnConst<std::string_view>)
        EQUAL_CASE(ColumnConst<std::string>, ColumnVector<std::string_view>)
        EQUAL_CASE(ColumnConst<std::string>, ColumnConst<std::string_view>)
        EQUAL_CASE(ColumnVector<CustomBool>, ColumnConst<CustomBool>)
        EQUAL_CASE(ColumnConst<CustomBool>, ColumnVector<CustomBool>)
        EQUAL_CASE(ColumnVector<double>, ColumnConst<double>)
        EQUAL_CASE(ColumnConst<double>, ColumnVector<double>)
        EQUAL_CASE(ColumnVector<int64_t>, ColumnConst<int64_t>)
        EQUAL_CASE(ColumnConst<int64_t>, ColumnVector<int64_t>)

        AND_CASE(ColumnMask, ColumnMask)
        OR_CASE(ColumnMask, ColumnMask)

        PROJECT_CASE(ColumnVector<size_t>, ColumnMask)
        PROJECT_CASE(ColumnMask, ColumnVector<size_t>)

        IN_CASE(ColumnVector<size_t>, ColumnSet<size_t>)
        IN_CASE(ColumnVector<EntityID>, ColumnSet<EntityID>)
        IN_CASE(ColumnVector<NodeID>, ColumnSet<NodeID>)
        IN_CASE(ColumnVector<EdgeID>, ColumnSet<EdgeID>)
        IN_CASE(ColumnVector<LabelSetID>, ColumnSet<LabelSetID>)
        IN_CASE(ColumnVector<LabelSet>, ColumnSet<LabelSet>)
        IN_CASE(ColumnVector<std::string>, ColumnSet<std::string>)
        IN_CASE(ColumnVector<double>, ColumnSet<double>)
        IN_CASE(ColumnVector<int64_t>, ColumnSet<int64_t>)

        default: {
            throw PipelineException(fmt::format("Operator not implemented (kinds: {} and {})",
                                    lhs->getKind(),
                                    rhs->getKind()));
        }
    }
}
