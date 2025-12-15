#include "ExprProgram.h"

#include <cstdint>
#include <iostream>
#include <spdlog/fmt/fmt.h>
#include <string_view>

#include "FatalException.h"
#include "columns/ColumnOperator.h"
#include "columns/ColumnOperators.h"
#include "columns/ColumnKind.h"
#include "columns/ColumnOptVector.h"
#include "columns/ColumnVector.h"
#include "metadata/LabelSet.h"

#include "PipelineV2.h"

#include "PipelineException.h"
#include "metadata/PropertyType.h"
#include "spdlog/fmt/bundled/base.h"
#include "spdlog/spdlog.h"

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

// Unary operator
constexpr ColumnKind::ColumnKindCode getOpCase(ColumnOperator op,
                                               ColumnKind::ColumnKindCode lhs) {
    const auto internalCount = ColumnKind::getInternalTypeKindCount();
    return (op * internalCount) + lhs;
}

template <ColumnOperator Op, typename Lhs, typename Rhs>
constexpr ColumnKind::ColumnKindCode OpCase =
    getOpCase(Op, Lhs::staticKind(), Rhs::staticKind());
}

template <ColumnOperator Op, typename Lhs>
constexpr ColumnKind::ColumnKindCode UnaryOpCase = getOpCase(Op, Lhs::staticKind());

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
            static_cast<const Lhs*>(instr._lhs),  \
            static_cast<const Rhs*>(instr._rhs)); \
        break;                                    \
    }

#define NOT_CASE(Lhs)                             \
    case UnaryOpCase<OP_NOT, Lhs>: {              \
        ColumnOperators::notOp(                   \
            static_cast<ColumnMask*>(instr._res), \
            static_cast<const Lhs*>(instr._lhs)); \
    break;                                        \
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


ExprProgram* ExprProgram::create(PipelineV2* pipeline) {
    ExprProgram* prog = new ExprProgram();
    pipeline->addExprProgram(prog);

    return prog;
}

void ExprProgram::evaluateInstructions() {
    fmt::println(" we have {} instrs", _instrs.size());
    for (const Instruction& instr : _instrs) {
        evalInstr(instr);
    }
}

void ExprProgram::evalInstr(const Instruction& instr) {
    const ColumnOperator op = instr._op;

    switch (getOperatorType(op)) {
        case ColumnOperatorType::OPTYPE_BINARY:
            evalBinaryInstr(instr);
        break;

        case ColumnOperatorType::OPTYPE_UNARY:
            evalUnaryInstr(instr);
        break;

        case ColumnOperatorType::OPTYPE_NOOP:
            return;
        break;
    }
    fmt::println("Evaluated instruction, result column:");
    instr._res->dump(std::cout);
}

void ExprProgram::evalBinaryInstr(const Instruction& instr) {
    fmt::println("Evalutaing binary expr");
    const ColumnOperator op = instr._op;
    const Column* lhs = instr._lhs;
    const Column* rhs = instr._rhs;

    if (!lhs) {
        throw FatalException("Binary instruction had null left input.");
    }
    if (!rhs) {
        throw FatalException("Binary instruction had null right input.");
    }

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

        // Property opts
        EQUAL_CASE(ColumnOptVector<types::String::Primitive>, ColumnOptVector<types::String::Primitive>)
        EQUAL_CASE(ColumnOptVector<types::String::Primitive>, ColumnConst<types::String::Primitive>)

        AND_CASE(ColumnMask, ColumnMask)
        OR_CASE(ColumnMask, ColumnMask)
        OR_CASE(ColumnVector<types::Bool::Primitive>, ColumnVector<types::Bool::Primitive>)

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
            const std::string_view opName = ColumnOperatorDescription::value(op);
            throw PipelineException(
                fmt::format("Operator {} not implemented (kinds: {} and {})", opName,
                            lhs->getKind(), rhs->getKind()));
        }
    }
}

void ExprProgram::evalUnaryInstr(const Instruction& instr) {
    const ColumnOperator op = instr._op;
    const Column* input = instr._lhs;

    switch (getOpCase(op, input->getKind())) {
        // XXX: What else can NOT be applied to?
        NOT_CASE(ColumnVector<types::Bool::Primitive>); // Also handles CustomBool

        default: {
            const std::string_view opName = ColumnOperatorDescription::value(op);
            throw PipelineException(
                fmt::format("Unary operator {} not implemented for input kind: {} ",
                            opName, (uint8_t)input->getKind()));
        }
    }
}
