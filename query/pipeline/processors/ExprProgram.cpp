#include "ExprProgram.h"

#include <stdint.h>
#include <string_view>

#include "columns/ColumnOperator.h"
#include "columns/ColumnOperators.h"
#include "columns/OperationKind.h"
#include "columns/ColumnKind.h"
#include "columns/ColumnOptVector.h"
#include "columns/ColumnVector.h"
#include "metadata/LabelSet.h"
#include "metadata/PropertyType.h"

#include "PipelineV2.h"

#include "PipelineException.h"
#include "FatalException.h"

using namespace db;

namespace {

template <ColumnOperator Op, typename Lhs, typename Rhs>
constexpr OperationKind::Code OpCase = OperationKind::code(Op, Lhs::staticKind(), Rhs::staticKind());

template <ColumnOperator Op, typename Lhs>
constexpr OperationKind::Code UnaryOpCase = OperationKind::code(Op, Lhs::staticKind());

}

// Mask operators
#define AND_MASK_CASE(Lhs, Rhs)                                      \
    case OpCase<OP_AND, Lhs, Rhs>: {                                 \
        ColumnOperators::andOp(static_cast<ColumnMask*>(instr._res), \
                               static_cast<const Lhs*>(instr._lhs),  \
                               static_cast<const Rhs*>(instr._rhs)); \
        break;                                                       \
    }

#define OR_MASK_CASE(Lhs, Rhs)                                      \
    case OpCase<OP_OR, Lhs, Rhs>: {                                 \
        ColumnOperators::orOp(static_cast<ColumnMask*>(instr._res), \
                              static_cast<const Lhs*>(instr._lhs),  \
                              static_cast<const Rhs*>(instr._rhs)); \
        break;                                                      \
    }

#define NOT_MASK_CASE(Lhs)                                           \
    case UnaryOpCase<OP_NOT, Lhs>: {                                 \
        ColumnOperators::notOp(static_cast<ColumnMask*>(instr._res), \
                               static_cast<const Lhs*>(instr._lhs)); \
        break;                                                       \
    }

// Property-based operators - these use a ColumnOpt as their result
#define EQUAL_CASE(Lhs, Rhs)                                            \
    case OpCase<OP_EQUAL, Lhs, Rhs>: {                                  \
        ColumnOperators::equal(static_cast<ColumnOptMask*>(instr._res), \
                               static_cast<const Lhs*>(instr._lhs),     \
                               static_cast<const Rhs*>(instr._rhs));    \
        break;                                                          \
    }

#define NOT_EQUAL_CASE(Lhs, Rhs)                                           \
    case OpCase<OP_NOT_EQUAL, Lhs, Rhs>: {                                 \
        ColumnOperators::notEqual(static_cast<ColumnOptMask*>(instr._res), \
                                  static_cast<const Lhs*>(instr._lhs),     \
                                  static_cast<const Rhs*>(instr._rhs));    \
        break;                                                             \
    }

#define GREATER_THAN_CASE(Lhs, Rhs)                                           \
    case OpCase<OP_GREATER_THAN, Lhs, Rhs>: {                                 \
        ColumnOperators::greaterThan(static_cast<ColumnOptMask*>(instr._res), \
                                     static_cast<const Lhs*>(instr._lhs),     \
                                     static_cast<const Rhs*>(instr._rhs));    \
        break;                                                                \
    }

#define LESS_THAN_CASE(Lhs, Rhs)                                           \
    case OpCase<OP_LESS_THAN, Lhs, Rhs>: {                                 \
        ColumnOperators::lessThan(static_cast<ColumnOptMask*>(instr._res), \
                                  static_cast<const Lhs*>(instr._lhs),     \
                                  static_cast<const Rhs*>(instr._rhs));    \
        break;                                                             \
    }

#define GREATER_THAN_OR_EQUAL_CASE(Lhs, Rhs)                                         \
    case OpCase<OP_GREATER_THAN_OR_EQUAL, Lhs, Rhs>: {                               \
        ColumnOperators::greaterThanOrEqual(static_cast<ColumnOptMask*>(instr._res), \
                                            static_cast<const Lhs*>(instr._lhs),     \
                                            static_cast<const Rhs*>(instr._rhs));    \
        break;                                                                       \
    }

#define LESS_THAN_OR_EQUAL_CASE(Lhs, Rhs)                                         \
    case OpCase<OP_LESS_THAN_OR_EQUAL, Lhs, Rhs>: {                               \
        ColumnOperators::lessThanOrEqual(static_cast<ColumnOptMask*>(instr._res), \
                                         static_cast<const Lhs*>(instr._lhs),     \
                                         static_cast<const Rhs*>(instr._rhs));    \
        break;                                                                    \
    }

#define AND_CASE(Lhs, Rhs)                                                     \
    case OpCase<OP_AND, Lhs, Rhs>: {                                           \
        ColumnOperators::andOp(                                                \
            static_cast<ColumnOptVector<types::Bool::Primitive>*>(instr._res), \
            static_cast<const Lhs*>(instr._lhs),                               \
            static_cast<const Rhs*>(instr._rhs));                              \
        break;                                                                 \
    }

#define OR_CASE(Lhs, Rhs)                                                      \
    case OpCase<OP_OR, Lhs, Rhs>: {                                            \
        ColumnOperators::orOp(                                                 \
            static_cast<ColumnOptVector<types::Bool::Primitive>*>(instr._res), \
            static_cast<const Lhs*>(instr._lhs),                               \
            static_cast<const Rhs*>(instr._rhs));                              \
        break;                                                                 \
    }

#define NOT_CASE(Lhs)                                                          \
    case UnaryOpCase<OP_NOT, Lhs>: {                                           \
        ColumnOperators::notOp(                                                \
            static_cast<ColumnOptVector<types::Bool::Primitive>*>(instr._res), \
            static_cast<const Lhs*>(instr._lhs));                              \
        break;                                                                 \
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

#define INSTANTIATE_PROPERTY_OPERATOR(CASE_NAME) \
    CASE_NAME(ColumnOptVector<types::Int64::Primitive>, ColumnOptVector<types::Int64::Primitive>)    \
    CASE_NAME(ColumnOptVector<types::Int64::Primitive>, ColumnConst<types::Int64::Primitive>)        \
    CASE_NAME(ColumnConst<types::Int64::Primitive>, ColumnOptVector<types::Int64::Primitive>)        \
                                                                                                     \
    CASE_NAME(ColumnOptVector<types::UInt64::Primitive>, ColumnOptVector<types::UInt64::Primitive>)  \
    CASE_NAME(ColumnOptVector<types::UInt64::Primitive>, ColumnConst<types::UInt64::Primitive>)      \
    CASE_NAME(ColumnConst<types::UInt64::Primitive>, ColumnOptVector<types::UInt64::Primitive>)      \
                                                                                                     \
    CASE_NAME(ColumnOptVector<types::Double::Primitive>, ColumnOptVector<types::Double::Primitive>)  \
    CASE_NAME(ColumnOptVector<types::Double::Primitive>, ColumnConst<types::Double::Primitive>)      \
    CASE_NAME(ColumnConst<types::Double::Primitive>, ColumnOptVector<types::Double::Primitive>)      \
                                                                                                     \
    CASE_NAME(ColumnOptVector<types::String::Primitive>, ColumnOptVector<types::String::Primitive>)  \
    CASE_NAME(ColumnOptVector<types::String::Primitive>, ColumnConst<types::String::Primitive>)      \
    CASE_NAME(ColumnConst<types::String::Primitive>, ColumnOptVector<types::String::Primitive>)      \
                                                                                                     \
    CASE_NAME(ColumnOptVector<types::Bool::Primitive>, ColumnOptVector<types::Bool::Primitive>)      \
    CASE_NAME(ColumnOptVector<types::Bool::Primitive>, ColumnConst<types::Bool::Primitive>)          \
    CASE_NAME(ColumnConst<types::Bool::Primitive>, ColumnOptVector<types::Bool::Primitive>)          \
                                                                                                     \
    /* Numeric types are totally ordered: allow comparisions between types.*/                        \
    /* NOTE: Some are blocked by planner */                                                          \
    CASE_NAME(ColumnOptVector<types::Int64::Primitive>, ColumnOptVector<types::UInt64::Primitive>)   \
    CASE_NAME(ColumnOptVector<types::Int64::Primitive>, ColumnOptVector<types::Double::Primitive>)   \
    CASE_NAME(ColumnOptVector<types::Int64::Primitive>, ColumnConst<types::UInt64::Primitive>)       \
    CASE_NAME(ColumnOptVector<types::Int64::Primitive>, ColumnConst<types::Double::Primitive>)       \
    CASE_NAME(ColumnOptVector<types::UInt64::Primitive>, ColumnOptVector<types::Int64::Primitive>)   \
    CASE_NAME(ColumnOptVector<types::UInt64::Primitive>, ColumnOptVector<types::Double::Primitive>)  \
    CASE_NAME(ColumnOptVector<types::UInt64::Primitive>, ColumnConst<types::Int64::Primitive>)       \
    CASE_NAME(ColumnOptVector<types::UInt64::Primitive>, ColumnConst<types::Double::Primitive>)      \
    CASE_NAME(ColumnOptVector<types::Double::Primitive>, ColumnOptVector<types::Int64::Primitive>)   \
    CASE_NAME(ColumnOptVector<types::Double::Primitive>, ColumnOptVector<types::UInt64::Primitive>)  \
    CASE_NAME(ColumnOptVector<types::Double::Primitive>, ColumnConst<types::Int64::Primitive>)       \
    CASE_NAME(ColumnOptVector<types::Double::Primitive>, ColumnConst<types::UInt64::Primitive>)      \

ExprProgram* ExprProgram::create(PipelineV2* pipeline) {
    ExprProgram* prog = new ExprProgram();
    pipeline->addExprProgram(prog);

    return prog;
}

void ExprProgram::evaluateInstructions() {
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
            break;
    }
}

void ExprProgram::evalBinaryInstr(const Instruction& instr) {
    const ColumnOperator op = instr._op;
    const Column* lhs = instr._lhs;
    const Column* rhs = instr._rhs;

    if (!lhs) {
        throw FatalException("Binary instruction had null left input.");
    }
    if (!rhs) {
        throw FatalException("Binary instruction had null right input.");
    }

    switch (OperationKind::code(op, lhs->getKind(), rhs->getKind())) {
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
        EQUAL_CASE(ColumnVector<EdgeTypeID>, ColumnVector<EdgeTypeID>)
        EQUAL_CASE(ColumnVector<EdgeTypeID>, ColumnConst<EdgeTypeID>)
        EQUAL_CASE(ColumnConst<EdgeTypeID>, ColumnVector<EdgeTypeID>)
        EQUAL_CASE(ColumnConst<EdgeTypeID>, ColumnConst<EdgeTypeID>)
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
        INSTANTIATE_PROPERTY_OPERATOR(EQUAL_CASE)
        INSTANTIATE_PROPERTY_OPERATOR(NOT_EQUAL_CASE)
        INSTANTIATE_PROPERTY_OPERATOR(GREATER_THAN_CASE)
        INSTANTIATE_PROPERTY_OPERATOR(LESS_THAN_CASE)
        INSTANTIATE_PROPERTY_OPERATOR(GREATER_THAN_OR_EQUAL_CASE)
        INSTANTIATE_PROPERTY_OPERATOR(LESS_THAN_OR_EQUAL_CASE)

        // Special cases for IS NOT NULL and IS NULL
        EQUAL_CASE(ColumnOptVector<types::Int64::Primitive>, ColumnConst<PropertyNull>)
        EQUAL_CASE(ColumnOptVector<types::UInt64::Primitive>, ColumnConst<PropertyNull>)
        EQUAL_CASE(ColumnOptVector<types::Double::Primitive>, ColumnConst<PropertyNull>)
        EQUAL_CASE(ColumnOptVector<types::String::Primitive>, ColumnConst<PropertyNull>)
        EQUAL_CASE(ColumnOptVector<types::Bool::Primitive>, ColumnConst<PropertyNull>)

        NOT_EQUAL_CASE(ColumnOptVector<types::Int64::Primitive>, ColumnConst<PropertyNull>)
        NOT_EQUAL_CASE(ColumnOptVector<types::UInt64::Primitive>, ColumnConst<PropertyNull>)
        NOT_EQUAL_CASE(ColumnOptVector<types::Double::Primitive>, ColumnConst<PropertyNull>)
        NOT_EQUAL_CASE(ColumnOptVector<types::String::Primitive>, ColumnConst<PropertyNull>)
        NOT_EQUAL_CASE(ColumnOptVector<types::Bool::Primitive>, ColumnConst<PropertyNull>)

        // Boolean property ops
        AND_CASE(ColumnOptVector<types::Bool::Primitive>, ColumnOptVector<types::Bool::Primitive>)
        AND_CASE(ColumnVector<types::Bool::Primitive>, ColumnOptVector<types::Bool::Primitive>)
        AND_CASE(ColumnOptVector<types::Bool::Primitive>, ColumnVector<types::Bool::Primitive>)
        AND_CASE(ColumnVector<types::Bool::Primitive>, ColumnVector<types::Bool::Primitive>)

        OR_CASE(ColumnVector<types::Bool::Primitive>, ColumnVector<types::Bool::Primitive>)
        OR_CASE(ColumnOptVector<types::Bool::Primitive>, ColumnVector<types::Bool::Primitive>)
        OR_CASE(ColumnVector<types::Bool::Primitive>, ColumnOptVector<types::Bool::Primitive>)
        OR_CASE(ColumnOptVector<types::Bool::Primitive>, ColumnOptVector<types::Bool::Primitive>)

        // Project ops
        PROJECT_CASE(ColumnVector<size_t>, ColumnMask)
        PROJECT_CASE(ColumnMask, ColumnVector<size_t>)

        // Mask ops
        AND_MASK_CASE(ColumnMask, ColumnMask)
        OR_MASK_CASE(ColumnMask, ColumnMask)

        // Set ops
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

    switch (OperationKind::code(op, input->getKind())) {
        // XXX: What else can NOT be applied to?
        NOT_CASE(ColumnVector<types::Bool::Primitive>);    // Also handles CustomBool
        NOT_CASE(ColumnOptVector<types::Bool::Primitive>); // Also handles CustomBool

        default: {
            const std::string_view opName = ColumnOperatorDescription::value(op);
            throw PipelineException(
                fmt::format("Unary operator {} not implemented for input kind: {} ",
                            opName, (uint8_t)input->getKind()));
        }
    }
}
