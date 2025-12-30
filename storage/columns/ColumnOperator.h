#pragma once

#include <cstdint>
#include <spdlog/fmt/bundled/format.h>

#include "EnumToString.h"
#include "FatalException.h"

namespace db {

enum ColumnOperator : uint8_t {
    // Binary operators
    OP_EQUAL = 0,
    OP_GREATER_THAN,
    OP_AND,
    OP_OR,
    OP_PROJECT,
    OP_IN,

    // Unary operators
    OP_MINUS,
    OP_PLUS,
    OP_NOT,

    OP_NOOP,

    _SIZE
};

enum class ColumnOperatorType : uint8_t {
    OPTYPE_BINARY = 0,
    OPTYPE_UNARY,
    OPTYPE_NOOP,
};

constexpr inline ColumnOperatorType getOperatorType(ColumnOperator op) {
    switch (op) {
        case OP_EQUAL:
        case OP_GREATER_THAN:
        case OP_AND:
        case OP_OR:
        case OP_PROJECT:
        case OP_IN:
            return ColumnOperatorType::OPTYPE_BINARY;
        break;

        case OP_MINUS:
        case OP_PLUS:
        case OP_NOT:
            return ColumnOperatorType::OPTYPE_UNARY;
        break;

        case OP_NOOP:
            return ColumnOperatorType::OPTYPE_NOOP;
        break;

        case _SIZE:
            throw FatalException(
                "Attempted to get ColumnOperatorType of invalid ColumnOperator.");
        break;
    }

    throw FatalException(
        fmt::format("Failed to get ColumnOperatorType of ColumnOperator : {}", (uint8_t) op));
}

using ColumnOperatorDescription = EnumToString<ColumnOperator>::Create<
    EnumStringPair<ColumnOperator::OP_EQUAL, "EQUAL">,
    EnumStringPair<ColumnOperator::OP_GREATER_THAN, "GREATER_THAN">,
    EnumStringPair<ColumnOperator::OP_AND, "AND">,
    EnumStringPair<ColumnOperator::OP_OR, "OR">,
    EnumStringPair<ColumnOperator::OP_PROJECT, "PROJECT">,
    EnumStringPair<ColumnOperator::OP_IN, "IN">,

    EnumStringPair<ColumnOperator::OP_MINUS, "MINUS">,
    EnumStringPair<ColumnOperator::OP_PLUS, "PLUS">,
    EnumStringPair<ColumnOperator::OP_NOT, "NOT">,

    EnumStringPair<ColumnOperator::OP_NOOP, "NOOP">>;
}
