#pragma once

#include <stdint.h>
#include <spdlog/fmt/bundled/format.h>
#include <utility>

#include "EnumToString.h"
#include "FatalException.h"

namespace db {

enum ColumnOperator : uint8_t {
    // Binary operators
    OP_EQUAL = 0,
    OP_NOT_EQUAL,

    OP_GREATER_THAN,
    OP_LESS_THAN,
    OP_GREATER_THAN_OR_EQUAL,
    OP_LESS_THAN_OR_EQUAL,

    OP_AND,
    OP_OR,
    OP_XOR,

    OP_ADD,
    OP_CONCAT,
    OP_SUB,
    OP_MUL,
    OP_DIV,
    OP_MOD,
    OP_POW,

    OP_STARTS_WITH,
    OP_ENDS_WITH,
    OP_CONTAINS,

    OP_PROJECT,
    OP_IN,

    // Unary operators
    OP_MINUS,
    OP_PLUS,
    OP_NOT,

    OP_NOOP,

    // Conversion operators (unary)
    OP_TO_INTEGER,
    OP_TO_FLOAT,
    OP_TO_BOOLEAN,

    OP_FUNC_LABELS,
    OP_FUNC_EDGE_TYPES,

    OP_FUNC_COSINE_SIMILARITY,
    OP_FUNC_EUCLIDEAN_DISTANCE,

    _SIZE
};

enum class ColumnOperatorType : uint8_t {
    OPTYPE_BINARY = 0,
    OPTYPE_UNARY,
    OPTYPE_FUNC,
    OPTYPE_NOOP,
};

constexpr ColumnOperatorType getOperatorType(ColumnOperator op) {
    switch (op) {
        case OP_EQUAL:
        case OP_NOT_EQUAL:

        case OP_GREATER_THAN:
        case OP_LESS_THAN:
        case OP_GREATER_THAN_OR_EQUAL:
        case OP_LESS_THAN_OR_EQUAL:

        case OP_AND:
        case OP_OR:
        case OP_XOR:

        case OP_ADD:
        case OP_CONCAT:
        case OP_SUB:
        case OP_MUL:
        case OP_DIV:
        case OP_MOD:
        case OP_POW:

        case OP_STARTS_WITH:
        case OP_ENDS_WITH:
        case OP_CONTAINS:

        case OP_PROJECT:
        case OP_IN:
            return ColumnOperatorType::OPTYPE_BINARY;
        break;

        case OP_MINUS:
        case OP_PLUS:
        case OP_NOT:
            return ColumnOperatorType::OPTYPE_UNARY;
        break;

        case OP_TO_INTEGER:
        case OP_TO_FLOAT:
        case OP_TO_BOOLEAN:
        case OP_FUNC_LABELS:
        case OP_FUNC_EDGE_TYPES:
        case OP_FUNC_COSINE_SIMILARITY:
        case OP_FUNC_EUCLIDEAN_DISTANCE:
            return ColumnOperatorType::OPTYPE_FUNC;
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
        fmt::format("Failed to get ColumnOperatorType of ColumnOperator : {}",
                    std::to_underlying(op)));
}

using ColumnOperatorDescription = EnumToString<ColumnOperator>::Create<
    EnumStringPair<ColumnOperator::OP_EQUAL, "EQUAL">,
    EnumStringPair<ColumnOperator::OP_NOT_EQUAL, "NOT_EQUAL">,

    EnumStringPair<ColumnOperator::OP_GREATER_THAN, "GREATER_THAN">,
    EnumStringPair<ColumnOperator::OP_LESS_THAN, "LESS_THAN">,
    EnumStringPair<ColumnOperator::OP_GREATER_THAN_OR_EQUAL, "GREATER_THAN_OR_EQUAL">,
    EnumStringPair<ColumnOperator::OP_LESS_THAN_OR_EQUAL, "LESS_THAN_OR_EQUAL">,

    EnumStringPair<ColumnOperator::OP_AND, "AND">,
    EnumStringPair<ColumnOperator::OP_OR, "OR">,
    EnumStringPair<ColumnOperator::OP_XOR, "XOR">,

    EnumStringPair<ColumnOperator::OP_ADD, "ADD">,
    EnumStringPair<ColumnOperator::OP_CONCAT, "CONCAT">,
    EnumStringPair<ColumnOperator::OP_SUB, "SUB">,
    EnumStringPair<ColumnOperator::OP_MUL, "MUL">,
    EnumStringPair<ColumnOperator::OP_DIV, "DIV">,
    EnumStringPair<ColumnOperator::OP_MOD, "MOD">,
    EnumStringPair<ColumnOperator::OP_POW, "POW">,

    EnumStringPair<ColumnOperator::OP_STARTS_WITH, "STARTS_WITH">,
    EnumStringPair<ColumnOperator::OP_ENDS_WITH, "ENDS_WITH">,
    EnumStringPair<ColumnOperator::OP_CONTAINS, "CONTAINS">,

    EnumStringPair<ColumnOperator::OP_PROJECT, "PROJECT">,
    EnumStringPair<ColumnOperator::OP_IN, "IN">,

    EnumStringPair<ColumnOperator::OP_MINUS, "MINUS">,
    EnumStringPair<ColumnOperator::OP_PLUS, "PLUS">,
    EnumStringPair<ColumnOperator::OP_NOT, "NOT">,

    EnumStringPair<ColumnOperator::OP_NOOP, "NOOP">,

    EnumStringPair<ColumnOperator::OP_TO_INTEGER, "TO_INTEGER">,
    EnumStringPair<ColumnOperator::OP_TO_FLOAT, "TO_FLOAT">,
    EnumStringPair<ColumnOperator::OP_TO_BOOLEAN, "TO_BOOLEAN">,

    EnumStringPair<ColumnOperator::OP_FUNC_LABELS, "FUNCTION_LABELS">,
    EnumStringPair<ColumnOperator::OP_FUNC_EDGE_TYPES, "FUNCTION_EDGE_TYPES">,

    EnumStringPair<ColumnOperator::OP_FUNC_COSINE_SIMILARITY, "COSINE_SIMILARITY">,
    EnumStringPair<ColumnOperator::OP_FUNC_EUCLIDEAN_DISTANCE, "EUCLIDEAN_DISTANCE">>;
}
